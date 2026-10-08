use super::*;

pub(super) fn manager_fixture_with_pi(
    workflow: &str,
    pi_source: Option<&str>,
) -> (tempfile::TempDir, AssignmentManager) {
    manager_fixture_with_harnesses(workflow, pi_source, None, None)
}

pub(super) fn manager_fixture_with_harnesses(
    workflow: &str,
    pi_source: Option<&str>,
    claude_code_source: Option<&str>,
    codex_source: Option<&str>,
) -> (tempfile::TempDir, AssignmentManager) {
    let (temporary, source, mut config) = base_manager_config(workflow);
    if let Some(pi_source) = pi_source {
        let executable = install_fixture_executable(&temporary, "pi-fixture", pi_source);
        config = config.with_pi_installation(ValidatedPiInstallation::fixture(executable));
    }
    if let Some(claude_code_source) = claude_code_source {
        let executable =
            install_fixture_executable(&temporary, "claude-fixture", claude_code_source);
        config = config
            .with_claude_code_installation(ValidatedClaudeCodeInstallation::fixture(executable));
    }
    if let Some(codex_source) = codex_source {
        let executable = install_fixture_executable(&temporary, "codex-fixture", codex_source);
        config = config.with_codex_installation(ValidatedCodexInstallation::fixture(executable));
    }
    let boot_id = "rbt_01k0z6r1w8f4jy2m7q9v3x5abe";
    let work_root =
        WorkRootLease::acquire_for_test(config.assignment().work_root(), boot_id).unwrap();
    let mut manager = manager_with_fixture_source(&config, &source, work_root);
    manager.retain_lease_policy(&policy()).unwrap();
    let mut environment = manager.environment.variables().clone();
    environment.insert(
        OsString::from("CLAUDE_CONFIG_DIR"),
        temporary.path().join("claude-config").into_os_string(),
    );
    let codex_home = temporary.path().join("codex-home");
    fs::create_dir(&codex_home).unwrap();
    for (name, value) in [
        ("CODEX_HOME", codex_home),
        (
            "CODEX_FIXTURE_HELPER",
            std::env::current_exe().expect("locate the runner test executable"),
        ),
        (
            "CODEX_FIXTURE_ARGUMENTS",
            temporary.path().join("codex.arguments"),
        ),
        (
            "CODEX_FIXTURE_REQUESTS",
            temporary.path().join("codex.requests"),
        ),
        (
            "CODEX_FIXTURE_PROCESS",
            temporary.path().join("codex.process"),
        ),
        ("CODEX_FIXTURE_READY", temporary.path().join("codex.ready")),
        (
            "CODEX_FIXTURE_PROCEED",
            temporary.path().join("codex.proceed"),
        ),
        (
            "CODEX_FIXTURE_DESCENDANT",
            temporary.path().join("codex.descendant"),
        ),
    ] {
        environment.insert(OsString::from(name), value.into_os_string());
    }
    environment.insert(
        OsString::from("CODEX_FIXTURE_SCENARIO"),
        OsString::from("no-value"),
    );
    environment.insert(
        OsString::from("CODEX_FIXTURE_VERSION"),
        OsString::from(CODEX_APP_SERVER_V1_QUALIFICATION_VERSION),
    );
    environment.insert(
        OsString::from("CODEX_FIXTURE_RESPONSE"),
        OsString::from("runner fixture response"),
    );
    manager.environment = EnvironmentSnapshot::new(environment);
    (temporary, manager)
}

fn install_fixture_executable(temporary: &tempfile::TempDir, name: &str, source: &str) -> PathBuf {
    let executable = temporary.path().join(name);
    fs::write(&executable, source).unwrap();
    fs::set_permissions(&executable, fs::Permissions::from_mode(0o700)).unwrap();
    executable
}

fn replace_manager_path_with_decoy(
    temporary: &tempfile::TempDir,
    manager: &mut AssignmentManager,
    executable_name: &str,
) -> PathBuf {
    let changed_path = temporary
        .path()
        .join(format!("changed-{executable_name}-path"));
    fs::create_dir(&changed_path).unwrap();
    let decoy = changed_path.join(executable_name);
    fs::write(
        &decoy,
        "#!/bin/sh\nprintf decoy > \"${0%/*}/decoy.calls\"\nexit 99\n",
    )
    .unwrap();
    fs::set_permissions(&decoy, fs::Permissions::from_mode(0o700)).unwrap();
    let mut environment = manager.environment.variables().clone();
    let inherited_path = environment
        .get(OsStr::new("PATH"))
        .into_iter()
        .flat_map(|path| std::env::split_paths(path));
    let path = std::env::join_paths(std::iter::once(changed_path.clone()).chain(inherited_path))
        .expect("join manager fixture PATH");
    environment.insert(OsString::from("PATH"), path);
    manager.environment = EnvironmentSnapshot::new(environment);
    changed_path
}

#[tokio::test]
async fn admission_requires_only_the_harness_selected_by_the_assignment() {
    let (pi_temporary, mut pi_manager) = manager_fixture_with_harnesses(
        PI_ONLY_WORKFLOW,
        None,
        Some(SUCCESSFUL_CLAUDE_CODE),
        Some(SUCCESSFUL_CODEX),
    );
    let pi_offer = offer("bg");
    offer_then_prepare(&mut pi_manager, &pi_offer).await;
    assert_workflow_environment_unsupported(&mut pi_manager).await;
    assert!(!pi_temporary.path().join("claude.calls").exists());
    assert!(!pi_temporary.path().join("codex.calls").exists());

    let (claude_temporary, mut claude_manager) = manager_fixture_with_harnesses(
        CLAUDE_CODE_ONLY_WORKFLOW,
        Some(SUCCESSFUL_PI),
        None,
        Some(SUCCESSFUL_CODEX),
    );
    let claude_offer = offer("bg");
    offer_then_prepare(&mut claude_manager, &claude_offer).await;
    assert_workflow_environment_unsupported(&mut claude_manager).await;
    assert!(!claude_temporary.path().join("pi.calls").exists());
    assert!(!claude_temporary.path().join("codex.calls").exists());

    let (codex_temporary, mut codex_manager) = manager_fixture_with_harnesses(
        CODEX_ONLY_WORKFLOW,
        Some(SUCCESSFUL_PI),
        Some(SUCCESSFUL_CLAUDE_CODE),
        None,
    );
    let codex_offer = offer("bg");
    offer_then_prepare(&mut codex_manager, &codex_offer).await;
    assert_workflow_environment_unsupported(&mut codex_manager).await;
    assert!(!codex_temporary.path().join("pi.calls").exists());
    assert!(!codex_temporary.path().join("claude.calls").exists());
}

#[tokio::test]
async fn command_and_each_harness_assignment_invoke_only_declared_snapshots() {
    let fixture_argv = serde_json::to_string(&command_fixture_arguments()).unwrap();
    let command_only_workflow = format!(
        "schemaVersion: 1\nsteps:\n  command:\n    kind: cmd\n    command:\n      argv: {fixture_argv}\n"
    );
    for (
        workflow,
        pi_source,
        claude_code_source,
        codex_source,
        command_count,
        expected_pi,
        expected_claude,
        expected_codex,
    ) in [
        (
            command_only_workflow.as_str(),
            None,
            None,
            None,
            1,
            false,
            false,
            false,
        ),
        (
            PI_ONLY_WORKFLOW,
            Some(SUCCESSFUL_PI),
            None,
            None,
            0,
            true,
            false,
            false,
        ),
        (
            CLAUDE_CODE_ONLY_WORKFLOW,
            None,
            Some(SUCCESSFUL_CLAUDE_CODE),
            None,
            0,
            false,
            true,
            false,
        ),
        (
            CODEX_ONLY_WORKFLOW,
            None,
            None,
            Some(SUCCESSFUL_CODEX),
            0,
            false,
            false,
            true,
        ),
        (
            ALL_HARNESS_WORKFLOW,
            Some(SUCCESSFUL_PI),
            Some(SUCCESSFUL_CLAUDE_CODE),
            Some(SUCCESSFUL_CODEX),
            0,
            true,
            true,
            true,
        ),
    ] {
        let (temporary, mut manager) =
            manager_fixture_with_harnesses(workflow, pi_source, claude_code_source, codex_source);
        let reports = if command_count == 0 {
            offer_and_execute(&mut manager).await
        } else {
            offer_and_execute_with_command_fixtures(&mut manager, command_count).await
        };

        assert_succeeded(&reports);
        assert_eq!(
            reports.iter().filter(|report| report.is_terminal()).count(),
            1
        );
        let transcript = format!("{reports:?}");
        assert!(!transcript.contains("stream_event"));
        assert!(!transcript.contains("00000000-0000-4000-8000-00000000009"));
        assert!(!transcript.contains("018f7f1e-7b5a-7d13-8f19-2b6a4c8d0e12"));
        assert_eq!(temporary.path().join("pi.calls").exists(), expected_pi);
        assert_eq!(
            temporary.path().join("claude.calls").exists(),
            expected_claude
        );
        assert_eq!(
            temporary.path().join("codex.calls").exists(),
            expected_codex
        );
        if expected_pi {
            let _ = only_harness_call(&temporary.path().join("pi.calls"));
        }
        if expected_claude {
            let _ = only_harness_call(&temporary.path().join("claude.calls"));
        }
        if expected_codex {
            let _ = only_harness_call(&temporary.path().join("codex.calls"));
        }
    }
}

#[tokio::test]
async fn cloud_commands_agents_and_finalizers_receive_exact_source_revision() {
    let repository_argv = serde_json::to_string(&[
            "sh",
            "-c",
            "test -d .git && test \"$(git rev-parse --is-inside-work-tree)\" = true && test -z \"$(git rev-parse --show-prefix)\" && test -z \"$(git branch --show-current)\" && test \"$UM_SOURCE_BRANCH\" = main && test \"$UM_SOURCE_COMMIT_OID\" = \"$(git rev-parse HEAD)\"",
        ])
        .unwrap();
    let workflow = format!(
        "schemaVersion: 1\nagentProfiles:\n  coding:\n    harness:\n      kind: pi\n      config:\n        model: openai/gpt-5\n        thinking: high\nsteps:\n  agent:\n    kind: agent\n    agent:\n      profile: coding\n      systemPrompt: system.md\n      message:\n        text: [{{ file: system.md }}]\n  consume:\n    kind: cmd\n    dependsOn: [agent]\n    command:\n      argv: {repository_argv}\nfinalizers:\n  verify:\n    kind: cmd\n    command:\n      argv: {repository_argv}\n"
    );
    let pi_source = SUCCESSFUL_PI.replacen(
            "set -eu",
            "set -eu\ntest \"$UM_SOURCE_BRANCH\" = main\ntest \"$UM_SOURCE_COMMIT_OID\" = \"$(git rev-parse HEAD)\"",
            1,
        );
    let (_temporary, mut manager) = manager_fixture_with_pi(&workflow, Some(&pi_source));
    let mut environment = manager.environment.variables().clone();
    environment.insert(
        OsString::from("UM_SOURCE_BRANCH"),
        OsString::from("inherited-branch"),
    );
    environment.insert(
        OsString::from("UM_SOURCE_COMMIT_OID"),
        OsString::from("inherited-commit"),
    );
    manager.environment = EnvironmentSnapshot::new(environment);

    let reports = offer_and_execute(&mut manager).await;

    assert!(matches!(
        reports.last(),
        Some(ExecutionReport::Finished { outcome, .. })
            if outcome["outcome"] == "succeeded"
                && outcome["finalization"]["finalizers"][0]["id"] == "verify"
    ));
}

#[tokio::test]
async fn all_harness_assignment_uses_each_snapshot_with_its_own_configuration() {
    let (temporary, mut manager) = manager_fixture_with_harnesses(
        ALL_HARNESS_WORKFLOW,
        Some(SUCCESSFUL_PI),
        Some(SUCCESSFUL_CLAUDE_CODE),
        Some(SUCCESSFUL_CODEX),
    );
    let changed_paths = [
        replace_manager_path_with_decoy(&temporary, &mut manager, "claude"),
        replace_manager_path_with_decoy(&temporary, &mut manager, "codex"),
    ];
    let reports = offer_and_execute(&mut manager).await;

    assert_succeeded(&reports);
    let pi_call = only_harness_call(&temporary.path().join("pi.calls"));
    let claude_code_call = only_harness_call(&temporary.path().join("claude.calls"));
    let _codex_call = only_harness_call(&temporary.path().join("codex.calls"));
    let codex_turn = &codex_requests(&temporary.path().join("codex.requests"))[4];
    assert!(pi_call.contains("--model fixture/pi"));
    assert!(!pi_call.contains("fixture/claude"));
    assert!(claude_code_call.contains("--model fixture/claude"));
    assert!(claude_code_call.contains("--effort xhigh"));
    assert!(!claude_code_call.contains("fixture/pi"));
    assert_eq!(codex_turn["params"]["model"], "gpt-5.4");
    assert_eq!(codex_turn["params"]["effort"], "high");
    for changed_path in changed_paths {
        assert!(!changed_path.join("decoy.calls").exists());
    }
}

#[tokio::test]
async fn all_harness_assignment_attributes_a_claude_failure_to_its_step() {
    let (temporary, mut manager) = manager_fixture_with_harnesses(
        ALL_HARNESS_WORKFLOW,
        Some(SUCCESSFUL_PI),
        Some(SUCCESSFUL_CLAUDE_CODE),
        Some(SUCCESSFUL_CODEX),
    );
    let mut environment = manager.environment.variables().clone();
    environment.insert(OsString::from("CLAUDE_FIXTURE_FAIL"), OsString::from("1"));
    manager.environment = EnvironmentSnapshot::new(environment);
    let reports = offer_and_execute(&mut manager).await;

    let _ = only_harness_call(&temporary.path().join("pi.calls"));
    let _ = only_harness_call(&temporary.path().join("claude.calls"));
    assert!(!temporary.path().join("codex.calls").exists());
    assert!(reports.iter().any(|report| matches!(
        report,
        ExecutionReport::Transition { workflow_event, .. }
            if workflow_event["eventType"] == "step_state_changed"
                && workflow_event["stepId"] == "claude"
                && workflow_event["to"] == "failed"
                && workflow_event["detail"]["code"] == "harness_protocol_failed"
    )));
    assert!(matches!(
        reports.last(),
        Some(ExecutionReport::Finished { outcome, .. })
            if outcome["outcome"] == "failed"
    ));
}

async fn run_codex_with_stubborn_descendant(
    scenario: &str,
) -> (tempfile::TempDir, AssignmentManager, Vec<ExecutionReport>) {
    let (temporary, mut manager) =
        manager_fixture_with_harnesses(CODEX_ONLY_WORKFLOW, None, None, Some(SUCCESSFUL_CODEX));
    select_codex_scenario(&mut manager, scenario);
    manager.guard_processes = true;
    let offered = offer("bg");
    offer_then_prepare(&mut manager, &offered).await;
    spawn_execution(&mut manager, &offered);
    wait_for_fixture_path(&temporary.path().join("codex.descendant")).await;
    let reports = with_watchdog(wait_for_terminal(&mut manager))
        .await
        .expect("stubborn descendant did not produce a terminal report");
    (temporary, manager, reports)
}

#[tokio::test]
async fn codex_success_contains_stubborn_descendants_and_releases_slot() {
    let (temporary, mut manager, reports) =
        run_codex_with_stubborn_descendant("success-stubborn").await;
    assert_codex_fixture_quiescent(&temporary);
    assert_normal_success_and_reuse(&mut manager, &reports).await;
}

#[tokio::test]
async fn codex_failure_reports_only_after_stubborn_descendants_quiesce() {
    let (temporary, _manager, reports) =
        run_codex_with_stubborn_descendant("failure-after-start-stubborn").await;
    assert_codex_failure_diagnostic(&reports);
    assert_codex_fixture_quiescent(&temporary);
}

#[tokio::test]
async fn codex_failed_node_and_terminal_report_carry_safe_sibling_diagnostics() {
    let (_temporary, mut manager) =
        manager_fixture_with_harnesses(CODEX_ONLY_WORKFLOW, None, None, Some(SUCCESSFUL_CODEX));
    select_codex_scenario(&mut manager, "failure-after-start");
    let reports = offer_and_execute(&mut manager).await;
    assert_codex_failure_diagnostic(&reports);
}

fn assert_codex_failure_diagnostic(reports: &[ExecutionReport]) {
    assert!(reports.iter().any(|report| matches!(
        report,
        ExecutionReport::Transition { workflow_event, diagnostic: Some(diagnostic), .. }
            if workflow_event["eventType"] == "step_state_changed"
                && workflow_event["stepId"] == "codex"
                && workflow_event["to"] == "failed"
                && workflow_event["detail"]["code"] == "harness_failed"
                && workflow_event.get("diagnostic").is_none()
                && diagnostic == &json!({"harnessError": "unauthorized"})
    )));
    assert!(matches!(
        reports.last(),
        Some(ExecutionReport::Finished { outcome, diagnostic: Some(diagnostic), .. })
            if outcome["outcome"] == "failed"
                && outcome["primaryIssue"]["detail"]["code"] == "harness_failed"
                && outcome["primaryIssue"].get("diagnostic").is_none()
                && diagnostic == &json!({"harnessError": "unauthorized"})
    ));
    for (sequence, report) in reports.iter().enumerate() {
        if matches!(
            report,
            ExecutionReport::Transition {
                diagnostic: Some(_),
                ..
            } | ExecutionReport::Finished {
                diagnostic: Some(_),
                ..
            }
        ) {
            let frame = report.runner_frame(
                RunnerEnvelope {
                    message_id: format!("rmsg_{:026}", sequence + 1),
                    runner_id: "rnr_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                    boot_id: "rbt_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                    sequence: (sequence + 1) as u64,
                    sent_at: NOW.to_owned(),
                },
                "asn_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                "atm_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
            );
            let encoded = encode_runner_frame(&frame).expect("diagnostic is valid on wire");
            assert!(
                !String::from_utf8(encoded)
                    .unwrap()
                    .contains("private sentinel")
            );
        }
    }
    assert!(!format!("{reports:?}").contains("private sentinel"));
}

#[tokio::test]
async fn codex_runner_cancellation_reports_only_after_stubborn_descendants_quiesce() {
    let (temporary, mut manager) =
        manager_fixture_with_harnesses(CODEX_ONLY_WORKFLOW, None, None, Some(SUCCESSFUL_CODEX));
    select_codex_scenario(&mut manager, "cancellation-stubborn");
    manager.guard_processes = true;
    let offered = offer("bg");
    offer_then_prepare(&mut manager, &offered).await;
    let reports = start_then_shut_down_and_wait(
        &mut manager,
        &offered,
        wait_for_fixture_path(&temporary.path().join("codex.ready")),
    )
    .await;

    let _ = runner_shutdown_outcome(&reports);
    assert_codex_fixture_quiescent(&temporary);
}
