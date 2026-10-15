use super::*;

#[tokio::test]
async fn retained_cloud_git_work_survives_continuation_and_captures_original_baseline() {
    let workflow = r#"schemaVersion: 1
steps:
  edit:
    kind: cmd
    command:
      argv:
        - /bin/sh
        - -c
        - 'if test -f .git/fixture-failed; then exit 0; fi; printf "retained edits\n" > change.txt; touch .git/fixture-failed; exit 75'
  capture:
    kind: cmd
    dependsOn: [edit]
    command:
      argv:
        - /bin/sh
        - -c
        - 'git add change.txt && git -c user.name=Fixture -c user.email=fixture@example.invalid -c commit.gpgsign=false -c core.hooksPath=/dev/null commit -m candidate'
    outputs:
      branch: {kind: git_branch, from: workspace}
exports:
  changes: {ref: outputs.capture.branch}
"#;
    let (_temporary, mut manager) = manager_fixture(workflow);
    resolve(&source_fixture_path(&manager), Path::new("workflow.yaml")).unwrap();
    let mut initial = offer("bg");
    align_offer_with_source_fixture(&manager, &mut initial);
    let baseline = initial
        .execution_spec
        .primary_workspace_source
        .commit_oid
        .clone();
    manager.handle_offer(initial.clone()).unwrap();
    prepare_current(&mut manager, &initial).await;
    let workspace = match &mut manager.slot {
        Some(LocalSlot::Accepted(accepted)) => {
            // Unit fixtures run commands directly rather than invoking the CLI's
            // private child helper. Retention still needs a durable guard journal
            // for the real workspace-claim and quiescence admission path.
            accepted.process_guards =
                AssignmentProcessGuards::durable(&accepted.root.private).unwrap();
            accepted.root.execution.clone()
        }
        _ => panic!(
            "initial Git workflow was not admitted: {:?}",
            manager.pending_observations(&BTreeSet::new(), 100)
        ),
    };
    let reports = execute_to_terminal(&mut manager, &initial).await;
    assert!(
        matches!(reports.last(), Some(ExecutionReport::Finished { outcome, .. })
        if outcome["outcome"] == "failed")
    );
    acknowledge_terminal_and_settle(&mut manager).await;
    assert_eq!(
        fs::read(workspace.join("change.txt")).unwrap(),
        b"retained edits\n"
    );
    assert!(!run_fixture_git(&workspace, &["status", "--porcelain"]).is_empty());

    let mut successor = initial.clone();
    successor.effect_id = "eff_01k0z6r1w8f4jy2m7q9v3x5abm".into();
    successor.assignment_id = "asn_01k0z6r1w8f4jy2m7q9v3x5abh".into();
    successor.attempt_id = "atm_01k0z6r1w8f4jy2m7q9v3x5abh".into();
    successor.execution_spec.execution_spec_id = "xsp_01k0z6r1w8f4jy2m7q9v3x5abh".into();
    successor.attempt_number = 2;
    successor.continuation = Some(Box::new(ContinuationOffer {
        prior_assignment_id: initial.assignment_id.clone(),
        prior_attempt_id: initial.attempt_id.clone(),
        required_runner_boot_id: manager.work_root.boot_id().unwrap().to_owned(),
        execution_root: workspace.to_str().unwrap().to_owned(),
        definition_source: successor.execution_spec.workflow_definition_source.clone(),
        effective_capacity: successor.execution_spec.capacity.clone(),
        prior_manifest_digest: initial
            .execution_spec
            .workflow_definition_source
            .workflow_source_closure_digest
            .clone(),
        request: json!({"fromSteps": ["edit"], "definition": "inherited"}),
        reexecuted_steps: vec!["edit".into(), "capture".into()],
        inherited_steps: Vec::new(),
        prior_settlement_snapshot: None,
    }));
    manager.handle_offer(successor.clone()).unwrap();
    let mut prepare = prepare_for(&manager, &successor);
    prepare.effect_id = "eff_01k0z6r1w8f4jy2m7q9v3x5abj".into();
    let preparation = wait_for_offer_preparation(&mut manager).await;
    manager.acknowledge_observation(preparation.id);
    manager.handle_prepare(prepare).unwrap();
    let mut ready_id = None;
    with_watchdog(wait_for_manager_state(&mut manager, |manager| {
        ready_id = manager
            .pending_observations(&BTreeSet::new(), 100)
            .into_iter()
            .find_map(|pending| {
                matches!(
                    pending.observation,
                    AssignmentObservation::ContinuationReady { .. }
                )
                .then_some(pending.id)
            });
        ready_id.is_some()
    }))
    .await
    .expect("continuation readiness proof was not produced");
    manager.acknowledge_observation(ready_id.unwrap());
    with_watchdog(wait_for_manager_state(&mut manager, |manager| {
        manager.drain_events();
        !matches!(
            manager.slot,
            Some(LocalSlot::Preparing(_) | LocalSlot::Releasing(_))
        )
    }))
    .await
    .expect("continuation admission did not complete");
    let accepted = match &manager.slot {
        Some(LocalSlot::Accepted(accepted)) => accepted,
        _ => panic!(
            "retained Git workflow was not admitted: {:?}",
            manager
                .decisions
                .iter()
                .map(|decision| &decision.response)
                .collect::<Vec<_>>()
        ),
    };
    assert_eq!(accepted.root.execution, workspace);
    assert_eq!(
        accepted.admitted.git_capture().unwrap().baseline_oid(),
        baseline
    );
    let mut start = start_for(&successor);
    start.effect_id = "eff_01k0z6r1w8f4jy2m7q9v3x5abn".into();
    let job = manager.handle_start(start).unwrap().unwrap();
    let mut authorization = start_authorization_for(&successor);
    authorization.effect_id = "eff_01k0z6r1w8f4jy2m7q9v3x5abp".into();
    manager.handle_start_authorized(authorization).unwrap();
    job.spawn();
    let reports = with_watchdog(wait_for_terminal(&mut manager))
        .await
        .unwrap();
    assert!(
        matches!(reports.last(), Some(ExecutionReport::Finished { outcome, .. })
        if outcome["outcome"] == "succeeded"),
        "{reports:?}"
    );
    acknowledge_terminal_and_settle(&mut manager).await;
    assert_eq!(
        fs::read(workspace.join("change.txt")).unwrap(),
        b"retained edits\n"
    );
    assert_ne!(
        run_fixture_git(&workspace, &["rev-parse", "HEAD"]),
        baseline
    );
    assert!(run_fixture_git(&workspace, &["status", "--porcelain"]).is_empty());
}
