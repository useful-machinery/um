use super::*;
use crate::service::assignment::test_support::{
    GITLAB_CANONICAL_URL, GitLabGrantBroker, accepted_roots, assert_no_runner_git_wiring,
};

// This assignment fixture exercises the preparation and execution lifecycle.
// The Gateway tests separately use an audited Cloud grant and real HTTPS Git.
fn gitlab_assignment_fixture(
    workflow: &str,
) -> (
    tempfile::TempDir,
    AssignmentManager,
    AssignmentOffer,
    Arc<GitLabGrantBroker>,
) {
    let (temporary, mut manager) = manager_fixture(workflow);
    let broker = Arc::new(GitLabGrantBroker::new(&source_fixture_path(&manager)));
    manager.source_broker = Some(broker.clone());
    let mut frame: Value = serde_json::from_slice(include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../tests/fixtures/runner-protocol/v1/valid/cloud-assignment-offer-source-display.json"
    )))
    .unwrap();
    frame["payload"]["executionSpec"]["primaryWorkspaceSource"]["providerKind"] =
        serde_json::json!("gitlab");
    frame["payload"]["executionSpec"]["sourceDisplaySnapshot"]["repository"] = serde_json::json!({
        "providerKind": "gitlab", "instanceUrl": "https://gitlab.com", "fullName": "group/sub/project"
    });
    let mut offered = decoded_offer_from_frame(&serde_json::to_vec(&frame).unwrap());
    align_offer_with_source_fixture(&manager, &mut offered);
    (temporary, manager, offered, broker)
}

// REQ-WORKSPACE-32, REQ-WORKSPACE-34: preparation renews, executes, and
// removes its relay wiring before terminal settlement.
#[tokio::test]
async fn gitlab_assignment_fetches_executes_and_restores_the_relay_origin() {
    let (_temporary, mut manager, offered, broker) = gitlab_assignment_fixture(
        "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command: {argv: [\"true\"]}\n",
    );
    manager.handle_offer(offered.clone()).unwrap();
    prepare_current(&mut manager, &offered).await;
    assert_eq!(broker.issued_preparation(), 2);
    assert_eq!(broker.revoked_preparation(), 2);
    let (workspace, _) = accepted_roots(&manager);
    assert_eq!(
        run_fixture_git(&workspace, &["remote", "get-url", "origin"]),
        broker.read_url()
    );
    let reports = execute_to_terminal(&mut manager, &offered).await;
    assert_succeeded(&reports);
    wait_for_execution_finalization(&mut manager).await;
    assert_eq!(
        run_fixture_git(&workspace, &["remote", "get-url", "origin"]),
        GITLAB_CANONICAL_URL
    );
    acknowledge_terminal_and_settle(&mut manager).await;
}

// REQ-WORKSPACE-26, REQ-WORKSPACE-34: failed GitLab work retains the exact
// workspace bytes but never retains the runner's grant, helper or relay origin.
#[tokio::test]
async fn failed_gitlab_assignment_retains_edits_without_relay_wiring() {
    let (_temporary, mut manager, offered, broker) = gitlab_assignment_fixture(
        "schemaVersion: 1\nsteps:\n  check:\n    kind: cmd\n    command: {argv: [\"sh\", \"-c\", \"printf retained > edit.txt; exit 23\"]}\n",
    );
    manager.handle_offer(offered.clone()).unwrap();
    prepare_current(&mut manager, &offered).await;
    let (workspace, private) = accepted_roots(&manager);
    let reports = execute_to_terminal(&mut manager, &offered).await;
    assert!(
        matches!(reports.last(), Some(ExecutionReport::Finished { outcome, .. })
        if outcome["outcome"] == "failed")
    );
    wait_for_execution_finalization(&mut manager).await;
    assert_eq!(fs::read(workspace.join("edit.txt")).unwrap(), b"retained");
    assert_eq!(
        run_fixture_git(&workspace, &["remote", "get-url", "origin"]),
        GITLAB_CANONICAL_URL
    );
    assert_no_runner_git_wiring(&workspace, &private);
    assert!(broker.revoked_preparation() >= 1);
    acknowledge_terminal_and_settle(&mut manager).await;
    assert!(workspace.exists());
}

// REQ-CONTINUE-20, REQ-CONTINUE-47, REQ-CONTINUE-53:
// A real failed GitLab attempt leaves a claimable workspace. Its successor, a
// continuation admitted before Cloud stopped continuing runs created by
// workflow path, fetches the effective definition separately and installs a
// new Cloud relay helper without cloning or resetting the retained primary
// work tree.
#[tokio::test]
async fn failed_gitlab_assignment_continues_with_fresh_grants_and_retained_workspace() {
    let workflow = "schemaVersion: 1\nsteps:\n  produce:\n    kind: cmd\n    command: {argv: [\"sh\", \"-c\", \"printf retained > note.txt\"]}\n    outputs:\n      note: {kind: text, from: path, path: note.txt}\n  check:\n    kind: cmd\n    dependsOn: [produce]\n    command: {argv: [\"sh\", \"-c\", \"if test -f .git/fixture-failed; then test $(cat note.txt) = repaired; else touch .git/fixture-failed; exit 75; fi\"]}\n";
    let (_temporary, mut manager, initial, broker) = gitlab_assignment_fixture(workflow);
    manager.handle_offer(initial.clone()).unwrap();
    prepare_current(&mut manager, &initial).await;
    let workspace = match held_assignment(&manager) {
        Some(HeldAssignment::Accepted(accepted)) => accepted.root.execution.clone(),
        _ => panic!("initial GitLab assignment was not admitted"),
    };
    let reports = execute_to_terminal(&mut manager, &initial).await;
    assert!(
        matches!(reports.last(), Some(ExecutionReport::Finished { outcome, .. }) if outcome["outcome"] == "failed")
    );
    acknowledge_terminal_and_settle(&mut manager).await;
    assert_eq!(fs::read(workspace.join("note.txt")).unwrap(), b"retained");
    fs::write(workspace.join("note.txt"), b"repaired").unwrap();
    assert_eq!(
        run_fixture_git(&workspace, &["remote", "get-url", "origin"]),
        GITLAB_CANONICAL_URL
    );

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
        request: json!({"fromSteps": ["check"], "definition": "inherited"}),
        reexecuted_steps: vec!["check".into()],
        inherited_steps: vec![
            json!({"id": "produce", "priorState": "succeeded", "definitionChanged": false}),
        ],
        prior_settlement_snapshot: None,
    }));
    manager.handle_offer(successor.clone()).unwrap();
    super::git_continuation_tests::prepare_retained_continuation(&mut manager, &successor).await;
    let accepted = match held_assignment(&manager) {
        Some(HeldAssignment::Accepted(accepted)) => accepted,
        _ => panic!("GitLab continuation was not admitted"),
    };
    assert_eq!(accepted.root.execution, workspace);
    let definition_clone = accepted.root.private.join("replacement-definition");
    assert_eq!(fs::read(workspace.join("note.txt")).unwrap(), b"repaired");
    assert_eq!(
        run_fixture_git(&workspace, &["remote", "get-url", "origin"]),
        broker.read_url()
    );
    assert!(broker.issued_preparation() >= 3);
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
        matches!(reports.last(), Some(ExecutionReport::Finished { outcome, .. }) if outcome["outcome"] == "succeeded"),
        "{reports:?}"
    );
    acknowledge_terminal_and_settle(&mut manager).await;
    assert_eq!(fs::read(workspace.join("note.txt")).unwrap(), b"repaired");
    assert_eq!(
        run_fixture_git(&workspace, &["remote", "get-url", "origin"]),
        GITLAB_CANONICAL_URL
    );
    assert_eq!(
        run_fixture_git(&definition_clone, &["remote", "get-url", "origin"]),
        GITLAB_CANONICAL_URL
    );
    assert!(broker.revoked_preparation() >= 3);
}
