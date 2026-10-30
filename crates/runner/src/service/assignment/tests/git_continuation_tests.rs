use super::*;

#[tokio::test]
async fn retained_cloud_git_work_survives_continuation_and_captures_original_baseline() {
    exercise_retained_cloud_git_continuation(false).await;
}

#[tokio::test]
async fn v3_continuation_executes_in_claimed_workspace_not_definition_checkout() {
    exercise_retained_cloud_git_continuation(true).await;
}

async fn exercise_retained_cloud_git_continuation(v3: bool) {
    let (_temporary, mut manager, initial, workspace, baseline) =
        retained_initial_attempt(v3).await;
    let successor = continuation_successor(&manager, &initial, &workspace);
    manager.handle_offer(successor.clone()).unwrap();
    prepare_retained_continuation(&mut manager, &successor).await;
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
    let job = start_continuation(&mut manager, &successor);
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

// REQ-TELEMETRY-39
#[tokio::test]
async fn execution_lost_before_the_engine_starts_ends_the_execution_phase_failed() {
    let (_temporary, mut manager, initial, workspace, _baseline) =
        retained_initial_attempt(false).await;
    let (recorder, capture) = crate::telemetry::test_recorder("rbt_fixture");
    manager.recorder = Some(recorder);
    let successor = continuation_successor(&manager, &initial, &workspace);
    manager.handle_offer(successor.clone()).unwrap();
    prepare_retained_continuation(&mut manager, &successor).await;
    let private = match &manager.slot {
        Some(LocalSlot::Accepted(accepted)) => accepted.root.private.path().to_path_buf(),
        _ => panic!("retained Git workflow was not admitted"),
    };
    let job = start_continuation(&mut manager, &successor);
    // The run binds its continuation context as a new file in the attempt's
    // private root; one already there loses the execution environment after
    // the execution phase has opened and before the engine starts.
    fs::write(private.join("continuation-context.json"), b"{}").unwrap();
    job.spawn();
    let reports = with_watchdog(wait_for_terminal(&mut manager))
        .await
        .unwrap();
    assert!(
        reports.iter().any(|report| matches!(
            report,
            ExecutionReport::Aborted { reason, .. } if reason == "execution_environment_lost"
        )),
        "{reports:?}"
    );
    assert_acknowledged_run(&mut manager, &capture, "aborted");
    assert_run_phase(
        &capture,
        &successor,
        "runner.run.execution",
        "failure",
        Some("runner_phase_failed"),
    );
}

// Runs the initial attempt of a retained Cloud Git workflow to its failed
// terminal report and settles it, leaving its workspace retained for a
// continuation. Returns the manager, the initial offer, the workspace and
// its baseline commit.
async fn retained_initial_attempt(
    v3: bool,
) -> (
    tempfile::TempDir,
    AssignmentManager,
    AssignmentOffer,
    PathBuf,
    String,
) {
    // The `edit` script uses only shell builtins (`test`, `printf`, `:` and
    // redirection), so its failing branch forks nothing and cannot be lost to a
    // host that refuses `fork`. Exit 75 is the intended first failure; 76 means
    // the workspace was retained but the marker is gone; 99 means the marker
    // could not be written.
    let workflow = r#"schemaVersion: 1
steps:
  edit:
    kind: cmd
    command:
      argv:
        - /bin/sh
        - -c
        - 'if test -f .git/fixture-failed; then exit 0; fi; if test -f change.txt; then exit 76; fi; printf "retained edits\n" > change.txt; : > .git/fixture-failed || exit 99; exit 75'
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
    let (temporary, mut manager) = manager_fixture(workflow);
    resolve(&source_fixture_path(&manager), Path::new("workflow.yaml")).unwrap();
    let mut initial = offer("bg");
    align_offer_with_source_fixture(&manager, &mut initial);
    if v3 {
        let source = source_fixture_path(&manager);
        fs::write(source.join("workflow.yaml"), "not: a workflow\n").unwrap();
        run_fixture_git(&source, &["add", "workflow.yaml"]);
        run_fixture_git(&source, &["commit", "--quiet", "-m", "workspace revision"]);
        initial.execution_spec.primary_workspace_source.commit_oid =
            run_fixture_git(&source, &["rev-parse", "HEAD"]);
    }
    let baseline = initial
        .execution_spec
        .primary_workspace_source
        .commit_oid
        .clone();
    if v3 {
        let spec = v3_spec();
        let mut membership = spec.workspace_membership.unwrap();
        membership.workspace_commit_oid = baseline.clone();
        membership.witness.tip_oid = baseline.clone();
        membership.witness.reference =
            format!("refs/heads/{}", initial.execution_spec.source_branch);
        initial.execution_spec.schema_version = 3;
        initial.execution_spec.source_display_snapshot = spec.source_display_snapshot;
        initial.execution_spec.workspace_branch =
            Some(initial.execution_spec.source_branch.clone());
        initial.execution_spec.workspace_membership = Some(membership);
    }
    manager.handle_offer(initial.clone()).unwrap();
    prepare_current(&mut manager, &initial).await;
    let workspace = match &manager.slot {
        Some(LocalSlot::Accepted(accepted)) => accepted.root.execution.clone(),
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
    (temporary, manager, initial, workspace, baseline)
}

// The continuation offer of `initial`'s retained workspace on this runner boot.
fn continuation_successor(
    manager: &AssignmentManager,
    initial: &AssignmentOffer,
    workspace: &Path,
) -> AssignmentOffer {
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
    successor
}

// Starts and authorizes the admitted continuation `successor`, returning its
// job unspawned.
fn start_continuation(
    manager: &mut AssignmentManager,
    successor: &AssignmentOffer,
) -> ExecutionJob {
    let mut start = start_for(successor);
    start.effect_id = "eff_01k0z6r1w8f4jy2m7q9v3x5abn".into();
    let job = manager.handle_start(start).unwrap().unwrap();
    let mut authorization = start_authorization_for(successor);
    authorization.effect_id = "eff_01k0z6r1w8f4jy2m7q9v3x5abp".into();
    manager.handle_start_authorized(authorization).unwrap();
    job
}

pub(super) async fn prepare_retained_continuation(
    manager: &mut AssignmentManager,
    successor: &AssignmentOffer,
) {
    let mut prepare = prepare_for(manager, successor);
    prepare.effect_id = "eff_01k0z6r1w8f4jy2m7q9v3x5abj".into();
    let preparation = wait_for_offer_preparation(manager).await;
    manager.acknowledge_observation(preparation.id);
    manager.handle_prepare(prepare).unwrap();
    let mut ready_id = None;
    with_watchdog(wait_for_manager_state(manager, |manager| {
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
    with_watchdog(wait_for_manager_state(manager, |manager| {
        manager.drain_events();
        !matches!(
            manager.slot,
            Some(LocalSlot::Preparing(_) | LocalSlot::Releasing(_))
        )
    }))
    .await
    .expect("continuation admission did not complete");
}
