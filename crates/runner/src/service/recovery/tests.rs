use super::host_fixture::fixture_host;
use super::*;
use crate::service::execution::AssignmentProcessGuards;
use crate::service::workspace::{CleanupResult, WorkRootLease};

const RUNNER: &str = "rnr_01k0z6r1w8f4jy2m7q9v3x5abc";
const BOOT_A: &str = "rbt_01k0z6r1w8f4jy2m7q9v3x5abc";
const BOOT_B: &str = "rbt_01k0z6r1w8f4jy2m7q9v3x5abd";
const ASSIGNMENT: &str = "asn_01k0z6r1w8f4jy2m7q9v3x5abc";
const RUN: &str = "run_01k0z6r1w8f4jy2m7q9v3x5abc";
const ATTEMPT: &str = "atm_01k0z6r1w8f4jy2m7q9v3x5abc";

fn private_tempdir() -> tempfile::TempDir {
    use std::os::unix::fs::PermissionsExt;
    // The CLI suite deliberately aliases TMPDIR through a symlink. Recovery
    // requires an unaliased root, so create fixtures under its physical parent.
    let parent = fs::canonicalize(std::env::temp_dir()).unwrap();
    let directory = tempfile::tempdir_in(parent).unwrap();
    fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700)).unwrap();
    directory
}

fn grant(manifest: &Manifest, generation: u64) -> FixtureGrant {
    let origin = &manifest.lineage[0];
    FixtureGrant {
        runner_id: manifest.runner_id.clone(),
        work_root_id: manifest.work_root_id.clone(),
        workspace_id: manifest.workspace_id.clone(),
        execution_root: manifest.execution_root.clone(),
        origin_boot_id: origin.boot_id.clone(),
        origin_assignment_id: origin.assignment_id.clone(),
        origin_run_id: origin.run_id.clone(),
        origin_attempt_id: origin.attempt_id.clone(),
        fenced_predecessor_assignment_id: manifest
            .claim
            .as_ref()
            .map(|claim| claim.assignment_id.clone())
            .unwrap_or_else(|| manifest.lineage.last().unwrap().assignment_id.clone()),
        claim: Claim {
            claim_generation: generation,
            assignment_id: "asn_01k0z6r1w8f4jy2m7q9v3x5abd".into(),
            attempt_id: "atm_01k0z6r1w8f4jy2m7q9v3x5abd".into(),
            runner_boot_id: BOOT_B.into(),
        },
    }
}

// INV-RUNNER-13
#[test]
fn retained_identity_and_local_generation_survive_fresh_boot_without_adopting_legacy() {
    let temp = private_tempdir();
    let first = WorkRootLease::acquire_for_test(temp.path(), BOOT_A).unwrap();
    first.enable_recovery(RUNNER, fixture_host()).unwrap();
    let original = first
        .create_assignment_for_attempt(ASSIGNMENT, RUN, ATTEMPT, None)
        .unwrap();
    let store = first.recovery().unwrap();
    let binding = store.host_boot().unwrap();
    let _guards =
        AssignmentProcessGuards::durable_recovery(&original.private, binding, fixture_host())
            .unwrap();
    let registration_origin = Origin::from_assignment(&original, BOOT_A);
    let workspace = store.register(registration_origin.clone()).unwrap();
    let inventory = store
        .initial_inventory(&workspace, &registration_origin)
        .unwrap();
    assert_eq!(inventory["priorAssignmentId"], ASSIGNMENT);
    assert_eq!(inventory["priorAttemptId"], ATTEMPT);
    assert!(inventory["observedClaim"].is_null());
    assert_eq!(inventory["lastClaimGeneration"], 0);
    let frame = um_runner_protocol::RunnerFrame::WorkspaceInventory {
        envelope: um_runner_protocol::RunnerEnvelope {
            message_id: "rmsg_01k0z6r1w8f4jy2m7q9v3x5abc".into(),
            runner_id: RUNNER.into(),
            boot_id: BOOT_A.into(),
            sequence: 2,
            sent_at: "2026-10-18T00:00:00Z".into(),
        },
        payload: inventory,
    };
    let encoded = um_runner_protocol::encode_runner_frame(&frame).unwrap();
    let value: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(value["type"], "workspace_inventory");
    assert_eq!(value["payload"]["originAssignmentId"], ASSIGNMENT);
    let origin = store.manifest(&workspace).unwrap();
    let private = store.private_by_attempt(&origin).unwrap();
    assert_eq!(private[ATTEMPT], original.private.path());
    let root_id = origin.work_root_id.clone();
    drop(original);
    drop(first);

    let second = WorkRootLease::acquire_for_test(temp.path(), BOOT_B).unwrap();
    second.enable_recovery(RUNNER, fixture_host()).unwrap();
    let target = second
        .create_assignment_for_attempt(
            "asn_01k0z6r1w8f4jy2m7q9v3x5abd",
            RUN,
            "atm_01k0z6r1w8f4jy2m7q9v3x5abd",
            None,
        )
        .unwrap();
    let store = second.recovery().unwrap();
    AssignmentProcessGuards::durable_recovery(
        &target.private,
        store.host_boot().unwrap(),
        fixture_host(),
    )
    .unwrap();
    let manifest = store.manifest(&workspace).unwrap();
    assert_eq!(manifest.work_root_id, root_id);
    assert_eq!(manifest.lineage[0].assignment_id, ASSIGNMENT);
    assert_eq!(manifest.lineage[0].attempt_id, ATTEMPT);
    assert!(
        store
            .claim_fixture(&workspace, &grant(&manifest, 1), false)
            .is_err()
    );
    let claimed = store
        .claim_fixture(&workspace, &grant(&manifest, 1), true)
        .unwrap();
    assert_eq!(claimed.last_claim_generation, 1);
    assert_eq!(claimed.lineage.len(), 2);
    assert_eq!(
        store.private_by_attempt(&claimed).unwrap()["atm_01k0z6r1w8f4jy2m7q9v3x5abd"],
        target.private.path()
    );
    assert_eq!(
        store
            .claim_fixture(&workspace, &grant(&manifest, 1), true)
            .unwrap(),
        claimed
    );
    assert!(
        store
            .claim_fixture(
                &workspace,
                &grant(&manifest, 1).with_different_holder(),
                true
            )
            .is_err()
    );
    assert!(
        store
            .claim_fixture(&workspace, &grant(&manifest, MAX_GENERATION + 1), true)
            .is_err()
    );
    assert!(
        store
            .claim_fixture(&workspace, &grant(&manifest, 1), true)
            .is_ok()
    );
    assert_eq!(store.manifest(&workspace).unwrap().last_claim_generation, 1);
    let original_directory = fs::File::open(temp.path().join(BOOT_A).join(ASSIGNMENT)).unwrap();
    assert!(!claim_absent_for_cleanup(&original_directory));
    assert_eq!(
        second.release_boot_root_pending().wait(),
        CleanupResult::Retained
    );
    let next = second
        .create_assignment_for_attempt(
            "asn_01k0z6r1w8f4jy2m7q9v3x5abe",
            RUN,
            "atm_01k0z6r1w8f4jy2m7q9v3x5abe",
            None,
        )
        .unwrap();
    AssignmentProcessGuards::durable_recovery(
        &next.private,
        store.host_boot().unwrap(),
        fixture_host(),
    )
    .unwrap();
    let mut final_grant = grant(&claimed, MAX_GENERATION);
    final_grant.claim.assignment_id = "asn_01k0z6r1w8f4jy2m7q9v3x5abe".into();
    final_grant.claim.attempt_id = "atm_01k0z6r1w8f4jy2m7q9v3x5abe".into();
    let exhausted = store.claim_fixture(&workspace, &final_grant, true).unwrap();
    assert_eq!(exhausted.last_claim_generation, MAX_GENERATION);
    assert_eq!(exhausted.lineage.len(), 3);
    assert!(
        store
            .claim_fixture(&workspace, &grant(&exhausted, MAX_GENERATION + 1), true)
            .is_err()
    );
    assert_eq!(
        store.manifest(&workspace).unwrap().last_claim_generation,
        MAX_GENERATION
    );
    assert!(
        store
            .release_fixture_claim(&workspace, exhausted.claim.as_ref().unwrap(), false)
            .is_err()
    );
    let released = store
        .release_fixture_claim(&workspace, exhausted.claim.as_ref().unwrap(), true)
        .unwrap();
    assert!(released.claim.is_none());
    assert_eq!(released.last_claim_generation, MAX_GENERATION);
    assert!(claim_absent_for_cleanup(&original_directory));
    assert!(store.claim_fixture(&workspace, &final_grant, true).is_err());
}

// INV-RUNNER-13
#[test]
fn competing_claims_and_cleanup_cannot_publish_a_removed_holder() {
    let temp = private_tempdir();
    let first = WorkRootLease::acquire_for_test(temp.path(), BOOT_A).unwrap();
    first.enable_recovery(RUNNER, fixture_host()).unwrap();
    let original = first
        .create_assignment_for_attempt(ASSIGNMENT, RUN, ATTEMPT, None)
        .unwrap();
    AssignmentProcessGuards::durable_recovery(
        &original.private,
        first.recovery().unwrap().host_boot().unwrap(),
        fixture_host(),
    )
    .unwrap();
    let id = first
        .recovery()
        .unwrap()
        .register(Origin::from_assignment(&original, BOOT_A))
        .unwrap();
    drop(original);
    drop(first);
    let second = WorkRootLease::acquire_for_test(temp.path(), BOOT_B).unwrap();
    second.enable_recovery(RUNNER, fixture_host()).unwrap();
    let candidate = second
        .create_assignment_for_attempt(
            "asn_01k0z6r1w8f4jy2m7q9v3x5abd",
            RUN,
            "atm_01k0z6r1w8f4jy2m7q9v3x5abd",
            None,
        )
        .unwrap();
    AssignmentProcessGuards::durable_recovery(
        &candidate.private,
        second.recovery().unwrap().host_boot().unwrap(),
        fixture_host(),
    )
    .unwrap();
    let grant = grant(&second.recovery().unwrap().manifest(&id).unwrap(), 1);
    let gate = std::sync::Arc::new(std::sync::Barrier::new(2));
    let claimant = std::sync::Arc::clone(&second);
    let id_for_claim = id.clone();
    let go = std::sync::Arc::clone(&gate);
    let worker = std::thread::spawn(move || {
        go.wait();
        claimant
            .recovery()
            .unwrap()
            .claim_fixture(&id_for_claim, &grant, true)
            .is_ok()
    });
    gate.wait();
    let removal = second.release_boot_root_pending().wait();
    let claimed = worker.join().unwrap();
    if claimed {
        assert_eq!(removal, CleanupResult::Retained);
        assert_eq!(
            second
                .recovery()
                .unwrap()
                .manifest(&id)
                .unwrap()
                .last_claim_generation,
            1
        );
    } else {
        assert!(matches!(
            removal,
            CleanupResult::Released | CleanupResult::Retained
        ));
        assert_eq!(
            second
                .recovery()
                .unwrap()
                .manifest(&id)
                .unwrap()
                .last_claim_generation,
            0
        );
    }
}

// INV-RUNNER-13
#[test]
fn copied_root_and_unknown_catalog_revision_fail_without_adopting_old_work() {
    let original = private_tempdir();
    let first = WorkRootLease::acquire_for_test(original.path(), BOOT_A).unwrap();
    let legacy = first
        .create_assignment_for_attempt(ASSIGNMENT, RUN, ATTEMPT, None)
        .unwrap();
    let legacy_path = legacy.private.path().to_owned();
    assert!(!legacy_path.join(MANIFEST).exists());
    first.enable_recovery(RUNNER, fixture_host()).unwrap();
    assert!(first.recovery().unwrap().catalog().unwrap().is_empty());
    let copied = private_tempdir();
    fs::copy(original.path().join(ROOT), copied.path().join(ROOT)).unwrap();
    let other = WorkRootLease::acquire_for_test(copied.path(), BOOT_B).unwrap();
    assert!(other.enable_recovery(RUNNER, fixture_host()).is_err());
    assert!(fs::read_dir(original.path()).unwrap().count() > 1);
    let mut root: serde_json::Value =
        serde_json::from_slice(&fs::read(original.path().join(ROOT)).unwrap()).unwrap();
    root["schemaVersion"] = serde_json::json!(2);
    fs::write(
        original.path().join(ROOT),
        serde_json::to_vec(&root).unwrap(),
    )
    .unwrap();
    drop(legacy);
    drop(first);
    let reopened = WorkRootLease::acquire_for_test(original.path(), BOOT_B).unwrap();
    assert!(reopened.enable_recovery(RUNNER, fixture_host()).is_err());
    assert!(legacy_path.exists());
    assert!(!legacy_path.join(MANIFEST).exists());
}

impl FixtureGrant {
    fn with_different_holder(mut self) -> Self {
        self.claim.attempt_id = ATTEMPT.into();
        self
    }
}

// INV-RUNNER-13
#[test]
fn missing_or_corrupt_manifest_never_recreates_a_catalog_identity() {
    let temp = private_tempdir();
    let first = WorkRootLease::acquire_for_test(temp.path(), BOOT_A).unwrap();
    first.enable_recovery(RUNNER, fixture_host()).unwrap();
    let original = first
        .create_assignment_for_attempt(ASSIGNMENT, RUN, ATTEMPT, None)
        .unwrap();
    AssignmentProcessGuards::durable_recovery(
        &original.private,
        first.recovery().unwrap().host_boot().unwrap(),
        fixture_host(),
    )
    .unwrap();
    let id = first
        .recovery()
        .unwrap()
        .register(Origin::from_assignment(&original, BOOT_A))
        .unwrap();
    let saved = first.recovery().unwrap().manifest(&id).unwrap();
    fs::remove_file(original.private.join(MANIFEST)).unwrap();
    drop(original);
    drop(first);
    let second = WorkRootLease::acquire_for_test(temp.path(), BOOT_B).unwrap();
    second.enable_recovery(RUNNER, fixture_host()).unwrap();
    assert_eq!(
        second.recovery().unwrap().catalog().unwrap(),
        vec![id.clone()]
    );
    assert!(second.recovery().unwrap().manifest(&id).is_err());
    assert!(
        second
            .recovery()
            .unwrap()
            .claim_fixture(&id, &grant(&saved, 1), true)
            .is_err()
    );
}

// INV-RUNNER-13
#[test]
fn invalid_manifest_identity_lineage_and_generation_cannot_authorize_a_claim() {
    let temp = private_tempdir();
    let owner = WorkRootLease::acquire_for_test(temp.path(), BOOT_A).unwrap();
    owner.enable_recovery(RUNNER, fixture_host()).unwrap();
    let original = owner
        .create_assignment_for_attempt(ASSIGNMENT, RUN, ATTEMPT, None)
        .unwrap();
    let store = owner.recovery().unwrap();
    AssignmentProcessGuards::durable_recovery(
        &original.private,
        store.host_boot().unwrap(),
        fixture_host(),
    )
    .unwrap();
    let id = store
        .register(Origin::from_assignment(&original, BOOT_A))
        .unwrap();
    let manifest = store.manifest(&id).unwrap();
    let saved = serde_json::to_value(&manifest).unwrap();
    for changed in [
        serde_json::json!({"schemaVersion": 2}),
        serde_json::json!({"lastClaimGeneration": MAX_GENERATION + 1}),
        serde_json::json!({"workspaceId": "rws_00000000000000000000000000"}),
        serde_json::json!({"lineage": [saved["lineage"][0].clone(), saved["lineage"][0].clone()]}),
        serde_json::json!({"lineage": []}),
    ] {
        let mut invalid = saved.clone();
        for (key, value) in changed.as_object().unwrap() {
            invalid[key] = value.clone();
        }
        fs::write(
            original.private.join(MANIFEST),
            serde_json::to_vec(&invalid).unwrap(),
        )
        .unwrap();
        assert!(store.manifest(&id).is_err());
        assert!(
            store
                .claim_fixture(&id, &grant(&manifest, 1), true)
                .is_err()
        );
        assert!(!store.may_clean_tree(&original.execution));
    }
    fs::write(
        original.private.join(MANIFEST),
        serde_json::to_vec(&saved).unwrap(),
    )
    .unwrap();
    assert_eq!(store.manifest(&id).unwrap(), manifest);
}

// INV-RUNNER-13
#[test]
fn record_reader_rejects_fifo_symlink_and_hardlink_without_trusting_the_contents() {
    use std::os::unix::fs::{PermissionsExt, symlink};
    let temp = private_tempdir();
    let directory = File::open(temp.path()).unwrap();
    let path = temp.path().join("record");
    nix::unistd::mkfifo(
        &path,
        nix::sys::stat::Mode::S_IRUSR | nix::sys::stat::Mode::S_IWUSR,
    )
    .unwrap();
    assert!(read_record::<serde_json::Value>(&directory, "record").is_err());
    fs::remove_file(&path).unwrap();
    let target = temp.path().join("target");
    fs::write(&target, b"{}").unwrap();
    fs::set_permissions(&target, fs::Permissions::from_mode(0o600)).unwrap();
    symlink(&target, &path).unwrap();
    assert!(read_record::<serde_json::Value>(&directory, "record").is_err());
    fs::remove_file(&path).unwrap();
    fs::hard_link(&target, &path).unwrap();
    assert!(read_record::<serde_json::Value>(&directory, "record").is_err());
}

// INV-RUNNER-13
#[test]
fn claimed_generation_survives_restart_and_uncommitted_staging_cannot_reset_it() {
    let temp = private_tempdir();
    let first = WorkRootLease::acquire_for_test(temp.path(), BOOT_A).unwrap();
    first.enable_recovery(RUNNER, fixture_host()).unwrap();
    let original = first
        .create_assignment_for_attempt(ASSIGNMENT, RUN, ATTEMPT, None)
        .unwrap();
    let store = first.recovery().unwrap();
    AssignmentProcessGuards::durable_recovery(
        &original.private,
        store.host_boot().unwrap(),
        fixture_host(),
    )
    .unwrap();
    let id = store
        .register(Origin::from_assignment(&original, BOOT_A))
        .unwrap();
    let initial = store.manifest(&id).unwrap();
    drop(original);
    drop(first);
    let second = WorkRootLease::acquire_for_test(temp.path(), BOOT_B).unwrap();
    second.enable_recovery(RUNNER, fixture_host()).unwrap();
    let target = second
        .create_assignment_for_attempt(
            "asn_01k0z6r1w8f4jy2m7q9v3x5abd",
            RUN,
            "atm_01k0z6r1w8f4jy2m7q9v3x5abd",
            None,
        )
        .unwrap();
    let store = second.recovery().unwrap();
    AssignmentProcessGuards::durable_recovery(
        &target.private,
        store.host_boot().unwrap(),
        fixture_host(),
    )
    .unwrap();
    let claimed = store.claim_fixture(&id, &grant(&initial, 1), true).unwrap();
    let private = store.private_by_attempt(&claimed).unwrap()[ATTEMPT].clone();
    fs::write(
        private.join(".um-recovery-incomplete-staging"),
        b"{\"lastClaimGeneration\":0",
    )
    .unwrap();
    drop(target);
    drop(second);
    let third =
        WorkRootLease::acquire_for_test(temp.path(), "rbt_01k0z6r1w8f4jy2m7q9v3x5abe").unwrap();
    third.enable_recovery(RUNNER, fixture_host()).unwrap();
    let store = third.recovery().unwrap();
    assert_eq!(store.manifest(&id).unwrap(), claimed);
    assert!(!store.may_clean_tree(&initial.execution_root));
    assert!(store.claim_fixture(&id, &grant(&initial, 1), true).is_err());
    // Losing the catalog must not mint a replacement identity over surviving V1 records.
    drop(third);
    fs::remove_file(temp.path().join(ROOT)).unwrap();
    let fourth =
        WorkRootLease::acquire_for_test(temp.path(), "rbt_01k0z6r1w8f4jy2m7q9v3x5abf").unwrap();
    assert!(fourth.enable_recovery(RUNNER, fixture_host()).is_err());
    assert!(!temp.path().join(ROOT).exists());
    assert!(private.join(MANIFEST).exists());
}

// Run the two phases in separate processes on an isolated supported host. Between
// phases the operator may reboot or detach/remount the fixture filesystem. This
// exercises the real record readers and claim transition, not a simulated boot ID.
#[test]
#[ignore = "requires an operator-owned persistent ext4 fixture and separate host phases"]
#[expect(
    clippy::zombie_processes,
    reason = "host qualification deliberately leaves a recorded writer alive across process exit"
)]
fn qualify_persistent_host_recovery() {
    use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
    use std::os::unix::process::CommandExt;
    use std::process::{Command, Stdio};
    use um_execution::{AuthenticatedProcessGroup, DurableProcessGuardStore};

    let path = PathBuf::from(std::env::var("UM_RECOVERY_FIXTURE_PATH").unwrap());
    let evidence = path.with_extension("evidence.json");
    let phase = std::env::var("UM_RECOVERY_FIXTURE_PHASE").unwrap();
    if phase == "inspect" {
        // Used after an external syscall-level crash/fault injector. Reopening
        // must never heal an uncertain catalog, manifest or claim by replacing it.
        let catalog_path = path.join(ROOT);
        let manifest_path = path
            .join(BOOT_A)
            .join(ASSIGNMENT)
            .join("private")
            .join(MANIFEST);
        let catalog_before = fs::read(&catalog_path).ok();
        let manifest_before = fs::read(&manifest_path).ok();
        let fresh = WorkRootLease::acquire_for_test(&path, "rbt_01k0z6r1w8f4jy2m7q9v3x5abf");
        if let Ok(owner) = fresh {
            match owner.enable_recovery(RUNNER, std::sync::Arc::new(SystemRecoveryHost)) {
                Ok(()) => {
                    let store = owner.recovery().unwrap();
                    // Operator fixture cleanup, never startup discovery: the
                    // literal origin is authenticated before stopping a writer
                    // left by the external crash injector.
                    let origin = Link {
                        boot_id: BOOT_A.to_owned(),
                        assignment_id: ASSIGNMENT.to_owned(),
                        run_id: RUN.to_owned(),
                        attempt_id: ATTEMPT.to_owned(),
                    };
                    if let Ok(private) = store.authority.authenticate_recovery_link(&origin)
                        && let Ok(guards) = AssignmentProcessGuards::recover_recovery(
                            &private,
                            store.host_boot().unwrap(),
                            store.host(),
                        )
                    {
                        AssignmentProcessGuards::quiesce_retained_chain(&[guards]).unwrap();
                    }
                    for workspace in store.catalog().unwrap() {
                        if let Ok(manifest) = store.manifest(&workspace) {
                            assert_eq!(manifest.lineage[0].attempt_id, ATTEMPT);
                            assert_eq!(manifest.lineage[0].assignment_id, ASSIGNMENT);
                            assert!(manifest.last_claim_generation <= 1);
                        }
                    }
                    if catalog_before.is_none() {
                        assert!(manifest_before.is_none());
                        assert!(store.catalog().unwrap().is_empty());
                    }
                }
                Err(error) => println!("incomplete recovery rejected: {}", error.error_type()),
            }
        }
        if let Some(catalog) = catalog_before {
            assert_eq!(fs::read(catalog_path).unwrap(), catalog);
        }
        assert_eq!(fs::read(manifest_path).ok(), manifest_before);
        println!(
            "fresh process preserved all existing recovery records after injected interruption"
        );
    } else if phase == "prepare" {
        fs::DirBuilder::new().mode(0o700).create(&path).unwrap();
        let first = WorkRootLease::acquire_for_test(&path, BOOT_A).unwrap();
        first
            .enable_recovery(RUNNER, std::sync::Arc::new(SystemRecoveryHost))
            .unwrap();
        let original = first
            .create_assignment_for_attempt(ASSIGNMENT, RUN, ATTEMPT, None)
            .unwrap();
        let store = first.recovery().unwrap();
        let binding = store.host_boot().unwrap();
        let guards = AssignmentProcessGuards::durable_recovery(
            &original.private,
            binding.clone(),
            store.host(),
        )
        .unwrap();
        let mut child = Command::new("/bin/sh")
            .args(["-c", "read release || exit 0; exec /bin/sleep 3600"])
            .process_group(0)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let pid = i32::try_from(child.id()).unwrap();
        let stat = fs::read_to_string(format!("/proc/{pid}/stat")).unwrap();
        let start = stat
            .rsplit_once(") ")
            .unwrap()
            .1
            .split_whitespace()
            .nth(19)
            .unwrap();
        let identity = AuthenticatedProcessGroup::new(
            rustix::process::Pid::from_raw(pid).unwrap(),
            start.to_owned(),
        )
        .unwrap();
        let guard = guards.register("hostProof", 1, &identity).unwrap();
        guards.mark_released(&guard).unwrap();
        child.stdin.take().unwrap().write_all(b"release\n").unwrap();
        let workspace_id = store
            .register(Origin::from_assignment(&original, BOOT_A))
            .unwrap();
        let manifest = store.manifest(&workspace_id).unwrap();
        fs::write(
            original.private.join("original-producer"),
            b"retained original bytes",
        )
        .unwrap();
        fs::write(&evidence, serde_json::to_vec(&serde_json::json!({
            "manifest": manifest, "osBootId": serde_json::to_value(&binding).unwrap()["osBootId"], "device": fs::metadata(&path).unwrap().dev(),
        })).unwrap()).unwrap();
        fs::set_permissions(&evidence, fs::Permissions::from_mode(0o600)).unwrap();
        File::open(&evidence).unwrap().sync_all().unwrap();
        File::open(path.parent().unwrap())
            .unwrap()
            .sync_all()
            .unwrap();
        println!("prepared persistent root and live writer; child PID {pid}");
    } else {
        assert_eq!(phase, "verify");
        let saved: serde_json::Value =
            serde_json::from_slice(&fs::read(&evidence).unwrap()).unwrap();
        let original: Manifest = serde_json::from_value(saved["manifest"].clone()).unwrap();
        let second = WorkRootLease::acquire_for_test(&path, BOOT_B).unwrap();
        second
            .enable_recovery(RUNNER, std::sync::Arc::new(SystemRecoveryHost))
            .unwrap();
        let target = second
            .create_assignment_for_attempt(
                "asn_01k0z6r1w8f4jy2m7q9v3x5abd",
                RUN,
                "atm_01k0z6r1w8f4jy2m7q9v3x5abd",
                None,
            )
            .unwrap();
        let store = second.recovery().unwrap();
        let current_boot = store.host_boot().unwrap();
        let rebooted =
            saved["osBootId"] != serde_json::to_value(&current_boot).unwrap()["osBootId"];
        assert_eq!(
            rebooted,
            std::env::var("UM_RECOVERY_EXPECT_REBOOT").unwrap() == "1"
        );
        let device_changed = saved["device"] != fs::metadata(&path).unwrap().dev();
        assert_eq!(
            device_changed,
            std::env::var("UM_RECOVERY_EXPECT_DEVICE_CHANGE").unwrap() == "1"
        );
        AssignmentProcessGuards::durable_recovery(&target.private, current_boot, store.host())
            .unwrap();
        let checked = store.manifest(&original.workspace_id).unwrap();
        assert_eq!(original, checked);
        let private = store.private_by_attempt(&checked).unwrap();
        assert_eq!(
            fs::read(private[ATTEMPT].join("original-producer")).unwrap(),
            b"retained original bytes"
        );
        let claimed = store
            .claim_fixture(&checked.workspace_id, &grant(&checked, 1), true)
            .unwrap();
        assert_eq!(claimed.last_claim_generation, 1);
        assert_eq!(claimed.lineage.len(), 2);
        println!(
            "verified original identities and bytes, stopped writers and claimed generation 1; rebooted={rebooted}, deviceChanged={device_changed}"
        );
    }
}
