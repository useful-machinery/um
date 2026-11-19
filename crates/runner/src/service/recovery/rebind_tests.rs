use std::os::unix::fs::PermissionsExt;

use super::host_fixture::fixture_host;
use super::*;
use crate::service::workspace::{
    CleanupResult, ProcessQuiescence, WorkRootLease, WorkspaceDisposition,
};

const OLD_RUNNER: &str = "rnr_01k0z6r1w8f4jy2m7q9v3x5abc";
const NEW_RUNNER: &str = "rnr_01k0z6r1w8f4jy2m7q9v3x5abd";
const OTHER_RUNNER: &str = "rnr_01k0z6r1w8f4jy2m7q9v3x5abe";
const OLD_BOOT: &str = "rbt_01k0z6r1w8f4jy2m7q9v3x5abc";
const ASSIGNMENT: &str = "asn_01k0z6r1w8f4jy2m7q9v3x5abc";
const RUN: &str = "run_01k0z6r1w8f4jy2m7q9v3x5abc";
const ATTEMPT: &str = "atm_01k0z6r1w8f4jy2m7q9v3x5abc";
const WORKSPACE: &str = "rws_01k0z6r1w8f4jy2m7q9v3x5abc";
const KEPT: &[u8] = b"kept by the replaced runner";

// A canonical private directory: the CLI suite aliases TMPDIR through a
// symlink, and the store refuses an aliased root.
fn private_directory() -> tempfile::TempDir {
    let parent = fs::canonicalize(std::env::temp_dir()).unwrap();
    let directory = tempfile::tempdir_in(parent).unwrap();
    fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700)).unwrap();
    directory
}

fn boot(index: u8) -> String {
    format!("rbt_01k0z6r1w8f4jy2m7q9v3x5{index:03}")
}

/// A work root whose recovery records belong to `OLD_RUNNER`: one cataloged
/// workspace with its manifest, left by a boot that never released it.
struct OldRoot {
    _directory: tempfile::TempDir,
    root: PathBuf,
    work_root_id: String,
    record: Vec<u8>,
    workspace_file: PathBuf,
    manifest: PathBuf,
    manifest_bytes: Vec<u8>,
}

impl OldRoot {
    fn new() -> Self {
        let directory = private_directory();
        let root = directory.path().to_owned();
        let lease = WorkRootLease::acquire_for_test(&root, OLD_BOOT).unwrap();
        lease.enable_recovery(OLD_RUNNER, fixture_host()).unwrap();
        let assignment = lease
            .create_assignment_for_attempt(ASSIGNMENT, RUN, ATTEMPT, None)
            .unwrap();
        let workspace_file = assignment.execution.join("kept.txt");
        fs::write(&workspace_file, KEPT).unwrap();
        let handle = File::open(&root).unwrap();
        let mut record: RootRecord = read_record(&handle, ROOT).unwrap();
        let entry = CatalogEntry {
            workspace_id: WORKSPACE.to_owned(),
            origin_boot_id: OLD_BOOT.to_owned(),
            origin_assignment_id: ASSIGNMENT.to_owned(),
            origin_run_id: RUN.to_owned(),
            origin_attempt_id: ATTEMPT.to_owned(),
            execution_root: root.join(OLD_BOOT).join(ASSIGNMENT).join("workspace"),
        };
        record.catalog.push(entry.clone());
        persist(&handle, ROOT, &record, true).unwrap();
        let manifest = Manifest {
            schema_version: 1,
            runner_id: OLD_RUNNER.to_owned(),
            work_root_id: record.work_root_id.clone(),
            workspace_id: WORKSPACE.to_owned(),
            binding: record.binding.clone(),
            execution_root: entry.execution_root,
            lineage: vec![Link {
                boot_id: OLD_BOOT.to_owned(),
                assignment_id: ASSIGNMENT.to_owned(),
                run_id: RUN.to_owned(),
                attempt_id: ATTEMPT.to_owned(),
            }],
            claim: None,
            last_claim_generation: 0,
        };
        let private = root.join(OLD_BOOT).join(ASSIGNMENT).join("private");
        persist(&File::open(&private).unwrap(), MANIFEST, &manifest, false).unwrap();
        let (work_root_id, _) = lease.recovery().unwrap().discovery(|_| {}).unwrap();
        assert_eq!(work_root_id, record.work_root_id);
        drop(assignment);
        drop(lease);
        let manifest = private.join(MANIFEST);
        Self {
            record: fs::read(root.join(ROOT)).unwrap(),
            manifest_bytes: fs::read(&manifest).unwrap(),
            _directory: directory,
            root,
            work_root_id,
            workspace_file,
            manifest,
        }
    }

    fn assert_old_tree_kept(&self) {
        assert_eq!(fs::read(&self.workspace_file).unwrap(), KEPT);
        assert_eq!(fs::read(&self.manifest).unwrap(), self.manifest_bytes);
    }
}

// The result of releasing a new assignment created under `lease`, and whether
// its directory is still present afterwards.
fn release_new_assignment(
    lease: &WorkRootLease,
    root: &Path,
    boot_id: &str,
) -> (CleanupResult, bool) {
    const NEW_ASSIGNMENT: &str = "asn_01k0z6r1w8f4jy2m7q9v3x5abd";
    let assignment = lease
        .create_assignment_for_attempt(
            NEW_ASSIGNMENT,
            "run_01k0z6r1w8f4jy2m7q9v3x5abd",
            "atm_01k0z6r1w8f4jy2m7q9v3x5abd",
            None,
        )
        .unwrap();
    let result = assignment
        .release_pending(ProcessQuiescence::Proven, WorkspaceDisposition::Remove)
        .wait();
    drop(assignment);
    (result, root.join(boot_id).join(NEW_ASSIGNMENT).exists())
}

// INV-RUNNER-37, REQ-RUNNERENROLL-129
#[test]
fn the_root_record_of_a_replaced_runner_is_replaced_for_the_hosts_runner() {
    let old = OldRoot::new();
    let replaced = [OLD_RUNNER.to_owned()];

    let lease = WorkRootLease::acquire_for_test(&old.root, &boot(1)).unwrap();
    lease
        .enable_recovery_replacing(NEW_RUNNER, &replaced, fixture_host())
        .unwrap();
    let (work_root_id, observations) = lease
        .recovery()
        .unwrap()
        .discovery(|error| panic!("unavailable anchor: {error}"))
        .unwrap();
    assert!(observations.is_empty());
    assert_ne!(work_root_id, old.work_root_id);
    let record: serde_json::Value =
        serde_json::from_slice(&fs::read(old.root.join(ROOT)).unwrap()).unwrap();
    assert_eq!(record["runnerId"], NEW_RUNNER);
    assert_eq!(record["workRootId"], work_root_id.as_str());
    assert_eq!(record["catalog"], serde_json::json!([]));
    old.assert_old_tree_kept();
    assert_eq!(
        release_new_assignment(&lease, &old.root, &boot(1)),
        (CleanupResult::Released, false)
    );
    drop(lease);

    let lease = WorkRootLease::acquire_for_test(&old.root, &boot(2)).unwrap();
    lease
        .enable_recovery_replacing(NEW_RUNNER, &replaced, fixture_host())
        .unwrap();
    let (again, _) = lease.recovery().unwrap().discovery(|_| {}).unwrap();
    assert_eq!(again, work_root_id);
    old.assert_old_tree_kept();
}

// INV-RUNNER-37, REQ-RUNNERENROLL-129
#[test]
fn the_root_record_of_a_runner_no_rejoin_replaced_stays_unopened() {
    for replaced in [Vec::new(), vec![OTHER_RUNNER.to_owned()]] {
        let old = OldRoot::new();
        let lease = WorkRootLease::acquire_for_test(&old.root, &boot(1)).unwrap();

        let error = lease
            .enable_recovery_replacing(NEW_RUNNER, &replaced, fixture_host())
            .unwrap_err();

        assert_eq!(error.error_type(), "local_recovery_record_invalid");
        assert_eq!(fs::read(old.root.join(ROOT)).unwrap(), old.record);
        assert!(lease.recovery().is_none());
        let (result, _) = release_new_assignment(&lease, &old.root, &boot(1));
        assert_eq!(result, CleanupResult::Retained);
        old.assert_old_tree_kept();
    }
}

// INV-RUNNER-13, INV-RUNNER-37
#[test]
fn a_replaced_runners_root_record_that_does_not_authenticate_is_kept() {
    let old = OldRoot::new();
    let replaced = [OLD_RUNNER.to_owned()];

    let copy = private_directory();
    let copied = copy.path().join(ROOT);
    fs::write(&copied, &old.record).unwrap();
    fs::set_permissions(&copied, fs::Permissions::from_mode(0o600)).unwrap();
    let lease = WorkRootLease::acquire_for_test(copy.path(), &boot(1)).unwrap();
    let error = lease
        .enable_recovery_replacing(NEW_RUNNER, &replaced, fixture_host())
        .unwrap_err();
    assert_eq!(error.error_type(), "local_recovery_record_invalid");
    assert_eq!(fs::read(&copied).unwrap(), old.record);
    assert!(lease.recovery().is_none());
    drop(lease);

    let mut record: RootRecord = serde_json::from_slice(&old.record).unwrap();
    record.schema_version = 2;
    persist(&File::open(&old.root).unwrap(), ROOT, &record, true).unwrap();
    let unknown = fs::read(old.root.join(ROOT)).unwrap();
    let lease = WorkRootLease::acquire_for_test(&old.root, &boot(1)).unwrap();
    let error = lease
        .enable_recovery_replacing(NEW_RUNNER, &replaced, fixture_host())
        .unwrap_err();
    assert_eq!(error.error_type(), "local_recovery_record_invalid");
    assert_eq!(fs::read(old.root.join(ROOT)).unwrap(), unknown);
    assert!(lease.recovery().is_none());
    old.assert_old_tree_kept();
}
