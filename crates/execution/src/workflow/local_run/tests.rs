use crate::workflow::publication::{CancellationReasonV1, WorkflowOutcomeV1};
use std::collections::BTreeMap;
use std::fs::{self, Permissions};
use std::num::NonZeroU64;
use std::os::unix::fs::{PermissionsExt as _, symlink};
use std::path::Path;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use serde_json::json;
use std::time::Duration;

use super::*;
use crate::workflow::admission::{
    CancellationPolicy, CancellationReason, CancellationSource, CaptureLimits, EnvironmentSnapshot,
    ExecutionContext, ExecutionPolicyLimits, InputLimits, ResolvedAttachment, ResolvedFile,
    ResolvedInput, ResolvedInputs, ResolvedJsonInput, admit_workflow,
};
use crate::workflow::archived_attempt::{
    ArchivedAttemptIneligibilityReason, ArchivedAttemptLoadError,
    ArchivedAttemptOperationalErrorCode, ArchivedStepDetail, ProjectionInvariant,
    load_local_archived_attempt, load_local_archived_attempt_observed,
};
use crate::workflow::definition_store::WorkflowHome;
use crate::workflow::resolution;

struct AdmittedFixture {
    _temporary: tempfile::TempDir,
    admitted: AdmittedWorkflow,
    execution_root: PathBuf,
    run_parent: PathBuf,
}

impl AdmittedFixture {
    fn new() -> Self {
        Self::new_with_environment(EnvironmentSnapshot::default())
    }

    fn new_with_environment(environment: EnvironmentSnapshot) -> Self {
        Self::from_source_with_inputs_and_environment(
            "schemaVersion: 1\ninputs:\n  request: {kind: text}\n  settings: {kind: json}\n  evidence: {kind: attachments}\nsteps:\n  first:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n  second:\n    kind: cmd\n    dependsOn: [first]\n    command:\n      argv: [\"true\"]\n",
            ResolvedInputs::new(BTreeMap::from([
                (
                    "request".to_owned(),
                    ResolvedInput::Text(Arc::from("durable request\n")),
                ),
                (
                    "evidence".to_owned(),
                    ResolvedInput::Attachments(Arc::from([ResolvedAttachment::new(
                        Arc::from("application/octet-stream"),
                        Arc::from([0_u8, 1, 0xff]),
                    )])),
                ),
                (
                    "settings".to_owned(),
                    ResolvedInput::Json(
                        ResolvedJsonInput::from_source(Arc::from(
                            b"{ \"z\": null, \"n\": 1.2300 }\n".as_slice(),
                        ))
                        .unwrap(),
                    ),
                ),
            ])),
            1024,
            environment,
        )
    }

    fn from_source(source: &str) -> Self {
        Self::from_source_with_inputs(source, ResolvedInputs::default(), 1024)
    }

    fn from_source_with_maximum_step_log_bytes(source: &str, maximum_step_log_bytes: u64) -> Self {
        Self::from_source_with_inputs(source, ResolvedInputs::default(), maximum_step_log_bytes)
    }

    fn from_source_with_inputs(
        source: &str,
        inputs: ResolvedInputs,
        maximum_step_log_bytes: u64,
    ) -> Self {
        Self::from_source_with_inputs_and_environment(
            source,
            inputs,
            maximum_step_log_bytes,
            EnvironmentSnapshot::default(),
        )
    }

    fn from_source_with_inputs_and_environment(
        source: &str,
        inputs: ResolvedInputs,
        maximum_step_log_bytes: u64,
        environment: EnvironmentSnapshot,
    ) -> Self {
        Self::from_source_with_git(source, inputs, maximum_step_log_bytes, environment, false)
    }

    fn from_source_with_git(
        source: &str,
        inputs: ResolvedInputs,
        maximum_step_log_bytes: u64,
        environment: EnvironmentSnapshot,
        git: bool,
    ) -> Self {
        Self::build(
            source,
            inputs,
            maximum_step_log_bytes,
            environment,
            git,
            None,
        )
    }

    /// A run admitted from a tree: `source` is the root `workflow.yaml` and each
    /// `(path, contents, mode)` is written under the source root before validation.
    fn from_tree(source: &str, files: &[(&str, &str, u32)]) -> Self {
        Self::build(
            source,
            ResolvedInputs::default(),
            1024,
            EnvironmentSnapshot::default(),
            false,
            Some(files),
        )
    }

    fn build(
        source: &str,
        inputs: ResolvedInputs,
        maximum_step_log_bytes: u64,
        environment: EnvironmentSnapshot,
        git: bool,
        tree_files: Option<&[(&str, &str, u32)]>,
    ) -> Self {
        let temporary = tempfile::tempdir().unwrap();
        let source_root = temporary.path().join("source");
        let execution_root = temporary.path().join("execution");
        let run_parent = temporary.path().join("runs");
        for directory in [&source_root, &execution_root, &run_parent] {
            fs::create_dir(directory).unwrap();
        }
        if git {
            let run_git = |args: &[&str]| {
                let output = std::process::Command::new("git")
                    .args(args)
                    .current_dir(&execution_root)
                    .output()
                    .unwrap();
                assert!(
                    output.status.success(),
                    "git {args:?}: {}",
                    String::from_utf8_lossy(&output.stderr)
                );
            };
            run_git(&["init", "--quiet"]);
            run_git(&["config", "user.name", "Checkpoint Fixture"]);
            run_git(&["config", "user.email", "checkpoint@example.invalid"]);
            run_git(&["config", "commit.gpgsign", "false"]);
            fs::write(execution_root.join("tracked"), b"baseline\n").unwrap();
            run_git(&["add", "tracked"]);
            run_git(&["commit", "--quiet", "-m", "baseline"]);
        }
        fs::write(source_root.join("workflow.yaml"), source).unwrap();
        for (path, contents, mode) in tree_files.unwrap_or_default() {
            let path = source_root.join(path);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(&path, contents).unwrap();
            fs::set_permissions(&path, Permissions::from_mode(*mode)).unwrap();
        }
        let tree = tree_files.map(|_| {
            crate::workflow::validate_workflow_tree(&source_root)
                .unwrap_or_else(|diagnostics| panic!("tree should be valid: {diagnostics:?}"))
        });
        let workflow = resolution::resolve(&source_root, Path::new("workflow.yaml")).unwrap();
        let admitted = admit_workflow(workflow, inputs, {
            let context = fixture_context(&execution_root, maximum_step_log_bytes, environment);
            let context = match tree {
                Some(tree) => context.with_tree(tree),
                None => context,
            };
            if git {
                context.with_local_git_capture()
            } else {
                context
            }
        })
        .unwrap();
        Self {
            _temporary: temporary,
            admitted,
            execution_root,
            run_parent,
        }
    }

    fn run_path(&self, name: &str) -> PathBuf {
        self.run_parent.join(name)
    }

    /// A child run's admission as the CLI's dispatcher makes it: `workflow` resolved from
    /// the parent's tree copy and admitted with that copy as its tree, against the fixture's
    /// execution root.
    fn admit_child(&self, tree_copy: &Path, workflow: &str) -> AdmittedWorkflow {
        let tree = crate::workflow::validate_workflow_tree(tree_copy)
            .unwrap_or_else(|diagnostics| panic!("copy should be a tree: {diagnostics:?}"));
        let resolved = resolution::resolve(tree_copy, Path::new(workflow)).unwrap();
        admit_workflow(
            resolved,
            ResolvedInputs::default(),
            fixture_context(&self.execution_root, 1024, EnvironmentSnapshot::default())
                .with_tree(tree),
        )
        .unwrap()
    }
}

fn fixture_context(
    execution_root: &Path,
    maximum_step_log_bytes: u64,
    environment: EnvironmentSnapshot,
) -> ExecutionContext {
    ExecutionContext::new(
        execution_root.to_owned(),
        ExecutionPolicyLimits::new(
            2,
            CaptureLimits::new(16, 1024, 4096),
            InputLimits::new(16, 1024, 4096, 4096),
            maximum_step_log_bytes,
        ),
        environment,
        CancellationPolicy::new(CancellationSource::new(), Duration::from_secs(10)),
    )
}

struct AssertLockedPublication {
    observed: bool,
}

impl InitialPublicationObserver for AssertLockedPublication {
    fn published(&mut self, path: &Path, owner_lock: &File) -> Result<(), LocalRunDirectoryError> {
        assert!(path.join(LOCK_FILE).is_file());
        assert!(owner_lock.metadata().unwrap().is_file());
        self.observed = true;
        Ok(())
    }
}

#[test]
fn retry_ineligibility_uses_the_closed_precedence_order() {
    let ownership_unproven = LocalRecoveryStatus::OwnershipUnproven {
        guard_ids: vec!["11111111-1111-4111-8111-111111111111".to_owned()],
        reason: OwnershipUnprovenReason::ProcessIdentityInspectionUnavailable,
    };
    assert_eq!(
        retry_eligibility(AttemptStateV1::Succeeded, &ownership_unproven, true, false),
        LocalRetryEligibility::Ineligible(RetryIneligibilityReason::RunLocked)
    );
    assert_eq!(
        retry_eligibility(AttemptStateV1::Succeeded, &ownership_unproven, false, false),
        LocalRetryEligibility::Ineligible(RetryIneligibilityReason::OwnershipUnproven)
    );
    assert_eq!(
        retry_eligibility(
            AttemptStateV1::Succeeded,
            &LocalRecoveryStatus::Settled,
            false,
            false,
        ),
        LocalRetryEligibility::Ineligible(RetryIneligibilityReason::LatestAttemptSucceeded)
    );
    assert_eq!(
        retry_eligibility(
            AttemptStateV1::Rejected,
            &LocalRecoveryStatus::Settled,
            false,
            false,
        ),
        LocalRetryEligibility::Ineligible(RetryIneligibilityReason::LatestAttemptRejected)
    );
    for state in [
        AttemptStateV1::WorkflowFailed,
        AttemptStateV1::Cancelled,
        AttemptStateV1::Interrupted,
    ] {
        assert_eq!(
            retry_eligibility(state, &LocalRecoveryStatus::Settled, false, false),
            LocalRetryEligibility::Eligible
        );
    }
    // REQ-LOCALRUN-129, REQ-LOCALRUN-196: a superseded record yields to the lock and to
    // unproven ownership, and refuses a last attempt `waiting` that a record without
    // `successor` admits.
    assert_eq!(
        retry_eligibility(AttemptStateV1::Waiting, &ownership_unproven, true, true),
        LocalRetryEligibility::Ineligible(RetryIneligibilityReason::RunLocked)
    );
    assert_eq!(
        retry_eligibility(AttemptStateV1::Waiting, &ownership_unproven, false, true),
        LocalRetryEligibility::Ineligible(RetryIneligibilityReason::OwnershipUnproven)
    );
    assert_eq!(
        retry_eligibility(
            AttemptStateV1::Waiting,
            &LocalRecoveryStatus::Settled,
            false,
            true
        ),
        LocalRetryEligibility::Ineligible(RetryIneligibilityReason::RunSuperseded)
    );
    assert_eq!(
        retry_eligibility(
            AttemptStateV1::Waiting,
            &LocalRecoveryStatus::Settled,
            false,
            false,
        ),
        LocalRetryEligibility::Eligible
    );
}

struct FixtureRecoveryAuthority {
    host: Result<ExecutionHostV1, ()>,
    observation: ProcessIdentityObservation,
}

impl LocalRecoveryAuthority for FixtureRecoveryAuthority {
    fn execution_host(&self) -> Result<ExecutionHostV1, ()> {
        self.host.clone()
    }

    fn observe_process(&self, _guard: &ProcessGuardV1) -> ProcessIdentityObservation {
        self.observation
    }
}

fn fixture_guarded_attempt() -> LocalAttemptV1 {
    let fixture = AdmittedFixture::new();
    let run = InitialLocalRun::create(&fixture.run_path("guarded"), &fixture.admitted).unwrap();
    let mut attempt = read_state(run.root_handle()).unwrap().attempts.remove(0);
    attempt.process_guards.push(ProcessGuardV1 {
        guard_id: "11111111-1111-4111-8111-111111111111".to_owned(),
        action_id: 1,
        step_id: "first".to_owned(),
        node_role: AttemptNodeRoleV1::Step,
        state: ProcessGuardStateV1::Released,
        execution_host: attempt.owner.execution_host.clone(),
        process_group_id: 41,
        liveness: ProcessLivenessV1 {
            kind: ProcessLivenessKindV1::LeaderStartIdentity,
            value: "9001".to_owned(),
        },
    });
    attempt
}

#[test]
fn deterministic_recovery_fixtures_classify_exact_absent_and_lost_inspection() {
    let attempt = fixture_guarded_attempt();
    let host = attempt.owner.execution_host.clone();
    let guard_ids = vec!["11111111-1111-4111-8111-111111111111".to_owned()];

    assert_eq!(
        recovery_status_with(
            &attempt,
            false,
            &FixtureRecoveryAuthority {
                host: Ok(host.clone()),
                observation: ProcessIdentityObservation::Exact {
                    leader: crate::workflow::process_group::LeaderState::Running,
                },
            },
        ),
        LocalRecoveryStatus::Abandoned
    );
    assert_eq!(
        recovery_status_with(
            &attempt,
            false,
            &FixtureRecoveryAuthority {
                host: Ok(host.clone()),
                observation: ProcessIdentityObservation::Absent,
            },
        ),
        LocalRecoveryStatus::Abandoned
    );
    assert_eq!(
        recovery_status_with(
            &attempt,
            false,
            &FixtureRecoveryAuthority {
                host: Ok(host),
                observation: ProcessIdentityObservation::Unavailable,
            },
        ),
        LocalRecoveryStatus::OwnershipUnproven {
            guard_ids,
            reason: OwnershipUnprovenReason::ProcessIdentityInspectionUnavailable,
        }
    );
}

#[test]
fn host_restart_proves_old_work_absent_without_process_inspection() {
    let attempt = fixture_guarded_attempt();
    let restarted_host = ExecutionHostV1 {
        kind: ExecutionHostKindV1::HostBoot,
        value: "22222222-2222-4222-8222-222222222222".to_owned(),
    };

    assert_eq!(
        recovery_status_with(
            &attempt,
            false,
            &FixtureRecoveryAuthority {
                host: Ok(restarted_host),
                observation: ProcessIdentityObservation::Unavailable,
            },
        ),
        LocalRecoveryStatus::Abandoned
    );
    assert_eq!(
        recovery_status_with(
            &attempt,
            false,
            &FixtureRecoveryAuthority {
                host: Err(()),
                observation: ProcessIdentityObservation::Absent,
            },
        ),
        LocalRecoveryStatus::OwnershipUnproven {
            guard_ids: vec!["11111111-1111-4111-8111-111111111111".to_owned()],
            reason: OwnershipUnprovenReason::ExecutionHostIdentityUnavailable,
        }
    );
}

// INV-ENGINE-10
#[test]
fn attempt_directory_names_are_exact_at_the_six_digit_boundary() {
    assert_eq!(attempt_directory_name(0), None);
    assert_eq!(attempt_directory_name(1).as_deref(), Some("000001"));
    assert_eq!(attempt_directory_name(42).as_deref(), Some("000042"));
    assert_eq!(attempt_directory_name(999_999).as_deref(), Some("999999"));
    assert_eq!(
        attempt_directory_name(1_000_000).as_deref(),
        Some("1000000")
    );
    assert_eq!(
        attempt_directory_name(u64::MAX).as_deref(),
        Some("18446744073709551615")
    );
}

#[test]
fn retained_read_budget_enforces_the_component_derivation() {
    assert_eq!(
        MAXIMUM_RETAINED_TOTAL_BYTES,
        MAXIMUM_RETAINED_CAPTURED_FILE_BYTES + MAXIMUM_RETAINED_RUN_JSON_BYTES
    );

    let mut captured_file_bytes = MAXIMUM_RETAINED_CAPTURED_FILE_BYTES - 1;
    account_retained_bytes(
        &mut captured_file_bytes,
        1,
        MAXIMUM_RETAINED_CAPTURED_FILE_BYTES,
    )
    .unwrap();
    assert_eq!(
        account_retained_bytes(
            &mut captured_file_bytes,
            1,
            MAXIMUM_RETAINED_CAPTURED_FILE_BYTES,
        ),
        Err(LocalRunDirectoryError::StateInvalid)
    );

    let mut budget = RetainedReadBudget::with_bytes(MAXIMUM_RETAINED_TOTAL_BYTES - 1).unwrap();
    budget.account(&[0]).unwrap();
    assert_eq!(
        budget.account(&[0]),
        Err(LocalRunDirectoryError::StateInvalid)
    );
}

#[test]
fn retained_manifest_rejects_source_files_out_of_canonical_order() {
    let source_file = |path: &str, ordinal: u64| ManifestSourceFileV1 {
        path: path.to_owned(),
        file: ManifestFileV1 {
            ordinal,
            relative_file: format!("files/{ordinal:04}"),
            size_bytes: 0,
            digest: DigestV1::sha256(&[]),
        },
    };
    let manifest = WorkflowManifestV1 {
        schema_version: 1,
        workflow_path: "workflow.yaml".to_owned(),
        source_root: "/source".to_owned(),
        maximum_parallel_steps: 1,
        source_files: vec![
            source_file("workflow.yaml", 1),
            source_file("auxiliary.txt", 2),
        ],
        inputs: BTreeMap::new(),
        agent_profile_replacement: None,
        tree: None,
    };

    assert_eq!(
        validate_manifest(&manifest),
        Err(LocalRunDirectoryError::SerializationUnavailable)
    );
}

#[test]
fn retained_manifest_rejects_duplicate_named_input_keys() {
    let fixture = AdmittedFixture::new();
    let run_path = fixture.run_path("duplicate-input-key");
    let run = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    settle_as_workflow_failed(&run);
    drop(run);

    let manifest_path = run_path.join("workflow/manifest.json");
    let manifest_bytes = fs::read(&manifest_path).unwrap();
    let manifest: WorkflowManifestV1 = decode_schema_one(&manifest_bytes).unwrap();
    let request = serde_json::to_string(&manifest.inputs["request"]).unwrap();
    let original_entry = format!("\"request\":{request}");
    let mut duplicate_manifest = serde_json::to_string(&manifest).unwrap();
    assert_eq!(duplicate_manifest.matches(&original_entry).count(), 1);
    duplicate_manifest = duplicate_manifest.replacen(
        &original_entry,
        &format!("{original_entry},{original_entry}"),
        1,
    );
    duplicate_manifest.push('\n');
    fs::set_permissions(&manifest_path, Permissions::from_mode(0o600)).unwrap();
    fs::write(&manifest_path, duplicate_manifest.as_bytes()).unwrap();

    let run_file = run_path.join(LOCAL_RUN_FILE);
    let mut run_document: LocalRunV1 = decode_schema_one(&fs::read(&run_file).unwrap()).unwrap();
    run_document.workflow_manifest_digest = DigestV1::sha256(duplicate_manifest.as_bytes());
    fs::set_permissions(&run_file, Permissions::from_mode(0o600)).unwrap();
    fs::write(&run_file, encode_json(&run_document).unwrap()).unwrap();

    match acquire_local_retry(&run_path) {
        Err(LocalRunDirectoryError::StateInvalid) => {}
        Err(other) => panic!("duplicate named input produced the wrong failure: {other:?}"),
        Ok(_) => panic!("retry accepted a retained manifest with a duplicate named input key"),
    }
    assert!(!run_path.join("attempts/000002").exists());
}

// REQ-LOCALRUN-98
// INV-ENGINE-10
#[test]
fn initial_publication_retains_the_staging_lock_and_immutable_execution_bytes() {
    let fixture = AdmittedFixture::new();
    let run_path = fixture.run_path("retained");
    let mut observer = AssertLockedPublication { observed: false };

    let run = create_with_observer(&run_path, &fixture.admitted, None, &mut observer).unwrap();

    assert!(observer.observed);
    assert_eq!(run.run_directory(), fs::canonicalize(&run_path).unwrap());
    assert_eq!(
        fs::metadata(&run_path).unwrap().permissions().mode() & 0o7777,
        0o700
    );
    assert!(run_path.join(LOCAL_RUN_FILE).is_file());
    assert!(run_path.join(STATE_FILE).is_file());
    assert!(run_path.join(LOCK_FILE).is_file());
    assert!(run_path.join("attempts/000001").is_dir());
    assert!(!run.result_directory().exists());
    assert_eq!(
        directory_names(&run_path.join("workflow")),
        ["files", "manifest.json"]
    );

    let manifest: Value =
        serde_json::from_slice(&fs::read(run_path.join("workflow/manifest.json")).unwrap())
            .unwrap();
    assert_eq!(manifest["schemaVersion"], 1);
    assert!(manifest.get("tree").is_none());
    assert_eq!(manifest["workflowPath"], "workflow.yaml");
    assert_eq!(manifest["maximumParallelSteps"], 2);
    assert_eq!(manifest["sourceFiles"][0]["relativeFile"], "files/0001");
    assert_eq!(
        manifest["inputs"]["evidence"]["items"][0]["relativeFile"],
        "files/0002"
    );
    assert_eq!(manifest["inputs"]["request"]["relativeFile"], "files/0003");
    assert_eq!(manifest["inputs"]["settings"]["relativeFile"], "files/0004");
    assert_eq!(
        fs::read(run_path.join("workflow/files/0001")).unwrap(),
        fixture.admitted.workflow().source_closure["workflow.yaml"].as_ref()
    );
    assert_eq!(
        fs::read(run_path.join("workflow/files/0002")).unwrap(),
        [0_u8, 1, 0xff]
    );
    assert_eq!(
        fs::read(run_path.join("workflow/files/0003")).unwrap(),
        b"durable request\n"
    );
    assert_eq!(
        fs::read(run_path.join("workflow/files/0004")).unwrap(),
        b"{ \"z\": null, \"n\": 1.2300 }\n"
    );

    let state = read_state(run.root_handle()).unwrap();
    assert_eq!(state.revision, 1);
    assert_eq!(state.attempts[0].state, AttemptStateV1::Created);
    assert_eq!(state.attempts[0].progress.steps[0].id, "first");
    assert_eq!(state.attempts[0].progress.steps[1].id, "second");

    drop(run);
}

#[test]
fn singular_file_input_is_retained_and_reused_for_retry() {
    let fixture = AdmittedFixture::from_source_with_inputs(
        "schemaVersion: 1\ninputs:\n  payload: {kind: file, mediaType: application/octet-stream}\nsteps:\n  first:\n    kind: cmd\n    command: {argv: [\"true\"]}\n  second:\n    kind: cmd\n    dependsOn: [first]\n    command: {argv: [\"true\"]}\n",
        ResolvedInputs::new(BTreeMap::from([(
            "payload".to_owned(),
            ResolvedInput::File(ResolvedFile::new(
                Arc::from("application/octet-stream"),
                Arc::from([0_u8, 0xff, 7]),
            )),
        )])),
        1024,
    );
    let run_path = fixture.run_path("retained-file");
    let initial = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    let manifest: Value =
        serde_json::from_slice(&fs::read(run_path.join("workflow/manifest.json")).unwrap())
            .unwrap();
    assert_eq!(manifest["inputs"]["payload"]["kind"], "file");
    assert_eq!(
        manifest["inputs"]["payload"]["mediaType"],
        "application/octet-stream"
    );
    assert_eq!(manifest["inputs"]["payload"]["relativeFile"], "files/0002");
    assert_eq!(
        fs::read(run_path.join("workflow/files/0002")).unwrap(),
        [0_u8, 0xff, 7]
    );

    settle_as_workflow_failed(&initial);
    drop(initial);
    let LocalRetryOpen::Acquired(pending) = acquire_local_retry(&run_path).unwrap() else {
        panic!("failed File-input attempt should be retryable");
    };
    let Some(ResolvedInput::File(file)) = pending.execution_specification().1.get("payload") else {
        panic!("retained File input is missing");
    };
    assert_eq!(file.media_type(), "application/octet-stream");
    assert_eq!(file.bytes(), [0_u8, 0xff, 7]);
    assert!(pending.tree_copy().is_none());
    assert!(pending.verify_tree_copy().is_none());
}

fn directory_names(directory: &Path) -> Vec<String> {
    let mut names = fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect::<Vec<_>>();
    names.sort();
    names
}

fn mode(path: &Path) -> u32 {
    fs::symlink_metadata(path).unwrap().permissions().mode() & 0o7777
}

/// Restores write permission under a run's read-only `workflow/tree` before the
/// temporary directory holding it drops: `tempfile` ignores a removal failure and
/// would leave the copy behind. Declare it after the fixture so it drops first.
struct WritableTreeCopyOnDrop(PathBuf);

impl Drop for WritableTreeCopyOnDrop {
    fn drop(&mut self) {
        fn restore(directory: &Path) {
            if fs::set_permissions(directory, Permissions::from_mode(0o755)).is_err() {
                return;
            }
            for entry in fs::read_dir(directory).into_iter().flatten().flatten() {
                if entry.file_type().is_ok_and(|kind| kind.is_dir()) {
                    restore(&entry.path());
                }
            }
        }
        restore(&self.0.join("workflow/tree"));
        // A successor's imported attempt holds its predecessor's copy (REQ-LOCALRUN-195).
        for attempt in fs::read_dir(self.0.join(ATTEMPTS_DIRECTORY))
            .into_iter()
            .flatten()
            .flatten()
        {
            restore(&attempt.path().join("workflow/tree"));
        }
    }
}

const TREE_ROOT_WORKFLOW: &str = "schemaVersion: 1\nprograms: [bin]\nexclude: [\"tests/\"]\nsteps:\n  first:\n    kind: cmd\n    command:\n      argv: [\"hello\"]\n  second:\n    kind: cmd\n    dependsOn: [first]\n    command:\n      argv: [\"true\"]\n";
const TREE_PROGRAM: &str = "#!/bin/sh\necho hello\n";

fn tree_fixture() -> AdmittedFixture {
    AdmittedFixture::from_tree(
        TREE_ROOT_WORKFLOW,
        &[
            ("bin/hello", TREE_PROGRAM, 0o755),
            ("prompts/a.md", "A prompt.\n", 0o644),
            ("tests/t.py", "assert True\n", 0o644),
        ],
    )
}

// REQ-LOCALRUN-170, REQ-LOCALRUN-98
#[test]
fn a_tree_run_directory_pins_the_tree_read_only_beside_the_closure() {
    let fixture = tree_fixture();
    let run_path = fixture.run_path("tree");
    let _writable = WritableTreeCopyOnDrop(run_path.clone());

    let run = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    drop(run);

    let workflow = run_path.join("workflow");
    assert_eq!(
        directory_names(&workflow),
        ["files", "manifest.json", "tree"]
    );
    let tree = workflow.join("tree");
    assert_eq!(directory_names(&tree), ["bin", "prompts", "workflow.yaml"]);
    assert_eq!(
        fs::read(tree.join("bin/hello")).unwrap(),
        TREE_PROGRAM.as_bytes()
    );
    assert_eq!(mode(&tree.join("bin/hello")), 0o555);
    assert_eq!(fs::read(tree.join("prompts/a.md")).unwrap(), b"A prompt.\n");
    assert_eq!(mode(&tree.join("prompts/a.md")), 0o444);
    assert_eq!(
        fs::read(tree.join("workflow.yaml")).unwrap(),
        TREE_ROOT_WORKFLOW.as_bytes()
    );
    assert_eq!(mode(&tree.join("workflow.yaml")), 0o444);
    assert!(!tree.join("tests").exists());
    for directory in [&tree, &tree.join("bin"), &tree.join("prompts")] {
        assert_eq!(mode(directory), 0o555, "{}", directory.display());
    }
    // The closure is retained as before, beside the copy.
    assert_eq!(
        fs::read(workflow.join("files/0001")).unwrap(),
        TREE_ROOT_WORKFLOW.as_bytes()
    );

    let manifest: Value =
        serde_json::from_slice(&fs::read(workflow.join("manifest.json")).unwrap()).unwrap();
    let admitted_tree = fixture.admitted.tree().unwrap();
    assert_eq!(
        manifest["tree"],
        json!({
            "digest": {
                "algorithm": "sha256",
                "value": admitted_tree.digest().value,
            },
            "relativeDirectory": "tree",
            "programs": ["bin"],
            "requiredTools": [],
        })
    );
}

const EDGE_ROOT_WORKFLOW: &str = "schemaVersion: 1\nsteps:\n  design:\n    kind: workflow\n    workflow: design.yaml\n    on:\n      succeeded: build\n      failed: exit.failed\n  build:\n    kind: workflow\n    workflow: build.yaml\n    on:\n      succeeded: exit.succeeded\n      failed: exit.failed\nexports:\n  parts:\n    ref: outputs.design.parts\n";
const EDGE_CHILD_WORKFLOW: &str = "schemaVersion: 1\nsteps:\n  work:\n    kind: cmd\n    command: {argv: [\"true\"]}\n    outputs:\n      parts: {kind: text, from: path, path: parts.txt}\nexports:\n  parts:\n    ref: outputs.work.parts\n";

fn retained_text(node: &str, output: &str, bytes: &[u8]) -> RetainedOutputV1 {
    RetainedOutputV1::Text {
        name: output.to_owned(),
        producer: None,
        carrier: RetainedCarrierV1 {
            relative_path: retained_value_relative_path(AttemptNodeRoleV1::Step, node, output),
            media_type: "text/plain; charset=utf-8".to_owned(),
            size_bytes: u64::try_from(bytes.len()).unwrap(),
            digest: DigestV1::sha256(bytes),
        },
    }
}

impl DurableDeadline for crate::workflow::test_support::step_clock::TestInstant {
    fn deadline_utc(&self) -> OffsetDateTime {
        OffsetDateTime::UNIX_EPOCH + self.0
    }
}

/// A child dispatcher with a stop control in force for every step (REQ-WFEXEC-62).
#[derive(Clone)]
struct StoppingChildDispatcher;

impl crate::workflow::step_runtime::ChildRunDispatcher for StoppingChildDispatcher {
    async fn run(
        &self,
        _request: crate::workflow::step_runtime::ChildRunRequest,
        _control: crate::workflow::step_runtime::ChildRunControl,
    ) -> crate::workflow::step_runtime::ChildRunOutcome {
        crate::workflow::step_runtime::ChildRunOutcome::Interrupted
    }

    async fn stop_before(&self, _step: &str) -> bool {
        true
    }
}

fn edge_tree_fixture() -> AdmittedFixture {
    AdmittedFixture::from_tree(
        EDGE_ROOT_WORKFLOW,
        &[
            ("design.yaml", EDGE_CHILD_WORKFLOW, 0o644),
            ("build.yaml", EDGE_CHILD_WORKFLOW, 0o644),
        ],
    )
}

/// Executes `run`'s attempt with a stop in force before every step, so it ends on the
/// `stop` wait entered before `design` (REQ-WFEXEC-62).
async fn execute_to_a_stop_wait(
    fixture: &AdmittedFixture,
    run: &InitialLocalRun,
) -> crate::workflow::runtime::EnteredWait {
    let artifacts =
        ArtifactStaging::create(fixture.admitted.execution(), run.private_directory()).unwrap();
    let inputs = crate::workflow::input::InputStaging::create(
        fixture.admitted.execution(),
        run.private_directory(),
    )
    .unwrap();
    let diagnostics = crate::workflow::diagnostic::StepDiagnosticLog::default();
    let result = crate::workflow::execution::execute_workflow_with_children(
        fixture.admitted.clone(),
        &artifacts,
        &inputs,
        &diagnostics,
        crate::workflow::step_runtime::AgentExecution::disabled(),
        crate::workflow::step_runtime::ChildRuns::Enabled {
            dispatcher: StoppingChildDispatcher,
            controls: crate::workflow::step_runtime::ControlContext::default(),
        },
        crate::workflow::test_support::step_clock::TestClock,
        run.commit_port(
            diagnostics.clone(),
            InvocationAccountingLog::default(),
            artifacts.clone(),
        ),
        crate::workflow::observation::NoopExecutionObserver,
        crate::workflow::step_runtime::WorkflowExecutionStart::initial(
            run.process_guard_registry(),
        ),
    )
    .await
    .unwrap();
    match result.outcome {
        crate::workflow::runtime::RunOutcome::Waiting { wait } => wait,
        other => panic!("the attempt did not end on the stop wait: {other:?}"),
    }
}

// REQ-WFEXEC-61, INV-ENGINE-20
#[tokio::test]
async fn a_committed_wait_settles_the_attempt_waiting_with_a_settlement_snapshot() {
    let fixture = edge_tree_fixture();
    let run_path = fixture.run_path("waiting");
    let _writable = WritableTreeCopyOnDrop(run_path.clone());
    let run = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    execute_to_a_stop_wait(&fixture, &run).await;

    let attempt = read_state(run.root_handle()).unwrap().attempts.remove(0);
    assert_eq!(attempt.state, AttemptStateV1::Waiting);
    assert!(attempt.state.is_terminal());
    assert!(attempt.settled_at.is_some());
    assert_eq!(
        attempt.settlement_snapshot.unwrap().settled_by,
        Some(WorkspaceSnapshotSettlementV1::Engine)
    );
    assert!(attempt.finalization.is_none());
    assert!(attempt.progress.steps.iter().all(|step| {
        step.state == AttemptStepStateV1::NotRun
            && step.detail
                == Some(NodeDetail::NotRun(
                    crate::workflow::evidence::NonExecutionDetail::not_reached(),
                ))
    }));
}

// REQ-WFEXEC-58
#[test]
fn an_inherited_workflow_step_retains_the_outcome_a_continuation_routes_from() {
    let fixture = AdmittedFixture::from_tree(
        EDGE_ROOT_WORKFLOW,
        &[
            ("design.yaml", EDGE_CHILD_WORKFLOW, 0o644),
            ("build.yaml", EDGE_CHILD_WORKFLOW, 0o644),
        ],
    );
    let run_path = fixture.run_path("edge");
    let _writable = WritableTreeCopyOnDrop(run_path.clone());
    let run = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    let mut attempt = read_state(run.root_handle()).unwrap().attempts.remove(0);
    drop(run);
    let workflow = fixture.admitted.workflow();
    let design = attempt
        .progress
        .steps
        .iter()
        .position(|step| step.id == "design")
        .unwrap();

    // A succeeded `workflow` step commits its outcome and only the child exports that were
    // available; one without its outcome is not a committed child result.
    attempt.progress.steps[design].state = AttemptStepStateV1::Succeeded;
    attempt.progress.steps[design].outputs =
        Some(vec![retained_text("design", "outcome", b"succeeded")]);
    assert!(validate_retained_outputs_against_definition(&attempt, workflow).is_ok());
    attempt.progress.steps[design].outputs = Some(vec![retained_text("design", "parts", b"parts")]);
    assert!(validate_retained_outputs_against_definition(&attempt, workflow).is_err());

    // Inherited, its outcome is loaded beside the exported output whatever the re-executed
    // steps read; a step that inherited a skip retains no outcome to load.
    attempt.progress.steps[design].state = AttemptStepStateV1::Inherited;
    attempt.progress.steps[design].outputs =
        Some(vec![retained_text("design", "outcome", b"succeeded")]);
    assert_eq!(
        required_inherited_outputs(&attempt, workflow).unwrap(),
        BTreeSet::from([
            ("design".to_owned(), "outcome".to_owned()),
            ("design".to_owned(), "parts".to_owned()),
        ])
    );
    attempt.progress.steps[design].outputs = Some(Vec::new());
    assert_eq!(
        required_inherited_outputs(&attempt, workflow).unwrap(),
        BTreeSet::from([("design".to_owned(), "parts".to_owned())])
    );
}

fn run_record_validator() -> jsonschema::Validator {
    let path =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../schemas/workflow-run-v1.schema.json");
    let schema = serde_json::from_slice::<Value>(&fs::read(path).unwrap()).unwrap();
    jsonschema::draft202012::new(&schema).unwrap()
}

fn tree_record(fixture: &AdmittedFixture, definition: Option<(String, u64)>) -> WorkflowRunRecord {
    let digest = format!("sha256:{}", fixture.admitted.tree().unwrap().digest().value);
    WorkflowRunRecord::new(definition, &digest, "workflow.yaml", "tester")
}

fn assert_layout_refused(run_path: &Path) {
    match verify_existing_run_layout(&open_directory_path(run_path).unwrap()) {
        Err(LocalRunDirectoryError::StateFile { source, .. })
            if matches!(*source, LocalRunDirectoryError::StateInvalid) => {}
        other => panic!("the layout check accepted or misreported the directory: {other:?}"),
    }
}

// REQ-LOCALRUN-175, REQ-LOCALRUN-98, REQ-WFDEF-4
#[test]
fn a_tree_run_created_with_a_record_holds_run_json_beside_local_run_json() {
    let fixture = tree_fixture();
    let validator = run_record_validator();
    let definition = "def_01jcf8y6m3q7v2k9x4t5r8w0zp";
    for (name, admitted_definition) in [
        ("definition-run", Some((definition.to_owned(), 3))),
        ("path-run", None),
    ] {
        let run_path = fixture.run_path(name);
        let _writable = WritableTreeCopyOnDrop(run_path.clone());
        let record = tree_record(&fixture, admitted_definition.clone());
        let run = InitialLocalRun::create_with_record(&run_path, &fixture.admitted, Some(&record))
            .unwrap();
        drop(run);

        assert_eq!(
            directory_names(&run_path),
            [
                ".private",
                "attempts",
                "local-run.json",
                "run.json",
                "run.lock",
                "state.json",
                "workflow"
            ]
        );
        assert_eq!(mode(&run_path.join(RUN_RECORD_FILE)), 0o600);
        let bytes = fs::read(run_path.join(RUN_RECORD_FILE)).unwrap();
        assert_eq!(bytes.last(), Some(&b'\n'));
        let document = serde_json::from_slice::<Value>(&bytes).unwrap();
        assert!(
            validator.is_valid(&document),
            "{name}: {:?}",
            validator.iter_errors(&document).collect::<Vec<_>>()
        );
        let parsed = serde_json::from_slice::<WorkflowRunRecord>(&bytes).unwrap();
        assert_eq!(parsed, record);
        assert!(parsed.is_valid());
        let (definition_value, revision_value) = match &admitted_definition {
            Some((id, revision)) => (json!(id), json!(revision)),
            None => (Value::Null, Value::Null),
        };
        assert_eq!(
            document,
            json!({
                "schemaVersion": 1,
                "id": record.id,
                "definition": definition_value,
                "revision": revision_value,
                "tree": format!("sha256:{}", fixture.admitted.tree().unwrap().digest().value),
                "workflow": "workflow.yaml",
                "subject": null,
                "parent": null,
                "owner": "tester",
                "state": "running",
                "position": [],
                "outcome": null,
                "startedAt": record.started_at,
                "attempts": 1,
                "predecessor": null,
                "successor": null,
                "external": {},
                "edges": {},
            })
        );
        verify_existing_run_layout(&open_directory_path(&run_path).unwrap()).unwrap();
    }
}

const CHILD_PARENT_WORKFLOW: &str = "schemaVersion: 1\nprograms: [bin]\nsteps:\n  design:\n    kind: workflow\n    workflow: design.yaml\n    on:\n      succeeded: exit.succeeded\n      failed: exit.failed\n";
const CHILD_DESIGN_WORKFLOW: &str = "schemaVersion: 1\nsteps:\n  first:\n    kind: cmd\n    command: {argv: [\"hello\"]}\n  second:\n    kind: cmd\n    dependsOn: [first]\n    command: {argv: [\"true\"]}\n";

/// Every file under `root` by relative path, with its bytes and mode.
fn tree_files(root: &Path) -> BTreeMap<String, (Vec<u8>, u32)> {
    fn walk(root: &Path, directory: &Path, files: &mut BTreeMap<String, (Vec<u8>, u32)>) {
        for entry in fs::read_dir(directory).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                walk(root, &path, files);
            } else {
                let relative = path
                    .strip_prefix(root)
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .to_owned();
                files.insert(relative, (fs::read(&path).unwrap(), mode(&path)));
            }
        }
    }
    let mut files = BTreeMap::new();
    walk(root, root, &mut files);
    files
}

fn manifest_tree(run_path: &Path) -> Value {
    serde_json::from_slice::<Value>(&fs::read(run_path.join("workflow/manifest.json")).unwrap())
        .unwrap()["tree"]
        .clone()
}

// REQ-LOCALRUN-177, REQ-LOCALRUN-98, REQ-LOCALRUN-101, REQ-WFDEF-4
// INV-ENGINE-19
#[test]
fn a_child_run_nests_under_its_parent_with_the_next_ordinal_and_the_parents_tree() {
    let fixture = AdmittedFixture::from_tree(
        CHILD_PARENT_WORKFLOW,
        &[
            ("bin/hello", TREE_PROGRAM, 0o755),
            ("design.yaml", CHILD_DESIGN_WORKFLOW, 0o644),
            ("prompts/a.md", "A prompt.\n", 0o644),
        ],
    );
    let validator = run_record_validator();
    let parent_path = fixture.run_path("parent");
    let _writable_parent = WritableTreeCopyOnDrop(parent_path.clone());
    let parent_record = tree_record(
        &fixture,
        Some(("def_01jcf8y6m3q7v2k9x4t5r8w0zp".to_owned(), 3)),
    );
    let parent =
        InitialLocalRun::create_with_record(&parent_path, &fixture.admitted, Some(&parent_record))
            .unwrap();
    assert_eq!(next_child_ordinal(&parent, "design").unwrap(), 1);
    let parent_copy = parent.tree_copy_directory();
    let parent_files = tree_files(&parent_copy);

    let mut children = Vec::new();
    for ordinal in ["000001", "000002"] {
        let admitted = fixture.admit_child(&parent_copy, "design.yaml");
        let record = WorkflowRunRecord::child_of(&parent_record, "design", "design.yaml", None);
        let child = create_child(&parent, "design", None, &admitted, &record).unwrap();
        let child_path = parent_path.join("children/design").join(ordinal);
        assert_eq!(child.run_directory(), child_path.canonicalize().unwrap());
        children.push((
            WritableTreeCopyOnDrop(child_path.clone()),
            child_path,
            child,
        ));
    }
    assert_eq!(directory_names(&parent_path.join("children")), ["design"]);
    assert_eq!(
        directory_names(&parent_path.join("children/design")),
        ["000001", "000002"]
    );
    assert_eq!(mode(&parent_path.join("children")), 0o700);
    assert_eq!(mode(&parent_path.join("children/design")), 0o700);
    assert_eq!(next_child_ordinal(&parent, "design").unwrap(), 3);
    assert_eq!(next_child_ordinal(&parent, "build").unwrap(), 1);

    let mut child_ids = BTreeSet::new();
    for (_, child_path, _) in &children {
        verify_existing_run_layout(&open_directory_path(child_path).unwrap()).unwrap();
        let child_copy = child_path.join("workflow/tree");
        assert_eq!(tree_files(&child_copy), parent_files);
        assert_eq!(manifest_tree(child_path), manifest_tree(&parent_path));

        let document =
            serde_json::from_slice::<Value>(&fs::read(child_path.join(RUN_RECORD_FILE)).unwrap())
                .unwrap();
        assert!(
            validator.is_valid(&document),
            "{:?}",
            validator.iter_errors(&document).collect::<Vec<_>>()
        );
        assert_eq!(
            document["parent"],
            json!({"run": parent_record.id, "step": "design"})
        );
        for field in ["definition", "revision", "tree", "owner"] {
            assert_eq!(
                document[field],
                serde_json::to_value(&parent_record).unwrap()[field],
                "{field}"
            );
        }
        assert_eq!(document["workflow"], "design.yaml");
        assert_eq!(document["state"], "running");
        let id = document["id"].as_str().unwrap().to_owned();
        assert_ne!(id, parent_record.id);
        assert!(child_ids.insert(id));
    }

    // The parent admits `children/` beside its run record, and only as a directory.
    assert_eq!(
        directory_names(&parent_path),
        [
            ".private",
            "attempts",
            "children",
            "local-run.json",
            "run.json",
            "run.lock",
            "state.json",
            "workflow"
        ]
    );
    verify_existing_run_layout(&open_directory_path(&parent_path).unwrap()).unwrap();
    fs::write(parent_path.join("children.txt"), b"stray").unwrap();
    assert_layout_refused(&parent_path);
    fs::remove_file(parent_path.join("children.txt")).unwrap();
    fs::rename(parent_path.join("children"), parent_path.join("moved")).unwrap();
    fs::write(parent_path.join("children"), b"not a directory").unwrap();
    assert_layout_refused(&parent_path);
    fs::remove_file(parent_path.join("children")).unwrap();
    fs::rename(parent_path.join("moved"), parent_path.join("children")).unwrap();

    // A child is a run by its own path: one that failed is retried from its retained copy
    // while its parent never settled.
    let (_writable_child, child_path, child) = children.remove(0);
    settle_as_workflow_failed(&child);
    drop(child);
    drop(parent);
    let LocalRetryOpen::Acquired(pending) = acquire_local_retry(&child_path).unwrap() else {
        panic!("a failed child run should be retryable by its own path");
    };
    assert_eq!(
        pending.verify_tree_copy().unwrap().unwrap().digest.value,
        fixture.admitted.tree().unwrap().digest().value
    );
}

// REQ-LOCALRUN-177
#[test]
fn a_child_run_whose_record_does_not_descend_from_the_parent_is_refused() {
    let fixture = AdmittedFixture::from_tree(
        CHILD_PARENT_WORKFLOW,
        &[
            ("bin/hello", TREE_PROGRAM, 0o755),
            ("design.yaml", CHILD_DESIGN_WORKFLOW, 0o644),
        ],
    );
    let parent_path = fixture.run_path("parent");
    let _writable_parent = WritableTreeCopyOnDrop(parent_path.clone());
    let parent_record = tree_record(&fixture, None);
    let parent =
        InitialLocalRun::create_with_record(&parent_path, &fixture.admitted, Some(&parent_record))
            .unwrap();
    let admitted = fixture.admit_child(&parent.tree_copy_directory(), "design.yaml");
    let child_of = WorkflowRunRecord::child_of(&parent_record, "design", "design.yaml", None);
    let unrelated =
        WorkflowRunRecord::child_of(&tree_record(&fixture, None), "design", "design.yaml", None);
    let mut other_owner = child_of.clone();
    other_owner.owner = "someone-else".to_owned();
    let mut other_tree = child_of.clone();
    other_tree.tree = format!("sha256:{}", "0".repeat(64));
    for (step, record) in [
        ("build", &child_of),
        ("design", &unrelated),
        ("design", &other_owner),
        ("design", &other_tree),
        ("../design", &child_of),
    ] {
        assert!(
            matches!(
                create_child(&parent, step, None, &admitted, record),
                Err(LocalRunDirectoryError::StateInvalid | LocalRunDirectoryError::InvalidPath)
            ),
            "{step}: {record:?}"
        );
    }
    assert!(!parent_path.join("children/design/000001").exists());
}

// REQ-LOCALRUN-177, REQ-WFDEF-4, REQ-WFEXEC-66, REQ-WFEXEC-68
// INV-ENGINE-19, INV-ENGINE-23
#[test]
fn a_fan_out_child_nests_under_its_element_and_the_parent_position_names_it() {
    let fixture = AdmittedFixture::from_tree(
        CHILD_PARENT_WORKFLOW,
        &[
            ("bin/hello", TREE_PROGRAM, 0o755),
            ("build.yaml", CHILD_DESIGN_WORKFLOW, 0o644),
            ("design.yaml", CHILD_DESIGN_WORKFLOW, 0o644),
        ],
    );
    let validator = run_record_validator();
    let parent_path = fixture.run_path("parent");
    let _writable_parent = WritableTreeCopyOnDrop(parent_path.clone());
    let parent_record = tree_record(&fixture, Some((HOME_DEFINITION.to_owned(), 1)));
    let parent =
        InitialLocalRun::create_with_record(&parent_path, &fixture.admitted, Some(&parent_record))
            .unwrap();
    let parent_copy = parent.tree_copy_directory();
    let position = || {
        serde_json::from_slice::<Value>(&fs::read(parent_path.join(RUN_RECORD_FILE)).unwrap())
            .unwrap()["position"]
            .clone()
    };
    assert_eq!(position(), json!([]));

    // A record and a directory disagree about the element: refused before anything is written.
    let admitted = fixture.admit_child(&parent_copy, "build.yaml");
    for (item, record_item) in [(Some(2), None), (None, Some(2)), (Some(2), Some(3))] {
        let record =
            WorkflowRunRecord::child_of(&parent_record, "build", "build.yaml", record_item);
        assert!(
            matches!(
                create_child(&parent, "build", item, &admitted, &record),
                Err(LocalRunDirectoryError::StateInvalid)
            ),
            "{item:?} {record_item:?}"
        );
    }
    assert!(!parent_path.join("children/build").exists());
    assert_eq!(position(), json!([]));

    let mut created = Vec::new();
    for (step, workflow, item, directory, expected_position) in [
        (
            "build",
            "build.yaml",
            Some(3),
            "children/build/000003/000001",
            json!(["build", {"item": 3}]),
        ),
        (
            "build",
            "build.yaml",
            Some(2),
            "children/build/000002/000001",
            json!(["build", {"item": 2}]),
        ),
        (
            "build",
            "build.yaml",
            Some(2),
            "children/build/000002/000002",
            json!(["build", {"item": 2}]),
        ),
        (
            "design",
            "design.yaml",
            None,
            "children/design/000001",
            json!(["design"]),
        ),
    ] {
        let admitted = fixture.admit_child(&parent_copy, workflow);
        let record = WorkflowRunRecord::child_of(&parent_record, step, workflow, item);
        let child = create_child(&parent, step, item, &admitted, &record).unwrap();
        let child_path = parent_path.join(directory);
        assert_eq!(child.run_directory(), child_path.canonicalize().unwrap());
        // The parent's position names the element; it is written before the child exists.
        assert_eq!(position(), expected_position, "{directory}");
        let document =
            serde_json::from_slice::<Value>(&fs::read(child_path.join(RUN_RECORD_FILE)).unwrap())
                .unwrap();
        assert!(
            validator.is_valid(&document),
            "{:?}",
            validator.iter_errors(&document).collect::<Vec<_>>()
        );
        let mut parent_field = json!({"run": parent_record.id, "step": step});
        if let Some(item) = item {
            parent_field["item"] = json!(item);
        }
        assert_eq!(document["parent"], parent_field);
        created.push((
            WritableTreeCopyOnDrop(child_path.clone()),
            (step.to_owned(), item, child_path, record.id.clone()),
            child,
        ));
        if step == "build" && item == Some(2) {
            // The element the position names, not the highest element, holds the latest child.
            let (latest, state, latest_item) =
                latest_child_run(&parent_path, "build").unwrap().unwrap();
            assert_eq!(latest, parent_path.join(directory).canonicalize().unwrap());
            assert_eq!((state, latest_item), (RunState::Running, Some(2)));
        }
    }
    assert_eq!(
        directory_names(&parent_path.join("children/build")),
        ["000002", "000003"]
    );
    assert_eq!(mode(&parent_path.join("children/build/000002")), 0o700);
    assert_eq!(mode(&parent_path.join("children/build/000003")), 0o700);

    // Without a position naming the step, the highest element's latest child is the latest.
    let (latest, _, latest_item) = latest_child_run(&parent_path, "build").unwrap().unwrap();
    assert_eq!(
        (latest, latest_item),
        (
            parent_path
                .join("children/build/000003/000001")
                .canonicalize()
                .unwrap(),
            Some(3)
        )
    );
    let (latest, _, latest_item) = latest_child_run(&parent_path, "design").unwrap().unwrap();
    assert_eq!(
        (latest, latest_item),
        (
            parent_path
                .join("children/design/000001")
                .canonicalize()
                .unwrap(),
            None
        )
    );
    assert_eq!(latest_child_run(&parent_path, "review").unwrap(), None);

    // `child_runs` lists step, then element, then ordinal.
    let listed = child_runs(&parent_path)
        .unwrap()
        .into_iter()
        .map(|child| {
            (
                child.step,
                child.item,
                child.ordinal,
                child.directory,
                child.record.id,
            )
        })
        .collect::<Vec<_>>();
    let mut expected = created
        .iter()
        .map(|(_, (step, item, path, id), _)| {
            let ordinal = path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap()
                .parse::<u64>()
                .unwrap();
            (
                step.clone(),
                *item,
                ordinal,
                path.canonicalize().unwrap(),
                id.clone(),
            )
        })
        .collect::<Vec<_>>();
    expected.sort_by(|left, right| (&left.0, left.1, left.2).cmp(&(&right.0, right.1, right.2)));
    assert_eq!(listed, expected);

    // The position is kept while the run lives and cleared once it exits.
    parent
        .update_run_record(RunState::Waiting, 1, None)
        .unwrap();
    assert_eq!(position(), json!(["design"]));
    parent
        .update_run_record(RunState::Exited, 1, Some("succeeded"))
        .unwrap();
    assert_eq!(position(), json!([]));

    // A position element or parent item below 1, or a non-identifier step, is not a record.
    for position in [
        json!([{"item": 0}]),
        json!(["build", {"item": 0}]),
        json!(["build", {"item": 2, "index": 1}]),
        json!(["Build"]),
        json!([7]),
    ] {
        let mut record = parent_record.clone();
        record.position = position.as_array().unwrap().clone();
        assert!(!record.is_valid(), "{position}");
    }
    let mut record = parent_record.clone();
    record.position = vec![json!("build"), json!({"item": 2})];
    assert!(record.is_valid());
    let mut child = WorkflowRunRecord::child_of(&parent_record, "build", "build.yaml", Some(1));
    assert!(child.is_valid());
    child.parent.as_mut().unwrap().item = Some(0);
    assert!(!child.is_valid());
}

/// A tree run with a run record whose first attempt ended on the `stop` wait before
/// `design`, left as the CLI leaves it: the wait record written, then `run.json` reading
/// `waiting` (REQ-LOCALRUN-178, INV-ENGINE-20).
async fn waiting_tree_run(
    fixture: &AdmittedFixture,
    name: &str,
) -> (PathBuf, WaitRecord, WritableTreeCopyOnDrop) {
    waiting_tree_run_on(fixture, name, |stop| stop).await
}

/// `waiting_tree_run` with the recorded wait `record` makes of the stop wait's record.
async fn waiting_tree_run_on(
    fixture: &AdmittedFixture,
    name: &str,
    record_of: impl FnOnce(WaitRecord) -> WaitRecord,
) -> (PathBuf, WaitRecord, WritableTreeCopyOnDrop) {
    let run_path = fixture.run_path(name);
    let writable = WritableTreeCopyOnDrop(run_path.clone());
    let record = tree_record(fixture, None);
    let run =
        InitialLocalRun::create_with_record(&run_path, &fixture.admitted, Some(&record)).unwrap();
    let wait = execute_to_a_stop_wait(fixture, &run).await;
    let payload = json!({"by": "saeed", "payload": {"before": "design"}});
    let wait = record_of(
        WaitRecord::entered(
            &wait,
            None,
            payload.as_object().unwrap().clone(),
            um_support::utc_now(),
            &ResolvedInputs::default(),
            &BTreeMap::new(),
        )
        .unwrap(),
    );
    run.record_wait(&wait).unwrap();
    run.update_run_record(RunState::Waiting, 1, None).unwrap();
    drop(run);
    (run_path, wait, writable)
}

/// A condition source that answers every observation with `answer` and counts them.
struct ScriptedConditions {
    answer:
        Result<Vec<crate::workflow::wait::ExternalEvent>, crate::workflow::wait::ConditionError>,
    observed: std::cell::Cell<usize>,
}

impl ScriptedConditions {
    fn answering(
        answer: Result<
            Vec<crate::workflow::wait::ExternalEvent>,
            crate::workflow::wait::ConditionError,
        >,
    ) -> Self {
        Self {
            answer,
            observed: std::cell::Cell::new(0),
        }
    }
}

impl crate::workflow::wait::ConditionSource for ScriptedConditions {
    fn supports(&self, _source: &str, _event: &str) -> bool {
        true
    }

    fn observe(
        &self,
        _wait: &WaitRecord,
    ) -> Result<Vec<crate::workflow::wait::ExternalEvent>, crate::workflow::wait::ConditionError>
    {
        self.observed.set(self.observed.get() + 1);
        self.answer.clone()
    }
}

const QUEUED_PULL_REQUEST: &str = "https://github.com/useful-machinery/platform/pull/3958";

fn queue_settled(state: &str) -> crate::workflow::wait::ExternalEvent {
    crate::workflow::wait::ExternalEvent {
        source: "github".to_owned(),
        event: "pull_request.queue_settled".to_owned(),
        at: "2026-11-07T22:30:00Z".to_owned(),
        payload: json!({"pullRequest": QUEUED_PULL_REQUEST, "state": state, "headSha": "4f2a"})
            .as_object()
            .unwrap()
            .clone(),
    }
}

// REQ-CONTINUE-74, REQ-WFEXEC-65: a waiting run's `external` wait is resolved by the events
// its condition source observes, never by the controls; a condition the source cannot read
// is reported as such, and a local wait never asks the source.
#[tokio::test]
async fn inspect_wait_asks_the_condition_source_for_an_external_wait_alone() {
    let fixture = edge_tree_fixture();
    let (run_path, wait, _writable) =
        waiting_tree_run_on(&fixture, "external", |stop| WaitRecord {
            kind: crate::workflow::wait::WaitKind::External,
            rule: crate::workflow::wait::WaitRule {
                source: "github".to_owned(),
                event: vec!["pull_request.queue_settled".to_owned()],
                where_: format!(
                    "event.pullRequest == \"{QUEUED_PULL_REQUEST}\" and event.state == \"merged\""
                ),
            },
            ..stop
        })
        .await;
    let inspect = |conditions: &ScriptedConditions| {
        let inspection = inspect_wait(&run_path, conditions).unwrap().inspection;
        assert_eq!(conditions.observed.get(), 1, "{inspection:?}");
        inspection
    };
    assert_eq!(
        inspect(&ScriptedConditions::answering(Ok(Vec::new()))),
        WaitInspection::Held { wait: wait.clone() }
    );
    assert_eq!(
        inspect(&ScriptedConditions::answering(Ok(vec![queue_settled(
            "dequeued"
        )]))),
        WaitInspection::Held { wait: wait.clone() }
    );
    let merged = queue_settled("merged");
    assert_eq!(
        inspect(&ScriptedConditions::answering(Ok(vec![
            queue_settled("dequeued"),
            merged.clone(),
        ]))),
        WaitInspection::Satisfied {
            wait: wait.clone(),
            by: ResolvedBy::External(merged),
        }
    );
    let error = crate::workflow::wait::ConditionError::unavailable("GitHub answered HTTP 500");
    assert_eq!(
        inspect(&ScriptedConditions::answering(Err(error.clone()))),
        WaitInspection::ConditionUnavailable { wait, error }
    );

    // An `answer` wait on source `um` reads the controls and never the condition source.
    let (run_path, wait, _writable) = waiting_tree_run_on(&fixture, "answer", |stop| WaitRecord {
        kind: crate::workflow::wait::WaitKind::Answer,
        rule: crate::workflow::wait::derive_rule(crate::workflow::wait::WaitKind::Answer, false)
            .unwrap(),
        ..stop
    })
    .await;
    let conditions = ScriptedConditions::answering(Ok(vec![queue_settled("merged")]));
    assert_eq!(
        inspect_wait(&run_path, &conditions).unwrap().inspection,
        WaitInspection::Held { wait }
    );
    assert_eq!(conditions.observed.get(), 0);
}

fn run_record_document(run_path: &Path) -> Value {
    let document =
        serde_json::from_slice::<Value>(&fs::read(run_path.join(RUN_RECORD_FILE)).unwrap())
            .unwrap();
    let validator = run_record_validator();
    assert!(validator.is_valid(&document), "{document}");
    document
}

// REQ-LOCALRUN-178, REQ-LOCALRUN-175, REQ-LOCALRUN-98, INV-ENGINE-20
#[tokio::test]
async fn a_waiting_run_holds_its_wait_record_until_a_continuation_begins() {
    let fixture = edge_tree_fixture();
    let (run_path, wait, _writable) = waiting_tree_run(&fixture, "waiting-record").await;
    let root_record = run_path.join(WAIT_RECORD_FILE);
    let attempt_record = run_path.join("attempts/000001").join(WAIT_RECORD_FILE);
    let bytes = fs::read(&root_record).unwrap();
    assert_eq!(bytes, fs::read(&attempt_record).unwrap());
    assert_eq!(serde_json::from_slice::<WaitRecord>(&bytes).unwrap(), wait);
    for path in [&root_record, &attempt_record] {
        assert_eq!(mode(path) & 0o077, 0, "{}", path.display());
    }
    assert!(
        !run_path
            .join(PRIVATE_DIRECTORY)
            .join(WAIT_RECORD_TEMPORARY_FILE)
            .exists()
    );
    let record = run_record_document(&run_path);
    assert_eq!(
        (&record["state"], &record["outcome"], &record["attempts"]),
        (&json!("waiting"), &Value::Null, &json!(1))
    );

    // The wait record and the controls beside it are part of the run directory.
    let lift = crate::workflow::wait::ControlEvent {
        source: "um".to_owned(),
        event: "control.lift".to_owned(),
        run: record["id"].as_str().unwrap().to_owned(),
        by: "saeed".to_owned(),
        at: wait.since.clone(),
        wait: None,
        payload: json!({"before": "design"}).as_object().cloned(),
    };
    crate::workflow::wait::append_control(&run_path, &lift).unwrap();
    verify_existing_run_layout(&open_directory_path(&run_path).unwrap()).unwrap();

    let LocalContinuationOpen::Acquired(pending) = acquire_local_continuation(&run_path).unwrap()
    else {
        panic!("a waiting run should be continuable");
    };
    assert_eq!(pending.wait().unwrap(), Some(wait));
    let owned = pending
        .begin(
            &fixture.admitted,
            vec!["design".to_owned()],
            ContinuationDefinitionMode::Inherited,
            crate::workflow::publication::ContinuationReasonV1::Resume,
            None,
        )
        .unwrap();
    assert!(!root_record.exists());
    assert_eq!(fs::read(&attempt_record).unwrap(), bytes);
    drop(owned);

    // `wait.json` is admitted only as a regular file.
    fs::create_dir(&root_record).unwrap();
    assert_layout_refused(&run_path);
}

// REQ-LOCALRUN-197, REQ-LOCALRUN-195: under the lock, `resolving_control` finds the control
// that resolves the run's wait, recorded after the run was inspected and the lock taken, and
// not one that names another run, as a copied control would on a successor.
#[tokio::test]
async fn a_pending_continuation_reads_the_control_that_resolves_its_wait_under_the_lock() {
    let fixture = edge_tree_fixture();
    let stopped_at = "2026-11-06T16:01:17Z";
    let (run_path, wait, _writable) =
        waiting_tree_run_on(&fixture, "resolving-control", |mut stop| {
            stop.payload.insert("at".to_owned(), json!(stopped_at));
            stop
        })
        .await;
    let record = run_record_document(&run_path);
    let lift = |run: &str| crate::workflow::wait::ControlEvent {
        source: "um".to_owned(),
        event: crate::workflow::wait::CONTROL_LIFT.to_owned(),
        run: run.to_owned(),
        by: "saeed".to_owned(),
        at: "2026-11-06T17:00:00Z".to_owned(),
        wait: Some(wait.id()),
        payload: json!({"before": "design", "stop": stopped_at})
            .as_object()
            .cloned(),
    };
    let LocalContinuationOpen::Acquired(pending) = acquire_local_continuation(&run_path).unwrap()
    else {
        panic!("a waiting run should be continuable");
    };
    assert_eq!(pending.resolving_control(&wait).unwrap(), None);

    crate::workflow::wait::append_control(&run_path, &lift("run_01j0000000000000000000zzzz"))
        .unwrap();
    assert_eq!(pending.resolving_control(&wait).unwrap(), None);

    let own = lift(record["id"].as_str().unwrap());
    crate::workflow::wait::append_control(&run_path, &own).unwrap();
    assert_eq!(pending.resolving_control(&wait).unwrap(), Some(own));
    assert_eq!(pending.wait().unwrap(), Some(wait));
    pending.settle_abandoned().unwrap();
}

// REQ-WFDEF-4, REQ-LOCALRUN-175, REQ-CONTINUE-77, REQ-WFEXEC-64: an `edge` continuation
// records the edge it took, and a counted one increments its count in `run.json` in the write
// that bumps `attempts`.
#[tokio::test]
async fn an_edge_continuation_records_the_edge_and_counts_a_counted_one() {
    let fixture = edge_tree_fixture();
    let (run_path, _, _writable) = waiting_tree_run(&fixture, "edge-counts").await;
    let edge = |counted| crate::workflow::publication::ContinuationEdgeV1 {
        step: "build".to_owned(),
        outcome: "hand_back".to_owned(),
        target: "design".to_owned(),
        counted,
    };
    let begin = |counted| {
        let LocalContinuationOpen::Acquired(pending) =
            acquire_local_continuation(&run_path).unwrap()
        else {
            panic!("the run should be continuable");
        };
        drop(
            pending
                .begin(
                    &fixture.admitted,
                    vec!["design".to_owned()],
                    ContinuationDefinitionMode::Inherited,
                    crate::workflow::publication::ContinuationReasonV1::Edge,
                    Some(edge(counted)),
                )
                .unwrap(),
        );
        let record = run_record_document(&run_path);
        (record["edges"].clone(), record["attempts"].clone())
    };
    assert_eq!(begin(true), (json!({"build.hand_back": 1}), json!(2)));
    assert_eq!(begin(true), (json!({"build.hand_back": 2}), json!(3)));
    // An uncounted edge, a backward one, leaves the count; `attempts` follows the caller.
    assert_eq!(begin(false), (json!({"build.hand_back": 2}), json!(3)));

    let state = read_state(&open_directory_path(&run_path).unwrap()).unwrap();
    for (attempt, counted) in state.attempts[1..].iter().zip([true, true, false]) {
        let continuation = attempt.continuation.as_ref().unwrap();
        assert_eq!(
            continuation.reason,
            Some(crate::workflow::publication::ContinuationReasonV1::Edge)
        );
        assert_eq!(continuation.edge, Some(edge(counted)));
        let document = serde_json::to_value(continuation).unwrap();
        assert_eq!(document["reason"], json!("edge"));
        assert_eq!(
            document["edge"],
            json!({"step": "build", "outcome": "hand_back", "target": "design", "counted": counted})
        );
    }
}

// REQ-LOCALRUN-178, REQ-LOCALRUN-175, REQ-CONTINUE-75
#[tokio::test]
async fn ending_a_waiting_run_exits_it_without_another_attempt() {
    type End = fn(PendingLocalContinuation) -> Result<(), LocalRunDirectoryError>;
    let withdraw: End = PendingLocalContinuation::withdraw;
    let cancel: End = |pending| pending.end_waiting(CANCELED_OUTCOME);
    let fixture = edge_tree_fixture();
    for (name, end, outcome) in [
        ("withdrawn", withdraw, "withdrawn"),
        ("canceled", cancel, "canceled"),
    ] {
        let (run_path, wait, _writable) = waiting_tree_run(&fixture, name).await;
        let state = fs::read(run_path.join(STATE_FILE)).unwrap();
        let waiting = run_record_document(&run_path);

        let LocalContinuationOpen::Acquired(pending) =
            acquire_local_continuation(&run_path).unwrap()
        else {
            panic!("a waiting run should be continuable");
        };
        end(*pending).unwrap();

        let mut ended = waiting;
        ended["state"] = json!("exited");
        ended["outcome"] = json!(outcome);
        assert_eq!(run_record_document(&run_path), ended, "{name}");
        assert!(!run_path.join(WAIT_RECORD_FILE).exists());
        assert_eq!(
            serde_json::from_slice::<WaitRecord>(
                &fs::read(run_path.join("attempts/000001").join(WAIT_RECORD_FILE)).unwrap()
            )
            .unwrap(),
            wait
        );
        assert_eq!(
            directory_names(&run_path.join(ATTEMPTS_DIRECTORY)),
            ["000001"]
        );
        assert_eq!(fs::read(run_path.join(STATE_FILE)).unwrap(), state);

        // A run that no longer waits cannot be ended again.
        let LocalContinuationOpen::Acquired(pending) =
            acquire_local_continuation(&run_path).unwrap()
        else {
            panic!("the lock was not released");
        };
        assert_eq!(end(*pending), Err(LocalRunDirectoryError::StateConflict));
    }
}

// REQ-LOCALRUN-178
#[tokio::test]
async fn a_retry_of_a_waiting_run_removes_the_root_wait_record() {
    let fixture = edge_tree_fixture();
    let (run_path, _, _writable) = waiting_tree_run(&fixture, "retried").await;
    let LocalRetryOpen::Acquired(pending) = acquire_local_retry(&run_path).unwrap() else {
        panic!("a waiting run should be retryable");
    };
    let Ok(owned) = pending.begin(&fixture.admitted) else {
        panic!("the retry of a waiting run did not begin");
    };
    assert!(!run_path.join(WAIT_RECORD_FILE).exists());
    assert!(
        run_path
            .join("attempts/000001")
            .join(WAIT_RECORD_FILE)
            .exists()
    );
    drop(owned);
}

/// `TREE_ROOT_WORKFLOW` with `second` changed: a later revision of the same definition.
const REVISED_TREE_ROOT_WORKFLOW: &str = "schemaVersion: 1\nprograms: [bin]\nexclude: [\"tests/\"]\nsteps:\n  first:\n    kind: cmd\n    command:\n      argv: [\"hello\"]\n  second:\n    kind: cmd\n    dependsOn: [first]\n    command:\n      argv: [\"false\"]\n";
/// `TREE_ROOT_WORKFLOW` with `second` changed another way: a replacement or a stray revision.
const REPLACED_TREE_ROOT_WORKFLOW: &str = "schemaVersion: 1\nprograms: [bin]\nexclude: [\"tests/\"]\nsteps:\n  first:\n    kind: cmd\n    command:\n      argv: [\"hello\"]\n  second:\n    kind: cmd\n    dependsOn: [first]\n    command:\n      argv: [\"hello\"]\n";

fn revised_tree_fixture() -> AdmittedFixture {
    AdmittedFixture::from_tree(
        REVISED_TREE_ROOT_WORKFLOW,
        &[
            ("bin/hello", TREE_PROGRAM, 0o755),
            ("prompts/a.md", "A prompt.\n", 0o644),
        ],
    )
}

/// `fixture`'s root workflow rewritten to `source` in its own source root and admitted again
/// against its execution root, as a replacement read from beside the original is.
fn readmitted(fixture: &AdmittedFixture, source: &str) -> AdmittedWorkflow {
    let source_root = fixture.admitted.workflow().source.source_root.clone();
    fs::write(source_root.join("workflow.yaml"), source).unwrap();
    let tree = crate::workflow::validate_workflow_tree(&source_root).unwrap();
    let workflow = resolution::resolve(&source_root, Path::new("workflow.yaml")).unwrap();
    admit_workflow(
        workflow,
        ResolvedInputs::default(),
        fixture_context(
            &fixture.execution_root,
            1024,
            EnvironmentSnapshot::default(),
        )
        .with_tree(tree),
    )
    .unwrap()
}

/// Settles `owner`'s attempt `waiting` on the stop before `second` (REQ-WFEXEC-61) with
/// `first` succeeded unless the attempt inherited it, then records the wait and sets
/// `run.json` `waiting`, as the CLI leaves a run whose attempt ended on a wait
/// (INV-ENGINE-20).
fn settle_waiting_before_second(owner: &LocalAttemptOwner) {
    settle_waiting_before_second_on(owner, &stop_wait_record());
}

/// Settles the owner's attempt waiting before `second` on `wait`, `first` succeeded.
fn settle_waiting_before_second_on(owner: &LocalAttemptOwner, wait: &WaitRecord) {
    owner
        .state
        .update(|state| {
            let attempt = current_attempt_mut(state)?;
            let settled = attempt.created_at.clone();
            attempt.started_at = Some(settled.clone());
            attempt.settled_at = Some(settled);
            attempt.settlement_snapshot = Some(fixture_settlement_snapshot());
            attempt.state = AttemptStateV1::Waiting;
            for step in &mut attempt.progress.steps {
                match (step.id.as_str(), step.state) {
                    (_, AttemptStepStateV1::Inherited) => {}
                    ("first", _) => step.state = AttemptStepStateV1::Succeeded,
                    _ => {
                        step.state = AttemptStepStateV1::NotRun;
                        step.detail = Some(NodeDetail::NotRun(
                            crate::workflow::evidence::NonExecutionDetail::not_reached(),
                        ));
                    }
                }
            }
            attempt.result = AttemptResultV1::NotPublished {
                reason: ResultAbsentReasonV1::PublicationPending,
            };
            Ok(())
        })
        .unwrap();
    owner.record_wait(wait).unwrap();
    owner
        .update_run_record(RunState::Waiting, owner.attempt_number(), None)
        .unwrap();
}

/// A definition run of `fixture` for the subject `AUTO-1` whose first attempt entered an
/// `answer` wait before `second` that a recorded `control.answer` with `answers` resolved
/// (REQ-LOCALRUN-182), and that `second` is waiting at.
fn answered_predecessor(
    fixture: &AdmittedFixture,
    name: &str,
) -> (PathBuf, WorkflowRunRecord, WritableTreeCopyOnDrop) {
    let wait = answer_wait_record();
    let (run_path, record, writable) = predecessor_waiting_on(fixture, name, &wait);
    let answer = crate::workflow::wait::ControlEvent {
        source: "um".to_owned(),
        event: crate::workflow::wait::CONTROL_ANSWER.to_owned(),
        run: record.id.clone(),
        by: "saeed".to_owned(),
        at: "2026-11-06T16:31:00Z".to_owned(),
        wait: Some(wait.id()),
        payload:
            json!({"question": "1", "text": "keep", "complete": true, "answers": {"1": "keep"}})
                .as_object()
                .cloned(),
    };
    crate::workflow::wait::append_control(&run_path, &answer).unwrap();
    (run_path, record, writable)
}

/// A definition run of `fixture` for the subject `AUTO-1` whose first attempt waits before
/// `second` on `wait` with `first` succeeded, holding no control yet.
fn predecessor_waiting_on(
    fixture: &AdmittedFixture,
    name: &str,
    wait: &WaitRecord,
) -> (PathBuf, WorkflowRunRecord, WritableTreeCopyOnDrop) {
    let run_path = fixture.run_path(name);
    let writable = WritableTreeCopyOnDrop(run_path.clone());
    let mut record = tree_record(fixture, Some((HOME_DEFINITION.to_owned(), 1)));
    record.subject = Some("AUTO-1".to_owned());
    let run =
        InitialLocalRun::create_with_record(&run_path, &fixture.admitted, Some(&record)).unwrap();
    settle_waiting_before_second_on(&run, wait);
    drop(run);
    (run_path, record, writable)
}

/// A definition run of `fixture` for the subject `AUTO-1` whose first attempt waits before
/// `second` with `first` succeeded, holding one stop control (REQ-LOCALRUN-185).
fn waiting_predecessor(
    fixture: &AdmittedFixture,
    name: &str,
) -> (PathBuf, WorkflowRunRecord, WritableTreeCopyOnDrop) {
    let (run_path, record, writable) = predecessor_waiting_on(fixture, name, &stop_wait_record());
    let stop = crate::workflow::wait::ControlEvent {
        source: "um".to_owned(),
        event: "control.stop".to_owned(),
        run: record.id.clone(),
        by: "saeed".to_owned(),
        at: "2026-11-06T16:01:17Z".to_owned(),
        wait: None,
        payload: json!({"before": "second"}).as_object().cloned(),
    };
    crate::workflow::wait::append_control(&run_path, &stop).unwrap();
    (run_path, record, writable)
}

/// The record `restart` writes for a successor of `predecessor` on `revised`'s revision 2.
fn successor_record(
    revised: &AdmittedFixture,
    predecessor: &WorkflowRunRecord,
    attempts: u64,
) -> WorkflowRunRecord {
    let mut record = tree_record(revised, Some((HOME_DEFINITION.to_owned(), 2)));
    record.subject.clone_from(&predecessor.subject);
    record.predecessor = Some(predecessor.id.clone());
    record.attempts = attempts;
    record
}

fn acquired(run_path: &Path) -> Box<PendingLocalContinuation> {
    match acquire_local_continuation(run_path).unwrap() {
        LocalContinuationOpen::Acquired(pending) => pending,
        LocalContinuationOpen::Rejected(rejection) => {
            panic!("the run should be continuable: {:?}", rejection.reason())
        }
    }
}

fn file_contents(directory: &Path) -> BTreeMap<String, Vec<u8>> {
    directory_names(directory)
        .into_iter()
        .map(|name| {
            let bytes = fs::read(directory.join(&name)).unwrap();
            (name, bytes)
        })
        .collect()
}

// REQ-LOCALRUN-195, INV-ENGINE-26
#[test]
fn a_successor_imports_the_predecessors_attempts_and_relocates_their_definitions() {
    let fixture = tree_fixture();
    let revised = revised_tree_fixture();
    assert_ne!(
        revised.admitted.workflow().content_digest,
        fixture.admitted.workflow().content_digest
    );
    let (predecessor_path, predecessor_record, _predecessor_writable) =
        waiting_predecessor(&fixture, "predecessor");
    let predecessor_root = open_directory_path(&predecessor_path).unwrap();
    let predecessor_run = read_run(&predecessor_root).unwrap();
    let predecessor_state = read_state(&predecessor_root).unwrap();
    let predecessor_bytes = |name: &str| fs::read(predecessor_path.join(name)).unwrap();
    let unchanged = [STATE_FILE, RUN_RECORD_FILE, WAIT_RECORD_FILE].map(predecessor_bytes);

    let successor_path = fixture.run_path("successor");
    let _successor_writable = WritableTreeCopyOnDrop(successor_path.clone());
    let record = successor_record(&revised, &predecessor_record, 1);
    let pending = acquired(&predecessor_path);
    let created = InitialLocalRun::create_successor(
        &successor_path,
        &revised.admitted,
        &record,
        &pending,
        SuccessorCarry::Continue,
    )
    .unwrap();
    assert_eq!(created, fs::canonicalize(&successor_path).unwrap());
    drop(pending);

    // The predecessor is untouched until it is superseded.
    assert_eq!(
        [STATE_FILE, RUN_RECORD_FILE, WAIT_RECORD_FILE].map(predecessor_bytes),
        unchanged
    );

    assert_eq!(
        directory_names(&successor_path),
        [
            ".private",
            "attempts",
            "controls",
            "local-run.json",
            "run.json",
            "run.lock",
            "state.json",
            "workflow"
        ]
    );
    let root = open_directory_path(&successor_path).unwrap();
    let run = read_run(&root).unwrap();
    assert_eq!(run.git_baseline, predecessor_run.git_baseline);
    assert_eq!(
        run.workflow_digest.value,
        revised.admitted.workflow().content_digest.value
    );
    assert_ne!(run.local_run_id, predecessor_run.local_run_id);
    assert_eq!(read_run_record(&root).unwrap(), record);

    // Each attempt is the predecessor's, marked imported, the one that ran under the
    // predecessor's definition now locating it under its own directory.
    let state = read_state(&root).unwrap();
    assert_eq!(state.local_run_id, run.local_run_id);
    assert_eq!(state.revision, 1);
    assert_eq!(
        state.current_attempt_number,
        predecessor_state.current_attempt_number
    );
    let mut expected = predecessor_state.attempts.clone();
    expected[0].imported = Some(ImportedAttemptV1 {
        run: predecessor_record.id.clone(),
    });
    expected[0].definition = Some(AttemptDefinitionV1 {
        digest: predecessor_run.workflow_digest.clone(),
        manifest_digest: predecessor_run.workflow_manifest_digest.clone(),
        locator: AttemptDefinitionLocatorV1::Attempt { attempt_number: 1 },
    });
    assert_eq!(state.attempts, expected);
    assert_eq!(state.diagnostics, predecessor_state.diagnostics);
    let document =
        serde_json::from_slice::<Value>(&fs::read(successor_path.join(STATE_FILE)).unwrap())
            .unwrap();
    assert_eq!(
        document["attempts"][0]["imported"],
        json!({"run": predecessor_record.id})
    );
    assert_eq!(
        document["attempts"][0]["definition"]["locator"],
        json!({"kind": "attempt", "attemptNumber": 1})
    );

    // The attempt directories are the predecessor's, with its definition under the first.
    let attempt = successor_path.join("attempts/000001");
    assert_eq!(
        fs::read(attempt.join("workflow/manifest.json")).unwrap(),
        predecessor_bytes("workflow/manifest.json")
    );
    assert_eq!(
        tree_files(&attempt.join("workflow/tree")),
        tree_files(&predecessor_path.join("workflow/tree"))
    );
    assert_eq!(
        file_contents(&attempt.join("workflow/files")),
        file_contents(&predecessor_path.join("workflow/files"))
    );
    assert_eq!(
        fs::read(attempt.join(WAIT_RECORD_FILE)).unwrap(),
        predecessor_bytes("attempts/000001/wait.json")
    );
    assert_eq!(
        mode(&attempt.join(WAIT_RECORD_FILE)),
        mode(&predecessor_path.join("attempts/000001/wait.json"))
    );
    // The controls are copied verbatim; the wait is not.
    assert_eq!(
        file_contents(&successor_path.join(CONTROLS_DIRECTORY)),
        file_contents(&predecessor_path.join(CONTROLS_DIRECTORY))
    );
    verify_existing_run_layout(&root).unwrap();

    // The imported attempt passes the run-and-state check its re-located definition would
    // otherwise fail, and names the predecessor; the predecessor's own attempt does not.
    let pending = acquired(&successor_path);
    assert_eq!(
        pending.imported_prior(),
        Some(predecessor_record.id.as_str())
    );
    assert_eq!(pending.prior_attempt_number(), 1);
    assert_eq!(pending.wait().unwrap(), None);
    assert_eq!(
        pending.previous_definition().content_digest,
        fixture.admitted.workflow().content_digest
    );
    drop(pending);
    assert_eq!(acquired(&predecessor_path).imported_prior(), None);
}

// REQ-WFEXEC-63, REQ-LOCALRUN-195: the control recorded on the predecessor that resolved an
// imported attempt's wait still resolves its copy on the successor, so the successor's
// `controls.answers` carries the predecessor's answers; a second import keeps the mark
// naming the run the attempt was recorded under, so a chain of successors keeps finding them.
#[test]
fn a_successors_imported_wait_copy_resolves_by_the_control_recorded_under_its_run() {
    let fixture = tree_fixture();
    let revised = revised_tree_fixture();
    let (predecessor_path, predecessor_record, _predecessor_writable) =
        answered_predecessor(&fixture, "predecessor");
    let answers = json!({"1": "keep"});
    let predecessor_controls = attempt_controls(&predecessor_path, 2).unwrap().controls;
    assert_eq!(
        predecessor_controls
            .answers
            .get("second")
            .map(|value| (**value).clone()),
        Some(answers.clone())
    );

    let successor_path = fixture.run_path("successor");
    let _successor_writable = WritableTreeCopyOnDrop(successor_path.clone());
    let record = successor_record(&revised, &predecessor_record, 1);
    let pending = acquired(&predecessor_path);
    InitialLocalRun::create_successor(
        &successor_path,
        &revised.admitted,
        &record,
        &pending,
        SuccessorCarry::Continue,
    )
    .unwrap();
    drop(pending);

    // The copied control names the predecessor and resolves the imported attempt's copy.
    let controls = attempt_controls(&successor_path, 2).unwrap().controls;
    assert_eq!(
        controls
            .answers
            .get("second")
            .map(|value| (**value).clone()),
        Some(answers)
    );
    assert_eq!(
        controls.wait.as_ref().map(|(step, _)| step.as_str()),
        Some("second")
    );

    // Imported again, the attempt keeps the mark of the run it was recorded under.
    let root = open_directory_path(&successor_path).unwrap();
    let run = read_run(&root).unwrap();
    let state = read_state(&root).unwrap();
    let (relocated, holder) = relocate_run_definitions(&run, &state, &record.id).unwrap();
    assert_eq!(holder, None);
    assert_eq!(
        relocated[0].imported,
        Some(ImportedAttemptV1 {
            run: predecessor_record.id.clone(),
        })
    );
    assert_eq!(relocated[0].definition, state.attempts[0].definition);
}

// REQ-WFEXEC-63, REQ-LOCALRUN-195, REQ-LOCALRUN-197: a wait carried by `upgrade` is resolved
// by a control naming the successor that carried it, not the run the attempt was recorded
// under; the next successor along the chain imports that control and the attempt's copy
// together and still carries the answers.
#[test]
fn a_chains_successor_carries_the_answer_recorded_on_the_run_that_carried_the_wait() {
    let fixture = tree_fixture();
    let revised = revised_tree_fixture();
    let wait = answer_wait_record();
    let (origin_path, _, _origin_writable) = predecessor_waiting_on(&fixture, "origin", &wait);
    let origin_record = read_run_record(&open_directory_path(&origin_path).unwrap()).unwrap();

    // The wait is carried to the first successor and answered there, under its id.
    let carrier_path = fixture.run_path("carrier");
    let _carrier_writable = WritableTreeCopyOnDrop(carrier_path.clone());
    let mut carrier_record = successor_record(&revised, &origin_record, 1);
    carrier_record.state = RunState::Waiting;
    carrier_record.position.clone_from(&origin_record.position);
    let pending = acquired(&origin_path);
    InitialLocalRun::create_successor(
        &carrier_path,
        &revised.admitted,
        &carrier_record,
        &pending,
        SuccessorCarry::Wait,
    )
    .unwrap();
    pending.supersede(&carrier_record.id).unwrap();
    let answers = json!({"1": "keep"});
    let answer = crate::workflow::wait::ControlEvent {
        source: "um".to_owned(),
        event: crate::workflow::wait::CONTROL_ANSWER.to_owned(),
        run: carrier_record.id.clone(),
        by: "saeed".to_owned(),
        at: "2026-11-06T17:00:00Z".to_owned(),
        wait: Some(wait.id()),
        payload: json!({"question": "1", "text": "keep", "complete": true, "answers": answers})
            .as_object()
            .cloned(),
    };
    crate::workflow::wait::append_control(&carrier_path, &answer).unwrap();
    let carried = |run_path: &Path| {
        let controls = attempt_controls(run_path, 2).unwrap().controls;
        (
            controls
                .answers
                .get("second")
                .map(|value| (**value).clone()),
            controls.wait.map(|(step, _)| step),
        )
    };
    let expected = (Some(answers), Some("second".to_owned()));
    assert_eq!(carried(&carrier_path), expected);

    // The next successor imports the attempt, marked with the run it was recorded under,
    // and the copied answer, which still resolves the attempt's wait copy.
    let successor_path = fixture.run_path("successor");
    let _successor_writable = WritableTreeCopyOnDrop(successor_path.clone());
    let record = successor_record(&revised, &carrier_record, 1);
    let pending = acquired(&carrier_path);
    InitialLocalRun::create_successor(
        &successor_path,
        &revised.admitted,
        &record,
        &pending,
        SuccessorCarry::Continue,
    )
    .unwrap();
    drop(pending);
    let state = read_state(&open_directory_path(&successor_path).unwrap()).unwrap();
    assert_eq!(
        state.attempts[0].imported,
        Some(ImportedAttemptV1 {
            run: origin_record.id.clone(),
        })
    );
    assert_eq!(carried(&successor_path), expected);
}

// REQ-CONTINUE-79, REQ-CONTINUE-77, INV-ENGINE-26
#[test]
fn a_successor_begins_a_run_definition_continuation_after_its_predecessor_is_deleted() {
    let fixture = tree_fixture();
    let revised = revised_tree_fixture();
    let (predecessor_path, predecessor_record, predecessor_writable) =
        waiting_predecessor(&fixture, "predecessor");
    let predecessor_run = read_run(&open_directory_path(&predecessor_path).unwrap()).unwrap();
    let predecessor_attempt = read_state(&open_directory_path(&predecessor_path).unwrap())
        .unwrap()
        .attempts
        .remove(0);
    let successor_path = fixture.run_path("successor");
    let _successor_writable = WritableTreeCopyOnDrop(successor_path.clone());
    let record = successor_record(&revised, &predecessor_record, 1);
    let pending = acquired(&predecessor_path);
    InitialLocalRun::create_successor(
        &successor_path,
        &revised.admitted,
        &record,
        &pending,
        SuccessorCarry::Continue,
    )
    .unwrap();

    // Superseding exits the predecessor and names the successor, and nothing else.
    let waiting = run_record_document(&predecessor_path);
    let state = fs::read(predecessor_path.join(STATE_FILE)).unwrap();
    let controls = file_contents(&predecessor_path.join(CONTROLS_DIRECTORY));
    pending.supersede(&record.id).unwrap();
    let mut superseded = waiting;
    superseded["state"] = json!("exited");
    superseded["outcome"] = json!(SUPERSEDED_OUTCOME);
    superseded["successor"] = json!(record.id);
    assert_eq!(run_record_document(&predecessor_path), superseded);
    assert_eq!(superseded["outcome"], json!("superseded"));
    assert_eq!(superseded["attempts"], json!(1));
    assert!(!predecessor_path.join(WAIT_RECORD_FILE).exists());
    assert_eq!(fs::read(predecessor_path.join(STATE_FILE)).unwrap(), state);
    assert_eq!(
        file_contents(&predecessor_path.join(CONTROLS_DIRECTORY)),
        controls
    );
    assert_eq!(
        directory_names(&predecessor_path.join(ATTEMPTS_DIRECTORY)),
        ["000001"]
    );
    assert!(!predecessor_path.join(MIRROR_DIRECTORY).exists());
    // A superseded run is refused every continuation, naming its successor, so it is never
    // superseded again (REQ-CONTINUE-29, REQ-LOCALRUN-196).
    match acquire_local_continuation(&predecessor_path).unwrap() {
        LocalContinuationOpen::Rejected(rejection) => {
            assert_eq!(rejection.reason(), RetryIneligibilityReason::RunSuperseded);
            assert_eq!(rejection.successor(), Some(record.id.as_str()));
        }
        LocalContinuationOpen::Acquired(_) => panic!("a superseded run was acquired"),
    }

    // The successor stands alone once the predecessor is gone.
    drop(predecessor_writable);
    fs::remove_dir_all(&predecessor_path).unwrap();
    let owned = acquired(&successor_path)
        .begin(
            &revised.admitted,
            vec!["second".to_owned()],
            ContinuationDefinitionMode::Run,
            crate::workflow::publication::ContinuationReasonV1::Restart,
            None,
        )
        .unwrap();
    assert_eq!(owned.attempt_number(), 2);
    let root = open_directory_path(&successor_path).unwrap();
    let run = read_run(&root).unwrap();
    let state = read_state(&root).unwrap();
    let attempt = &state.attempts[1];
    assert_eq!(attempt.imported, None);
    assert_eq!(attempt.definition, Some(attempt_definition_for_run(&run)));
    assert!(!successor_path.join("attempts/000002/workflow").exists());
    let continuation = attempt.continuation.as_ref().unwrap();
    assert_eq!(continuation.reexecuted_steps, ["second"]);
    let document = serde_json::to_value(continuation).unwrap();
    assert_eq!(document["reason"], json!("restart"));
    assert_eq!(document["request"]["definition"], json!("inherited"));
    assert_eq!(
        document["definitionSource"],
        json!({
            "kind": "replaced",
            "manifestDigest": run.workflow_manifest_digest,
            "priorManifestDigest": predecessor_run.workflow_manifest_digest,
        })
    );
    assert_eq!(
        document["inheritedSteps"],
        json!([{"id": "first", "priorState": "succeeded", "definitionChanged": false}])
    );
    // The inherited step is read from the imported attempt.
    let first = attempt
        .progress
        .steps
        .iter()
        .find(|step| step.id == "first")
        .unwrap();
    assert_eq!(first.state, AttemptStepStateV1::Inherited);
    let Some(NodeDetail::Inherited(detail)) = &first.detail else {
        panic!("the inherited step has no inherited detail");
    };
    assert_eq!(
        (
            detail.prior_attempt_number,
            detail.prior_attempt_id.as_str()
        ),
        (1, predecessor_attempt.attempt_id.as_str())
    );
    drop(owned);
}

// REQ-CONTINUE-79
#[test]
fn a_successor_begun_by_resume_records_reason_restart() {
    let fixture = tree_fixture();
    let revised = revised_tree_fixture();
    let (predecessor_path, predecessor_record, _predecessor_writable) =
        waiting_predecessor(&fixture, "predecessor");
    let successor_path = fixture.run_path("successor");
    let _successor_writable = WritableTreeCopyOnDrop(successor_path.clone());
    let record = successor_record(&revised, &predecessor_record, 1);
    let pending = acquired(&predecessor_path);
    InitialLocalRun::create_successor(
        &successor_path,
        &revised.admitted,
        &record,
        &pending,
        SuccessorCarry::Continue,
    )
    .unwrap();
    pending.supersede(&record.id).unwrap();

    // A restart interrupted before its first own attempt is finished by `resume --from`.
    let owned = acquired(&successor_path)
        .begin(
            &revised.admitted,
            vec!["second".to_owned()],
            ContinuationDefinitionMode::Run,
            crate::workflow::publication::ContinuationReasonV1::Resume,
            None,
        )
        .unwrap();
    assert_eq!(owned.attempt_number(), 2);
    drop(owned);
    let state = read_state(&open_directory_path(&successor_path).unwrap()).unwrap();
    assert_eq!(state.attempts.len(), 2);
    let document = serde_json::to_value(state.attempts[1].continuation.as_ref().unwrap()).unwrap();
    assert_eq!(document["reason"], json!("restart"));
    assert_eq!(document["request"]["definition"], json!("inherited"));
    assert_eq!(document["definitionSource"]["kind"], json!("replaced"));
}

// REQ-CONTINUE-79, REQ-LOCALRUN-195
#[test]
fn a_run_definition_continuation_refuses_another_definition_and_a_replaced_attempt_keeps_its_own() {
    let fixture = tree_fixture();
    let revised = revised_tree_fixture();
    let (predecessor_path, predecessor_record, _predecessor_writable) =
        waiting_predecessor(&fixture, "predecessor");
    // The predecessor's second attempt ran under a replacement.
    let replacement = readmitted(&fixture, REPLACED_TREE_ROOT_WORKFLOW);
    let owned = acquired(&predecessor_path)
        .begin(
            &replacement,
            vec!["second".to_owned()],
            ContinuationDefinitionMode::Replaced,
            crate::workflow::publication::ContinuationReasonV1::Resume,
            None,
        )
        .unwrap();
    settle_waiting_before_second(&owned);
    drop(owned);
    let predecessor_root = open_directory_path(&predecessor_path).unwrap();
    let predecessor_run = read_run(&predecessor_root).unwrap();
    let predecessor_state = read_state(&predecessor_root).unwrap();
    let replaced = predecessor_state.attempts[1].definition.clone().unwrap();
    assert_eq!(
        replaced.locator,
        AttemptDefinitionLocatorV1::Attempt { attempt_number: 2 }
    );

    let successor_path = fixture.run_path("successor");
    let _successor_writable = WritableTreeCopyOnDrop(successor_path.clone());
    let record = successor_record(&revised, &predecessor_record, 2);
    let pending = acquired(&predecessor_path);
    InitialLocalRun::create_successor(
        &successor_path,
        &revised.admitted,
        &record,
        &pending,
        SuccessorCarry::Continue,
    )
    .unwrap();
    drop(pending);
    let state = read_state(&open_directory_path(&successor_path).unwrap()).unwrap();
    assert_eq!(
        state
            .attempts
            .iter()
            .map(|attempt| attempt.definition.clone().unwrap())
            .collect::<Vec<_>>(),
        [
            AttemptDefinitionV1 {
                digest: predecessor_run.workflow_digest.clone(),
                manifest_digest: predecessor_run.workflow_manifest_digest.clone(),
                locator: AttemptDefinitionLocatorV1::Attempt { attempt_number: 1 },
            },
            replaced.clone(),
        ]
    );
    assert!(state.attempts.iter().all(|attempt| attempt.imported
        == Some(ImportedAttemptV1 {
            run: predecessor_record.id.clone(),
        })));
    for (successor, predecessor) in [
        ("attempts/000001/workflow", "workflow"),
        ("attempts/000002/workflow", "attempts/000002/workflow"),
    ] {
        assert_eq!(
            fs::read(successor_path.join(successor).join(WORKFLOW_MANIFEST_FILE)).unwrap(),
            fs::read(
                predecessor_path
                    .join(predecessor)
                    .join(WORKFLOW_MANIFEST_FILE)
            )
            .unwrap(),
            "{successor}"
        );
    }

    // The run-definition mode admits only the successor's own definition.
    let stray = readmitted(&revised, REPLACED_TREE_ROOT_WORKFLOW);
    assert_ne!(
        stray.workflow().content_digest,
        revised.admitted.workflow().content_digest
    );
    assert!(matches!(
        acquired(&successor_path).begin(
            &stray,
            vec!["second".to_owned()],
            ContinuationDefinitionMode::Run,
            crate::workflow::publication::ContinuationReasonV1::Restart,
            None,
        ),
        Err(LocalRunDirectoryError::StateConflict)
    ));
    assert!(!successor_path.join("attempts/000003").exists());

    // Its prior definition is the replacement the last imported attempt ran under.
    let owned = acquired(&successor_path)
        .begin(
            &revised.admitted,
            vec!["second".to_owned()],
            ContinuationDefinitionMode::Run,
            crate::workflow::publication::ContinuationReasonV1::Restart,
            None,
        )
        .unwrap();
    let state = read_state(&open_directory_path(&successor_path).unwrap()).unwrap();
    let document = serde_json::to_value(state.attempts[2].continuation.as_ref().unwrap()).unwrap();
    assert_eq!(
        document["definitionSource"]["priorManifestDigest"],
        json!(replaced.manifest_digest)
    );
    assert_eq!(document["definitionSource"]["kind"], json!("replaced"));
    drop(owned);
}

/// The predecessor `waiting_predecessor` builds, its record rewritten as a run that took the
/// counted edge `first.failed` once and waits at `second` would read.
fn waiting_predecessor_with_position_and_edges(
    fixture: &AdmittedFixture,
    name: &str,
) -> (PathBuf, WorkflowRunRecord, WritableTreeCopyOnDrop) {
    let (run_path, _, writable) = waiting_predecessor(fixture, name);
    let pending = acquired(&run_path);
    rewrite_run_record(&pending.root, |record| {
        record.position = vec![json!("second")];
        record.edges = BTreeMap::from([("first.failed".to_owned(), 1)]);
        Ok(())
    })
    .unwrap();
    drop(pending);
    let record = read_run_record(&open_directory_path(&run_path).unwrap()).unwrap();
    (run_path, record, writable)
}

// REQ-LOCALRUN-195, REQ-CONTINUE-80, INV-ENGINE-26
#[test]
fn an_upgraded_successor_carries_the_wait_position_and_edges() {
    let fixture = tree_fixture();
    let revised = revised_tree_fixture();
    let (predecessor_path, predecessor_record, _predecessor_writable) =
        waiting_predecessor_with_position_and_edges(&fixture, "predecessor");
    let predecessor_wait = fs::read(predecessor_path.join(WAIT_RECORD_FILE)).unwrap();
    let predecessor_run = read_run(&open_directory_path(&predecessor_path).unwrap()).unwrap();

    let successor_path = fixture.run_path("successor");
    let _successor_writable = WritableTreeCopyOnDrop(successor_path.clone());
    let mut record = successor_record(&revised, &predecessor_record, 1);
    record.state = RunState::Waiting;
    record.position.clone_from(&predecessor_record.position);
    record.edges.clone_from(&predecessor_record.edges);
    record.external.clone_from(&predecessor_record.external);
    let pending = acquired(&predecessor_path);

    // The carry and the record's state agree, or nothing is staged.
    let mut running = record.clone();
    running.state = RunState::Running;
    for (record, carry) in [
        (&running, SuccessorCarry::Wait),
        (&record, SuccessorCarry::Continue),
    ] {
        assert_eq!(
            InitialLocalRun::create_successor(
                &successor_path,
                &revised.admitted,
                record,
                &pending,
                carry
            ),
            Err(LocalRunDirectoryError::StateInvalid)
        );
        assert!(!successor_path.exists());
    }

    InitialLocalRun::create_successor(
        &successor_path,
        &revised.admitted,
        &record,
        &pending,
        SuccessorCarry::Wait,
    )
    .unwrap();
    assert_eq!(
        directory_names(&successor_path),
        [
            ".private",
            "attempts",
            "controls",
            "local-run.json",
            "run.json",
            "run.lock",
            "state.json",
            "wait.json",
            "workflow"
        ]
    );
    assert_eq!(
        fs::read(successor_path.join(WAIT_RECORD_FILE)).unwrap(),
        predecessor_wait
    );
    assert_eq!(
        mode(&successor_path.join(WAIT_RECORD_FILE)),
        mode(&predecessor_path.join(WAIT_RECORD_FILE))
    );
    let document = run_record_document(&successor_path);
    assert_eq!(document["state"], json!("waiting"));
    assert_eq!(document["position"], json!(["second"]));
    assert_eq!(document["edges"], json!({"first.failed": 1}));
    assert_eq!(document["attempts"], json!(1));
    assert_eq!(document["outcome"], Value::Null);
    assert_eq!(document["predecessor"], json!(predecessor_record.id));
    assert_eq!(
        directory_names(&successor_path.join(ATTEMPTS_DIRECTORY)),
        ["000001"]
    );
    assert_eq!(
        fs::read(successor_path.join("attempts/000001/wait.json")).unwrap(),
        predecessor_wait
    );
    verify_existing_run_layout(&open_directory_path(&successor_path).unwrap()).unwrap();

    // The carried wait is the successor's own, and the copied control resolves nothing.
    let wait = stop_wait_record();
    assert_eq!(
        inspect_wait(&successor_path, &crate::workflow::wait::NoConditions)
            .unwrap()
            .inspection,
        WaitInspection::Held { wait: wait.clone() }
    );

    // Superseding exits the predecessor as for a restart.
    pending.supersede(&record.id).unwrap();
    let superseded = run_record_document(&predecessor_path);
    assert_eq!(superseded["state"], json!("exited"));
    assert_eq!(superseded["outcome"], json!(SUPERSEDED_OUTCOME));
    assert_eq!(superseded["successor"], json!(record.id));
    assert_eq!(superseded["position"], json!([]));
    assert!(!predecessor_path.join(WAIT_RECORD_FILE).exists());

    // A control recorded on the successor resolves the carried wait.
    let lift = crate::workflow::wait::ControlEvent {
        source: "um".to_owned(),
        event: crate::workflow::wait::CONTROL_LIFT.to_owned(),
        run: record.id.clone(),
        by: "saeed".to_owned(),
        at: "2026-11-06T17:00:00Z".to_owned(),
        wait: Some(wait.id()),
        payload: json!({"before": "second", "stop": "2026-11-06T16:01:17Z"})
            .as_object()
            .cloned(),
    };
    crate::workflow::wait::append_control(&successor_path, &lift).unwrap();
    assert_eq!(
        inspect_wait(&successor_path, &crate::workflow::wait::NoConditions)
            .unwrap()
            .inspection,
        WaitInspection::Satisfied {
            wait,
            by: ResolvedBy::Control(lift),
        }
    );
    assert_eq!(
        attempt_controls(&successor_path, 2)
            .unwrap()
            .controls
            .wait
            .map(|(step, _)| step),
        Some("second".to_owned())
    );

    // Its first own attempt continues from `then` under the successor's definition.
    let owned = acquired(&successor_path)
        .begin(
            &revised.admitted,
            vec![wait_then()],
            ContinuationDefinitionMode::Run,
            crate::workflow::publication::ContinuationReasonV1::Resume,
            None,
        )
        .unwrap();
    assert_eq!(owned.attempt_number(), 2);
    drop(owned);
    assert!(!successor_path.join(WAIT_RECORD_FILE).exists());
    let root = open_directory_path(&successor_path).unwrap();
    let run = read_run(&root).unwrap();
    let state = read_state(&root).unwrap();
    let document = serde_json::to_value(state.attempts[1].continuation.as_ref().unwrap()).unwrap();
    assert_eq!(document["reason"], json!("resume"));
    assert_eq!(document["request"]["definition"], json!("inherited"));
    assert_eq!(
        document["definitionSource"],
        json!({
            "kind": "replaced",
            "manifestDigest": run.workflow_manifest_digest,
            "priorManifestDigest": predecessor_run.workflow_manifest_digest,
        })
    );
    assert_eq!(
        document["inheritedSteps"],
        json!([{"id": "first", "priorState": "succeeded", "definitionChanged": false}])
    );
    assert_eq!(read_run_record(&root).unwrap().edges, record.edges);
}

/// The step the stop wait of `stop_wait_record` continues at.
fn wait_then() -> String {
    stop_wait_record().then
}

/// An edge-scheduled root: `design` plans a list of parts, `build` fans out over it and
/// waits for approval, `merge` lands.
const CARRY_ROOT_WORKFLOW: &str = "schemaVersion: 1\noutcomes: [landed, failed]\nsteps:\n  design:\n    kind: workflow\n    workflow: design.yaml\n    on:\n      planned: build\n      failed: exit.failed\n  build:\n    kind: workflow\n    workflow: build.yaml\n    forEach: outputs.design.parts\n    on:\n      built: merge\n      review:\n        wait: approval\n        then: merge\n        onHandBack: build\n      failed: exit.failed\n  merge:\n    kind: workflow\n    workflow: merge.yaml\n    on:\n      landed: exit.landed\n      failed: exit.failed\n";
/// `CARRY_ROOT_WORKFLOW` without `merge`, with `design` routing `drafted` rather than
/// `planned` and with `build` no longer fanning out.
const UNRESOLVED_CARRY_ROOT_WORKFLOW: &str = "schemaVersion: 1\noutcomes: [landed, failed]\nsteps:\n  design:\n    kind: workflow\n    workflow: draft.yaml\n    on:\n      drafted: build\n      failed: exit.failed\n  build:\n    kind: workflow\n    workflow: build.yaml\n    on:\n      built: exit.landed\n      review:\n        wait: approval\n        then: build\n        onHandBack: build\n      failed: exit.failed\n";
const CARRY_DESIGN_WORKFLOW: &str = "schemaVersion: 1\noutcomes: [planned, failed]\noutcome:\n  from: outputs.decide.verdict\n  onFailure: failed\nsteps:\n  decide:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n    outputs:\n      verdict:\n        kind: text\n        from: path\n        path: verdict-design.txt\n      parts:\n        kind: json\n        from: path\n        path: parts.json\n        schema: parts.schema.json\nexports:\n  parts:\n    ref: outputs.decide.parts\n";
const CARRY_DRAFT_WORKFLOW: &str = "schemaVersion: 1\noutcomes: [drafted, failed]\noutcome:\n  from: outputs.decide.verdict\n  onFailure: failed\nsteps:\n  decide:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n    outputs:\n      verdict:\n        kind: text\n        from: path\n        path: verdict-design.txt\n";
const CARRY_BUILD_WORKFLOW: &str = "schemaVersion: 1\noutcomes: [built, review, failed]\noutcome:\n  from: outputs.decide.verdict\n  onFailure: failed\nsteps:\n  decide:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n    outputs:\n      verdict:\n        kind: text\n        from: path\n        path: verdict-build.txt\n";
const CARRY_MERGE_WORKFLOW: &str = "schemaVersion: 1\noutcomes: [landed, failed]\noutcome:\n  from: outputs.decide.verdict\n  onFailure: failed\nsteps:\n  decide:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n    outputs:\n      verdict:\n        kind: text\n        from: path\n        path: verdict-merge.txt\n";
const CARRY_PARTS_SCHEMA: &str =
    r#"{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"array"}"#;
const CARRY_PARTS: &[u8] = br#"[{"name":"a"},{"name":"b"},{"name":"c"}]"#;

fn carry_tree_fixture(root: &str) -> AdmittedFixture {
    AdmittedFixture::from_tree(
        root,
        &[
            ("design.yaml", CARRY_DESIGN_WORKFLOW, 0o644),
            ("draft.yaml", CARRY_DRAFT_WORKFLOW, 0o644),
            ("build.yaml", CARRY_BUILD_WORKFLOW, 0o644),
            ("merge.yaml", CARRY_MERGE_WORKFLOW, 0o644),
            ("parts.schema.json", CARRY_PARTS_SCHEMA, 0o644),
        ],
    )
}

/// The approval `build` waits on at its second element: approved it continues at `merge`,
/// handed back at `build`.
fn fan_out_approval_wait() -> WaitRecord {
    serde_json::from_value(json!({
        "kind": "approval",
        "since": "2026-11-06T16:30:02Z",
        "step": ["build", {"item": 2}],
        "then": "merge",
        "onHandBack": "build",
        "rule": {
            "source": "um",
            "event": ["control.approve", "control.hand_back"],
            "where": "event.run == run.id and event.wait == wait.id"
        },
        "payload": {}
    }))
    .unwrap()
}

/// Settles `owner`'s attempt `waiting` on `fan_out_approval_wait` with `design` succeeded,
/// retaining the outcome `planned` and the three-element `parts`, unless the attempt
/// inherited it, and sets `run.json` `waiting` at `["build", {"item": 2}]`.
fn settle_waiting_at_the_second_element(owner: &LocalAttemptOwner) {
    let inherited = read_state(owner.root_handle())
        .unwrap()
        .attempts
        .last()
        .unwrap()
        .progress
        .steps
        .iter()
        .any(|step| step.id == "design" && step.state == AttemptStepStateV1::Inherited);
    let outputs = (!inherited).then(|| {
        vec![
            write_retained_carrier(owner, "design", "outcome", b"planned", false),
            write_retained_carrier(owner, "design", "parts", CARRY_PARTS, true),
        ]
    });
    owner
        .state
        .update(|state| {
            let attempt = current_attempt_mut(state)?;
            let settled = attempt.created_at.clone();
            attempt.started_at = Some(settled.clone());
            attempt.settled_at = Some(settled);
            attempt.settlement_snapshot = Some(fixture_settlement_snapshot());
            attempt.state = AttemptStateV1::Waiting;
            for step in &mut attempt.progress.steps {
                match (step.id.as_str(), step.state) {
                    (_, AttemptStepStateV1::Inherited) => {}
                    ("design", _) => {
                        step.state = AttemptStepStateV1::Succeeded;
                        step.outputs.clone_from(&outputs);
                    }
                    _ => {
                        step.state = AttemptStepStateV1::NotRun;
                        step.detail = Some(NodeDetail::NotRun(
                            crate::workflow::evidence::NonExecutionDetail::not_reached(),
                        ));
                    }
                }
            }
            attempt.result = AttemptResultV1::NotPublished {
                reason: ResultAbsentReasonV1::PublicationPending,
            };
            Ok(())
        })
        .unwrap();
    owner.record_wait(&fan_out_approval_wait()).unwrap();
    rewrite_run_record(owner.root_handle(), |record| {
        record.state = RunState::Waiting;
        record.attempts = owner.attempt_number();
        record.position = vec![json!("build"), json!({"item": 2})];
        Ok(())
    })
    .unwrap();
}

/// A definition run of `CARRY_ROOT_WORKFLOW` waiting as
/// `settle_waiting_at_the_second_element` leaves it.
fn waiting_fan_out_predecessor(
    fixture: &AdmittedFixture,
    name: &str,
) -> (PathBuf, WritableTreeCopyOnDrop) {
    let run_path = fixture.run_path(name);
    let writable = WritableTreeCopyOnDrop(run_path.clone());
    let record = tree_record(fixture, Some((HOME_DEFINITION.to_owned(), 1)));
    let run =
        InitialLocalRun::create_with_record(&run_path, &fixture.admitted, Some(&record)).unwrap();
    settle_waiting_at_the_second_element(&run);
    drop(run);
    (run_path, writable)
}

// REQ-LOCALRUN-197
#[test]
fn the_resolve_check_lists_every_miss() {
    let fixture = carry_tree_fixture(CARRY_ROOT_WORKFLOW);
    let unresolved = carry_tree_fixture(UNRESOLVED_CARRY_ROOT_WORKFLOW);
    let (run_path, _writable) = waiting_fan_out_predecessor(&fixture, "predecessor");
    let pending = acquired(&run_path);
    let inputs = ResolvedInputs::default();
    assert_eq!(
        pending
            .resolve_carry(fixture.admitted.workflow(), &inputs)
            .unwrap(),
        Vec::new()
    );
    let misses = pending
        .resolve_carry(unresolved.admitted.workflow(), &inputs)
        .unwrap();
    assert_eq!(
        misses.iter().map(|miss| miss.code).collect::<Vec<_>>(),
        [
            "position_unresolved",
            "wait_target_unresolved",
            "outcome_unrouted",
            "items_unresolved"
        ]
    );
    for (miss, named) in misses.iter().zip(["build", "merge", "planned", "build"]) {
        assert!(miss.message.contains(named), "{miss:?}");
    }

    // A run that does not wait carries nothing to resolve.
    drop(pending);
    let pending = acquired(&run_path);
    remove_root_wait_record(&pending.root).unwrap();
    assert_eq!(
        pending.resolve_carry(fixture.admitted.workflow(), &inputs),
        Err(LocalRunDirectoryError::StateConflict)
    );
}

// REQ-LOCALRUN-197
#[test]
fn the_carried_readers_answer_retained_child_outcomes_and_fan_out_lengths() {
    let fixture = carry_tree_fixture(CARRY_ROOT_WORKFLOW);
    let (run_path, _writable) = waiting_fan_out_predecessor(&fixture, "predecessor");
    let workflow = fixture.admitted.workflow();
    let inputs = ResolvedInputs::default();
    let assert_readers = |pending: &PendingLocalContinuation| {
        assert_eq!(
            pending.inherited_child_outcomes().unwrap(),
            BTreeMap::from([("design".to_owned(), "planned".to_owned())])
        );
        assert_eq!(
            pending
                .inherited_items_length(workflow, "build", &inputs)
                .unwrap(),
            Some(3)
        );
        for step in ["design", "merge", "absent"] {
            assert_eq!(
                pending
                    .inherited_items_length(workflow, step, &inputs)
                    .unwrap(),
                None,
                "{step}"
            );
        }
    };
    assert_readers(&acquired(&run_path));

    // An inherited step's outputs are read from the attempt that produced them.
    let owned = acquired(&run_path)
        .begin(
            &fixture.admitted,
            vec!["build".to_owned()],
            ContinuationDefinitionMode::Inherited,
            crate::workflow::publication::ContinuationReasonV1::Resume,
            None,
        )
        .unwrap();
    settle_waiting_at_the_second_element(&owned);
    drop(owned);
    let pending = acquired(&run_path);
    let design = pending.state.attempts[1]
        .progress
        .steps
        .iter()
        .find(|step| step.id == "design")
        .unwrap();
    assert_eq!(design.state, AttemptStepStateV1::Inherited);
    assert_readers(&pending);
    assert_eq!(
        pending.resolve_carry(workflow, &inputs).unwrap(),
        Vec::new()
    );

    // A carrier that no longer matches its record is refused rather than read.
    fs::write(
        run_path
            .join("attempts/000001")
            .join(retained_value_relative_path(
                AttemptNodeRoleV1::Step,
                "design",
                "outcome",
            )),
        b"drafted",
    )
    .unwrap();
    assert_eq!(
        pending.inherited_child_outcomes(),
        Err(LocalRunDirectoryError::StateInvalid)
    );
}

const HOME_DEFINITION: &str = "def_01jcf8y6m3q7v2k9x4t5r8w0zp";
const OTHER_HOME_DEFINITION: &str = "def_01jcf8y6m3q7v2k9x4t5r8w0zq";
const UNKNOWN_RUN_ID: &str = "run_7zzzzzzzzzzzzzzzzzzzzzzzzz";

/// A valid `stop` wait record, as the contract fixture spells one.
/// An `answer` wait before `second` with the wait tree's one text question.
fn answer_wait_record() -> WaitRecord {
    serde_json::from_value(json!({
        "kind": "answer",
        "since": "2026-11-06T16:30:02Z",
        "step": ["second"],
        "then": "second",
        "rule": {
            "source": "um",
            "event": ["control.answer"],
            "where": "event.run == run.id and event.wait == wait.id and event.payload.complete == true"
        },
        "payload": {"questions": "keep the cache?"}
    }))
    .unwrap()
}

fn stop_wait_record() -> WaitRecord {
    serde_json::from_value(json!({
        "kind": "stop",
        "since": "2026-11-06T16:30:02Z",
        "step": ["second"],
        "then": "second",
        "rule": {
            "source": "um",
            "event": ["control.lift"],
            "where": "event.run == run.id and event.payload.before == wait.then and event.payload.stop == wait.payload.at"
        },
        "payload": {"by": "saeed", "at": "2026-11-06T16:01:17Z", "payload": {"before": "second"}}
    }))
    .unwrap()
}

/// A repair wait at `step`'s position whose payload names `child`.
fn repair_wait_naming(step: Value, child: Value) -> WaitRecord {
    serde_json::from_value(json!({
        "kind": "repair",
        "since": "2026-11-10T12:00:00Z",
        "step": step,
        "then": "design",
        "rule": {
            "source": "um",
            "event": ["control.retry", "control.withdraw"],
            "where": "event.run == run.id and event.wait == wait.id"
        },
        "payload": {"outcome": "failed", "child": child}
    }))
    .unwrap()
}

// REQ-CONTINUE-82, REQ-LOCALRUN-177: a wait's child locator is owned only in the exact
// canonical form of the waiting step's child, and of its element for a fan-out.
#[test]
fn a_wait_names_its_own_child_only_by_the_canonical_locator() {
    let plain = json!(["design"]);
    let element = json!(["design", {"item": 2}]);
    let owned = |locator: &str, item| WaitChild::Owned {
        locator: PathBuf::from(locator),
        item,
    };
    for (step, child, expected) in [
        (
            &plain,
            json!("children/design/000001"),
            owned("children/design/000001", None),
        ),
        (
            &element,
            json!("children/design/000002/000003"),
            owned("children/design/000002/000003", Some(2)),
        ),
        (&plain, json!("children/review/000001"), WaitChild::Unowned),
        (&plain, json!("children/design/1"), WaitChild::Unowned),
        (&plain, json!("children/design/000000"), WaitChild::Unowned),
        (
            &plain,
            json!("children/design/../design/000001"),
            WaitChild::Unowned,
        ),
        (&plain, json!("/children/design/000001"), WaitChild::Unowned),
        (&plain, json!("children/design/000001/"), WaitChild::Unowned),
        (
            &plain,
            json!("children/design/000002/000001"),
            WaitChild::Unowned,
        ),
        (
            &element,
            json!("children/design/000001/000001"),
            WaitChild::Unowned,
        ),
        (
            &element,
            json!("children/design/000001"),
            WaitChild::Unowned,
        ),
        (&plain, json!(1), WaitChild::Unowned),
    ] {
        assert_eq!(
            wait_child(&repair_wait_naming(step.clone(), child.clone())),
            expected,
            "{step} {child}"
        );
    }
    let mut absent = repair_wait_naming(plain, json!(null));
    absent.payload.remove("child");
    assert_eq!(wait_child(&absent), WaitChild::Absent);
}

// REQ-LOCALRUN-180
#[test]
fn a_run_id_locates_its_run_under_the_home_and_the_home_lists_every_run() {
    let fixture = tree_fixture();
    let home_root = fixture.run_path("home");
    let home = WorkflowHome::from_lookup(|name| {
        (name == "UM_HOME").then(|| home_root.clone().into_os_string())
    })
    .unwrap();
    assert_eq!(
        locate_run(&home, UNKNOWN_RUN_ID),
        Err(LocalRunDirectoryError::RunNotFound)
    );
    let empty = list_runs(&home).unwrap();
    assert!(empty.runs.is_empty() && empty.unreadable.is_empty());

    let mut created = Vec::new();
    for definition in [HOME_DEFINITION, HOME_DEFINITION, OTHER_HOME_DEFINITION] {
        let runs = home.runs_directory(definition);
        fs::create_dir_all(&runs).unwrap();
        let record = tree_record(&fixture, Some((definition.to_owned(), 1)));
        let directory = runs.join(&record.id);
        let writable = WritableTreeCopyOnDrop(directory.clone());
        let run = InitialLocalRun::create_with_record(&directory, &fixture.admitted, Some(&record))
            .unwrap();
        created.push((run, directory.canonicalize().unwrap(), record, writable));
    }
    let wait = stop_wait_record();
    created[1].0.record_wait(&wait).unwrap();
    let created = created
        .into_iter()
        .map(|(run, directory, record, writable)| {
            drop(run);
            (directory, record, writable)
        })
        .collect::<Vec<_>>();
    // A `runs/` entry without `run.json` is no run, even under a run id's name.
    let unreadable = home
        .runs_directory(OTHER_HOME_DEFINITION)
        .join("run_0aaaaaaaaaaaaaaaaaaaaaaaaa");
    fs::create_dir(&unreadable).unwrap();

    for (directory, record, _) in &created {
        assert_eq!(locate_run(&home, &record.id).as_ref(), Ok(directory));
    }
    for missing in [UNKNOWN_RUN_ID, "run_0aaaaaaaaaaaaaaaaaaaaaaaaa"] {
        assert_eq!(
            locate_run(&home, missing),
            Err(LocalRunDirectoryError::RunNotFound),
            "{missing}"
        );
    }
    for invalid in [
        "",
        "../run",
        "run_short",
        HOME_DEFINITION,
        "RUN_7ZZZZZZZZZZZZZZZZZZZZZZZZZ",
    ] {
        assert_eq!(
            locate_run(&home, invalid),
            Err(LocalRunDirectoryError::InvalidPath),
            "{invalid}"
        );
    }

    let listing = list_runs(&home).unwrap();
    let mut expected = created
        .iter()
        .enumerate()
        .map(|(index, (directory, record, _))| {
            (
                directory.clone(),
                record.clone(),
                (index == 1).then(|| wait.clone()),
            )
        })
        .collect::<Vec<_>>();
    expected.sort_by(|left, right| left.1.id.cmp(&right.1.id));
    assert_eq!(
        listing
            .runs
            .into_iter()
            .map(|run| (run.directory, run.record, run.wait))
            .collect::<Vec<_>>(),
        expected
    );
    assert_eq!(listing.unreadable.len(), 1, "{:?}", listing.unreadable);
    assert_eq!(listing.unreadable[0].0.file_name(), unreadable.file_name());
}

// REQ-LOCALRUN-177
#[test]
fn child_runs_lists_every_child_by_step_then_ordinal() {
    let fixture = AdmittedFixture::from_tree(
        CHILD_PARENT_WORKFLOW,
        &[
            ("bin/hello", TREE_PROGRAM, 0o755),
            ("design.yaml", CHILD_DESIGN_WORKFLOW, 0o644),
        ],
    );
    let parent_path = fixture.run_path("parent");
    let _writable_parent = WritableTreeCopyOnDrop(parent_path.clone());
    let parent_record = tree_record(&fixture, Some((HOME_DEFINITION.to_owned(), 1)));
    let parent =
        InitialLocalRun::create_with_record(&parent_path, &fixture.admitted, Some(&parent_record))
            .unwrap();
    assert_eq!(child_runs(&parent_path).unwrap(), Vec::new());

    let parent_copy = parent.tree_copy_directory();
    let mut expected = Vec::new();
    let mut writable = Vec::new();
    for (step, ordinal) in [("review", 1), ("design", 1), ("design", 2)] {
        let admitted = fixture.admit_child(&parent_copy, "design.yaml");
        let record = WorkflowRunRecord::child_of(&parent_record, step, "design.yaml", None);
        let child = create_child(&parent, step, None, &admitted, &record).unwrap();
        writable.push(WritableTreeCopyOnDrop(child.run_directory().to_owned()));
        let outcome = (step == "design" && ordinal == 1).then_some("blocked");
        if let Some(outcome) = outcome {
            child
                .update_run_record(RunState::Exited, 1, Some(outcome))
                .unwrap();
        }
        expected.push((
            step.to_owned(),
            ordinal,
            child.run_directory().to_owned(),
            record.id.clone(),
            if outcome.is_some() {
                RunState::Exited
            } else {
                RunState::Running
            },
            outcome.map(str::to_owned),
        ));
    }
    drop(parent);
    expected.sort_by(|left, right| (&left.0, left.1).cmp(&(&right.0, right.1)));

    let children = child_runs(&parent_path)
        .unwrap()
        .into_iter()
        .map(|child| {
            (
                child.step,
                child.ordinal,
                child.directory,
                child.record.id,
                child.record.state,
                child.record.outcome,
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(children, expected);
}

const LOCK_HOLDER_RUN: &str = "UM_TEST_LOCK_HOLDER_RUN";

/// The process fixture of `lock_holder_names_the_process_that_holds_the_run_lock`: holds
/// the lock of the run at `$UM_TEST_LOCK_HOLDER_RUN` through `acquire_local_retry`, creates
/// `<run>.held` once it does, and keeps the lock until standard input closes.
#[test]
#[ignore = "process fixture of lock_holder_names_the_process_that_holds_the_run_lock"]
fn lock_holder_process_fixture() {
    let Some(run_path) = std::env::var_os(LOCK_HOLDER_RUN).map(PathBuf::from) else {
        return;
    };
    let LocalRetryOpen::Acquired(pending) = acquire_local_retry(&run_path).unwrap() else {
        panic!("the fixture run should be retryable");
    };
    fs::write(run_path.with_extension("held"), b"").unwrap();
    let _ = io::stdin().read_to_end(&mut Vec::new());
    drop(pending);
}

// REQ-LOCALRUN-175
#[tokio::test]
async fn lock_holder_names_the_process_that_holds_the_run_lock() {
    let fixture = edge_tree_fixture();
    let (run_path, _, _writable) = waiting_tree_run(&fixture, "locked").await;
    assert_eq!(lock_holder(&run_path), Ok(None));

    // A process's own `fcntl` lock never blocks its own query, so another process holds it.
    let ready = run_path.with_extension("held");
    let mut holder = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "workflow::local_run::tests::lock_holder_process_fixture",
            "--ignored",
            "--test-threads=1",
        ])
        .env(LOCK_HOLDER_RUN, &run_path)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let started = um_support::monotonic_now();
    while !ready.exists() {
        assert!(
            holder.try_wait().unwrap().is_none(),
            "the holder exited early"
        );
        assert!(um_support::elapsed(started) < Duration::from_secs(10));
        um_support::sleep(Duration::from_millis(10));
    }
    let holder_pid = rustix::process::Pid::from_raw(i32::try_from(holder.id()).unwrap());
    assert_eq!(lock_holder(&run_path), Ok(holder_pid));

    drop(holder.stdin.take());
    assert!(holder.wait().unwrap().success());
    assert_eq!(lock_holder(&run_path), Ok(None));
}

// REQ-LOCALRUN-175
#[test]
fn update_run_record_rewrites_state_attempts_and_outcome_and_nothing_else() {
    let fixture = tree_fixture();
    let run_path = fixture.run_path("record-update");
    let _writable = WritableTreeCopyOnDrop(run_path.clone());
    let record = tree_record(&fixture, None);
    let run =
        InitialLocalRun::create_with_record(&run_path, &fixture.admitted, Some(&record)).unwrap();
    let record_path = run_path.join(RUN_RECORD_FILE);
    let created = fs::read_to_string(&record_path).unwrap();
    let validator = run_record_validator();
    let assert_record_valid = |text: &str| {
        let document = serde_json::from_str::<Value>(text).unwrap();
        assert!(validator.is_valid(&document), "{document}");
    };
    // A temporary left by an interrupted rewrite is private and replaced.
    let temporary_path = run_path
        .join(PRIVATE_DIRECTORY)
        .join(RUN_RECORD_TEMPORARY_FILE);
    fs::write(&temporary_path, b"interrupted").unwrap();

    run.update_run_record(RunState::Exited, 1, Some("planned"))
        .unwrap();
    let exited = fs::read_to_string(&record_path).unwrap();
    assert_eq!(
        exited,
        created
            .replace("\"state\": \"running\"", "\"state\": \"exited\"")
            .replace("\"outcome\": null", "\"outcome\": \"planned\"")
    );
    assert_record_valid(&exited);
    assert_eq!(mode(&record_path), 0o600);
    assert!(!temporary_path.exists());

    // A record that does not validate is refused at its path and left as it is.
    let invalid = created.replace("\"owner\": \"tester\"", "\"owner\": \"\"");
    fs::write(&record_path, &invalid).unwrap();
    match run.update_run_record(RunState::Exited, 2, Some("failed")) {
        Err(LocalRunDirectoryError::StateFile { path, source, .. })
            if path == record_path.canonicalize().unwrap()
                && matches!(*source, LocalRunDirectoryError::StateInvalid) => {}
        other => panic!("an invalid run record was rewritten: {other:?}"),
    }
    assert_eq!(fs::read_to_string(&record_path).unwrap(), invalid);
    fs::write(&record_path, &exited).unwrap();

    settle_as_workflow_failed(&run);
    drop(run);
    let LocalRetryOpen::Acquired(pending) = acquire_local_retry(&run_path).unwrap() else {
        panic!("a failed tree run should be retryable");
    };
    // A live attempt has no outcome yet.
    pending
        .update_run_record(RunState::Running, 2, None)
        .unwrap();
    drop(pending);
    let retried = fs::read_to_string(&record_path).unwrap();
    assert_eq!(
        retried,
        created.replace("\"attempts\": 1", "\"attempts\": 2")
    );
    assert_record_valid(&retried);

    let LocalContinuationOpen::Acquired(pending) = acquire_local_continuation(&run_path).unwrap()
    else {
        panic!("a failed tree run should be continuable");
    };
    pending
        .update_run_record(RunState::Exited, 3, Some("planned"))
        .unwrap();
    drop(pending);
    assert_eq!(
        fs::read_to_string(&record_path).unwrap(),
        exited.replace("\"attempts\": 1", "\"attempts\": 3")
    );
    verify_existing_run_layout(&open_directory_path(&run_path).unwrap()).unwrap();
}

// REQ-LOCALRUN-175, REQ-LOCALRUN-98
#[test]
fn a_run_created_without_a_record_has_no_run_json_and_the_old_layout_is_refused() {
    let fixture = AdmittedFixture::new();
    let run_path = fixture.run_path("closure");
    let run = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    assert_eq!(
        directory_names(&run_path),
        [
            ".private",
            "attempts",
            "local-run.json",
            "run.lock",
            "state.json",
            "workflow"
        ]
    );
    run.update_run_record(RunState::Exited, 1, Some("failed"))
        .unwrap();
    assert!(!run_path.join(RUN_RECORD_FILE).exists());
    settle_as_workflow_failed(&run);
    drop(run);
    verify_existing_run_layout(&open_directory_path(&run_path).unwrap()).unwrap();
    let LocalRetryOpen::Acquired(pending) = acquire_local_retry(&run_path).unwrap() else {
        panic!("a failed closure run should be retryable");
    };
    pending
        .update_run_record(RunState::Running, 2, None)
        .unwrap();
    drop(pending);
    assert!(!run_path.join(RUN_RECORD_FILE).exists());

    // The layout before the run record: the identity record as `run.json`.
    fs::rename(
        run_path.join(LOCAL_RUN_FILE),
        run_path.join(RUN_RECORD_FILE),
    )
    .unwrap();
    assert_layout_refused(&run_path);
    match acquire_local_retry(&run_path) {
        Err(LocalRunDirectoryError::StateFile { source, .. })
            if matches!(*source, LocalRunDirectoryError::StateInvalid) => {}
        Err(other) => panic!("the old layout produced the wrong failure: {other:?}"),
        Ok(_) => panic!("retry accepted a run directory without local-run.json"),
    }
}

// REQ-LOCALRUN-171
#[test]
fn a_modified_tree_copy_is_rejected_on_reopen() {
    let fixture = tree_fixture();
    let run_path = fixture.run_path("tree-modified");
    let _writable = WritableTreeCopyOnDrop(run_path.clone());
    let run = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    settle_as_workflow_failed(&run);
    drop(run);
    let expected = &fixture.admitted.tree().unwrap().digest().value;

    let LocalRetryOpen::Acquired(pending) = acquire_local_retry(&run_path).unwrap() else {
        panic!("a failed tree run should be retryable");
    };
    let (copy, record) = pending
        .tree_copy()
        .expect("the retained read reports the tree");
    assert_eq!(copy, run_path.join("workflow/tree").canonicalize().unwrap());
    assert_eq!(record.digest(), expected);
    let verified = pending.verify_tree_copy().unwrap().unwrap();
    assert_eq!(&verified.digest.value, expected);
    assert_eq!(verified.programs, ["bin"]);
    drop(pending);

    let program = run_path.join("workflow/tree/bin/hello");
    fs::set_permissions(program.parent().unwrap(), Permissions::from_mode(0o755)).unwrap();
    fs::set_permissions(&program, Permissions::from_mode(0o755)).unwrap();
    fs::write(&program, "#!/bin/sh\necho modified\n").unwrap();

    let LocalRetryOpen::Acquired(pending) = acquire_local_retry(&run_path).unwrap() else {
        panic!("the retained read does not judge the copy's contents");
    };
    let (_, record) = pending.tree_copy().unwrap();
    assert_eq!(record.digest(), expected);
    let mismatch = pending.verify_tree_copy().unwrap().unwrap_err();
    assert_eq!(mismatch.expected_digest(), expected);
    let actual = mismatch
        .actual_digest()
        .expect("the modified copy is still a tree");
    assert_ne!(actual, expected);
    assert!(mismatch.diagnostics().is_empty());
    assert!(!run_path.join("attempts/000002").exists());
}

// REQ-LOCALRUN-97
#[test]
fn run_target_validation_rejects_overlap_in_both_path_directions() {
    assert!(paths_overlap(
        Path::new("/workspace/run"),
        Path::new("/workspace/run/execution")
    ));
    assert!(paths_overlap(
        Path::new("/workspace/execution/run"),
        Path::new("/workspace/execution")
    ));
    assert!(!paths_overlap(
        Path::new("/workspace/runs/run"),
        Path::new("/workspace/execution")
    ));

    let fixture = AdmittedFixture::new();
    let disjoint_lexical_run = fixture.run_path("aliased");
    let aliased_parent = open_directory_path(&fixture.execution_root).unwrap();
    assert!(!paths_overlap(
        &disjoint_lexical_run,
        &fixture.execution_root
    ));
    assert!(
        run_directory_overlaps_execution_root(
            &disjoint_lexical_run,
            &fixture.execution_root,
            fixture.admitted.execution().root_identity(),
            &aliased_parent,
        )
        .unwrap(),
        "directory identity must reject an alias that lexical paths miss"
    );

    let nested_run = fixture.execution_root.join("run");
    let failure = match InitialLocalRun::create(&nested_run, &fixture.admitted) {
        Ok(_) => panic!("an overlapping run directory must be rejected"),
        Err(failure) => failure,
    };
    assert_eq!(failure, LocalRunDirectoryError::ExecutionRootOverlap);
    assert!(!nested_run.exists());
}

#[test]
fn closed_durable_documents_reject_versions_fields_nulls_and_corruption() {
    let fixture = AdmittedFixture::new();
    let run = InitialLocalRun::create(&fixture.run_path("closed"), &fixture.admitted).unwrap();
    let run_document = read_run(run.root_handle()).unwrap();
    let state = read_state(run.root_handle()).unwrap();

    let mut value = serde_json::to_value(&run_document).unwrap();
    value["schemaVersion"] = Value::from(2);
    assert_eq!(
        decode_run(&json_bytes(value)).unwrap_err(),
        LocalRunDirectoryError::StateSchemaInvalid
    );
    let mut value = serde_json::to_value(&run_document).unwrap();
    value["unknown"] = Value::Bool(true);
    assert!(matches!(
        decode_run(&json_bytes(value)),
        Err(LocalRunDirectoryError::Json { .. })
    ));

    let mut value = serde_json::to_value(&state).unwrap();
    value["schemaVersion"] = Value::from(99);
    let corrupt_state = json_bytes(value);
    assert_eq!(
        decode_state(&corrupt_state).unwrap_err(),
        LocalRunDirectoryError::StateSchemaInvalid
    );
    let state_path = run.run_directory().join(STATE_FILE);
    fs::write(&state_path, corrupt_state).unwrap();
    let error = read_state(run.root_handle()).unwrap_err();
    assert!(
        matches!(&error, LocalRunDirectoryError::StateFile { path, operation: "validate", source }
        if path == &state_path && **source == LocalRunDirectoryError::StateSchemaInvalid)
    );
    assert!(
        error
            .to_string()
            .contains(&state_path.display().to_string())
    );
    assert!(error.to_string().contains("state schema invalid"));
    let mut value = serde_json::to_value(&state).unwrap();
    value["attempts"][0]["owner"]["unknown"] = Value::Bool(true);
    assert!(matches!(
        decode_state(&json_bytes(value)),
        Err(LocalRunDirectoryError::Json { .. })
    ));
    let mut value = serde_json::to_value(&state).unwrap();
    value["attempts"][0]["startedAt"] = Value::Null;
    assert_eq!(
        decode_state(&json_bytes(value)).unwrap_err(),
        LocalRunDirectoryError::DocumentNullInvalid
    );
    let mut value = serde_json::to_value(&state).unwrap();
    value["attempts"][0]["executionRoot"] = Value::String("/tmp/../tmp".to_owned());
    assert_eq!(
        decode_state(&json_bytes(value)),
        Err(LocalRunDirectoryError::AttemptExecutionRootInvalid)
    );
    let mut value = serde_json::to_value(&state).unwrap();
    value["attempts"][0]["state"] = Value::String("future_state".to_owned());
    assert!(matches!(
        decode_state(&json_bytes(value)),
        Err(LocalRunDirectoryError::Json { .. })
    ));
    assert_eq!(
        decode_state(b"{\"schemaVersion\":1"),
        Err(LocalRunDirectoryError::DocumentFramingInvalid)
    );
    let mut bom = vec![0xef, 0xbb, 0xbf];
    bom.extend(json_bytes(serde_json::to_value(&state).unwrap()));
    assert_eq!(
        decode_state(&bom),
        Err(LocalRunDirectoryError::DocumentFramingInvalid)
    );
}

#[cfg(target_os = "macos")]
#[test]
fn file_locator_uses_the_open_directory_on_macos() {
    let fixture = AdmittedFixture::new();
    let run =
        InitialLocalRun::create(&fixture.run_path("macos-locator"), &fixture.admitted).unwrap();
    assert_eq!(
        file_locator(run.root_handle(), STATE_FILE),
        run.run_directory().join(STATE_FILE)
    );
}

#[test]
fn state_index_rejects_corrupt_attempt_number_and_duplicate_step_with_specific_errors() {
    let fixture = AdmittedFixture::new();
    let run =
        InitialLocalRun::create(&fixture.run_path("index-corruption"), &fixture.admitted).unwrap();
    let state = read_state(run.root_handle()).unwrap();
    let mut document = serde_json::to_value(&state).unwrap();
    document["currentAttemptNumber"] = json!(2);
    let mut second = document["attempts"][0].clone();
    second["attemptNumber"] = json!(2);
    document["attempts"].as_array_mut().unwrap().push(second);
    document["attempts"][0]["attemptNumber"] = json!(0);
    assert_eq!(
        decode_state(&json_bytes(document)),
        Err(LocalRunDirectoryError::AttemptNumberInvalid)
    );

    let mut document = serde_json::to_value(&state).unwrap();
    let step = document["attempts"][0]["progress"]["steps"][0].clone();
    document["attempts"][0]["progress"]["steps"]
        .as_array_mut()
        .unwrap()
        .push(step);
    assert_eq!(
        decode_state(&json_bytes(document.clone())),
        Err(LocalRunDirectoryError::AttemptStepDuplicate)
    );
    fs::write(run.run_directory().join(STATE_FILE), json_bytes(document)).unwrap();
    assert!(
        matches!(read_state(run.root_handle()), Err(LocalRunDirectoryError::StateFile { path, source, .. })
        if path == run.run_directory().join(STATE_FILE) && *source == LocalRunDirectoryError::AttemptStepDuplicate)
    );
}

#[test]
fn attempt_recovery_and_result_corruption_report_distinct_invariants() {
    let fixture = AdmittedFixture::new();
    let run = InitialLocalRun::create(&fixture.run_path("attempt-predicates"), &fixture.admitted)
        .unwrap();
    let state = read_state(run.root_handle()).unwrap();
    let mut document = serde_json::to_value(&state).unwrap();
    document["attempts"][0]["progress"]["accounting"]["maximumInvocations"] = json!(0);
    assert_eq!(
        decode_state(&json_bytes(document)),
        Err(LocalRunDirectoryError::AttemptRecoveryAccountingInvalid)
    );
    let mut document = serde_json::to_value(&state).unwrap();
    document["attempts"][0]["result"] = json!({"status": "not_published", "reason": "rejected"});
    assert_eq!(
        decode_state(&json_bytes(document)),
        Err(LocalRunDirectoryError::AttemptResultInvalid)
    );
}

struct PartialTemporaryWrite;

impl StateCommitObserver for PartialTemporaryWrite {
    fn write_temporary(&mut self, file: &mut File, bytes: &[u8]) -> io::Result<()> {
        file.write_all(&bytes[..bytes.len() / 2])?;
        Err(io::Error::other("injected process loss"))
    }
}

struct FailBeforeReplace;

impl StateCommitObserver for FailBeforeReplace {
    fn temporary_complete(&mut self) -> Result<(), LocalRunDirectoryError> {
        Err(LocalRunDirectoryError::StateWriteUnavailable)
    }
}

struct StorageFullTemporaryWrite;

impl StateCommitObserver for StorageFullTemporaryWrite {
    fn write_temporary(&mut self, _file: &mut File, _bytes: &[u8]) -> io::Result<()> {
        Err(io::Error::from_raw_os_error(libc::ENOSPC))
    }
}

struct CorruptBeforeReplace {
    state_path: PathBuf,
}

impl StateCommitObserver for CorruptBeforeReplace {
    fn temporary_complete(&mut self) -> Result<(), LocalRunDirectoryError> {
        fs::write(&self.state_path, b"partial")
            .map_err(|_| LocalRunDirectoryError::StateWriteUnavailable)
    }
}

struct FailAfterReplace;

impl StateCommitObserver for FailAfterReplace {
    fn replaced(&mut self) -> Result<(), LocalRunDirectoryError> {
        Err(LocalRunDirectoryError::StateWriteUnavailable)
    }
}

#[test]
fn invocation_evidence_republication_preserves_first_authoritative_bytes() {
    let temporary = tempfile::tempdir().unwrap();
    let directory = open_directory_path(temporary.path()).unwrap();
    let original = b"first authoritative diagnostic";

    write_or_verify_immutable_file(&directory, "diagnostic.bin", original).unwrap();
    let first = fs::read(temporary.path().join("diagnostic.bin")).unwrap();
    write_or_verify_immutable_file(&directory, "diagnostic.bin", original).unwrap();
    assert_eq!(
        fs::read(temporary.path().join("diagnostic.bin")).unwrap(),
        first
    );

    assert_eq!(
        write_or_verify_immutable_file(&directory, "diagnostic.bin", b"changed"),
        Err(LocalRunDirectoryError::StateConflict)
    );
    assert_eq!(
        fs::read(temporary.path().join("diagnostic.bin")).unwrap(),
        first
    );
}

// INV-ENGINE-10
#[test]
fn atomic_state_crash_boundaries_expose_only_complete_snapshots() {
    let fixture = AdmittedFixture::new();
    let run = InitialLocalRun::create(&fixture.run_path("atomic"), &fixture.admitted).unwrap();
    let before = read_state(run.root_handle()).unwrap();
    let mutate = |state: &mut LocalRunStateV1| {
        append_diagnostic(
            state,
            INITIAL_ATTEMPT_NUMBER,
            DiagnosticCodeV1::StaleOccurrence,
        )
    };

    let failure = run
        .state
        .update_with_observer(mutate, &mut PartialTemporaryWrite)
        .unwrap_err();
    assert!(
        matches!(failure, LocalRunDirectoryError::File { path, operation: "write temporary state", source }
        if path.parent() == Some(run.run_directory().join(PRIVATE_DIRECTORY).as_path())
            && source.kind() == io::ErrorKind::Other)
    );
    assert_eq!(read_state(run.root_handle()).unwrap(), before);

    let failure = run
        .state
        .update_with_observer(mutate, &mut FailBeforeReplace)
        .unwrap_err();
    assert_eq!(failure, LocalRunDirectoryError::StateWriteUnavailable);
    assert_eq!(read_state(run.root_handle()).unwrap(), before);

    let failure = run
        .state
        .update_with_observer(mutate, &mut FailAfterReplace)
        .unwrap_err();
    assert_eq!(failure, LocalRunDirectoryError::StateWriteUnavailable);
    let after = read_state(run.root_handle()).unwrap();
    assert_eq!(after.revision, before.revision + 1);
    assert_eq!(after.diagnostics.len(), 1);
    assert_eq!(*lock_state(&run.state.current).unwrap(), after);
    assert!(decode_state(&encode_json(&after).unwrap()).is_ok());

    fs::set_permissions(
        run.run_directory().join(STATE_FILE),
        Permissions::from_mode(0o600),
    )
    .unwrap();
    fs::write(run.run_directory().join(STATE_FILE), b"{partial").unwrap();
    assert!(matches!(
        read_state(run.root_handle()),
        Err(LocalRunDirectoryError::StateFile { path, source, .. })
            if path == run.run_directory().join(STATE_FILE) && matches!(*source, LocalRunDirectoryError::DocumentFramingInvalid)
    ));
}

// INV-ENGINE-33
#[test]
fn failed_state_commit_classifies_its_cause_without_path_or_message() {
    use crate::workflow::coordinator::{
        ClassifiedCommitError as _, CommitFailure, CommitFailureClass,
    };

    let fixture = AdmittedFixture::new();
    let run = InitialLocalRun::create(&fixture.run_path("classified"), &fixture.admitted).unwrap();
    let mutate = |state: &mut LocalRunStateV1| {
        append_diagnostic(
            state,
            INITIAL_ATTEMPT_NUMBER,
            DiagnosticCodeV1::StaleOccurrence,
        )
    };

    let storage_full = run
        .state
        .update_with_observer(mutate, &mut StorageFullTemporaryWrite)
        .unwrap_err();
    assert_eq!(
        storage_full.commit_failure(),
        CommitFailure {
            class: CommitFailureClass::StorageFull,
            operation: Some("write temporary state"),
            os_error: Some(28),
        }
    );
    let state_file = LocalRunDirectoryError::StateFile {
        path: run.run_directory().join(STATE_FILE),
        operation: "read state",
        source: Box::new(storage_full),
    };
    assert_eq!(
        state_file.commit_failure().class,
        CommitFailureClass::StorageFull
    );

    assert_eq!(
        LocalRunDirectoryError::StateConflict.commit_failure(),
        CommitFailure {
            class: CommitFailureClass::StateConflict,
            operation: None,
            os_error: None,
        }
    );

    let unjoined = run
        .state
        .update_with_observer(mutate, &mut FailBeforeReplace)
        .unwrap_err();
    assert_eq!(
        unjoined.commit_failure(),
        CommitFailure {
            class: CommitFailureClass::Unclassified,
            operation: None,
            os_error: None,
        }
    );
}

#[test]
fn retry_reports_state_path_and_failed_attempt_invariant() {
    let fixture = AdmittedFixture::new();
    let run_path = fixture.run_path("invalid-attempt-root");
    let run = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    let mut document = serde_json::to_value(read_state(run.root_handle()).unwrap()).unwrap();
    drop(run);
    document["attempts"][0]["executionRoot"] = json!("/tmp/../tmp");
    fs::write(run_path.join(STATE_FILE), json_bytes(document)).unwrap();
    let state_path = fs::canonicalize(&run_path).unwrap().join(STATE_FILE);

    let Err(error) = acquire_local_retry(&run_path) else {
        panic!("corrupt state accepted");
    };
    assert!(
        matches!(&error, LocalRunDirectoryError::StateFile { path, source, .. }
        if path == &state_path
            && matches!(**source, LocalRunDirectoryError::AttemptExecutionRootInvalid))
    );
    assert!(
        error
            .to_string()
            .contains(&state_path.display().to_string())
    );
    assert!(error.to_string().contains("attempt execution root invalid"));
}

// REQ-WFRECOVERY-41
#[test]
fn status_preserves_unsupported_recovery_schema_through_state_file_context() {
    let fixture = AdmittedFixture::new();
    let run_path = fixture.run_path("unsupported-recovery");
    let run = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    let mut document = serde_json::to_value(read_state(run.root_handle()).unwrap()).unwrap();
    drop(run);
    document["attempts"][0]["progress"]["steps"][0]["recovery"] = json!({ "schemaVersion": 2 });
    fs::write(run_path.join(STATE_FILE), json_bytes(document)).unwrap();
    let error = read_local_run_status(&run_path).unwrap_err();
    assert_eq!(error.code, LocalStatusErrorCode::RecoverySchemaUnsupported);
}

#[test]
fn retry_preserves_missing_state_file_io_cause() {
    let fixture = AdmittedFixture::new();
    let run_path = fixture.run_path("missing-state");
    let run = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    drop(run);
    fs::remove_file(run_path.join(STATE_FILE)).unwrap();
    let state_path = fs::canonicalize(&run_path).unwrap().join(STATE_FILE);
    let root = open_directory_path(&run_path).unwrap();
    assert!(
        matches!(read_state(&root), Err(LocalRunDirectoryError::File { path, operation: "open", source })
        if path == state_path && source.kind() == io::ErrorKind::NotFound)
    );
    assert!(
        matches!(acquire_local_retry(&run_path), Err(LocalRunDirectoryError::StateFile { path, .. })
        if path == state_path)
    );
}

#[test]
fn locked_state_store_replaces_without_reloading_disk() {
    let fixture = AdmittedFixture::new();
    let run = InitialLocalRun::create(&fixture.run_path("concurrent"), &fixture.admitted).unwrap();
    let mut observer = CorruptBeforeReplace {
        state_path: run.run_directory().join(STATE_FILE),
    };

    run.state
        .update_with_observer(
            |state| {
                append_diagnostic(
                    state,
                    INITIAL_ATTEMPT_NUMBER,
                    DiagnosticCodeV1::StaleOccurrence,
                )
            },
            &mut observer,
        )
        .unwrap();
    let state = read_state(run.root_handle()).unwrap();
    assert_eq!(state.diagnostics.len(), 1);
    assert_eq!(*lock_state(&run.state.current).unwrap(), state);
}

#[test]
fn oversized_state_update_preserves_the_last_readable_snapshot() {
    let fixture = AdmittedFixture::new();
    let run = InitialLocalRun::create(&fixture.run_path("oversized"), &fixture.admitted).unwrap();
    let before = read_state(run.root_handle()).unwrap();
    let oversized_id = "x".repeat(usize::try_from(MAXIMUM_DURABLE_JSON_BYTES).unwrap());
    let failure = run.state.update(|state| {
        state.attempts[0].progress.steps[0].id = oversized_id;
        // The document is otherwise valid; only its encoded size exceeds the reader limit.
        validate_state(state)
    });

    assert_eq!(failure, Err(LocalRunDirectoryError::StateInvalid));
    assert_eq!(read_state(run.root_handle()).unwrap(), before);
    assert_eq!(*lock_state(&run.state.current).unwrap(), before);
}

struct NoExchange;

impl StateCommitObserver for NoExchange {
    fn exchange(
        &mut self,
        _private: &OwnedFd,
        _temporary_name: &str,
        _root: &OwnedFd,
    ) -> rustix::io::Result<()> {
        // Filesystems without exchange support return EOPNOTSUPP.
        Err(Errno::OPNOTSUPP)
    }
}

#[test]
fn unsupported_atomic_exchange_leaves_the_last_snapshot_authoritative() {
    let fixture = AdmittedFixture::new();
    let run = InitialLocalRun::create(&fixture.run_path("no-exchange"), &fixture.admitted).unwrap();
    let before = read_state(run.root_handle()).unwrap();
    assert!(matches!(
        run.state.update_with_observer(
            |state| append_diagnostic(state, INITIAL_ATTEMPT_NUMBER, DiagnosticCodeV1::StaleOccurrence),
            &mut NoExchange,
        ),
        Err(LocalRunDirectoryError::File { path, operation: "exchange state", source })
            if path == run.run_directory().join(STATE_FILE) && source.raw_os_error() == Some(Errno::OPNOTSUPP.raw_os_error())
    ));
    assert_eq!(read_state(run.root_handle()).unwrap(), before);
    assert_eq!(*lock_state(&run.state.current).unwrap(), before);
}

#[test]
fn guard_preparation_is_durable_before_release_and_quiescence_is_one_later_commit() {
    let fixture = AdmittedFixture::new();
    let run = InitialLocalRun::create(&fixture.run_path("guard-batch"), &fixture.admitted).unwrap();
    let initial_revision = read_state(run.root_handle()).unwrap().revision;
    let store = run.process_guard_registry();
    for (action_id, step) in [(1, "first"), (2, "second")] {
        run.state
            .update(|state| {
                let attempt = current_attempt_mut(state)?;
                attempt.progress.last_transition_sequence = action_id;
                attempt
                    .progress
                    .outstanding_actions
                    .push(OutstandingActionV1 {
                        action_id,
                        kind: OutstandingActionKindV1::StartStep,
                        step_id: Some(step.to_owned()),
                        node_role: Some(AttemptNodeRoleV1::Step),
                        target_execution: Some(1),
                        recovery_round: None,
                    });
                Ok(())
            })
            .unwrap();
        let identity = AuthenticatedProcessGroup::new(
            rustix::process::Pid::from_raw(41 + i32::try_from(action_id).unwrap()).unwrap(),
            "9001".to_owned(),
        )
        .unwrap();
        let mut registration = store.register(step, action_id, &identity).unwrap();
        let prepared = read_state(run.root_handle()).unwrap();
        assert_eq!(prepared.revision, initial_revision + 3 * action_id - 1);
        assert_eq!(
            prepared.attempts[0].process_guards.last().unwrap().state,
            ProcessGuardStateV1::Prepared
        );
        registration.mark_released().unwrap();
        let during = read_state(run.root_handle()).unwrap();
        assert_eq!(during, prepared);
        let guard = during.attempts[0].process_guards.last().unwrap();
        assert_eq!(
            recovery_status_with(
                &during.attempts[0],
                false,
                &FixtureRecoveryAuthority {
                    host: Ok(guard.execution_host.clone()),
                    observation: ProcessIdentityObservation::Unavailable,
                }
            ),
            LocalRecoveryStatus::OwnershipUnproven {
                guard_ids: vec![guard.guard_id.clone()],
                reason: OwnershipUnprovenReason::ProcessIdentityInspectionUnavailable,
            }
        );
        if action_id == 2 {
            let failure = run.state.update_with_observer(
                |state| {
                    current_attempt_mut(state)?
                        .process_guards
                        .last_mut()
                        .unwrap()
                        .state = ProcessGuardStateV1::Quiesced;
                    Ok(())
                },
                &mut FailBeforeReplace,
            );
            assert_eq!(failure, Err(LocalRunDirectoryError::StateWriteUnavailable));
            assert_eq!(read_state(run.root_handle()).unwrap(), prepared);
        }
        registration.mark_quiesced().unwrap();
        let after = read_state(run.root_handle()).unwrap();
        assert_eq!(after.revision, prepared.revision + 1);
        assert_eq!(
            after.attempts[0].process_guards.last().unwrap().state,
            ProcessGuardStateV1::Quiesced
        );
    }
}

fn fixture_settlement_snapshot() -> WorkspaceSnapshotV1 {
    capture_settlement_snapshot(
        Path::new("/fixture-workspace-is-unavailable"),
        WorkspaceSnapshotSettlementV1::Engine,
    )
}

fn settle_as_workflow_failed(run: &InitialLocalRun) {
    run.state
        .update(|state| {
            let attempt = current_attempt_mut(state)?;
            let settled = attempt.created_at.clone();
            attempt.started_at = Some(settled.clone());
            attempt.settled_at = Some(settled);
            attempt.settlement_snapshot = Some(fixture_settlement_snapshot());
            attempt.state = AttemptStateV1::WorkflowFailed;
            attempt.progress.steps[0].state = AttemptStepStateV1::Failed;
            attempt.progress.steps[0].detail = Some(crate::workflow::evidence::NodeDetail::Failed(
                crate::workflow::evidence::FailureDetail::new(
                    crate::workflow::evidence::FailurePhase::Execution,
                    crate::workflow::evidence::FailureCode::CommandExit,
                    None,
                    None,
                    None,
                    Some(23),
                )
                .unwrap(),
            ));
            attempt.progress.steps[1].state = AttemptStepStateV1::Blocked;
            attempt.progress.steps[1].detail =
                Some(crate::workflow::evidence::NodeDetail::Blocked(
                    crate::workflow::evidence::BlockedDetail::new([
                        crate::workflow::evidence::Prerequisite::control("first").unwrap(),
                    ])
                    .unwrap(),
                ));
            attempt.result = AttemptResultV1::NotPublished {
                reason: ResultAbsentReasonV1::PublicationPending,
            };
            Ok(())
        })
        .unwrap();
}

fn harness_failure_detail() -> crate::workflow::evidence::FailureDetail {
    crate::workflow::evidence::FailureDetail::new(
        crate::workflow::evidence::FailurePhase::Execution,
        crate::workflow::evidence::FailureCode::HarnessFailed,
        None,
        None,
        None,
        None,
    )
    .unwrap()
}

fn rate_limited_diagnostic() -> crate::workflow::publication::FailureDiagnosticV1 {
    crate::workflow::publication::FailureDiagnosticV1 {
        harness_error: Some("rate_limited".to_owned()),
        harness_failure: Some("model_error".to_owned()),
        http_status: Some(429),
        resets_at: Some("2025-10-24T20:00:00Z".to_owned()),
        ..Default::default()
    }
}

// REQ-LOCALRUN-198
#[test]
fn a_failed_nodes_diagnostic_reads_back_and_is_refused_anywhere_else() {
    let fixture = AdmittedFixture::from_source(
        "schemaVersion: 1\nsteps:\n  work:\n    kind: cmd\n    command:\n      argv: [\"false\"]\nfinalizers:\n  cleanup:\n    kind: cmd\n    command:\n      argv: [\"false\"]\n",
    );
    let run = InitialLocalRun::create(&fixture.run_path("diagnostic"), &fixture.admitted).unwrap();
    let pending = serde_json::to_value(read_state(run.root_handle()).unwrap()).unwrap();
    run.state
        .update(|state| {
            let attempt = current_attempt_mut(state)?;
            let settled = attempt.created_at.clone();
            attempt.started_at = Some(settled.clone());
            attempt.settled_at = Some(settled);
            attempt.settlement_snapshot = Some(fixture_settlement_snapshot());
            attempt.state = AttemptStateV1::WorkflowFailed;
            let step = &mut attempt.progress.steps[0];
            step.state = AttemptStepStateV1::Failed;
            step.detail = Some(NodeDetail::Failed(harness_failure_detail()));
            step.diagnostic = Some(rate_limited_diagnostic());
            let finalizers = vec![DurableFinalizerV1 {
                id: "cleanup".to_owned(),
                role: AttemptNodeRoleV1::Finalizer,
                failure_policy: FailurePolicy::Required,
                state: AttemptStepStateV1::Failed,
                outputs: Some(Vec::new()),
                detail: Some(NodeDetail::Failed(harness_failure_detail())),
                diagnostic: Some(rate_limited_diagnostic()),
            }];
            attempt.finalization = Some(AttemptFinalizationV1::Complete(
                AttemptFinalizationCompleteV1 {
                    complete: true,
                    trigger: FinalizationTriggerV1::Failed,
                    issues: durable_finalization_issues(&finalizers),
                    finalizers,
                    cancellation: None,
                    force_abort: false,
                },
            ));
            attempt.result = AttemptResultV1::NotPublished {
                reason: ResultAbsentReasonV1::PublicationPending,
            };
            Ok(())
        })
        .unwrap();
    let state = read_state(run.root_handle()).unwrap();
    let attempt = &state.attempts[0];
    assert_eq!(
        attempt.progress.steps[0].diagnostic,
        Some(rate_limited_diagnostic())
    );
    let Some(AttemptFinalizationV1::Complete(complete)) = &attempt.finalization else {
        panic!("finalization should be complete");
    };
    assert_eq!(
        complete.finalizers[0].diagnostic,
        Some(rate_limited_diagnostic())
    );
    let failed = serde_json::to_value(&state).unwrap();
    assert_eq!(decode_state(&json_bytes(failed.clone())).unwrap(), state);
    let step = "/attempts/0/progress/steps/0";
    let finalizer = "/attempts/0/finalization/finalizers/0";

    // A state an earlier `um` wrote carries no diagnostic anywhere and still reads.
    let mut earlier = failed.clone();
    for node in [step, finalizer] {
        earlier
            .pointer_mut(node)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .remove("diagnostic")
            .unwrap();
    }
    let earlier = decode_state(&json_bytes(earlier)).unwrap();
    assert_eq!(earlier.attempts[0].progress.steps[0].diagnostic, None);

    let refused = |document: &Value, pointer: &str, diagnostic: Value| {
        let mut document = document.clone();
        document.pointer_mut(pointer).unwrap()["diagnostic"] = diagnostic;
        decode_state(&json_bytes(document)).unwrap_err()
    };
    assert_eq!(
        refused(&failed, step, json!({})),
        LocalRunDirectoryError::AttemptStepDetailInvalid
    );
    assert_eq!(
        refused(&failed, finalizer, json!({})),
        LocalRunDirectoryError::AttemptFinalizationCompleteInvalid
    );
    let unknown = json!({"harnessFailure": "model_error", "unknown": 1});
    for pointer in [step, finalizer] {
        assert!(matches!(
            refused(&failed, pointer, unknown.clone()),
            LocalRunDirectoryError::Json { .. }
        ));
    }
    assert_eq!(
        refused(&pending, step, json!({"harnessFailure": "model_error"})),
        LocalRunDirectoryError::AttemptStepDetailInvalid
    );
}

/// The run's own guard journal, except that it refuses to register `refused`'s launch.
struct GuardRegistrationRefusal {
    store: Arc<StateStore>,
    refused: &'static str,
}

impl DurableProcessGuardStore for GuardRegistrationRefusal {
    fn register(
        &self,
        step: &str,
        action_id: u64,
        identity: &AuthenticatedProcessGroup,
    ) -> Result<String, ProcessGuardStoreError> {
        if step == self.refused {
            return Err(ProcessGuardStoreError);
        }
        self.store.register(step, action_id, identity)
    }

    fn mark_released(&self, guard_id: &str) -> Result<(), ProcessGuardStoreError> {
        self.store.mark_released(guard_id)
    }

    fn mark_quiesced(&self, guard_id: &str) -> Result<(), ProcessGuardStoreError> {
        self.store.mark_quiesced(guard_id)
    }
}

// REQ-LOCALRUN-198, REQ-DIAG-38
#[tokio::test]
async fn a_finalizer_refused_at_launch_records_its_diagnostic_when_finalization_completes() {
    let fixture = AdmittedFixture::from_source(
        "schemaVersion: 1\nsteps:\n  work:\n    kind: cmd\n    command: {argv: [\"/bin/sh\", \"-c\", \"exit 0\"]}\nfinalizers:\n  cleanup:\n    kind: cmd\n    command: {argv: [\"/bin/sh\", \"-c\", \"exit 0\"]}\n",
    );
    let run_path = fixture.run_path("refused-finalizer");
    let _writable = WritableTreeCopyOnDrop(run_path.clone());
    let run = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    let artifacts =
        ArtifactStaging::create(fixture.admitted.execution(), run.private_directory()).unwrap();
    let inputs = crate::workflow::input::InputStaging::create(
        fixture.admitted.execution(),
        run.private_directory(),
    )
    .unwrap();
    let diagnostics = crate::workflow::diagnostic::StepDiagnosticLog::default();
    let guards = ProcessGuardRegistry::durable(Arc::new(GuardRegistrationRefusal {
        store: Arc::clone(&run.state),
        refused: "cleanup",
    }));
    cloud_test_watchdog(crate::workflow::execution::execute_workflow(
        fixture.admitted.clone(),
        &artifacts,
        &inputs,
        &diagnostics,
        crate::workflow::step_runtime::AgentExecution::disabled(),
        crate::workflow::test_support::step_clock::TestClock,
        run.commit_port(
            diagnostics.clone(),
            InvocationAccountingLog::default(),
            artifacts.clone(),
        ),
        crate::workflow::observation::NoopExecutionObserver,
        crate::workflow::step_runtime::WorkflowExecutionStart::initial(guards),
    ))
    .await
    .unwrap();

    let document =
        serde_json::from_slice::<Value>(&fs::read(run_path.join(STATE_FILE)).unwrap()).unwrap();
    assert_eq!(
        document.pointer("/attempts/0/finalization/finalizers/0/diagnostic"),
        Some(&json!({"launchFailure": "guard_registration"}))
    );
    let state = read_state(run.root_handle()).unwrap();
    assert_eq!(serde_json::to_value(&state).unwrap(), document);
    let attempt = &state.attempts[0];
    let Some(AttemptFinalizationV1::Complete(complete)) = &attempt.finalization else {
        panic!(
            "finalization should be complete: {:?}",
            attempt.finalization
        );
    };
    let [cleanup] = complete.finalizers.as_slice() else {
        panic!("one finalizer should run: {:?}", complete.finalizers);
    };
    assert_eq!(cleanup.id, "cleanup");
    assert_eq!(cleanup.state, AttemptStepStateV1::Failed);
    let Some(NodeDetail::Failed(detail)) = &cleanup.detail else {
        panic!("the finalizer should carry a failure: {:?}", cleanup.detail);
    };
    assert_eq!(
        (detail.phase, detail.code),
        (
            crate::workflow::evidence::FailurePhase::Start,
            crate::workflow::evidence::FailureCode::CommandLaunchFailed
        )
    );
    let [work] = attempt.progress.steps.as_slice() else {
        panic!("one step should run: {:?}", attempt.progress.steps);
    };
    assert_eq!(
        (work.id.as_str(), work.state, &work.diagnostic),
        ("work", AttemptStepStateV1::Succeeded, &None)
    );
}

// REQ-LOCALRUN-201
#[test]
fn a_nodes_start_reads_back_and_is_refused_where_the_node_never_started() {
    let fixture = AdmittedFixture::from_source(
        "schemaVersion: 1\nsteps:\n  work:\n    kind: cmd\n    command:\n      argv: [\"false\"]\n",
    );
    let run = InitialLocalRun::create(&fixture.run_path("started-at"), &fixture.admitted).unwrap();
    let pending = serde_json::to_value(read_state(run.root_handle()).unwrap()).unwrap();
    let started = "2025-10-24T20:00:00.123456Z";
    run.state
        .update(|state| {
            let attempt = current_attempt_mut(state)?;
            let settled = attempt.created_at.clone();
            attempt.started_at = Some(settled.clone());
            attempt.settled_at = Some(settled);
            attempt.settlement_snapshot = Some(fixture_settlement_snapshot());
            attempt.state = AttemptStateV1::WorkflowFailed;
            let step = &mut attempt.progress.steps[0];
            step.state = AttemptStepStateV1::Failed;
            step.detail = Some(NodeDetail::Failed(harness_failure_detail()));
            step.started_at = Some(started.to_owned());
            attempt.result = AttemptResultV1::NotPublished {
                reason: ResultAbsentReasonV1::PublicationPending,
            };
            Ok(())
        })
        .unwrap();
    let state = read_state(run.root_handle()).unwrap();
    assert_eq!(
        state.attempts[0].progress.steps[0].started_at.as_deref(),
        Some(started)
    );
    let failed = serde_json::to_value(&state).unwrap();
    let step = "/attempts/0/progress/steps/0";
    assert_eq!(failed.pointer(step).unwrap()["startedAt"], started);
    assert_eq!(decode_state(&json_bytes(failed.clone())).unwrap(), state);

    // A state an earlier `um` wrote records no start and still reads.
    let mut earlier = failed.clone();
    earlier
        .pointer_mut(step)
        .unwrap()
        .as_object_mut()
        .unwrap()
        .remove("startedAt")
        .unwrap();
    let earlier = decode_state(&json_bytes(earlier)).unwrap();
    assert_eq!(earlier.attempts[0].progress.steps[0].started_at, None);

    let refused = |document: &Value, started_at: &str| {
        let mut document = document.clone();
        document.pointer_mut(step).unwrap()["startedAt"] = json!(started_at);
        decode_state(&json_bytes(document)).unwrap_err()
    };
    assert_eq!(
        refused(&pending, started),
        LocalRunDirectoryError::AttemptStepDetailInvalid
    );
    for invalid in [
        "2025-10-24T20:00:00+00:00",
        "2025-10-24T20:00:00",
        "yesterday",
        "",
    ] {
        assert_eq!(
            refused(&failed, invalid),
            LocalRunDirectoryError::AttemptStepDetailInvalid,
            "{invalid}"
        );
    }
}

// REQ-LOCALRUN-201
#[test]
fn a_nodes_start_is_recorded_once_when_it_becomes_active() {
    let fixture = AdmittedFixture::from_source(
        "schemaVersion: 1\nsteps:\n  work:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n  after:\n    kind: cmd\n    dependsOn: [work]\n    command:\n      argv: [\"true\"]\n",
    );
    let run =
        InitialLocalRun::create(&fixture.run_path("started-once"), &fixture.admitted).unwrap();
    let mut nodes = read_state(run.root_handle()).unwrap().attempts[0]
        .progress
        .steps
        .clone();
    let runtime = |work: StepState<CapturedValue>| {
        BTreeMap::from(
            [("work", work), ("after", StepState::Pending)].map(|(id, state)| {
                (
                    id.to_owned(),
                    super::super::runtime::StepRuntimeState::<StepFailureCause, CapturedValue> {
                        state,
                        current_action: None,
                        target_execution: None,
                        target_invocation: None,
                        active_invocation: None,
                        recovery: None,
                        condition_passed: false,
                        failure_kind: None,
                    },
                )
            }),
        )
    };
    let first = "2025-10-24T20:00:00Z";
    for (state, now) in [
        (StepState::Starting, first),
        (StepState::Running, "2025-10-24T20:00:01Z"),
        (StepState::CapturingOutputs, "2025-10-24T20:00:02Z"),
    ] {
        update_progress_nodes(&mut nodes, &runtime(state), &BTreeMap::new(), now).unwrap();
        assert_eq!(nodes[0].started_at.as_deref(), Some(first));
        assert_eq!(nodes[1].started_at, None);
    }
}

#[test]
fn schema_one_documents_without_continuation_foundation_fields_remain_readable() {
    let fixture = AdmittedFixture::new();
    let run =
        InitialLocalRun::create(&fixture.run_path("schema-one-old"), &fixture.admitted).unwrap();
    settle_as_succeeded(&run);

    let mut run_document: serde_json::Value =
        serde_json::from_slice(&read_regular_file(run.root_handle(), LOCAL_RUN_FILE).unwrap())
            .unwrap();
    run_document.as_object_mut().unwrap().remove("gitBaseline");
    assert!(decode_run(&json_bytes(run_document)).is_ok());

    let mut state_document: serde_json::Value =
        serde_json::from_slice(&read_regular_file(run.root_handle(), STATE_FILE).unwrap()).unwrap();
    let attempt = state_document["attempts"][0].as_object_mut().unwrap();
    attempt.remove("definition");
    attempt.remove("settlementSnapshot");
    for step in attempt["progress"]["steps"].as_array_mut().unwrap() {
        step.as_object_mut().unwrap().remove("outputs");
    }
    assert!(decode_state(&json_bytes(state_document)).is_ok());
}

#[test]
fn archived_attempt_loads_schema_one_state_without_retained_output_foundation() {
    let fixture = AdmittedFixture::new();
    let run_path = fixture.run_path("schema-one-old-archive");
    let run = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    settle_as_workflow_failed(&run);
    publish_result_fixture(&fixture, &run);

    let mut state: Value =
        serde_json::from_slice(&read_regular_file(run.root_handle(), STATE_FILE).unwrap()).unwrap();
    let attempt = state["attempts"][0].as_object_mut().unwrap();
    attempt.remove("definition");
    attempt.remove("settlementSnapshot");
    for step in attempt["progress"]["steps"].as_array_mut().unwrap() {
        step.as_object_mut().unwrap().remove("outputs");
    }
    fs::write(run_path.join(STATE_FILE), json_bytes(state)).unwrap();

    let archived = load_local_archived_attempt(&run_path, None).unwrap();
    assert_eq!(archived.projection.attempt_number, 1);
    assert_eq!(archived.projection.state, AttemptStateV1::WorkflowFailed);
}

// REQ-LOCALRUN-168, REQ-LOCALRUN-98
#[test]
fn run_retains_original_git_baseline_for_descendant_retries() {
    let fixture = AdmittedFixture::new_with_environment(EnvironmentSnapshot::new([(
        "PATH",
        std::env::var_os("PATH").unwrap(),
    )]));
    let git = |arguments: &[&str]| {
        let output = std::process::Command::new("git")
            .args(arguments)
            .current_dir(&fixture.execution_root)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {arguments:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        output.stdout
    };
    git(&["init", "--quiet"]);
    git(&["config", "user.name", "Baseline Fixture"]);
    git(&["config", "user.email", "baseline@example.invalid"]);
    git(&["config", "commit.gpgsign", "false"]);
    fs::write(fixture.execution_root.join("tracked"), b"original\n").unwrap();
    git(&["add", "tracked"]);
    git(&["commit", "--quiet", "-m", "original"]);
    let original = String::from_utf8(git(&["rev-parse", "HEAD"]))
        .unwrap()
        .trim()
        .to_owned();

    let run_path = fixture.run_path("git-baseline");
    let run = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    let retained = read_run(run.root_handle()).unwrap();
    assert_eq!(
        retained.git_baseline,
        Some(GitBaselineV1::Available {
            object_format: "sha1".to_owned(),
            commit_oid: original.clone(),
        })
    );
    // REQ-LOCALRUN-168, REQ-LOCALRUN-98: the identity record is `local-run.json`.
    let identity: Value =
        serde_json::from_slice(&fs::read(run_path.join("local-run.json")).unwrap()).unwrap();
    assert_eq!(
        identity["gitBaseline"],
        json!({"status": "available", "objectFormat": "sha1", "commitOid": original})
    );
    assert!(!run_path.join("run.json").exists());
    settle_as_workflow_failed(&run);
    drop(run);
    let LocalRetryOpen::Acquired(pending) = acquire_local_retry(&run_path).unwrap() else {
        panic!("failed attempt should be retryable");
    };
    let baseline = pending.git_baseline().unwrap().clone();

    fs::write(fixture.execution_root.join("tracked"), b"descendant\n").unwrap();
    git(&["add", "tracked"]);
    git(&["commit", "--quiet", "-m", "descendant"]);
    let capture = GitCaptureContext::admit_local_with_baseline(
        fixture.admitted.execution(),
        &baseline,
        &super::super::artifact::CaptureCancellation::default(),
    )
    .unwrap();
    assert_eq!(capture.baseline().commit_oid(), original);

    git(&["checkout", "--quiet", "--orphan", "rewritten"]);
    fs::write(fixture.execution_root.join("tracked"), b"rewritten\n").unwrap();
    git(&["add", "-A"]);
    git(&["commit", "--quiet", "-m", "rewritten"]);
    assert_eq!(
        GitCaptureContext::admit_local_with_baseline(
            fixture.admitted.execution(),
            &baseline,
            &super::super::artifact::CaptureCancellation::default(),
        )
        .unwrap_err(),
        GitWorkspaceAdmissionFailure::BaselineUnavailable
    );
    let missing = LocalGitBaseline::new(GitObjectFormat::Sha1, Arc::from("0".repeat(40))).unwrap();
    assert_eq!(
        GitCaptureContext::admit_local_with_baseline(
            fixture.admitted.execution(),
            &missing,
            &super::super::artifact::CaptureCancellation::default(),
        )
        .unwrap_err(),
        GitWorkspaceAdmissionFailure::BaselineUnavailable
    );
}

#[test]
fn retained_output_carriers_are_verified_and_orphans_are_removed() {
    let fixture = AdmittedFixture::new();
    let run =
        InitialLocalRun::create(&fixture.run_path("retained-output"), &fixture.admitted).unwrap();
    settle_as_succeeded(&run);
    let private = run.create_private_staging().unwrap();
    let artifacts = ArtifactStaging::create_bound(
        fixture.admitted.execution(),
        private.path(),
        private.root_handle(),
    )
    .unwrap();
    let attempts = open_directory_at(run.root_handle(), ATTEMPTS_DIRECTORY).unwrap();
    let attempt = open_directory_at(&attempts, "000001").unwrap();
    let values = create_or_open_directory(&attempt, VALUES_DIRECTORY).unwrap();
    let steps = create_or_open_directory(&values, "steps").unwrap();
    let first = create_or_open_directory(&steps, "first").unwrap();
    let retained = retain_output_value(
        &artifacts,
        &first,
        AttemptNodeRoleV1::Step,
        "first",
        "message",
        &crate::workflow::value::CapturedValue::text(Arc::from("evidence\n")),
    )
    .unwrap();
    sync_directory(&first).unwrap();
    sync_directory(&steps).unwrap();
    sync_directory(&values).unwrap();
    sync_directory(&attempt).unwrap();
    run.state
        .update(|state| {
            state.attempts[0].progress.steps[0].outputs = Some(vec![retained.clone()]);
            Ok(())
        })
        .unwrap();
    let state = read_state(run.root_handle()).unwrap();
    verify_retained_output_evidence(run.root_handle(), &state, 1).unwrap();

    let carrier = retained.carrier().unwrap();
    let carrier_path = run
        .run_directory()
        .join("attempts/000001")
        .join(&carrier.relative_path);
    let orphan = carrier_path.parent().unwrap().join("orphan");
    fs::write(&orphan, b"not committed").unwrap();
    cleanup_unreferenced_retained_values(run.root_handle(), &state, 1).unwrap();
    assert!(!orphan.exists());
    assert!(carrier_path.is_file());

    let mut permissions = fs::metadata(&carrier_path).unwrap().permissions();
    permissions.set_mode(0o600);
    fs::set_permissions(&carrier_path, permissions).unwrap();
    fs::write(&carrier_path, b"tampered\n").unwrap();
    assert!(matches!(
        verify_retained_output_evidence(run.root_handle(), &state, 1),
        Err(LocalRunDirectoryError::StateFile { path, source, .. })
            if path == carrier_path && matches!(*source, LocalRunDirectoryError::CarrierInvalid)
    ));
}

#[tokio::test]
async fn cloud_continuation_recovers_unexported_output_across_three_attempts() {
    let fixture = AdmittedFixture::from_source(
        "schemaVersion: 1\nsteps:\n  first:\n    kind: cmd\n    command: {argv: [\"true\"]}\n    outputs:\n      message: {kind: text, from: path, path: message.txt}\n      unused: {kind: text, from: path, path: unused.txt}\n  second:\n    kind: cmd\n    inputs:\n      prompt: {ref: outputs.first.message}\n    command: {argv: [\"/bin/sh\", \"-c\", \"IFS= read -r value < \\\"$UM_STEP_INPUTS/values/prompt\\\" || exit 1; printf '%s\\\\n' \\\"$value\\\" > consumed.txt\"]}\n",
    );
    let root = fixture.run_path("cloud-private-chain");
    fs::create_dir_all(&root).unwrap();
    // The clean suite deliberately aliases TMPDIR through a symlink; the
    // engine-bound context requires a physical, no-symlink absolute path.
    let root = fs::canonicalize(root).unwrap();
    let private1 = root.join("first");
    let private2 = root.join("second");
    fs::create_dir(&private1).unwrap();
    fs::create_dir(&private2).unwrap();
    let artifacts = ArtifactStaging::create(fixture.admitted.execution(), &private1).unwrap();
    let attempt1 = "atm_01k0z6r1w8f4jy2m7q9v3x5abc";
    let attempt2 = "atm_01k0z6r1w8f4jy2m7q9v3x5abd";
    let output = BTreeMap::from([
        (
            "message".to_owned(),
            CapturedValue::text(Arc::from("unexported evidence\n")),
        ),
        (
            "unused".to_owned(),
            CapturedValue::text(Arc::from("unused\n")),
        ),
    ]);
    let first: crate::workflow::execution::WorkflowExecutionResult =
        crate::workflow::execution::WorkflowExecutionResult {
            outcome: crate::workflow::runtime::RunOutcome::Succeeded,
            steps: BTreeMap::from([(
                "first".to_owned(),
                crate::workflow::runtime::StepState::Succeeded { outputs: output },
            )]),
            recoveries: BTreeMap::new(),
            output_producers: BTreeMap::new(),
            finalization_summary: None,
            force_abort: None,
            exports: BTreeMap::new(),
            provenance: fixture.admitted.workflow().source.clone(),
            content_digest: fixture.admitted.workflow().content_digest.clone(),
        };
    retain_cloud_completed_steps(
        &private1,
        attempt1,
        1,
        &first.steps,
        &first.output_producers,
        &artifacts,
        None,
    )
    .unwrap();
    assert!(!private1.join("cloud-retained-v1/state.json").exists());
    let first_marker: serde_json::Value = serde_json::from_slice(
        &fs::read(private1.join("cloud-retained-v1/commits/first.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(first_marker["steps"]["first"]["state"], "succeeded");
    retain_cloud_workflow_evidence(&private1, attempt1, 1, &first, &artifacts).unwrap();
    let mut roots = BTreeMap::from([(attempt1.to_owned(), private1.clone())]);
    let inherited = vec![serde_json::json!({
        "id":"first", "priorState":"succeeded", "definitionChanged": false,
    })];
    let reexecuted = vec!["second".to_owned()];
    let consumed = load_cloud_continuation_seed(
        &fixture.admitted,
        &artifacts,
        &roots,
        attempt1,
        1,
        &inherited,
        &reexecuted,
    )
    .unwrap();
    assert_eq!(consumed.inherited_step("first").unwrap().outputs.len(), 1);
    // B inherits A but does not read either output. C first consumes A.
    let second = load_cloud_continuation_seed(
        &fixture.admitted,
        &artifacts,
        &roots,
        attempt1,
        1,
        &inherited,
        &[],
    )
    .unwrap();
    let first_seed = second.inherited_step("first").unwrap();
    assert!(first_seed.outputs.is_empty());
    assert!(first_seed.producers.is_empty());
    let second_result: crate::workflow::execution::WorkflowExecutionResult =
        crate::workflow::execution::WorkflowExecutionResult {
            outcome: crate::workflow::runtime::RunOutcome::Succeeded,
            steps: BTreeMap::from([(
                "first".to_owned(),
                crate::workflow::runtime::StepState::Inherited {
                    detail: first_seed.detail.clone(),
                    disposition: first_seed.disposition,
                    outputs: first_seed.outputs.clone(),
                },
            )]),
            recoveries: BTreeMap::new(),
            output_producers: BTreeMap::new(),
            finalization_summary: None,
            force_abort: None,
            exports: BTreeMap::new(),
            provenance: fixture.admitted.workflow().source.clone(),
            content_digest: fixture.admitted.workflow().content_digest.clone(),
        };
    retain_cloud_completed_steps(
        &private2,
        attempt2,
        2,
        &second_result.steps,
        &second_result.output_producers,
        &artifacts,
        Some((&private1, attempt1, 1)),
    )
    .unwrap();
    let inherited_marker: serde_json::Value = serde_json::from_slice(
        &fs::read(private2.join("cloud-retained-v1/commits/first.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(
        inherited_marker["steps"]["first"]["references"][0]["attemptId"],
        attempt1
    );
    retain_cloud_continuation_evidence(
        &private2,
        attempt2,
        2,
        &second_result,
        &artifacts,
        Some((&private1, attempt1, 1)),
    )
    .unwrap();
    roots.insert(attempt2.to_owned(), private2.clone());
    let third = load_cloud_continuation_seed(
        &fixture.admitted,
        &artifacts,
        &roots,
        attempt2,
        2,
        &[serde_json::json!({"id":"first","priorState":"inherited","definitionChanged":false})],
        &reexecuted,
    )
    .unwrap();
    let third_seed = third.inherited_step("first").unwrap();
    assert_eq!(third_seed.producers["message"].attempt_id, attempt1);
    assert!(matches!(third_seed.outputs.get("message"),
        Some(CapturedValue::Text(value)) if value.as_str() == "unexported evidence\n"));
    assert!(
        fixture
            .admitted
            .execution()
            .environment()
            .variable(std::ffi::OsStr::new("UM_CONTINUATION_CONTEXT"))
            .is_none()
    );
    let digest = super::super::publication::DigestV1 {
        algorithm: "sha256".into(),
        value: "a".repeat(64),
    };
    let record = super::super::publication::cloud_continuation_record(
        serde_json::json!({"fromSteps":["second"],"definition":"inherited"}),
        reexecuted.clone(),
        vec![serde_json::json!({"id":"first","priorState":"inherited","definitionChanged":false})],
        digest.clone(), digest,
        super::super::publication::CloudContinuationEvidence {
            execution_root: "/retained/workspace".into(),
            prior_execution_root: "/retained/workspace".into(),
            start_snapshot: serde_json::json!({"algorithm":"git_worktree_sha256_v1","unavailable":"git_unavailable"}),
            prior_settlement_snapshot: None,
            modified: serde_json::json!("unknown"),
            quiescence: serde_json::json!({"groupsRecorded":0,"groupsTerminated":0,
                "groupsAbsent":0,"provenAt":"2026-08-02T12:01:42Z"}),
        },
    ).unwrap();
    let private3 = root.join("third");
    fs::create_dir(&private3).unwrap();
    let bound = bind_cloud_continuation_context(
        fixture.admitted.clone(),
        &private3,
        &record,
        Some((&private2, attempt2, 2)),
    )
    .unwrap();
    let path = bound
        .execution()
        .environment()
        .variable(std::ffi::OsStr::new("UM_CONTINUATION_CONTEXT"))
        .unwrap();
    assert_eq!(
        bound
            .execution()
            .environment()
            .variable(std::ffi::OsStr::new("SCHERZO_CONTINUATION_CONTEXT")),
        Some(path)
    );
    let context: serde_json::Value = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
    assert_eq!(
        context["inheritedOutputs"]["first"]["message"]["attemptId"],
        attempt1
    );
    assert_eq!(
        context["inheritedOutputs"]["first"]["unused"]["attemptId"],
        attempt1
    );
    let inputs =
        crate::workflow::input::InputStaging::create(bound.execution(), &private3).unwrap();
    let execute = |admitted, seed| async {
        crate::workflow::execution::execute_workflow(
            admitted,
            &artifacts,
            &inputs,
            &crate::workflow::diagnostic::StepDiagnosticLog::default(),
            crate::workflow::step_runtime::AgentExecution::disabled(),
            crate::workflow::test_support::step_clock::TestClock,
            crate::workflow::execution::NoopCommitPort,
            crate::workflow::observation::NoopExecutionObserver,
            crate::workflow::step_runtime::WorkflowExecutionStart::seeded(
                crate::workflow::process_group::ProcessGuardRegistry::default(),
                seed,
            ),
        )
        .await
        .unwrap()
    };
    let third_result = execute(bound.clone(), third).await;
    assert_eq!(
        third_result.outcome,
        crate::workflow::runtime::RunOutcome::Succeeded
    );
    assert!(matches!(
        third_result.steps["first"],
        StepState::Inherited { .. }
    ));
    assert!(matches!(
        third_result.steps["second"],
        StepState::Succeeded { .. }
    ));
    assert_eq!(
        fs::read(fixture.execution_root.join("consumed.txt")).unwrap(),
        b"unexported evidence\n"
    );
    fs::remove_file(fixture.execution_root.join("consumed.txt")).unwrap();
    let carrier = private1.join("cloud-retained-v1/values/steps/first/message");
    fs::set_permissions(&carrier, fs::Permissions::from_mode(0o600)).unwrap();
    fs::write(&carrier, b"tampered\n").unwrap();
    let unavailable = load_cloud_continuation_seed(
        &fixture.admitted,
        &artifacts,
        &roots,
        attempt2,
        2,
        &[serde_json::json!({"id":"first","priorState":"inherited","definitionChanged":false})],
        &reexecuted,
    )
    .unwrap();
    let inherited = unavailable.inherited_step("first").unwrap();
    assert!(inherited.outputs.is_empty());
    assert!(inherited.producers.is_empty());
    let failure = execute(bound.clone(), unavailable).await;
    assert!(matches!(
        failure.outcome,
        crate::workflow::runtime::RunOutcome::Failed { .. }
    ));
    assert!(!fixture.execution_root.join("consumed.txt").exists());

    // A protected producer's private directory can become unavailable after
    // the predecessor retained the reference. Do not reject the admitted
    // continuation or reexecute the inherited step on that account.
    roots.remove(attempt1);
    let missing_producer = load_cloud_continuation_seed(
        &fixture.admitted,
        &artifacts,
        &roots,
        attempt2,
        2,
        &[serde_json::json!({"id":"first","priorState":"inherited","definitionChanged":false})],
        &reexecuted,
    )
    .unwrap();
    let inherited = missing_producer.inherited_step("first").unwrap();
    assert!(inherited.outputs.is_empty());
    assert!(inherited.producers.is_empty());
    let missing = execute(bound, missing_producer).await;
    assert!(matches!(
        missing.outcome,
        crate::workflow::runtime::RunOutcome::Failed { .. }
    ));
    assert!(!fixture.execution_root.join("consumed.txt").exists());
}

#[test]
fn inherited_seed_loads_required_values_and_preserves_full_durable_references() {
    let fixture = AdmittedFixture::from_source(
        "schemaVersion: 1\nsteps:\n  first:\n    kind: cmd\n    command: {argv: [\"true\"]}\n    outputs:\n      message:\n        kind: text\n        from: path\n        path: message.txt\n      unused:\n        kind: text\n        from: path\n        path: unused.txt\n",
    );
    let run = InitialLocalRun::create(
        &fixture.run_path("inherited-output-chain"),
        &fixture.admitted,
    )
    .unwrap();
    let private = run.create_private_staging().unwrap();
    let artifacts = ArtifactStaging::create_bound(
        fixture.admitted.execution(),
        private.path(),
        private.root_handle(),
    )
    .unwrap();
    let mut state = read_state(run.root_handle()).unwrap();
    let producer_attempt_id = state.attempts[0].attempt_id.clone();
    let producer = crate::workflow::runtime::OutputProducer {
        attempt_id: producer_attempt_id.clone(),
        attempt_number: 1,
        node: "first".to_owned(),
        output: "message".to_owned(),
    };
    let unused_producer = crate::workflow::runtime::OutputProducer {
        output: "unused".to_owned(),
        ..producer.clone()
    };
    let producers = BTreeMap::from([
        (("first".to_owned(), "message".to_owned()), producer.clone()),
        (("first".to_owned(), "unused".to_owned()), unused_producer),
    ]);
    let attempts = open_directory_at(run.root_handle(), ATTEMPTS_DIRECTORY).unwrap();
    let attempt = open_directory_at(&attempts, "000001").unwrap();
    let values = create_or_open_directory(&attempt, VALUES_DIRECTORY).unwrap();
    let steps = create_or_open_directory(&values, "steps").unwrap();
    let first = create_or_open_directory(&steps, "first").unwrap();
    let source_message = retain_output_value_with_producer(
        &artifacts,
        &first,
        AttemptNodeRoleV1::Step,
        "first",
        "message",
        &crate::workflow::value::CapturedValue::text(Arc::from("evidence\n")),
        None,
    )
    .unwrap();
    let source_unused = retain_output_value_with_producer(
        &artifacts,
        &first,
        AttemptNodeRoleV1::Step,
        "first",
        "unused",
        &crate::workflow::value::CapturedValue::text(Arc::from("unused\n")),
        None,
    )
    .unwrap();
    sync_directory(&first).unwrap();
    sync_directory(&steps).unwrap();
    sync_directory(&values).unwrap();
    sync_directory(&attempt).unwrap();
    state.attempts[0].progress.steps[0].state = AttemptStepStateV1::Succeeded;
    state.attempts[0].progress.steps[0].outputs = Some(vec![source_message, source_unused]);
    let retained = inherited_retained_outputs(
        &state,
        2,
        "first",
        &BTreeMap::from([
            (
                "message".to_owned(),
                crate::workflow::value::CapturedValue::text(Arc::from("evidence\n")),
            ),
            (
                "unused".to_owned(),
                crate::workflow::value::CapturedValue::text(Arc::from("unused\n")),
            ),
        ]),
        &producers,
    )
    .unwrap();
    let retained_message = retained
        .iter()
        .find(|output| output.name() == "message")
        .unwrap()
        .clone();

    let run_metadata = read_run(run.root_handle()).unwrap();
    let mut second = fresh_attempt(
        &fixture.admitted,
        2,
        AttemptTriggerV1::Continuation,
        Some(1),
        attempt_definition_for_run(&run_metadata),
        timestamp(um_support::utc_now()).unwrap(),
    )
    .unwrap();
    second.progress.steps[0].state = AttemptStepStateV1::Inherited;
    second.progress.steps[0].detail = Some(NodeDetail::Inherited(
        crate::workflow::evidence::InheritedDetail {
            prior_attempt_id: producer_attempt_id,
            prior_attempt_number: 1,
            prior_state: crate::workflow::evidence::InheritedPriorState::Succeeded,
            definition_changed: false,
        },
    ));
    second.progress.steps[0].outputs = Some(retained.clone());
    let second_attempt_id = second.attempt_id.clone();
    state.attempts.push(second);

    let mut third = fresh_attempt(
        &fixture.admitted,
        3,
        AttemptTriggerV1::Continuation,
        Some(2),
        attempt_definition_for_run(&run_metadata),
        timestamp(um_support::utc_now()).unwrap(),
    )
    .unwrap();
    third.progress.steps[0].state = AttemptStepStateV1::Inherited;
    third.progress.steps[0].detail = Some(NodeDetail::Inherited(
        crate::workflow::evidence::InheritedDetail {
            prior_attempt_id: second_attempt_id,
            prior_attempt_number: 2,
            prior_state: crate::workflow::evidence::InheritedPriorState::Inherited,
            definition_changed: true,
        },
    ));
    third.progress.steps[0].outputs = Some(retained.clone());
    state.attempts.push(third);
    state.current_attempt_number = 3;

    let replacement_source = "schemaVersion: 1\nsteps:\n  first:\n    kind: cmd\n    command: {argv: [\"true\"]}\n    outputs:\n      message:\n        kind: text\n        from: path\n        path: message.txt\n      added:\n        kind: text\n        from: path\n        path: added.txt\nexports:\n  message:\n    ref: outputs.first.message\n";
    fs::write(
        fixture
            .admitted
            .workflow()
            .source
            .source_root
            .join("workflow.yaml"),
        replacement_source,
    )
    .unwrap();
    let replacement = admit_workflow(
        resolution::resolve(
            &fixture.admitted.workflow().source.source_root,
            Path::new("workflow.yaml"),
        )
        .unwrap(),
        ResolvedInputs::default(),
        ExecutionContext::new(
            fixture.execution_root.clone(),
            ExecutionPolicyLimits::new(
                2,
                CaptureLimits::new(16, 1024, 4096),
                InputLimits::new(16, 1024, 4096, 4096),
                1024,
            ),
            EnvironmentSnapshot::default(),
            CancellationPolicy::new(CancellationSource::new(), Duration::from_secs(10)),
        ),
    )
    .unwrap();
    let current = Mutex::new(state);
    let seed =
        load_execution_seed(run.root_handle(), &current, 3, &replacement, &artifacts).unwrap();
    let inherited = seed.inherited_step("first").unwrap();
    assert_eq!(inherited.disposition, InheritedDisposition::Succeeded);
    assert_eq!(inherited.producers["message"], producer);
    assert!(matches!(
        &inherited.outputs["message"],
        CapturedValue::Text(value) if value.as_str() == "evidence\n"
    ));
    assert!(!inherited.outputs.contains_key("unused"));

    let reduction = super::super::runtime::initialize_seeded_with_operation::<
        (),
        StepFailureCause,
        CapturedValue,
        (),
    >(&replacement, seed.clone(), None);
    let mut durable_nodes = lock_state(&current).unwrap().attempts[2]
        .progress
        .steps
        .clone();
    update_progress_nodes(
        &mut durable_nodes,
        &reduction.state.steps,
        &BTreeMap::from([(
            (AttemptNodeRoleV1::Step, "first".to_owned()),
            vec![retained_message],
        )]),
        "2025-10-24T20:00:00Z",
    )
    .unwrap();
    assert_eq!(durable_nodes[0].outputs.as_ref(), Some(&retained));
    assert_eq!(durable_nodes[0].started_at, None);
    assert!(
        !run.run_directory()
            .join("attempts/000002/values/steps/first/message")
            .exists()
    );

    let staged_carrier = inherited.outputs["message"]
        .private_capture_carrier()
        .unwrap();
    let staged = fstat(artifacts.open_artifact(staged_carrier.handle()).unwrap()).unwrap();
    let original_path = run
        .run_directory()
        .join("attempts/000001/values/steps/first/message");
    let original = rustix::fs::stat(&original_path).unwrap();
    assert_eq!(staged.st_dev, original.st_dev);
    assert_eq!(staged.st_ino, original.st_ino);
}

// INV-ENGINE-11
#[test]
fn continuation_validation_binds_inheritance_to_the_immediate_prior_step() {
    let fixture = AdmittedFixture::from_source(
        "schemaVersion: 1
steps:
  first:
    kind: cmd
    command: {argv: [\"true\"]}
    outputs:
      message:
        kind: text
        from: path
        path: message.txt
  second:
    kind: cmd
    dependsOn: [first]
    command: {argv: [\"true\"]}
",
    );
    let run = InitialLocalRun::create(
        &fixture.run_path("continuation-prior-step-binding"),
        &fixture.admitted,
    )
    .unwrap();
    settle_as_succeeded(&run);
    let mut state = read_state(run.root_handle()).unwrap();
    let run_metadata = read_run(run.root_handle()).unwrap();
    let producer_attempt_id = state.attempts[0].attempt_id.clone();
    let source = RetainedOutputV1::Text {
        name: "message".to_owned(),
        producer: None,
        carrier: RetainedCarrierV1 {
            relative_path: retained_value_relative_path(
                AttemptNodeRoleV1::Step,
                "first",
                "message",
            ),
            media_type: "text/plain; charset=utf-8".to_owned(),
            size_bytes: 9,
            digest: DigestV1::sha256(b"evidence\n"),
        },
    };
    state.attempts[0].progress.steps[0].outputs = Some(vec![source.clone()]);

    let mut second = fresh_attempt(
        &fixture.admitted,
        2,
        AttemptTriggerV1::ExplicitRetry,
        Some(1),
        attempt_definition_for_run(&run_metadata),
        timestamp(um_support::utc_now()).unwrap(),
    )
    .unwrap();
    let second_settled = second.created_at.clone();
    second.started_at = Some(second_settled.clone());
    second.settled_at = Some(second_settled);
    second.settlement_snapshot = Some(fixture_settlement_snapshot());
    let second_settlement_snapshot = second.settlement_snapshot.clone().unwrap();
    second.state = AttemptStateV1::Succeeded;
    second.progress.steps[0].state = AttemptStepStateV1::Skipped;
    second.progress.steps[0].detail = Some(NodeDetail::Skipped(
        crate::workflow::evidence::ConditionFalseDetail::new([
            crate::workflow::evidence::EvaluatedPredicateEvidence {
                path: String::new(),
                result: false,
            },
        ])
        .unwrap(),
    ));
    second.progress.steps[1].state = AttemptStepStateV1::Succeeded;
    second.result = AttemptResultV1::NotPublished {
        reason: ResultAbsentReasonV1::PublicationPending,
    };
    let second_attempt_id = second.attempt_id.clone();
    let second_execution_root = second.execution_root.clone();
    state.attempts.push(second);

    let mut third = fresh_attempt(
        &fixture.admitted,
        3,
        AttemptTriggerV1::Continuation,
        Some(2),
        attempt_definition_for_run(&run_metadata),
        timestamp(um_support::utc_now()).unwrap(),
    )
    .unwrap();
    third.progress.steps[0].state = AttemptStepStateV1::Inherited;
    third.progress.steps[0].detail = Some(NodeDetail::Inherited(
        crate::workflow::evidence::InheritedDetail {
            prior_attempt_id: second_attempt_id.clone(),
            prior_attempt_number: 2,
            prior_state: crate::workflow::evidence::InheritedPriorState::Skipped,
            definition_changed: false,
        },
    ));
    let execution_root = third.execution_root.clone();
    third.continuation = Some(
        serde_json::from_value(json!({
            "request": {
                "fromSteps": ["second"],
                "definition": "inherited"
            },
            "fromSteps": ["second"],
            "reexecutedSteps": ["second"],
            "inheritedSteps": [{
                "id": "first",
                "priorState": "skipped",
                "definitionChanged": false
            }],
            "definitionSource": {
                "kind": "inherited",
                "manifestDigest": {"algorithm": "sha256", "value": "2".repeat(64)},
                "priorManifestDigest": {"algorithm": "sha256", "value": "2".repeat(64)}
            },
            "workspace": {
                "executionRoot": execution_root,
                "priorExecutionRoot": second_execution_root,
                "startSnapshot": {
                    "algorithm": "git_worktree_sha256_v1",
                    "unavailable": "not_work_tree"
                },
                "priorSettlementSnapshot": second_settlement_snapshot,
                "modified": "unknown",
                "quiescence": {
                    "groupsRecorded": 0,
                    "groupsTerminated": 0,
                    "groupsAbsent": 0,
                    "provenAt": "2026-08-02T12:01:42Z"
                }
            }
        }))
        .unwrap(),
    );
    state.current_attempt_number = 3;
    state.attempts.push(third);
    assert_eq!(validate_state(&state), Ok(()));

    let mut missing_full_reexecution_record = state.clone();
    let current = missing_full_reexecution_record.attempts.last_mut().unwrap();
    current.continuation = None;
    current.progress.steps[0].state = AttemptStepStateV1::Pending;
    current.progress.steps[0].detail = None;
    current.progress.steps[0].outputs = None;
    assert_eq!(
        validate_state(&missing_full_reexecution_record),
        Err(LocalRunDirectoryError::AttemptStepOutputsInvalid)
    );

    let mut explicit_request_root = state.clone();
    let execution_root = explicit_request_root.attempts[2]
        .continuation
        .as_ref()
        .unwrap()
        .workspace
        .execution_root
        .clone();
    explicit_request_root.attempts[2]
        .continuation
        .as_mut()
        .unwrap()
        .request
        .execution_root = Some(execution_root);
    assert_eq!(validate_state(&explicit_request_root), Ok(()));

    let mut mismatched_request_root = state.clone();
    mismatched_request_root
        .attempts
        .last_mut()
        .unwrap()
        .continuation
        .as_mut()
        .unwrap()
        .request
        .execution_root = Some("/different-execution-root".to_owned());
    assert_eq!(
        validate_state(&mismatched_request_root),
        Err(LocalRunDirectoryError::AttemptContinuationInvalid)
    );

    let mut mismatched_prior_workspace = state.clone();
    mismatched_prior_workspace
        .attempts
        .last_mut()
        .unwrap()
        .continuation
        .as_mut()
        .unwrap()
        .workspace
        .prior_execution_root = "/different-prior-root".to_owned();
    assert_eq!(
        validate_state(&mismatched_prior_workspace),
        Err(LocalRunDirectoryError::AttemptContinuationInvalid)
    );

    let mut missing_prior_snapshot = state.clone();
    missing_prior_snapshot
        .attempts
        .last_mut()
        .unwrap()
        .continuation
        .as_mut()
        .unwrap()
        .workspace
        .prior_settlement_snapshot = None;
    assert_eq!(
        validate_state(&missing_prior_snapshot),
        Err(LocalRunDirectoryError::AttemptContinuationInvalid)
    );

    let mut mismatched_prior_state = state.clone();
    let current = mismatched_prior_state.attempts.last_mut().unwrap();
    let Some(NodeDetail::Inherited(detail)) = &mut current.progress.steps[0].detail else {
        panic!("fixture must contain inherited detail");
    };
    detail.prior_state = crate::workflow::evidence::InheritedPriorState::Succeeded;
    current.continuation.as_mut().unwrap().inherited_steps[0].prior_state =
        crate::workflow::evidence::InheritedPriorState::Succeeded;
    assert_eq!(
        validate_state(&mismatched_prior_state),
        Err(LocalRunDirectoryError::AttemptInheritedStepInvalid)
    );

    let mut impossible_skipped_output = state.clone();
    let mut inherited = source.clone();
    inherited.set_producer(crate::workflow::runtime::OutputProducer {
        attempt_id: producer_attempt_id.clone(),
        attempt_number: 1,
        node: "first".to_owned(),
        output: "message".to_owned(),
    });
    impossible_skipped_output.attempts[2].progress.steps[0].outputs = Some(vec![inherited]);
    assert_eq!(
        validate_state(&impossible_skipped_output),
        Err(LocalRunDirectoryError::AttemptInheritedStepInvalid)
    );

    let mut flattened_chain = state.clone();
    let initial_attempt_id = flattened_chain.attempts[0].attempt_id.clone();
    let initial_execution_root = flattened_chain.attempts[0].execution_root.clone();
    let initial_settlement_snapshot = flattened_chain.attempts[0].settlement_snapshot.clone();
    let mut flattened = source.clone();
    flattened.set_producer(crate::workflow::runtime::OutputProducer {
        attempt_id: initial_attempt_id.clone(),
        attempt_number: 1,
        node: "first".to_owned(),
        output: "message".to_owned(),
    });
    let mut second_record = flattened_chain.attempts[2].continuation.clone().unwrap();
    let second = &mut flattened_chain.attempts[1];
    second.trigger = AttemptTriggerV1::Continuation;
    second.progress.steps[0].state = AttemptStepStateV1::Inherited;
    second.progress.steps[0].detail = Some(NodeDetail::Inherited(
        crate::workflow::evidence::InheritedDetail {
            prior_attempt_id: initial_attempt_id,
            prior_attempt_number: 1,
            prior_state: crate::workflow::evidence::InheritedPriorState::Succeeded,
            definition_changed: false,
        },
    ));
    second.progress.steps[0].outputs = Some(vec![flattened.clone()]);
    second_record.inherited_steps[0].prior_state =
        crate::workflow::evidence::InheritedPriorState::Succeeded;
    second_record.workspace.prior_execution_root = initial_execution_root;
    second_record.workspace.prior_settlement_snapshot = initial_settlement_snapshot;
    second.continuation = Some(second_record);
    let third = &mut flattened_chain.attempts[2];
    let Some(NodeDetail::Inherited(detail)) = &mut third.progress.steps[0].detail else {
        panic!("fixture must contain inherited detail");
    };
    detail.prior_state = crate::workflow::evidence::InheritedPriorState::Inherited;
    third.continuation.as_mut().unwrap().inherited_steps[0].prior_state =
        crate::workflow::evidence::InheritedPriorState::Inherited;
    third.progress.steps[0].outputs = Some(vec![flattened]);
    assert_eq!(validate_state(&flattened_chain), Ok(()));

    let mut false_flattening = flattened_chain;
    false_flattening.attempts[2].progress.steps[0]
        .outputs
        .as_mut()
        .unwrap()[0]
        .set_producer(crate::workflow::runtime::OutputProducer {
            attempt_id: second_attempt_id.clone(),
            attempt_number: 2,
            node: "first".to_owned(),
            output: "message".to_owned(),
        });
    assert_eq!(
        validate_state(&false_flattening),
        Err(LocalRunDirectoryError::AttemptInheritedOutputInvalid)
    );

    let mut direct_prior_producer = state;
    direct_prior_producer.attempts[1].progress.steps[0].state = AttemptStepStateV1::Succeeded;
    direct_prior_producer.attempts[1].progress.steps[0].detail = None;
    direct_prior_producer.attempts[1].progress.steps[0].outputs = Some(vec![source.clone()]);
    let current = &mut direct_prior_producer.attempts[2];
    let Some(NodeDetail::Inherited(detail)) = &mut current.progress.steps[0].detail else {
        panic!("fixture must contain inherited detail");
    };
    detail.prior_state = crate::workflow::evidence::InheritedPriorState::Succeeded;
    current.continuation.as_mut().unwrap().inherited_steps[0].prior_state =
        crate::workflow::evidence::InheritedPriorState::Succeeded;
    let mut inherited = source;
    inherited.set_producer(crate::workflow::runtime::OutputProducer {
        attempt_id: second_attempt_id,
        attempt_number: 2,
        node: "first".to_owned(),
        output: "message".to_owned(),
    });
    current.progress.steps[0].outputs = Some(vec![inherited]);
    assert_eq!(validate_state(&direct_prior_producer), Ok(()));

    let mut stale_producer = direct_prior_producer;
    stale_producer.attempts[2].progress.steps[0]
        .outputs
        .as_mut()
        .unwrap()[0]
        .set_producer(crate::workflow::runtime::OutputProducer {
            attempt_id: producer_attempt_id,
            attempt_number: 1,
            node: "first".to_owned(),
            output: "message".to_owned(),
        });
    assert_eq!(
        validate_state(&stale_producer),
        Err(LocalRunDirectoryError::AttemptInheritedOutputInvalid)
    );
}

#[test]
fn retained_output_verification_rejects_a_fifo_without_waiting_for_a_writer() {
    let fixture = AdmittedFixture::from_source(
        "schemaVersion: 1\nsteps:\n  first:\n    kind: cmd\n    command: {argv: [\"true\"]}\n    outputs:\n      message:\n        kind: text\n        from: path\n        path: message.txt\n",
    );
    let run =
        InitialLocalRun::create(&fixture.run_path("retained-fifo"), &fixture.admitted).unwrap();
    settle_as_succeeded(&run);
    retain_text_output_for_step(&run, "first", "message", b"evidence\n");
    let state = read_state(run.root_handle()).unwrap();
    let carrier = run
        .run_directory()
        .join("attempts/000001/values/steps/first/message");
    fs::remove_file(&carrier).unwrap();
    nix::unistd::mkfifo(
        &carrier,
        nix::sys::stat::Mode::S_IRUSR | nix::sys::stat::Mode::S_IWUSR,
    )
    .unwrap();

    assert!(matches!(
        verify_retained_output_evidence(run.root_handle(), &state, 1),
        Err(LocalRunDirectoryError::StateFile { path, source, .. })
            if path == carrier && matches!(*source, LocalRunDirectoryError::CarrierInvalid)
    ));
}

#[test]
fn retry_rejects_incomplete_succeeded_outputs_before_orphan_cleanup() {
    let fixture = AdmittedFixture::from_source(
        "schemaVersion: 1\nsteps:\n  first:\n    kind: cmd\n    command: {argv: [\"true\"]}\n    outputs:\n      message:\n        kind: text\n        from: path\n        path: message.txt\n  second:\n    kind: cmd\n    dependsOn: [first]\n    command: {argv: [\"false\"]}\n",
    );
    let run_path = fixture.run_path("retry-incomplete-output-set");
    let run = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    settle_as_workflow_failed(&run);
    run.state
        .update(|state| {
            let steps = &mut current_attempt_mut(state)?.progress.steps;
            let failed_detail = steps[0].detail.take();
            steps[0].state = AttemptStepStateV1::Succeeded;
            steps[0].outputs = Some(Vec::new());
            steps[1].state = AttemptStepStateV1::Failed;
            steps[1].detail = failed_detail;
            Ok(())
        })
        .unwrap();
    let carrier = run_path.join("attempts/000001/values/steps/first/message");
    fs::create_dir_all(carrier.parent().unwrap()).unwrap();
    fs::write(&carrier, b"producer evidence\n").unwrap();
    drop(run);

    assert!(matches!(
        acquire_local_retry(&run_path),
        Err(LocalRunDirectoryError::StateInvalid)
    ));
    assert_eq!(fs::read(carrier).unwrap(), b"producer evidence\n");
    assert!(!run_path.join("attempts/000002").exists());
}

// REQ-WFFINALIZE-3
#[test]
fn retry_commits_only_fresh_attempt_state_and_retained_inputs() {
    let fixture = AdmittedFixture::new();
    let run_path = fixture.run_path("retry-fresh");
    let initial = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    settle_as_workflow_failed(&initial);
    let documents = read_state(initial.root_handle()).unwrap();
    let predecessor = documents.attempts[0].clone();
    assert_eq!(
        predecessor.definition.as_ref().unwrap().locator,
        AttemptDefinitionLocatorV1::Run
    );
    assert_eq!(
        predecessor.settlement_snapshot.as_ref().unwrap().settled_by,
        Some(WorkspaceSnapshotSettlementV1::Engine)
    );
    drop(initial);

    let LocalRetryOpen::Acquired(pending) = acquire_local_retry(&run_path).unwrap() else {
        panic!("failed attempt should be retryable");
    };
    let pending = *pending;
    let (_, inputs, maximum_parallel_steps) = pending.execution_specification();
    assert_eq!(maximum_parallel_steps, 2);
    assert!(matches!(
        inputs.get("request"),
        Some(ResolvedInput::Text(value)) if value.as_ref() == "durable request\n"
    ));
    let Some(ResolvedInput::Attachments(attachments)) = inputs.get("evidence") else {
        panic!("retained named attachment collection is missing");
    };
    assert_eq!(attachments[0].bytes(), [0_u8, 1, 0xff]);
    let Some(ResolvedInput::Json(settings)) = inputs.get("settings") else {
        panic!("retained named JSON input is missing");
    };
    assert_eq!(settings.source(), b"{ \"z\": null, \"n\": 1.2300 }\n");
    assert_eq!(settings.canonical(), b"{\"n\":1.2300,\"z\":null}");
    assert!(settings.value()["z"].is_null());
    let retry = pending.begin(&fixture.admitted).unwrap_or_else(|_| {
        panic!("eligible retry should commit");
    });

    assert_eq!(retry.attempt_number(), 2);
    let state = read_state(retry.root_handle()).unwrap();
    assert_eq!(state.current_attempt_number, 2);
    assert_eq!(state.attempts[0], predecessor);
    let attempt = &state.attempts[1];
    assert_eq!(
        retry.finalization_identity().unwrap(),
        (state.local_run_id.clone(), attempt.attempt_id.clone())
    );
    assert_ne!(attempt.attempt_id, predecessor.attempt_id);
    assert_eq!(attempt.trigger, AttemptTriggerV1::ExplicitRetry);
    assert_eq!(attempt.prior_attempt_number, Some(1));
    assert_eq!(
        attempt.definition.as_ref().unwrap().locator,
        AttemptDefinitionLocatorV1::Run
    );
    assert_eq!(attempt.state, AttemptStateV1::Created);
    assert_eq!(attempt.progress.accepted_occurrence_ordinal, 0);
    assert_eq!(attempt.progress.last_transition_sequence, 0);
    assert!(attempt.progress.outstanding_actions.is_empty());
    assert!(attempt.process_guards.is_empty());
    assert!(
        attempt
            .progress
            .steps
            .iter()
            .all(|step| step.state == AttemptStepStateV1::Pending)
    );
    assert!(run_path.join("attempts/000002").is_dir());
    assert!(!run_path.join("attempts/000002/result").exists());
}

#[test]
fn attempt_definition_locator_resolves_attempt_retention_without_run_fallback() {
    let fixture = AdmittedFixture::new();
    let run_path = fixture.run_path("attempt-definition");
    let initial = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    settle_as_workflow_failed(&initial);
    drop(initial);
    let LocalRetryOpen::Acquired(pending) = acquire_local_retry(&run_path).unwrap() else {
        panic!("failed attempt should be retryable");
    };
    let retry = pending.begin(&fixture.admitted).unwrap_or_else(|_| {
        panic!("retry should commit");
    });
    settle_as_workflow_failed(&retry);

    let attempts = open_directory_at(retry.root_handle(), ATTEMPTS_DIRECTORY).unwrap();
    let attempt = open_directory_at(&attempts, "000002").unwrap();
    mkdir(&attempt, WORKFLOW_DIRECTORY).unwrap();
    let workflow = open_directory_at(&attempt, WORKFLOW_DIRECTORY).unwrap();
    mkdir(&workflow, WORKFLOW_FILES_DIRECTORY).unwrap();
    let files = open_directory_at(&workflow, WORKFLOW_FILES_DIRECTORY).unwrap();
    let manifest = retain_execution_specification(&files, &fixture.admitted).unwrap();
    let manifest_bytes = encode_json(&manifest).unwrap();
    write_new_immutable_file(&workflow, WORKFLOW_MANIFEST_FILE, &manifest_bytes).unwrap();
    sync_directory(&files).unwrap();
    sync_directory(&workflow).unwrap();
    sync_directory(&attempt).unwrap();
    retry
        .state
        .update(|state| {
            let prior = state.attempts[0].clone();
            let current = current_attempt_mut(state)?;
            let manifest_digest = DigestV1::sha256(&manifest_bytes);
            current.trigger = AttemptTriggerV1::Continuation;
            current.definition = Some(AttemptDefinitionV1 {
                digest: DigestV1 {
                    algorithm: fixture
                        .admitted
                        .workflow()
                        .content_digest
                        .algorithm
                        .as_str()
                        .to_owned(),
                    value: fixture.admitted.workflow().content_digest.value.clone(),
                },
                manifest_digest: manifest_digest.clone(),
                locator: AttemptDefinitionLocatorV1::Attempt { attempt_number: 2 },
            });
            current.continuation = Some(
                serde_json::from_value(json!({
                    "request": {
                        "fromSteps": ["first"],
                        "definition": {
                            "replaced": {
                                "path": fixture.admitted.workflow().source.source_root.join("workflow.yaml")
                            }
                        }
                    },
                    "fromSteps": ["first"],
                    "reexecutedSteps": ["first", "second"],
                    "inheritedSteps": [],
                    "definitionSource": {
                        "kind": "replaced",
                        "manifestDigest": manifest_digest,
                        "priorManifestDigest": prior.definition.as_ref().unwrap().manifest_digest
                    },
                    "workspace": {
                        "executionRoot": current.execution_root,
                        "priorExecutionRoot": prior.execution_root,
                        "startSnapshot": {
                            "algorithm": "git_worktree_sha256_v1",
                            "unavailable": "not_work_tree"
                        },
                        "priorSettlementSnapshot": prior.settlement_snapshot,
                        "modified": "unknown",
                        "quiescence": {
                            "groupsRecorded": 0,
                            "groupsTerminated": 0,
                            "groupsAbsent": 0,
                            "provenAt": "2026-08-02T12:01:42Z"
                        }
                    }
                }))
                .unwrap(),
            );
            Ok(())
        })
        .unwrap();

    let retained_run = read_run(retry.root_handle()).unwrap();
    let retained_state = read_state(retry.root_handle()).unwrap();
    validate_run_state_pair(&retained_run, &retained_state).unwrap();
    let run_source = run_path.join("workflow/files/0001");
    let mut permissions = fs::metadata(&run_source).unwrap().permissions();
    permissions.set_mode(0o600);
    fs::set_permissions(&run_source, permissions).unwrap();
    fs::write(run_source, b"corrupted run-level closure\n").unwrap();

    let mut budget = RetainedReadBudget::with_bytes(0).unwrap();
    let (resolved, _, maximum_parallel_steps) = load_attempt_retained_execution_with_budget(
        retry.root_handle(),
        &retained_run,
        &retained_state,
        &retained_state.attempts[1],
        &mut budget,
    )
    .unwrap();
    assert_eq!(
        resolved.content_digest,
        fixture.admitted.workflow().content_digest
    );
    assert_eq!(maximum_parallel_steps, 2);
}

#[test]
fn finalizer_retry_uses_fresh_identity_and_omits_prior_finalization_bytes() {
    let fixture = AdmittedFixture::from_source(
        "schemaVersion: 1\nsteps:\n  work:\n    kind: cmd\n    command:\n      argv: [\"false\"]\nfinalizers:\n  cleanup:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n",
    );
    let run_path = fixture.run_path("finalizer-retry-fresh");
    let initial = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    assert!(
        read_state(initial.root_handle()).unwrap().attempts[0]
            .finalization
            .is_none(),
        "an untriggered finalizer graph must not synthesize progress"
    );
    assert_eq!(initial.finalizers.len(), 1);

    initial
        .state
        .update(|state| {
            let attempt = current_attempt_mut(state)?;
            let settled = attempt.created_at.clone();
            attempt.started_at = Some(settled.clone());
            attempt.settled_at = Some(settled);
            attempt.settlement_snapshot = Some(fixture_settlement_snapshot());
            attempt.state = AttemptStateV1::WorkflowFailed;
            attempt.progress.steps[0].state = AttemptStepStateV1::Failed;
            attempt.progress.steps[0].detail = Some(crate::workflow::evidence::NodeDetail::Failed(
                crate::workflow::evidence::FailureDetail::new(
                    crate::workflow::evidence::FailurePhase::Execution,
                    crate::workflow::evidence::FailureCode::CommandExit,
                    None,
                    None,
                    None,
                    Some(1),
                )
                .unwrap(),
            ));
            attempt.finalization = Some(AttemptFinalizationV1::Complete(
                AttemptFinalizationCompleteV1 {
                    complete: true,
                    trigger: FinalizationTriggerV1::Failed,
                    finalizers: vec![DurableFinalizerV1 {
                        id: "cleanup".to_owned(),
                        role: AttemptNodeRoleV1::Finalizer,
                        failure_policy: FailurePolicy::Required,
                        state: AttemptStepStateV1::Succeeded,
                        outputs: Some(Vec::new()),
                        detail: None,
                        diagnostic: None,
                    }],
                    issues: Vec::new(),
                    cancellation: None,
                    force_abort: false,
                },
            ));
            attempt.progress.last_transition_sequence = 1;
            attempt.progress.invocations.push(DurableInvocationV1 {
                invocation_id: 1,
                step_id: "cleanup".to_owned(),
                node_role: AttemptNodeRoleV1::Finalizer,
                role: crate::workflow::publication::RecoveryInvocationRoleV1::Target,
                target_execution: Some(1),
                recovery_round: None,
                state: DurableInvocationStateV1::Settled,
                started_at: attempt.created_at.clone(),
                finished_at: attempt.settled_at.clone(),
                usage: crate::workflow::publication::RecoveryInvocationUsageV1::default(),
                diagnostics: Vec::new(),
                diagnostic_reference: None,
            });
            recalculate_invocation_accounting(&mut attempt.progress)?;
            attempt.result = AttemptResultV1::NotPublished {
                reason: ResultAbsentReasonV1::PublicationPending,
            };
            Ok(())
        })
        .unwrap();
    let predecessor = read_state(initial.root_handle()).unwrap().attempts[0].clone();
    drop(initial);

    let LocalRetryOpen::Acquired(pending) = acquire_local_retry(&run_path).unwrap() else {
        panic!("failed finalizer attempt should be retryable");
    };
    let retry = (*pending)
        .begin(&fixture.admitted)
        .unwrap_or_else(|_| panic!("eligible finalizer retry should commit"));
    let state = read_state(retry.root_handle()).unwrap();
    let attempt = &state.attempts[1];

    assert_eq!(state.attempts[0], predecessor);
    assert_ne!(attempt.attempt_id, predecessor.attempt_id);
    assert_ne!(attempt.owner.owner_nonce, predecessor.owner.owner_nonce);
    assert_eq!(attempt.progress.accepted_occurrence_ordinal, 0);
    assert_eq!(attempt.progress.last_transition_sequence, 0);
    assert!(attempt.progress.outstanding_actions.is_empty());
    assert!(attempt.finalization.is_none());
    assert_eq!(retry.finalizers.len(), 1);
    assert_eq!(retry.finalizers[0].id, "cleanup");
    assert_eq!(retry.finalizers[0].state, AttemptStepStateV1::Pending);
}

#[test]
fn owner_loss_after_retry_commit_consumes_the_attempt_number() {
    let fixture = AdmittedFixture::new();
    let run_path = fixture.run_path("retry-crash");
    let initial = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    settle_as_workflow_failed(&initial);
    drop(initial);

    let LocalRetryOpen::Acquired(pending) = acquire_local_retry(&run_path).unwrap() else {
        panic!("failed attempt should be retryable");
    };
    let retry = (*pending).begin(&fixture.admitted).unwrap_or_else(|_| {
        panic!("first retry should commit");
    });
    assert_eq!(retry.attempt_number(), 2);
    drop(retry);

    let LocalRetryOpen::Acquired(pending) = acquire_local_retry(&run_path).unwrap() else {
        panic!("abandoned retry should itself be retryable");
    };
    let next = (*pending).begin(&fixture.admitted).unwrap_or_else(|_| {
        panic!("abandoned retry should settle and advance");
    });
    assert_eq!(next.attempt_number(), 3);
    let state = read_state(next.root_handle()).unwrap();
    assert_eq!(state.attempts.len(), 3);
    assert_eq!(state.attempts[1].state, AttemptStateV1::Interrupted);
    assert_eq!(
        state.attempts[1].interruption,
        Some(AttemptInterruptionV1 {
            cause: InterruptionCauseV1::ExecutionOwnerLost,
            execution_may_have_started: false,
            cancellation_requested: false,
        })
    );
    assert_eq!(state.attempts[2].attempt_number, 3);
    assert!(run_path.join("attempts/000002").is_dir());
    assert!(run_path.join("attempts/000003").is_dir());
}

struct ExactThenAbsentAuthority {
    host: ExecutionHostV1,
    observations: std::cell::RefCell<VecDeque<ProcessIdentityObservation>>,
    terminations: std::cell::Cell<usize>,
}

impl LocalRecoveryAuthority for ExactThenAbsentAuthority {
    fn execution_host(&self) -> Result<ExecutionHostV1, ()> {
        Ok(self.host.clone())
    }

    fn observe_process(&self, _guard: &ProcessGuardV1) -> ProcessIdentityObservation {
        self.observations.borrow_mut().pop_front().unwrap()
    }
}

impl LocalQuiescenceAuthority for ExactThenAbsentAuthority {
    fn terminate_process(&self, _guard: &ProcessGuardV1) -> AuthenticatedSignalResult {
        self.terminations.set(self.terminations.get() + 1);
        AuthenticatedSignalResult::Signalled
    }

    fn wait_for_process_change(&self) {}
}

#[test]
fn abandoned_exact_group_is_authenticated_terminated_and_proven_absent() {
    let attempt = fixture_guarded_attempt();
    let authority = ExactThenAbsentAuthority {
        host: attempt.owner.execution_host.clone(),
        observations: std::cell::RefCell::new(VecDeque::from([
            ProcessIdentityObservation::Exact {
                leader: crate::workflow::process_group::LeaderState::Running,
            },
            ProcessIdentityObservation::Absent,
        ])),
        terminations: std::cell::Cell::new(0),
    };

    assert_eq!(quiesce_attempt(&attempt, &authority), Ok(()));
    assert_eq!(authority.terminations.get(), 1);
}

#[test]
fn corrupted_retained_definition_after_quiescence_still_settles_abandonment() {
    let fixture = AdmittedFixture::new();
    let path = fixture.run_path("abandoned-invalid-retention");
    let original = InitialLocalRun::create(&path, &fixture.admitted).unwrap();
    drop(original);
    fs::remove_file(path.join("workflow/manifest.json")).unwrap();
    fs::write(path.join("workflow/manifest.json"), b"corrupt manifest").unwrap();
    let manifest_path = fs::canonicalize(&path)
        .unwrap()
        .join("workflow/manifest.json");
    let Err(error) = acquire_local_continuation(&path) else {
        panic!("corrupted retained definition accepted");
    };
    assert!(
        matches!(
            &error,
            LocalRunDirectoryError::StateFile { path: file, source, operation: "validate manifest" }
                if file == &manifest_path
                    && **source == LocalRunDirectoryError::ManifestDigestInvalid
        ),
        "{error:?}"
    );
    let state = read_state(&open_directory_path(&path).unwrap()).unwrap();
    assert_eq!(state.attempts.len(), 1);
    assert_eq!(state.attempts[0].state, AttemptStateV1::Interrupted);
    assert_eq!(
        state.attempts[0]
            .settlement_snapshot
            .as_ref()
            .unwrap()
            .settled_by,
        Some(WorkspaceSnapshotSettlementV1::AbandonmentRecovery)
    );
}

#[test]
fn abandoned_continuation_rejection_settles_only_the_prior_attempt() {
    let fixture = AdmittedFixture::new();
    let path = fixture.run_path("abandoned-continuation-rejection");
    let original = InitialLocalRun::create(&path, &fixture.admitted).unwrap();
    drop(original);
    let LocalContinuationOpen::Acquired(pending) = acquire_local_continuation(&path).unwrap()
    else {
        panic!("unowned created attempt must be recoverable");
    };
    assert!(
        pending
            .partition(pending.previous_definition(), &["second".to_owned()])
            .is_err()
    );
    pending.settle_abandoned().unwrap();
    let state = read_state(&open_directory_path(&path).unwrap()).unwrap();
    assert_eq!(state.attempts.len(), 1);
    assert_eq!(state.attempts[0].state, AttemptStateV1::Interrupted);
    assert_eq!(
        state.attempts[0].interruption.as_ref().unwrap().cause,
        InterruptionCauseV1::ExecutionOwnerLost
    );
    assert_eq!(
        state.attempts[0]
            .settlement_snapshot
            .as_ref()
            .unwrap()
            .settled_by,
        Some(WorkspaceSnapshotSettlementV1::AbandonmentRecovery)
    );
    assert!(!path.join("attempts/000002").exists());
}

#[test]
fn abandoned_continuation_claim_settles_and_claims_in_one_state_revision() {
    let fixture = AdmittedFixture::new();
    let path = fixture.run_path("abandoned-continuation-claim");
    let original = InitialLocalRun::create(&path, &fixture.admitted).unwrap();
    drop(original);
    let LocalContinuationOpen::Acquired(pending) = acquire_local_continuation(&path).unwrap()
    else {
        panic!("unowned created attempt must be recoverable");
    };
    let owned = pending
        .begin(
            &fixture.admitted,
            vec!["first".to_owned()],
            ContinuationDefinitionMode::Inherited,
            crate::workflow::publication::ContinuationReasonV1::Resume,
            None,
        )
        .unwrap();
    let state = read_state(owned.state.root.as_ref()).unwrap();
    assert_eq!(state.revision, 2);
    assert_eq!(state.attempts.len(), 2);
    assert_eq!(state.attempts[0].state, AttemptStateV1::Interrupted);
    assert_eq!(
        state.attempts[0]
            .settlement_snapshot
            .as_ref()
            .unwrap()
            .settled_by,
        Some(WorkspaceSnapshotSettlementV1::AbandonmentRecovery)
    );
    assert_eq!(state.attempts[1].state, AttemptStateV1::Created);
    assert_eq!(
        state.attempts[1]
            .continuation
            .as_ref()
            .unwrap()
            .workspace
            .modified,
        super::super::publication::WorkspaceModifiedV1::Unknown(
            super::super::publication::WorkspaceModifiedUnknownV1::Unknown
        )
    );
}

#[test]
fn continuation_context_rejects_a_rebound_run_path() {
    let fixture = AdmittedFixture::new();
    let path = fixture.run_path("continuation-context-rebound");
    let moved = fixture.run_path("continuation-context-original");
    let original = InitialLocalRun::create(&path, &fixture.admitted).unwrap();
    settle_as_workflow_failed(&original);
    drop(original);
    let LocalContinuationOpen::Acquired(pending) = acquire_local_continuation(&path).unwrap()
    else {
        panic!("eligible failed run");
    };
    let owned = pending
        .begin(
            &fixture.admitted,
            vec!["first".to_owned()],
            ContinuationDefinitionMode::Inherited,
            crate::workflow::publication::ContinuationReasonV1::Resume,
            None,
        )
        .unwrap();

    fs::rename(&path, moved).unwrap();
    fs::create_dir(&path).unwrap();
    fs::create_dir(path.join(ATTEMPTS_DIRECTORY)).unwrap();
    fs::create_dir(path.join(ATTEMPTS_DIRECTORY).join("000002")).unwrap();

    assert!(matches!(
        owned.bind_continuation_context(fixture.admitted.clone()),
        Err(LocalRunDirectoryError::StateInvalid)
    ));
}

#[test]
fn continuation_context_rejects_a_symlink_rebound_run_path() {
    let fixture = AdmittedFixture::new();
    let path = fixture.run_path("continuation-context-symlink");
    let moved = fixture.run_path("continuation-context-symlink-original");
    let original = InitialLocalRun::create(&path, &fixture.admitted).unwrap();
    settle_as_workflow_failed(&original);
    drop(original);
    let LocalContinuationOpen::Acquired(pending) = acquire_local_continuation(&path).unwrap()
    else {
        panic!("eligible failed run");
    };
    let owned = pending
        .begin(
            &fixture.admitted,
            vec!["first".to_owned()],
            ContinuationDefinitionMode::Inherited,
            crate::workflow::publication::ContinuationReasonV1::Resume,
            None,
        )
        .unwrap();

    fs::rename(&path, &moved).unwrap();
    std::os::unix::fs::symlink(&moved, &path).unwrap();

    assert!(matches!(
        owned.bind_continuation_context(fixture.admitted.clone()),
        Err(LocalRunDirectoryError::StateInvalid)
    ));
}

#[test]
fn postclaim_owner_loss_remains_auditable_and_can_be_settled_again() {
    let fixture = AdmittedFixture::new();
    let path = fixture.run_path("postclaim-owner-loss");
    let first = InitialLocalRun::create(&path, &fixture.admitted).unwrap();
    settle_as_workflow_failed(&first);
    drop(first);
    let LocalContinuationOpen::Acquired(pending) = acquire_local_continuation(&path).unwrap()
    else {
        panic!("eligible failed run");
    };
    let owned = pending
        .begin(
            &fixture.admitted,
            vec!["first".to_owned()],
            ContinuationDefinitionMode::Inherited,
            crate::workflow::publication::ContinuationReasonV1::Resume,
            None,
        )
        .unwrap();
    drop(owned); // Simulate owner death before seed/dispatch.
    let LocalContinuationOpen::Acquired(pending) = acquire_local_continuation(&path).unwrap()
    else {
        panic!("committed continuation must be recoverable");
    };
    assert_eq!(pending.prior_attempt_number(), 2);
    pending.settle_abandoned().unwrap();
    let state = read_state(&open_directory_path(&path).unwrap()).unwrap();
    assert_eq!(state.attempts.len(), 2);
    assert_eq!(state.attempts[1].trigger, AttemptTriggerV1::Continuation);
    assert_eq!(state.attempts[1].state, AttemptStateV1::Interrupted);
    assert_eq!(
        state.attempts[1].interruption.as_ref().unwrap().cause,
        InterruptionCauseV1::ExecutionOwnerLost
    );
}

// REQ-LOCALRUN-205: the retry, the continuation and an abandoned continuation's rejection
// settle an unsettled attempt `interrupted{state_persistence_failure}` when the run recorded
// that diagnostic for it, and `interrupted{execution_owner_lost}` otherwise.
#[test]
fn a_recorded_state_persistence_failure_names_the_abandoned_attempts_interruption() {
    let fixture = AdmittedFixture::new();
    let failure = CommitFailure {
        class: CommitFailureClass::StorageFull,
        operation: Some("write temporary state"),
        os_error: Some(28),
    };
    for (recorded, cause) in [
        (true, InterruptionCauseV1::StatePersistenceFailure),
        (false, InterruptionCauseV1::ExecutionOwnerLost),
    ] {
        let unsettled = |name: &str| {
            let path = fixture.run_path(&format!("{name}-{recorded}"));
            let original = InitialLocalRun::create(&path, &fixture.admitted).unwrap();
            if recorded {
                record_state_persistence_failure(&original.state, failure).unwrap();
            }
            drop(original);
            path
        };
        let interruption = |path: &Path| {
            let state = read_state(&open_directory_path(path).unwrap()).unwrap();
            assert_eq!(state.attempts[0].state, AttemptStateV1::Interrupted);
            state.attempts[0].interruption.as_ref().unwrap().cause
        };

        let path = unsettled("retried");
        let LocalRetryOpen::Acquired(pending) = acquire_local_retry(&path).unwrap() else {
            panic!("an abandoned attempt is retryable");
        };
        let Ok(_retry) = (*pending).begin(&fixture.admitted) else {
            panic!("an abandoned attempt's retry begins");
        };
        assert_eq!(interruption(&path), cause, "retry, recorded {recorded}");

        let path = unsettled("continued");
        let LocalContinuationOpen::Acquired(pending) = acquire_local_continuation(&path).unwrap()
        else {
            panic!("an abandoned attempt is continuable");
        };
        pending
            .begin(
                &fixture.admitted,
                vec!["first".to_owned()],
                ContinuationDefinitionMode::Inherited,
                crate::workflow::publication::ContinuationReasonV1::Resume,
                None,
            )
            .unwrap();
        assert_eq!(
            interruption(&path),
            cause,
            "continuation, recorded {recorded}"
        );

        let path = unsettled("rejected");
        let LocalContinuationOpen::Acquired(pending) = acquire_local_continuation(&path).unwrap()
        else {
            panic!("an abandoned attempt is continuable");
        };
        pending.settle_abandoned().unwrap();
        assert_eq!(interruption(&path), cause, "rejection, recorded {recorded}");
    }
}

#[test]
fn orphaned_preclaim_attempt_directory_is_removed_under_the_next_lock() {
    let fixture = AdmittedFixture::new();
    let path = fixture.run_path("orphaned-continuation-stage");
    let original = InitialLocalRun::create(&path, &fixture.admitted).unwrap();
    settle_as_workflow_failed(&original);
    drop(original);
    fs::create_dir_all(path.join("attempts/000002/workflow/files")).unwrap();
    fs::write(
        path.join("attempts/000002/workflow/manifest.json"),
        b"staged only",
    )
    .unwrap();
    let LocalContinuationOpen::Acquired(pending) = acquire_local_continuation(&path).unwrap()
    else {
        panic!("staged directory cannot count as an attempt");
    };
    assert_eq!(pending.prior_attempt_number(), 1);
    assert!(!path.join("attempts/000002").exists());
}

#[test]
fn continuation_claim_commits_one_partitioned_attempt_after_failed_run() {
    let fixture = AdmittedFixture::new();
    let path = fixture.run_path("continuation-claim");
    let original = InitialLocalRun::create(&path, &fixture.admitted).unwrap();
    settle_as_workflow_failed(&original);
    drop(original);
    let LocalContinuationOpen::Acquired(pending) = acquire_local_continuation(&path).unwrap()
    else {
        panic!("failed run should allow claim");
    };
    let owned = pending
        .begin(
            &fixture.admitted,
            vec!["first".to_owned()],
            ContinuationDefinitionMode::Inherited,
            crate::workflow::publication::ContinuationReasonV1::Resume,
            None,
        )
        .unwrap();
    assert_eq!(owned.attempt_number(), 2);
    let state = read_state(owned.state.root.as_ref()).unwrap();
    assert_eq!(state.attempts.len(), 2);
    let current = state.attempts.last().unwrap();
    assert_eq!(current.trigger, AttemptTriggerV1::Continuation);
    let record = current.continuation.as_ref().unwrap();
    assert_eq!(record.reexecuted_steps, ["first", "second"]);
    assert!(record.inherited_steps.is_empty());
    assert_eq!(
        record
            .workspace
            .quiescence
            .as_ref()
            .unwrap()
            .groups_recorded,
        0
    );
    assert_eq!(
        record.workspace.preparation,
        super::super::publication::ContinuationPreparationV1::Ready
    );
    assert!(record.workspace.start_snapshot.is_some());
    drop(owned);
}

#[test]
fn continuation_claim_rechecks_immutable_source_and_execution_policy() {
    let fixture = AdmittedFixture::new();
    let path = fixture.run_path("continuation-bound-admission");
    let original = InitialLocalRun::create(&path, &fixture.admitted).unwrap();
    settle_as_workflow_failed(&original);
    drop(original);

    let altered_policy = admit_workflow(
        fixture.admitted.workflow().clone(),
        fixture.admitted.inputs().clone(),
        ExecutionContext::new(
            fixture.execution_root.clone(),
            ExecutionPolicyLimits::new(
                3,
                CaptureLimits::new(16, 1024, 4096),
                InputLimits::new(16, 1024, 4096, 4096),
                1024,
            ),
            EnvironmentSnapshot::default(),
            CancellationPolicy::new(CancellationSource::new(), Duration::from_secs(10)),
        ),
    )
    .unwrap();
    let LocalContinuationOpen::Acquired(pending) = acquire_local_continuation(&path).unwrap()
    else {
        panic!("failed run should allow claim inspection");
    };
    assert!(matches!(
        pending.begin(
            &altered_policy,
            vec!["first".to_owned()],
            ContinuationDefinitionMode::Inherited,
            crate::workflow::publication::ContinuationReasonV1::Resume,
            None,
        ),
        Err(LocalRunDirectoryError::StateConflict)
    ));

    let alternate_source = fixture._temporary.path().join("alternate-source");
    fs::create_dir(&alternate_source).unwrap();
    fs::copy(
        fixture
            .admitted
            .workflow()
            .source
            .source_root
            .join("workflow.yaml"),
        alternate_source.join("workflow.yaml"),
    )
    .unwrap();
    let alternate_workflow =
        resolution::resolve(&alternate_source, Path::new("workflow.yaml")).unwrap();
    let altered_source = admit_workflow(
        alternate_workflow,
        fixture.admitted.inputs().clone(),
        ExecutionContext::new(
            fixture.execution_root.clone(),
            ExecutionPolicyLimits::new(
                2,
                CaptureLimits::new(16, 1024, 4096),
                InputLimits::new(16, 1024, 4096, 4096),
                1024,
            ),
            EnvironmentSnapshot::default(),
            CancellationPolicy::new(CancellationSource::new(), Duration::from_secs(10)),
        ),
    )
    .unwrap();
    let LocalContinuationOpen::Acquired(pending) = acquire_local_continuation(&path).unwrap()
    else {
        panic!("failed run should remain available after rejected claim");
    };
    assert!(matches!(
        pending.begin(
            &altered_source,
            vec!["first".to_owned()],
            ContinuationDefinitionMode::Replaced,
            crate::workflow::publication::ContinuationReasonV1::Resume,
            None,
        ),
        Err(LocalRunDirectoryError::StateConflict)
    ));
    assert_eq!(
        read_state(&open_directory_path(&path).unwrap())
            .unwrap()
            .attempts
            .len(),
        1
    );
}

#[test]
fn producer_workflow_cache_reads_a_shared_closure_once() {
    let fixture = AdmittedFixture::new();
    let path = fixture.run_path("shared-producer-closure-cache");
    let run = InitialLocalRun::create(&path, &fixture.admitted).unwrap();
    let metadata = read_run(run.root_handle()).unwrap();
    let definition = attempt_definition_for_run(&metadata);
    let workflow = fixture.admitted.workflow().clone();
    let loads = std::cell::Cell::new(0);
    let mut workflows = BTreeMap::new();

    for _ in 0..16 {
        let loaded = cached_producer_workflow(&mut workflows, definition.clone(), || {
            loads.set(loads.get() + 1);
            Ok(workflow.clone())
        })
        .unwrap();
        assert_eq!(loaded.content_digest, workflow.content_digest);
    }

    assert_eq!(loads.get(), 1);
    assert_eq!(workflows.len(), 1);
}

#[test]
fn continuation_state_index_resolves_long_inheritance_chains_once() {
    let fixture = AdmittedFixture::new();
    let path = fixture.run_path("long-continuation-chain");
    let initial = InitialLocalRun::create(&path, &fixture.admitted).unwrap();
    let mut state = read_state(initial.state.root.as_ref()).unwrap();
    drop(initial);
    let first = state.attempts[0]
        .progress
        .steps
        .iter_mut()
        .find(|step| step.id == "first")
        .unwrap();
    first.state = AttemptStepStateV1::Succeeded;
    first.detail = None;

    for attempt_number in 2..=128 {
        let prior = state.attempts.last().unwrap();
        let prior_attempt_id = prior.attempt_id.clone();
        let prior_attempt_number = prior.attempt_number;
        let mut attempt = prior.clone();
        attempt.attempt_number = attempt_number;
        attempt.prior_attempt_number = Some(prior_attempt_number);
        attempt.trigger = AttemptTriggerV1::Continuation;
        let first = attempt
            .progress
            .steps
            .iter_mut()
            .find(|step| step.id == "first")
            .unwrap();
        first.state = AttemptStepStateV1::Inherited;
        first.detail = Some(NodeDetail::Inherited(
            super::super::evidence::InheritedDetail {
                prior_attempt_id,
                prior_attempt_number,
                prior_state: if attempt_number == 2 {
                    super::super::evidence::InheritedPriorState::Succeeded
                } else {
                    super::super::evidence::InheritedPriorState::Inherited
                },
                definition_changed: false,
            },
        ));
        state.attempts.push(attempt);
    }

    let index = LocalRunStateIndex::new(&state).unwrap();
    assert_eq!(
        index.disposition(128, "first"),
        Some(InheritedDisposition::Succeeded)
    );
}

#[test]
fn continuation_acquisition_keeps_run_locked_during_partition_admission() {
    let fixture = AdmittedFixture::new();
    let run_path = fixture.run_path("continue-admission");
    let initial = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    settle_as_workflow_failed(&initial);
    drop(initial);

    let LocalContinuationOpen::Acquired(pending) = acquire_local_continuation(&run_path).unwrap()
    else {
        panic!("failed run should admit a continuation inspection");
    };
    pending.verify_locked_history().unwrap();
    assert_eq!(pending.quiescence_counts(), (0, 0, 0));
    let unchanged =
        pending.definition_changes(pending.previous_definition(), &["first".to_owned()]);
    assert_eq!(unchanged.get("first"), Some(&false));
    let mut revised = pending.previous_definition().clone();
    if let Some(super::super::validated::ValidatedStep::Command(command)) =
        revised.definition.steps.get_mut("first")
    {
        command.argv.push("new-argument".to_owned());
    }
    let changed = pending.definition_changes(&revised, &["first".to_owned()]);
    assert_eq!(changed.get("first"), Some(&true));
    let mut subset = pending.previous_definition().clone();
    subset.definition.declared_inputs.remove("request");
    assert!(
        pending
            .projected_inputs(&subset)
            .unwrap()
            .get("request")
            .is_none()
    );
    subset.definition.declared_inputs.insert(
        "extra".to_owned(),
        super::super::validated::WorkflowValueType::Text,
    );
    assert_eq!(
        pending.projected_inputs(&subset).err(),
        Some(vec!["extra".to_owned()])
    );
    let selection = pending.partition(pending.previous_definition(), &["second".to_owned()]);
    assert!(selection.is_err()); // 'first' has not succeeded and cannot be inherited.
    assert_eq!(read_state(&pending.root).unwrap().attempts.len(), 1);
    drop(pending);
    let LocalRetryOpen::Acquired(_) = acquire_local_retry(&run_path).unwrap() else {
        panic!("planning must release the lock without creating an attempt");
    };
}

#[test]
fn malformed_inherited_disposition_is_returned_for_combined_admission_reporting() {
    let fixture = AdmittedFixture::new();
    let path = fixture.run_path("malformed-disposition");
    let owned = InitialLocalRun::create(&path, &fixture.admitted).unwrap();
    let state = read_state(&open_directory_path(&path).unwrap()).unwrap();
    drop(owned);
    let (skipped, violations) = effective_skipped_sources(
        &state,
        1,
        &["first".to_owned()],
        &BTreeMap::from([(
            "first".to_owned(),
            super::super::continuation::PriorState::Inherited,
        )]),
    );
    assert!(skipped.is_empty());
    assert_eq!(
        violations,
        vec![super::super::continuation::AdmissionViolation::Node {
            id: "first".to_owned(),
            prior: Some(super::super::continuation::PriorState::Inherited)
        }]
    );
}

#[test]
fn continuation_admission_follows_skipped_disposition_through_inherited_history() {
    let fixture = AdmittedFixture::from_source(
        "schemaVersion: 1\nsteps:\n  first:\n    kind: cmd\n    command: {argv: [\"true\"]}\n    outputs:\n      value: {kind: text, from: path, path: value.txt}\n  second:\n    kind: cmd\n    inputs:\n      value: {ref: outputs.first.value}\n    command: {argv: [\"true\"]}\n",
    );
    let run =
        InitialLocalRun::create(&fixture.run_path("skipped-history"), &fixture.admitted).unwrap();
    let mut state = read_state(run.root_handle()).unwrap();
    state.attempts[0].progress.steps[0].state = AttemptStepStateV1::Skipped;
    let prior_id = state.attempts[0].attempt_id.clone();
    let mut inherited = state.attempts[0].clone();
    inherited.attempt_number = 2;
    inherited.progress.steps[0].state = AttemptStepStateV1::Inherited;
    inherited.progress.steps[0].detail = Some(NodeDetail::Inherited(
        crate::workflow::evidence::InheritedDetail {
            prior_attempt_id: prior_id,
            prior_attempt_number: 1,
            prior_state: crate::workflow::evidence::InheritedPriorState::Skipped,
            definition_changed: false,
        },
    ));
    state.attempts.push(inherited);
    let (skipped, violations) = effective_skipped_sources(
        &state,
        2,
        &["first".to_owned()],
        &BTreeMap::from([(
            "first".to_owned(),
            super::super::continuation::PriorState::Inherited,
        )]),
    );
    assert!(violations.is_empty());
    assert_eq!(skipped, BTreeSet::from(["first".to_owned()]));
    let workflow = &fixture.admitted.workflow().definition;
    let partition =
        super::super::continuation::partition(workflow, &["second".to_owned()], &BTreeSet::new())
            .unwrap();
    let violations = super::super::continuation::check_inheritance(
        workflow,
        workflow,
        &partition,
        &BTreeMap::from([(
            "first".to_owned(),
            super::super::continuation::PriorState::Inherited,
        )]),
        &skipped,
    );
    assert_eq!(
        violations,
        vec![
            super::super::continuation::AdmissionViolation::RequiredSkipped {
                consumer: "second".to_owned(),
                producer: "first".to_owned(),
            }
        ]
    );
}

#[test]
fn continuation_with_no_groups_needs_no_host_identity() {
    struct NoHost;
    impl LocalRecoveryAuthority for NoHost {
        fn execution_host(&self) -> Result<ExecutionHostV1, ()> {
            Err(())
        }
        fn observe_process(&self, _: &ProcessGuardV1) -> ProcessIdentityObservation {
            panic!("no group should be inspected")
        }
    }
    impl LocalQuiescenceAuthority for NoHost {
        fn terminate_process(&self, _: &ProcessGuardV1) -> AuthenticatedSignalResult {
            panic!("no group should be signaled")
        }
        fn wait_for_process_change(&self) {}
    }
    let fixture = AdmittedFixture::new();
    let run = InitialLocalRun::create(&fixture.run_path("no-guards"), &fixture.admitted).unwrap();
    let state = read_state(run.root_handle()).unwrap();
    let proof = quiesce_run(&state, &NoHost).unwrap();
    assert_eq!(
        (
            proof.groups_recorded,
            proof.groups_terminated,
            proof.groups_absent
        ),
        (0, 0, 0)
    );
}

#[test]
fn continuation_quiescence_covers_prior_attempts_and_ignores_foreign_host_ids() {
    let first = fixture_guarded_attempt();
    let mut foreign = first.clone();
    foreign.attempt_number = 2;
    foreign.process_guards[0].execution_host.value = "another-boot".to_owned();
    let authority = ExactThenAbsentAuthority {
        host: first.owner.execution_host.clone(),
        observations: std::cell::RefCell::new(VecDeque::from([
            ProcessIdentityObservation::Exact {
                leader: crate::workflow::process_group::LeaderState::Running,
            },
            ProcessIdentityObservation::Absent,
        ])),
        terminations: std::cell::Cell::new(0),
    };
    let state = LocalRunStateV1 {
        schema_version: 1,
        local_run_id: "fixture".to_owned(),
        revision: 0,
        current_attempt_number: 2,
        attempts: vec![first, foreign],
        diagnostics: Vec::new(),
    };
    let proof = quiesce_run(&state, &authority).unwrap();
    assert_eq!(
        (
            proof.groups_recorded,
            proof.groups_terminated,
            proof.groups_absent
        ),
        (2, 1, 1)
    );
    assert_eq!(authority.terminations.get(), 1);
    assert!(authority.observations.borrow().is_empty());
}

struct ControlledQuiescenceAuthority {
    host: ExecutionHostV1,
    observations: std::cell::RefCell<VecDeque<ProcessIdentityObservation>>,
    signal: AuthenticatedSignalResult,
    signalled: std::cell::Cell<usize>,
}

impl LocalRecoveryAuthority for ControlledQuiescenceAuthority {
    fn execution_host(&self) -> Result<ExecutionHostV1, ()> {
        Ok(self.host.clone())
    }
    fn observe_process(&self, _guard: &ProcessGuardV1) -> ProcessIdentityObservation {
        self.observations.borrow_mut().pop_front().unwrap()
    }
}

impl LocalQuiescenceAuthority for ControlledQuiescenceAuthority {
    fn terminate_process(&self, _guard: &ProcessGuardV1) -> AuthenticatedSignalResult {
        self.signalled.set(self.signalled.get() + 1);
        self.signal
    }
    fn wait_for_process_change(&self) {}
}

#[test]
fn continuation_quiescence_rejects_unproven_identity_before_any_signal() {
    let mut attempt = fixture_guarded_attempt();
    let mut second = attempt.process_guards[0].clone();
    second.guard_id = "22222222-2222-4222-8222-222222222222".to_owned();
    attempt.process_guards.push(second);
    let state = LocalRunStateV1 {
        schema_version: 1,
        local_run_id: "fixture".to_owned(),
        revision: 0,
        current_attempt_number: 1,
        attempts: vec![attempt.clone()],
        diagnostics: Vec::new(),
    };
    for observations in [
        vec![
            ProcessIdentityObservation::Exact {
                leader: crate::workflow::process_group::LeaderState::Running,
            },
            ProcessIdentityObservation::Unavailable,
        ],
        vec![
            ProcessIdentityObservation::Exact {
                leader: crate::workflow::process_group::LeaderState::Running,
            },
            ProcessIdentityObservation::Exact {
                leader: crate::workflow::process_group::LeaderState::Running,
            },
        ],
    ] {
        let signal = if observations.len() == 2
            && matches!(observations[1], ProcessIdentityObservation::Unavailable)
        {
            AuthenticatedSignalResult::Signalled
        } else {
            AuthenticatedSignalResult::Unavailable
        };
        let authority = ControlledQuiescenceAuthority {
            host: attempt.owner.execution_host.clone(),
            observations: std::cell::RefCell::new(VecDeque::from(observations)),
            signal,
            signalled: std::cell::Cell::new(0),
        };
        assert!(quiesce_run(&state, &authority).is_err());
        assert_eq!(
            authority.signalled.get(),
            if signal == AuthenticatedSignalResult::Signalled {
                0
            } else {
                2
            }
        );
    }
}

struct DelayedAbsentAuthority {
    host: ExecutionHostV1,
    observations: std::cell::Cell<usize>,
    waits: std::cell::Cell<usize>,
}

impl LocalRecoveryAuthority for DelayedAbsentAuthority {
    fn execution_host(&self) -> Result<ExecutionHostV1, ()> {
        Ok(self.host.clone())
    }

    fn observe_process(&self, _guard: &ProcessGuardV1) -> ProcessIdentityObservation {
        let observation = self.observations.get() + 1;
        self.observations.set(observation);
        if observation <= 2_002 {
            ProcessIdentityObservation::Exact {
                leader: crate::workflow::process_group::LeaderState::Running,
            }
        } else {
            ProcessIdentityObservation::Absent
        }
    }
}

impl LocalQuiescenceAuthority for DelayedAbsentAuthority {
    fn terminate_process(&self, _guard: &ProcessGuardV1) -> AuthenticatedSignalResult {
        AuthenticatedSignalResult::Signalled
    }

    fn wait_for_process_change(&self) {
        self.waits.set(self.waits.get() + 1);
    }
}

#[test]
fn quiescence_wait_allows_exit_after_ten_seconds() {
    let attempt = fixture_guarded_attempt();
    let authority = DelayedAbsentAuthority {
        host: attempt.owner.execution_host.clone(),
        observations: std::cell::Cell::new(0),
        waits: std::cell::Cell::new(0),
    };

    assert_eq!(quiesce_attempt(&attempt, &authority), Ok(()));
    assert_eq!(authority.waits.get(), 2_001);
    assert!(
        authority.waits.get() * usize::try_from(QUIESCENCE_POLL_INTERVAL.as_millis()).unwrap()
            > 10_000
    );
}

#[test]
fn retained_named_input_corruption_rejects_retry_without_fallback() {
    let fixture = AdmittedFixture::new();
    let run_path = fixture.run_path("corrupt-retained-input");
    let run = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    settle_as_workflow_failed(&run);
    drop(run);

    let retained_json = run_path.join("workflow/files/0004");
    fs::set_permissions(&retained_json, Permissions::from_mode(0o600)).unwrap();
    fs::write(&retained_json, br#"{"key":1,"key":2}"#).unwrap();

    assert!(matches!(
        acquire_local_retry(&run_path),
        Err(LocalRunDirectoryError::StateInvalid)
    ));
    assert!(!run_path.join("attempts/000002").exists());
}

#[test]
fn retry_execution_setup_rejects_a_rebound_run_path() {
    let fixture = AdmittedFixture::new();
    let run_path = fixture.run_path("retry-rebound");
    let moved_path = fixture.run_path("retry-original");
    let initial = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    settle_as_workflow_failed(&initial);
    drop(initial);

    let LocalRetryOpen::Acquired(pending) = acquire_local_retry(&run_path).unwrap() else {
        panic!("failed attempt should be retryable");
    };
    let retry = (*pending)
        .begin(&fixture.admitted)
        .unwrap_or_else(|_| panic!("eligible retry should commit"));

    fs::rename(&run_path, moved_path).unwrap();
    fs::create_dir(&run_path).unwrap();
    fs::create_dir(run_path.join(PRIVATE_DIRECTORY)).unwrap();
    fs::create_dir_all(run_path.join("attempts/000002")).unwrap();

    let prepared = crate::workflow::publication::prepare_attempt_result_destination(
        retry.result_directory(),
        retry.private_directory(),
        retry.attempt_directory_handle(),
        retry.private_directory_handle(),
    );
    assert!(
        prepared.is_err(),
        "execution setup must not adopt a replacement at the run path"
    );
}

fn json_bytes(value: Value) -> Vec<u8> {
    let mut bytes = serde_json::to_vec_pretty(&value).unwrap();
    bytes.push(b'\n');
    bytes
}

fn settle_as_succeeded(run: &LocalAttemptOwner) {
    run.state
        .update(|state| {
            let attempt = current_attempt_mut(state)?;
            let settled = attempt.created_at.clone();
            attempt.started_at = Some(settled.clone());
            attempt.settled_at = Some(settled);
            attempt.settlement_snapshot = Some(fixture_settlement_snapshot());
            attempt.state = AttemptStateV1::Succeeded;
            for step in &mut attempt.progress.steps {
                step.state = AttemptStepStateV1::Succeeded;
            }
            attempt.result = AttemptResultV1::NotPublished {
                reason: ResultAbsentReasonV1::PublicationPending,
            };
            Ok(())
        })
        .unwrap();
}

fn retain_text_output_for_step(
    run: &LocalAttemptOwner,
    step_id: &str,
    output_name: &str,
    bytes: &[u8],
) {
    let retained = write_retained_carrier(run, step_id, output_name, bytes, false);
    run.state
        .update(|state| {
            let step = current_attempt_mut(state)?
                .progress
                .steps
                .iter_mut()
                .find(|step| step.id == step_id)
                .ok_or(LocalRunDirectoryError::StateInvalid)?;
            step.outputs = Some(vec![retained.clone()]);
            Ok(())
        })
        .unwrap();
}

/// Writes `bytes` as the carrier of `step_id`'s output `output_name` under `run`'s current
/// attempt and answers its descriptor: a `json` output when `json`, a `text` one otherwise.
fn write_retained_carrier(
    run: &LocalAttemptOwner,
    step_id: &str,
    output_name: &str,
    bytes: &[u8],
    json: bool,
) -> RetainedOutputV1 {
    let state = read_state(run.root_handle()).unwrap();
    let attempt_name = attempt_directory_name(state.current_attempt_number).unwrap();
    let relative_path = retained_value_relative_path(AttemptNodeRoleV1::Step, step_id, output_name);
    let path = run
        .run_directory()
        .join(ATTEMPTS_DIRECTORY)
        .join(attempt_name)
        .join(&relative_path);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, bytes).unwrap();
    let carrier = |media_type: &str| RetainedCarrierV1 {
        relative_path: relative_path.clone(),
        media_type: media_type.to_owned(),
        size_bytes: u64::try_from(bytes.len()).unwrap(),
        digest: DigestV1::sha256(bytes),
    };
    let name = output_name.to_owned();
    if json {
        RetainedOutputV1::Json {
            name,
            producer: None,
            carrier: carrier("application/json"),
        }
    } else {
        RetainedOutputV1::Text {
            name,
            producer: None,
            carrier: carrier("text/plain; charset=utf-8"),
        }
    }
}

fn retain_file_outputs_for_step(
    run: &LocalAttemptOwner,
    step_id: &str,
    outputs: &[(&str, &str, &[u8])],
) {
    let state = read_state(run.root_handle()).unwrap();
    let attempt_number = state.current_attempt_number;
    let attempt_name = attempt_directory_name(attempt_number).unwrap();
    let mut retained = Vec::with_capacity(outputs.len());
    for (name, media_type, bytes) in outputs {
        let relative_path = retained_value_relative_path(AttemptNodeRoleV1::Step, step_id, name);
        let path = run
            .run_directory()
            .join(ATTEMPTS_DIRECTORY)
            .join(&attempt_name)
            .join(&relative_path);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, bytes).unwrap();
        retained.push(RetainedOutputV1::File {
            name: (*name).to_owned(),
            producer: None,
            media_type: (*media_type).to_owned(),
            carrier: RetainedCarrierV1 {
                relative_path,
                media_type: (*media_type).to_owned(),
                size_bytes: u64::try_from(bytes.len()).unwrap(),
                digest: DigestV1::sha256(bytes),
            },
        });
    }
    run.state
        .update(|state| {
            let step = current_attempt_mut(state)?
                .progress
                .steps
                .iter_mut()
                .find(|step| step.id == step_id)
                .ok_or(LocalRunDirectoryError::StateInvalid)?;
            step.outputs = Some(retained.clone());
            Ok(())
        })
        .unwrap();
}

fn settle_as_cancelled(run: &LocalAttemptOwner) {
    run.state
        .update(|state| {
            let attempt = current_attempt_mut(state)?;
            let settled = attempt.created_at.clone();
            attempt.started_at = Some(settled.clone());
            attempt.settled_at = Some(settled.clone());
            attempt.settlement_snapshot = Some(fixture_settlement_snapshot());
            attempt.state = AttemptStateV1::Cancelled;
            attempt.cancellation = Some(AttemptCancellationV1 {
                reason: CancellationReasonV1::UserRequest,
                requested_at: settled.clone(),
                force_stop_deadline: settled,
                workflow_confirmed: true,
            });
            for step in &mut attempt.progress.steps {
                step.state = AttemptStepStateV1::Cancelled;
                step.detail = Some(crate::workflow::evidence::NodeDetail::Cancellation(
                    crate::workflow::evidence::CancellationDetail::new(
                        crate::workflow::admission::CancellationReason::UserRequest,
                    ),
                ));
            }
            attempt.result = AttemptResultV1::NotPublished {
                reason: ResultAbsentReasonV1::PublicationPending,
            };
            Ok(())
        })
        .unwrap();
}

fn settle_as_force_cancelled_with_finalizer(
    run: &LocalAttemptOwner,
    phase: super::super::runtime::RunCancellationPhase,
) {
    run.state
        .update(|state| {
            let attempt = current_attempt_mut(state)?;
            let settled = attempt.created_at.clone();
            attempt.started_at = Some(settled.clone());
            attempt.settled_at = Some(settled.clone());
            attempt.settlement_snapshot = Some(fixture_settlement_snapshot());
            attempt.state = AttemptStateV1::Cancelled;
            attempt.force_abort = Some(super::super::runtime::ForceAbortEvidence {
                reason: CancellationReason::ForceAbort,
                phase,
            });
            let ordinary_reason = match phase {
                super::super::runtime::RunCancellationPhase::Ordinary => {
                    attempt.cancellation = None;
                    CancellationReason::ForceAbort
                }
                super::super::runtime::RunCancellationPhase::Finalization => {
                    attempt.cancellation = Some(AttemptCancellationV1 {
                        reason: CancellationReasonV1::UserRequest,
                        requested_at: settled.clone(),
                        force_stop_deadline: settled,
                        workflow_confirmed: true,
                    });
                    CancellationReason::UserRequest
                }
            };
            for step in &mut attempt.progress.steps {
                step.state = AttemptStepStateV1::Cancelled;
                step.detail = Some(NodeDetail::Cancellation(
                    super::super::evidence::CancellationDetail::new(ordinary_reason),
                ));
            }
            attempt.finalization = Some(AttemptFinalizationV1::Complete(
                AttemptFinalizationCompleteV1 {
                    complete: true,
                    trigger: FinalizationTriggerV1::Cancelled,
                    finalizers: vec![DurableFinalizerV1 {
                        id: "cleanup".to_owned(),
                        role: AttemptNodeRoleV1::Finalizer,
                        failure_policy: FailurePolicy::Required,
                        state: AttemptStepStateV1::Cancelled,
                        outputs: Some(Vec::new()),
                        detail: Some(NodeDetail::Cancellation(
                            super::super::evidence::CancellationDetail::new(
                                CancellationReason::ForceAbort,
                            ),
                        )),
                        diagnostic: None,
                    }],
                    issues: Vec::new(),
                    cancellation: Some(DurableFinalizationCancellationV1 {
                        reason: CancellationReasonV1::ForceAbort,
                        force_stop_deadline: None,
                    }),
                    force_abort: true,
                },
            ));
            attempt.result = AttemptResultV1::NotPublished {
                reason: ResultAbsentReasonV1::PublicationPending,
            };
            Ok(())
        })
        .unwrap();
}

#[test]
fn retained_attempt_rejects_force_evidence_on_success() {
    let fixture = AdmittedFixture::from_source(
        "schemaVersion: 1\nsteps:\n  work:\n    kind: cmd\n    command: { argv: [\"true\"] }\n",
    );
    let run =
        InitialLocalRun::create(&fixture.run_path("forced-success"), &fixture.admitted).unwrap();
    settle_as_succeeded(&run);
    let state = read_state(run.root_handle()).unwrap();
    assert!(decode_state(&encode_json(&state).unwrap()).is_ok());
    let mut fabricated_force = serde_json::to_value(state).unwrap();
    fabricated_force["attempts"][0]["forceAbort"] =
        json!({ "reason": "force_abort", "phase": "ordinary" });
    assert_eq!(
        decode_state(&json_bytes(fabricated_force)),
        Err(LocalRunDirectoryError::AttemptForceAbortInvalid)
    );
}

#[test]
fn retained_attempt_rejects_phase_impossible_force_evidence() {
    let workflow = "schemaVersion: 1\nsteps:\n  work:\n    kind: cmd\n    command: { argv: [\"true\"] }\nfinalizers:\n  cleanup:\n    kind: cmd\n    command: { argv: [\"true\"] }\n";

    let ordinary_fixture = AdmittedFixture::from_source(workflow);
    let ordinary_run = InitialLocalRun::create(
        &ordinary_fixture.run_path("ordinary-force"),
        &ordinary_fixture.admitted,
    )
    .unwrap();
    settle_as_force_cancelled_with_finalizer(
        &ordinary_run,
        super::super::runtime::RunCancellationPhase::Ordinary,
    );
    let ordinary_state = read_state(ordinary_run.root_handle()).unwrap();
    assert!(decode_state(&encode_json(&ordinary_state).unwrap()).is_ok());
    let mut graceful_finalization = serde_json::to_value(ordinary_state).unwrap();
    let deadline = graceful_finalization["attempts"][0]["createdAt"].clone();
    graceful_finalization["attempts"][0]["finalization"]["cancellation"] = json!({
        "reason": "runner_shutdown",
        "forceStopDeadline": deadline
    });
    assert_eq!(
        decode_state(&json_bytes(graceful_finalization)),
        Err(LocalRunDirectoryError::AttemptFinalizationInterruptionInvalid)
    );

    let finalization_fixture = AdmittedFixture::from_source(workflow);
    let finalization_run = InitialLocalRun::create(
        &finalization_fixture.run_path("finalization-force"),
        &finalization_fixture.admitted,
    )
    .unwrap();
    settle_as_force_cancelled_with_finalizer(
        &finalization_run,
        super::super::runtime::RunCancellationPhase::Finalization,
    );
    let finalization_state = read_state(finalization_run.root_handle()).unwrap();
    assert!(decode_state(&encode_json(&finalization_state).unwrap()).is_ok());
    let mut rewritten_ordinary = serde_json::to_value(finalization_state).unwrap();
    rewritten_ordinary["attempts"][0]["progress"]["steps"][0]["detail"] =
        json!({ "code": "force_abort" });
    assert_eq!(
        decode_state(&json_bytes(rewritten_ordinary)),
        Err(LocalRunDirectoryError::AttemptStepCancellationInvalid)
    );
}

fn publish_result_fixture(fixture: &AdmittedFixture, run: &LocalAttemptOwner) -> PathBuf {
    let durable = read_state(run.root_handle()).unwrap();
    let attempt = durable.attempts.last().unwrap();
    let run_document = read_run(run.root_handle()).unwrap();
    let command_output = serde_json::json!({
        "stdout": {
            "encoding": "base64",
            "data": BASE64_STANDARD.encode([0_u8, 0xff, b'\n']),
            "retainedBytes": 3,
            "discardedBytes": 0,
            "truncated": false,
            "fullyDrained": false
        },
        "stderr": {
            "encoding": "base64",
            "data": BASE64_STANDARD.encode(b"warning\n"),
            "retainedBytes": 8,
            "discardedBytes": 0,
            "truncated": false,
            "fullyDrained": true
        }
    });
    let (outcome, mut steps, primary_issue) = match attempt.state {
        AttemptStateV1::Succeeded => (
            "succeeded",
            vec![
                serde_json::json!({
                    "id": "first",
                    "kind": "cmd",
                    "failurePolicy": "required",
                    "state": "succeeded",
                    "startedAt": "2026-08-02T12:01:44Z",
                    "durationMilliseconds": 100,
                    "commandOutput": command_output.clone()
                }),
                serde_json::json!({
                    "id": "second",
                    "kind": "cmd",
                    "failurePolicy": "required",
                    "state": "succeeded",
                    "startedAt": "2026-08-02T12:01:44.1Z",
                    "durationMilliseconds": 200,
                    "commandOutput": command_output.clone()
                }),
            ],
            None,
        ),
        AttemptStateV1::WorkflowFailed => (
            "failed",
            vec![
                serde_json::json!({
                    "id": "first",
                    "kind": "cmd",
                    "failurePolicy": "required",
                    "state": "failed",
                    "startedAt": "2026-08-02T12:01:44Z",
                    "durationMilliseconds": 100,
                    "detail": {
                        "phase": "execution",
                        "code": "command_exit",
                        "exitCode": 23
                    },
                    "commandOutput": command_output
                }),
                serde_json::json!({
                    "id": "second",
                    "kind": "cmd",
                    "failurePolicy": "required",
                    "state": "blocked",
                    "detail": {
                        "code": "prerequisites_unsatisfied",
                        "prerequisites": [{"kind": "control", "node": "first"}]
                    }
                }),
            ],
            Some(serde_json::json!({
                "node": { "id": "first", "role": "step" },
                "state": "failed",
                "detail": { "phase": "execution", "code": "command_exit", "exitCode": 23 }
            })),
        ),
        AttemptStateV1::Cancelled => (
            "cancelled",
            vec![
                serde_json::json!({
                    "id": "first",
                    "kind": "cmd",
                    "failurePolicy": "required",
                    "state": "cancelled",
                    "detail": { "code": "user_request" }
                }),
                serde_json::json!({
                    "id": "second",
                    "kind": "cmd",
                    "failurePolicy": "required",
                    "state": "cancelled",
                    "detail": { "code": "user_request" }
                }),
            ],
            None,
        ),
        state => panic!("unsupported fixture state: {state:?}"),
    };
    for step in &mut steps {
        step["role"] = Value::String("step".to_owned());
    }
    let mut result = serde_json::json!({
        "schemaVersion": 1,
        "attemptNumber": attempt.attempt_number,
        "workflow": {
            "path": fixture.admitted.workflow().source.workflow_path,
            "provenance": {
                "kind": "local",
                "sourceRoot": fixture.admitted.workflow().source.source_root
            },
            "digest": {
                "algorithm": run_document.workflow_digest.algorithm,
                "value": run_document.workflow_digest.value
            }
        },
        "execution": {
            "executionRoot": attempt.execution_root,
            "maximumParallelSteps": 2,
            "startedAt": "2026-08-02T12:01:44Z",
            "finishedAt": "2026-08-02T12:01:45.25Z",
            "durationMilliseconds": 1250
        },
        "commandOutputPolicy": {
            "encoding": "base64",
            "maximumRetainedBytesPerStream": crate::workflow::MAXIMUM_RETAINED_BYTES_PER_STREAM
        },
        "outcome": outcome,
        "forceAbort": attempt.force_abort,
        "steps": steps,
        "exports": {}
    });
    if let Some(primary_issue) = primary_issue {
        result["primaryIssue"] = primary_issue;
    }
    if let Some(cancellation) = &attempt.cancellation {
        result["cancellation"] = serde_json::json!({
            "reason": cancellation.reason,
            "forceStopDeadline": cancellation.force_stop_deadline,
        });
    }
    let result_directory = run
        .run_directory()
        .join(attempt_result_relative_path(attempt.attempt_number));
    fs::create_dir_all(result_directory.join("exports")).unwrap();
    fs::write(result_directory.join("result.json"), json_bytes(result)).unwrap();
    run.record_result_published().unwrap();
    result_directory
}

#[test]
fn archived_attempt_preserves_advisory_issues_on_a_succeeded_attempt() {
    let fixture = AdmittedFixture::from_source(
        "schemaVersion: 1\nsteps:\n  first:\n    kind: cmd\n    failurePolicy: advisory\n    command:\n      argv: [\"true\"]\n    outputs:\n      report:\n        kind: file\n        from: path\n        path: report.txt\n        mediaType: text/plain\n  second:\n    kind: cmd\n    failurePolicy: advisory\n    inputs:\n      report:\n        ref: outputs.first.report\n    command:\n      argv: [\"true\"]\n",
    );
    let run_path = fixture.run_path("archive-advisory-success");
    let run = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    settle_as_succeeded(&run);
    let result_directory = publish_result_fixture(&fixture, &run);
    let mut result = result_value(&result_directory);
    result["steps"][0]["failurePolicy"] = Value::String("advisory".to_owned());
    result["steps"][0]["state"] = Value::String("failed".to_owned());
    result["steps"][0]["detail"] = serde_json::json!({
        "phase": "execution",
        "code": "command_exit",
        "exitCode": 9
    });
    result["steps"][1] = serde_json::json!({
        "id": "second",
        "role": "step",
        "kind": "cmd",
        "failurePolicy": "advisory",
        "state": "blocked",
        "detail": {
            "code": "prerequisites_unsatisfied",
            "prerequisites": [{"kind": "body", "ref": "outputs.first.report"}]
        }
    });
    overwrite_result(&result_directory, result);
    run.state
        .update(|state| {
            let progress = &mut current_attempt_mut(state)?.progress.steps;
            progress[0].state = AttemptStepStateV1::Failed;
            progress[0].detail = Some(crate::workflow::evidence::NodeDetail::Failed(
                crate::workflow::evidence::FailureDetail::new(
                    crate::workflow::evidence::FailurePhase::Execution,
                    crate::workflow::evidence::FailureCode::CommandExit,
                    None,
                    None,
                    None,
                    Some(9),
                )
                .unwrap(),
            ));
            progress[1].state = AttemptStepStateV1::Blocked;
            progress[1].detail = Some(crate::workflow::evidence::NodeDetail::Blocked(
                crate::workflow::evidence::BlockedDetail::new([
                    crate::workflow::evidence::Prerequisite::body("outputs.first.report").unwrap(),
                ])
                .unwrap(),
            ));
            Ok(())
        })
        .unwrap();

    let archived = load_local_archived_attempt(&run_path, None).unwrap();

    assert_eq!(archived.projection.state, AttemptStateV1::Succeeded);
    assert_eq!(archived.projection.outcome, WorkflowOutcomeV1::Succeeded);
    assert!(archived.projection.primary_issue.is_none());
    assert!(matches!(
        archived.projection.steps[0].detail,
        ArchivedStepDetail::Evidence(crate::workflow::evidence::NodeDetail::Failed(_))
    ));
    assert!(matches!(
        archived.projection.steps[1].detail,
        ArchivedStepDetail::Evidence(crate::workflow::evidence::NodeDetail::Blocked(_))
    ));
    assert!(
        archived
            .projection
            .steps
            .iter()
            .all(|step| step.failure_policy == super::super::document::FailurePolicy::Advisory)
    );
}

fn result_value(result_directory: &Path) -> Value {
    serde_json::from_slice(&fs::read(result_directory.join("result.json")).unwrap()).unwrap()
}

fn overwrite_result(result_directory: &Path, value: Value) {
    fs::write(result_directory.join("result.json"), json_bytes(value)).unwrap();
}

fn assert_archive_ineligible(
    failure: ArchivedAttemptLoadError,
    reason: ArchivedAttemptIneligibilityReason,
) {
    let ArchivedAttemptLoadError::Ineligible(failure) = failure else {
        panic!("expected attempt ineligibility, got {failure:?}");
    };
    assert_eq!(failure.reason, reason);
}

fn assert_archive_operational(
    failure: ArchivedAttemptLoadError,
    code: ArchivedAttemptOperationalErrorCode,
) {
    let ArchivedAttemptLoadError::Operational(failure) = failure else {
        panic!("expected archive operational failure, got {failure:?}");
    };
    assert_eq!(failure.code, code);
}

fn durable_tree(path: &Path) -> BTreeMap<PathBuf, Option<Vec<u8>>> {
    fn visit(root: &Path, path: &Path, entries: &mut BTreeMap<PathBuf, Option<Vec<u8>>>) {
        let mut children = fs::read_dir(path)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect::<Vec<_>>();
        children.sort();
        for child in children {
            let relative = child.strip_prefix(root).unwrap().to_owned();
            if child.is_dir() {
                entries.insert(relative, None);
                visit(root, &child, entries);
            } else {
                entries.insert(relative, Some(fs::read(&child).unwrap()));
            }
        }
    }
    let mut entries = BTreeMap::new();
    visit(path, path, &mut entries);
    entries
}

#[test]
fn archived_attempt_loads_failed_current_result_and_raw_stream_prefixes_read_only() {
    let fixture = AdmittedFixture::new();
    let run_path = fixture.run_path("archive-current");
    let run = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    settle_as_workflow_failed(&run);
    let result_directory = publish_result_fixture(&fixture, &run);
    let expected_result = result_value(&result_directory);
    let before = durable_tree(&run_path);
    let result_open_count = std::cell::Cell::new(0_u8);

    let archived = load_local_archived_attempt_observed(
        &run_path,
        None,
        |_| {},
        |_| result_open_count.set(result_open_count.get().saturating_add(1)),
    )
    .unwrap();

    assert_eq!(archived.projection.current_attempt_number, 1);
    assert_eq!(archived.projection.attempt_number, 1);
    assert_eq!(archived.projection.state, AttemptStateV1::WorkflowFailed);
    assert_eq!(archived.projection.outcome, WorkflowOutcomeV1::Failed);
    assert_eq!(archived.projection.result_directory, result_directory);
    assert_eq!(
        serde_json::to_value(&archived.result).unwrap(),
        expected_result,
        "the loader must expose the complete value from its validated immutable read"
    );
    assert_eq!(result_open_count.get(), 1);
    assert_eq!(
        archived.projection.workflow.presentation_order,
        ["first", "second"]
    );
    assert_eq!(archived.projection.steps.len(), 2);
    assert!(matches!(
        archived.projection.steps[0].detail,
        ArchivedStepDetail::Evidence(crate::workflow::evidence::NodeDetail::Failed(_))
    ));
    let output = archived.projection.steps[0]
        .command_output
        .as_ref()
        .unwrap();
    assert_eq!(output.stdout.bytes.as_ref(), [0_u8, 0xff, b'\n']);
    assert_eq!(output.stdout.retained_bytes, 3);
    assert_eq!(output.stdout.discarded_bytes, 0);
    assert!(!output.stdout.truncated);
    assert!(!output.stdout.fully_drained);
    assert_eq!(durable_tree(&run_path), before);
    assert_eq!(read_state(run.root_handle()).unwrap().attempts.len(), 1);
}

#[test]
fn archived_attempt_selects_current_and_explicit_historical_publications() {
    let fixture = AdmittedFixture::new();
    let run_path = fixture.run_path("archive-history");
    let initial = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    settle_as_workflow_failed(&initial);
    publish_result_fixture(&fixture, &initial);
    drop(initial);

    let LocalRetryOpen::Acquired(pending) = acquire_local_retry(&run_path).unwrap() else {
        panic!("failed initial attempt should be retryable");
    };
    let retry = (*pending)
        .begin(&fixture.admitted)
        .unwrap_or_else(|_| panic!("retry should begin"));
    settle_as_succeeded(&retry);
    publish_result_fixture(&fixture, &retry);

    let current = load_local_archived_attempt(&run_path, None).unwrap();
    assert_eq!(current.projection.current_attempt_number, 2);
    assert_eq!(current.projection.attempt_number, 2);
    assert_eq!(current.projection.outcome, WorkflowOutcomeV1::Succeeded);

    let historical =
        load_local_archived_attempt(&run_path, Some(NonZeroU64::new(1).unwrap())).unwrap();
    assert_eq!(historical.projection.current_attempt_number, 2);
    assert_eq!(historical.projection.attempt_number, 1);
    assert_eq!(historical.projection.outcome, WorkflowOutcomeV1::Failed);

    assert_archive_ineligible(
        load_local_archived_attempt(&run_path, Some(NonZeroU64::new(3).unwrap())).unwrap_err(),
        ArchivedAttemptIneligibilityReason::Unknown,
    );
}

#[test]
fn archived_attempt_reports_each_nonpublished_disposition_without_fallback() {
    let fixture = AdmittedFixture::new();
    let run_path = fixture.run_path("archive-nonterminal");
    let run = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    assert_archive_ineligible(
        load_local_archived_attempt(&run_path, None).unwrap_err(),
        ArchivedAttemptIneligibilityReason::Nonterminal,
    );

    run.record_executor_fault_before_execution().unwrap();
    assert_archive_ineligible(
        load_local_archived_attempt(&run_path, None).unwrap_err(),
        ArchivedAttemptIneligibilityReason::Interrupted,
    );

    let fixture = AdmittedFixture::new();
    let run_path = fixture.run_path("archive-rejected");
    let run = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    run.state
        .update(|state| {
            let attempt = current_attempt_mut(state)?;
            attempt.state = AttemptStateV1::Rejected;
            attempt.settled_at = Some(attempt.created_at.clone());
            attempt.settlement_snapshot = Some(fixture_settlement_snapshot());
            attempt.rejection = Some(AttemptRejectionV1 {
                code: RejectionCodeV1::ImmutableSpecificationUnusable,
            });
            attempt.result = AttemptResultV1::NotPublished {
                reason: ResultAbsentReasonV1::Rejected,
            };
            Ok(())
        })
        .unwrap();
    assert_archive_ineligible(
        load_local_archived_attempt(&run_path, None).unwrap_err(),
        ArchivedAttemptIneligibilityReason::Rejected,
    );

    let fixture = AdmittedFixture::new();
    let run_path = fixture.run_path("archive-pending");
    let run = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    settle_as_workflow_failed(&run);
    assert_archive_ineligible(
        load_local_archived_attempt(&run_path, None).unwrap_err(),
        ArchivedAttemptIneligibilityReason::Unpublished,
    );

    run.record_result_publication_failed(PublicationFailurePhaseV1::Serialization, None)
        .unwrap();
    assert_archive_ineligible(
        load_local_archived_attempt(&run_path, None).unwrap_err(),
        ArchivedAttemptIneligibilityReason::PublicationFailed,
    );
}

#[test]
fn archived_load_does_not_finalize_pending_publication_and_reconciliation_reports_failure() {
    let fixture = AdmittedFixture::new();
    let run_path = fixture.run_path("archive-pending-finalization");
    let run = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    settle_as_succeeded(&run);
    let result_directory = publish_result_fixture(&fixture, &run);
    run.state
        .update(|state| {
            state.attempts[0].result = AttemptResultV1::NotPublished {
                reason: ResultAbsentReasonV1::PublicationPending,
            };
            Ok(())
        })
        .unwrap();
    drop(run);

    load_local_archived_attempt(&run_path, None).unwrap();
    assert!(matches!(
        read_stable_local_run_snapshot(&run_path)
            .unwrap()
            .state
            .attempts[0]
            .result,
        AttemptResultV1::NotPublished {
            reason: ResultAbsentReasonV1::PublicationPending
        }
    ));
    let valid_result = fs::read(result_directory.join("result.json")).unwrap();
    fs::write(result_directory.join("result.json"), b"invalid").unwrap();
    assert_archive_operational(
        super::super::archived_attempt::reconcile_current_result_publication(&run_path)
            .unwrap_err(),
        ArchivedAttemptOperationalErrorCode::ProjectionInvariant(ProjectionInvariant::ArtifactSet),
    );
    assert!(matches!(
        read_stable_local_run_snapshot(&run_path)
            .unwrap()
            .state
            .attempts[0]
            .result,
        AttemptResultV1::NotPublished {
            reason: ResultAbsentReasonV1::PublicationPending
        }
    ));
    fs::write(result_directory.join("result.json"), valid_result).unwrap();
    let syncs = std::cell::Cell::new(0);
    assert!(
        mark_validated_result_published_with(&run_path, 1, |directory| {
            syncs.set(syncs.get() + 1);
            if syncs.get() == 2 {
                Err(file_error(
                    directory,
                    ".",
                    "sync directory",
                    std::io::Error::other("injected attempt-parent sync failure"),
                ))
            } else {
                sync_directory(directory)
            }
        })
        .is_err()
    );
    assert_eq!(syncs.get(), 2);
    assert!(matches!(
        read_stable_local_run_snapshot(&run_path)
            .unwrap()
            .state
            .attempts[0]
            .result,
        AttemptResultV1::NotPublished {
            reason: ResultAbsentReasonV1::PublicationPending
        }
    ));
    super::super::archived_attempt::reconcile_current_result_publication(&run_path).unwrap();
    assert!(matches!(
        read_stable_local_run_snapshot(&run_path)
            .unwrap()
            .state
            .attempts[0]
            .result,
        AttemptResultV1::Published { .. }
    ));
}

#[test]
fn publication_failure_persists_only_the_closed_result_invariant() {
    let fixture = AdmittedFixture::new();
    let run_path = fixture.run_path("publication-invariant");
    let run = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    settle_as_workflow_failed(&run);

    run.record_result_publication_failed(
        PublicationFailurePhaseV1::Serialization,
        Some(RunResultInvariant::ExportValues),
    )
    .unwrap();

    let state = read_state(run.root_handle()).unwrap();
    assert!(matches!(
        &state.attempts[0].result,
        AttemptResultV1::PublicationFailed {
            phase: PublicationFailurePhaseV1::Serialization,
            result_invariant: Some(RunResultInvariant::ExportValues),
        }
    ));
    let status = read_local_run_status(&run_path).unwrap();
    assert_eq!(
        status.state["attempts"][0]["result"],
        serde_json::json!({
            "status": "publication_failed",
            "phase": "serialization",
            "resultInvariant": "export_values"
        })
    );

    let diagnostic = state.diagnostics.last().unwrap();
    assert_eq!(diagnostic.code, DiagnosticCodeV1::ResultPublicationFailure);
    assert_eq!(diagnostic.step_id, None);
    assert_eq!(diagnostic.action_id, None);
    assert_eq!(diagnostic.guard_id, None);

    let mut inconsistent = state;
    inconsistent.attempts[0].result = AttemptResultV1::PublicationFailed {
        phase: PublicationFailurePhaseV1::Rename,
        result_invariant: Some(RunResultInvariant::ExportValues),
    };
    assert_eq!(
        decode_state(&encode_json(&inconsistent).unwrap()),
        Err(LocalRunDirectoryError::AttemptResultInvalid)
    );
}

#[test]
fn archived_attempt_rejects_malformed_and_cross_document_mismatched_results() {
    let fixture = AdmittedFixture::new();
    let run_path = fixture.run_path("archive-invalid-result");
    let run = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    settle_as_workflow_failed(&run);
    let result_directory = publish_result_fixture(&fixture, &run);
    let valid = result_value(&result_directory);

    fs::write(
        result_directory.join("result.json"),
        b"{\"schemaVersion\":1}",
    )
    .unwrap();
    assert_archive_operational(
        load_local_archived_attempt(&run_path, None).unwrap_err(),
        ArchivedAttemptOperationalErrorCode::ProjectionInvariant(ProjectionInvariant::ArtifactSet),
    );

    let mut invalid_values = Vec::new();
    let mut value = valid.clone();
    value["unknown"] = Value::Bool(true);
    invalid_values.push((value, ProjectionInvariant::ArtifactSet));
    let mut value = valid.clone();
    value["attemptNumber"] = Value::from(2);
    invalid_values.push((value, ProjectionInvariant::AttemptMetadata));
    let mut value = valid.clone();
    value["workflow"]["digest"]["value"] = Value::String("0".repeat(64));
    invalid_values.push((value, ProjectionInvariant::AttemptMetadata));
    let mut value = valid.clone();
    value["execution"]["executionRoot"] = Value::String("/different".to_owned());
    invalid_values.push((value, ProjectionInvariant::AttemptMetadata));
    let mut value = valid.clone();
    value["outcome"] = Value::String("succeeded".to_owned());
    value.as_object_mut().unwrap().remove("primaryIssue");
    invalid_values.push((value, ProjectionInvariant::ArtifactSet));
    let mut value = valid.clone();
    value["steps"].as_array_mut().unwrap().swap(0, 1);
    invalid_values.push((value, ProjectionInvariant::Steps));
    let mut value = valid.clone();
    value["steps"][0]["commandOutput"]["stdout"]["retainedBytes"] = Value::from(2);
    invalid_values.push((value, ProjectionInvariant::ArtifactSet));
    let mut value = valid.clone();
    value["steps"][0]["detail"]["code"] = Value::String("future_code".to_owned());
    value["primaryIssue"]["detail"]["code"] = Value::String("future_code".to_owned());
    invalid_values.push((value, ProjectionInvariant::ArtifactSet));

    for (value, invariant) in invalid_values {
        overwrite_result(&result_directory, value);
        assert_archive_operational(
            load_local_archived_attempt(&run_path, None).unwrap_err(),
            ArchivedAttemptOperationalErrorCode::ProjectionInvariant(invariant),
        );
    }
}

#[test]
fn archived_attempt_uses_only_the_authoritative_recorded_result_location() {
    let fixture = AdmittedFixture::new();
    let run_path = fixture.run_path("archive-result-location");
    let run = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    settle_as_workflow_failed(&run);
    publish_result_fixture(&fixture, &run);
    let mut state: Value =
        serde_json::from_slice(&fs::read(run_path.join(STATE_FILE)).unwrap()).unwrap();
    state["attempts"][0]["result"]["relativeDirectory"] =
        Value::String("attempts/000001/other".to_owned());
    fs::write(run_path.join(STATE_FILE), json_bytes(state)).unwrap();

    assert_archive_operational(
        load_local_archived_attempt(&run_path, None).unwrap_err(),
        ArchivedAttemptOperationalErrorCode::RunDirectoryInvalid,
    );
}

// REQ-LOCALRUN-166
#[test]
fn archived_attempt_rejects_a_broken_retained_workflow_closure() {
    let fixture = AdmittedFixture::new();
    let run_path = fixture.run_path("archive-broken-closure");
    let run = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    settle_as_workflow_failed(&run);
    publish_result_fixture(&fixture, &run);
    let retained_source = run_path.join("workflow/files/0001");
    fs::set_permissions(&retained_source, Permissions::from_mode(0o600)).unwrap();
    fs::write(retained_source, b"changed\n").unwrap();

    assert_archive_operational(
        load_local_archived_attempt(&run_path, None).unwrap_err(),
        ArchivedAttemptOperationalErrorCode::RetainedWorkflowInvalid,
    );
}

#[test]
fn archived_attempt_loads_cancelled_commands_that_never_started() {
    let fixture = AdmittedFixture::new();
    let run_path = fixture.run_path("archive-pending-cancellation");
    let run = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    settle_as_cancelled(&run);
    publish_result_fixture(&fixture, &run);

    let archived = load_local_archived_attempt(&run_path, None).unwrap();

    assert_eq!(archived.projection.state, AttemptStateV1::Cancelled);
    assert_eq!(archived.projection.outcome, WorkflowOutcomeV1::Cancelled);
    assert!(archived.projection.steps.iter().all(|step| {
        matches!(
            step.detail,
            ArchivedStepDetail::Evidence(crate::workflow::evidence::NodeDetail::Cancellation(_))
        ) && step.started_at.is_none()
            && step.duration.is_none()
            && step.command_output.is_none()
    }));
}

#[test]
fn archived_attempt_loads_ordinary_force_with_suppressed_finalization() {
    let fixture = AdmittedFixture::from_source(
        "schemaVersion: 1\nsteps:\n  first:\n    kind: cmd\n    command: { argv: [\"true\"] }\n  second:\n    kind: cmd\n    dependsOn: [first]\n    command: { argv: [\"true\"] }\nfinalizers:\n  cleanup:\n    kind: cmd\n    command: { argv: [\"true\"] }\n",
    );
    let run_path = fixture.run_path("archive-ordinary-force");
    let run = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    settle_as_force_cancelled_with_finalizer(
        &run,
        super::super::runtime::RunCancellationPhase::Ordinary,
    );
    let result_directory = publish_result_fixture(&fixture, &run);
    let mut result = result_value(&result_directory);
    for step in result["steps"].as_array_mut().unwrap() {
        step["detail"] = json!({ "code": "force_abort" });
    }
    result["finalization"] = json!({
        "trigger": "cancelled",
        "finalizers": [{
            "id": "cleanup",
            "role": "finalizer",
            "kind": "cmd",
            "failurePolicy": "required",
            "state": "cancelled",
            "detail": { "code": "force_abort" }
        }],
        "issues": [],
        "cancellation": { "reason": "force_abort" },
        "forceAbort": true
    });
    overwrite_result(&result_directory, result);

    let archived = load_local_archived_attempt(&run_path, None).unwrap();

    assert_eq!(
        archived
            .projection
            .force_abort
            .map(|force_abort| force_abort.phase),
        Some(super::super::publication::ForceAbortPhaseV1::Ordinary)
    );
    assert_eq!(
        archived
            .projection
            .finalization
            .as_ref()
            .and_then(|finalization| finalization.cancellation.as_ref())
            .map(|cancellation| cancellation.reason),
        Some(CancellationReasonV1::ForceAbort)
    );
}

#[test]
fn archived_attempt_loads_valid_result_larger_than_state_document_limit() {
    let maximum_retained_bytes_per_stream = 131_072_u64;
    let mut source = String::from("schemaVersion: 1\nsteps:\n");
    for index in 0..256 {
        source.push_str(&format!(
            "  step{index}:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n"
        ));
    }
    let fixture = AdmittedFixture::from_source_with_maximum_step_log_bytes(
        &source,
        maximum_retained_bytes_per_stream,
    );
    let run_path = fixture.run_path("archive-large-result");
    let run = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    settle_as_succeeded(&run);

    let durable = read_state(run.root_handle()).unwrap();
    let attempt = durable.attempts.last().unwrap();
    let run_document = read_run(run.root_handle()).unwrap();
    let stream = serde_json::json!({
        "encoding": "base64",
        "data": BASE64_STANDARD.encode(vec![
            b'x';
            usize::try_from(maximum_retained_bytes_per_stream).unwrap()
        ]),
        "retainedBytes": maximum_retained_bytes_per_stream,
        "discardedBytes": 0,
        "truncated": false,
        "fullyDrained": true
    });
    let steps = attempt
        .progress
        .steps
        .iter()
        .map(|step| {
            serde_json::json!({
                "id": step.id,
                "role": "step",
                "kind": "cmd",
                "failurePolicy": "required",
                "state": "succeeded",
                "startedAt": "2026-08-02T12:01:44Z",
                "durationMilliseconds": 1,
                "commandOutput": {
                    "stdout": stream.clone(),
                    "stderr": stream.clone()
                }
            })
        })
        .collect::<Vec<_>>();
    let result = serde_json::json!({
        "schemaVersion": 1,
        "attemptNumber": attempt.attempt_number,
        "workflow": {
            "path": fixture.admitted.workflow().source.workflow_path,
            "provenance": {
                "kind": "local",
                "sourceRoot": fixture.admitted.workflow().source.source_root
            },
            "digest": {
                "algorithm": run_document.workflow_digest.algorithm,
                "value": run_document.workflow_digest.value
            }
        },
        "execution": {
            "executionRoot": attempt.execution_root,
            "maximumParallelSteps": 2,
            "startedAt": "2026-08-02T12:01:44Z",
            "finishedAt": "2026-08-02T12:01:45Z",
            "durationMilliseconds": 1000
        },
        "commandOutputPolicy": {
            "encoding": "base64",
            "maximumRetainedBytesPerStream": fixture
                .admitted
                .execution()
                .limits()
                .maximum_step_log_bytes()
                .get()
        },
        "outcome": "succeeded",
        "forceAbort": null,
        "steps": steps,
        "exports": {}
    });
    let result_bytes = json_bytes(result);
    assert!(u64::try_from(result_bytes.len()).unwrap() > MAXIMUM_DURABLE_JSON_BYTES);
    let result_directory = run
        .run_directory()
        .join(attempt_result_relative_path(attempt.attempt_number));
    fs::create_dir_all(result_directory.join("exports")).unwrap();
    fs::write(result_directory.join("result.json"), result_bytes).unwrap();
    run.record_result_published().unwrap();

    let archived = load_local_archived_attempt(&run_path, None)
        .expect("the result schema bounds streams independently, not the whole document");
    assert_eq!(archived.projection.steps.len(), 256);
}

#[test]
fn artifact_set_carrier_boundary_is_shared_by_archive_and_portable_inspection() {
    // REQ-WFFORMAT-76 bounds the reachable metadata: the largest declared media types that fit
    // a 1 MiB workflow file beside 4,096 maximal export names.
    let media_type = format!("a/b;x={}", "\u{1f600}".repeat(20));
    let mut source = String::from(
        "schemaVersion: 1\nsteps:\n  produce:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n    outputs:\n",
    );
    for index in 0..4_096 {
        source.push_str(&format!("      o{index:04}: {{kind: file, from: path, path: p{index:04}.bin, mediaType: \"{media_type}\"}}\n"));
    }
    source.push_str("exports:\n");
    for index in 0..4_096 {
        let name = format!("e{}{index:04}", "a".repeat(59));
        source.push_str(&format!("  {name}: {{ref: outputs.produce.o{index:04}}}\n"));
    }
    let fixture = AdmittedFixture::from_source(&source);
    let run_path = fixture.run_path("large-result");
    let run = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    settle_as_succeeded(&run);
    let output_names = (0..4_096)
        .map(|index| format!("o{index:04}"))
        .collect::<Vec<_>>();
    let retained = output_names
        .iter()
        .map(|name| (name.as_str(), media_type.as_str(), b"x".as_slice()))
        .collect::<Vec<_>>();
    retain_file_outputs_for_step(&run, "produce", &retained);

    let durable = read_state(run.root_handle()).unwrap();
    let attempt = durable.attempts.last().unwrap();
    let run_document = read_run(run.root_handle()).unwrap();
    let metadata = serde_json::json!({
        "state": "available",
        "kind": "file",
        "mediaType": media_type,
        "path": "exports/0001",
        "sizeBytes": 1,
        "digest": {
            "algorithm": "sha256",
            "value": "2d711642b726b04401627ca9fbac32f5c8530fb1903cc4db02258717921a4881"
        }
    });
    let exports = (0..4_096)
        .map(|index| {
            let mut entry = metadata.clone();
            entry["path"] = format!("exports/{:04}", index + 1).into();
            (format!("e{}{index:04}", "a".repeat(59)), entry)
        })
        .collect::<serde_json::Map<_, _>>();
    let result = serde_json::json!({
        "schemaVersion": 1,
        "attemptNumber": attempt.attempt_number,
        "workflow": {
            "path": fixture.admitted.workflow().source.workflow_path,
            "provenance": {
                "kind": "local",
                "sourceRoot": fixture.admitted.workflow().source.source_root
            },
            "digest": {
                "algorithm": run_document.workflow_digest.algorithm,
                "value": run_document.workflow_digest.value
            }
        },
        "execution": {
            "executionRoot": attempt.execution_root,
            "maximumParallelSteps": 2,
            "startedAt": "2026-08-02T12:01:44Z",
            "finishedAt": "2026-08-02T12:01:45Z",
            "durationMilliseconds": 1000
        },
        "commandOutputPolicy": {
            "encoding": "base64",
            "maximumRetainedBytesPerStream": crate::workflow::MAXIMUM_RETAINED_BYTES_PER_STREAM
        },
        "outcome": "succeeded",
        "forceAbort": null,
        "steps": [{
            "id": "produce",
            "role": "step",
            "kind": "cmd",
            "failurePolicy": "required",
            "state": "succeeded",
            "startedAt": "2026-08-02T12:01:44Z",
            "durationMilliseconds": 1000,
            "commandOutput": {
                "stdout": {
                    "encoding": "base64",
                    "data": "",
                    "retainedBytes": 0,
                    "discardedBytes": 0,
                    "truncated": false,
                    "fullyDrained": true
                },
                "stderr": {
                    "encoding": "base64",
                    "data": "",
                    "retainedBytes": 0,
                    "discardedBytes": 0,
                    "truncated": false,
                    "fullyDrained": true
                }
            }
        }],
        "exports": exports
    });
    let result_bytes = json_bytes(result);
    assert!(
        u64::try_from(result_bytes.len()).unwrap()
            <= crate::workflow::result_metadata::MAXIMUM_RESULT_JSON_BYTES
    );
    assert!(
        u64::try_from(result_bytes.len()).unwrap() <= MAXIMUM_DURABLE_JSON_BYTES,
        "maximal artifact metadata must fit the unified durable document budget"
    );
    let result_directory = run
        .run_directory()
        .join(attempt_result_relative_path(attempt.attempt_number));
    fs::create_dir_all(result_directory.join("exports")).unwrap();
    for index in 1..=4_096 {
        fs::write(result_directory.join(format!("exports/{index:04}")), b"x").unwrap();
    }
    fs::write(result_directory.join("result.json"), &result_bytes).unwrap();
    let result_root = open_directory_path(&result_directory).unwrap();
    let valid_result = crate::workflow::result_metadata::decode(&result_bytes).unwrap();
    crate::workflow::artifact_set::validate(&result_root, &valid_result).unwrap();
    crate::workflow::artifact_set::read_and_validate(
        &result_root,
        crate::workflow::result_metadata::MAXIMUM_RESULT_JSON_BYTES,
    )
    .expect("fixture must be valid Artifact Set V1");
    run.record_result_published().unwrap();

    load_local_archived_attempt(&run_path, None)
        .expect("a valid published Artifact Set V1 result must remain inspectable");
    let inspect = || {
        crate::workflow::portable_artifact::validate_portable_artifact_set(
            &result_directory,
            &std::sync::atomic::AtomicBool::new(false),
        )
        .unwrap()
    };
    let valid_report = inspect();
    assert!(valid_report.is_valid(), "{:?}", valid_report.diagnostics);
    assert_eq!(valid_report.summary.unwrap().referenced_carriers, 4_096);

    let mut overflow = serde_json::from_slice::<Value>(&result_bytes).unwrap();
    let mut extra = metadata;
    extra["path"] = "exports/4097".into();
    overflow["exports"]
        .as_object_mut()
        .unwrap()
        .insert("overflow".into(), extra);
    let overflow_result = serde_json::from_value(overflow.clone()).unwrap();
    fs::write(result_directory.join("exports/4097"), b"x").unwrap();
    overwrite_result(&result_directory, overflow);
    let direct_error =
        crate::workflow::artifact_set::validate(&result_root, &overflow_result).unwrap_err();
    assert_eq!(direct_error.code(), Some("carrier_limit_exceeded"));
    let set_error = crate::workflow::artifact_set::read_and_validate(
        &result_root,
        crate::workflow::result_metadata::MAXIMUM_RESULT_JSON_BYTES,
    )
    .unwrap_err();
    assert_eq!(set_error.code(), Some("carrier_limit_exceeded"));
    let archive_error = load_local_archived_attempt(&run_path, None).unwrap_err();
    assert_archive_operational(
        archive_error,
        ArchivedAttemptOperationalErrorCode::CarrierLimitExceeded,
    );
    assert_eq!(
        crate::workflow::archived_presentation::operational_error_code(
            ArchivedAttemptOperationalErrorCode::CarrierLimitExceeded,
        ),
        set_error.code().unwrap(),
    );
    let invalid_report = inspect();
    assert!(!invalid_report.is_valid());
    assert!(
        invalid_report
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code() == "carrier_limit_exceeded")
    );
}

#[test]
fn archived_attempt_binds_retained_output_kind_and_export_metadata() {
    let fixture = AdmittedFixture::from_source(
        "schemaVersion: 1\nsteps:\n  first:\n    kind: cmd\n    command: {argv: [\"true\"]}\n    outputs:\n      message:\n        kind: text\n        from: path\n        path: message.txt\n  second:\n    kind: cmd\n    dependsOn: [first]\n    command: {argv: [\"true\"]}\nexports:\n  message:\n    ref: outputs.first.message\n",
    );
    let run_path = fixture.run_path("archive-output-binding");
    let run = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    settle_as_succeeded(&run);
    let retained_bytes = b"\"evidence\"";
    retain_text_output_for_step(&run, "first", "message", retained_bytes);
    let result_directory = publish_result_fixture(&fixture, &run);
    let mut valid = result_value(&result_directory);
    valid["exports"] = json!({
        "message": {
            "state": "available",
            "kind": "text",
            "mediaType": "text/plain; charset=utf-8",
            "path": "exports/0001",
            "sizeBytes": retained_bytes.len(),
            "digest": {
                "algorithm": "sha256",
                "value": DigestV1::sha256(retained_bytes).value
            }
        }
    });
    fs::write(result_directory.join("exports/0001"), retained_bytes).unwrap();
    overwrite_result(&result_directory, valid.clone());
    load_local_archived_attempt(&run_path, None).unwrap();

    let substituted = b"different";
    let mut substituted_result = valid.clone();
    substituted_result["exports"]["message"]["sizeBytes"] = substituted.len().into();
    substituted_result["exports"]["message"]["digest"]["value"] =
        DigestV1::sha256(substituted).value.into();
    fs::write(result_directory.join("exports/0001"), substituted).unwrap();
    overwrite_result(&result_directory, substituted_result);
    assert_archive_operational(
        load_local_archived_attempt(&run_path, None).unwrap_err(),
        ArchivedAttemptOperationalErrorCode::ProjectionInvariant(ProjectionInvariant::Exports),
    );

    fs::write(result_directory.join("exports/0001"), retained_bytes).unwrap();
    overwrite_result(&result_directory, valid);
    run.state
        .update(|state| {
            let outputs = current_attempt_mut(state)?.progress.steps[0]
                .outputs
                .as_mut()
                .ok_or(LocalRunDirectoryError::StateInvalid)?;
            let carrier = match outputs.first() {
                Some(RetainedOutputV1::Text { carrier, .. }) => carrier.clone(),
                _ => return Err(LocalRunDirectoryError::StateInvalid),
            };
            outputs[0] = RetainedOutputV1::Json {
                name: "message".to_owned(),
                producer: None,
                carrier: RetainedCarrierV1 {
                    media_type: "application/json".to_owned(),
                    ..carrier
                },
            };
            Ok(())
        })
        .unwrap();
    assert_archive_operational(
        load_local_archived_attempt(&run_path, None).unwrap_err(),
        ArchivedAttemptOperationalErrorCode::RetainedWorkflowInvalid,
    );
}

#[test]
fn archived_attempt_enforces_alias_source_identity() {
    let fixture = AdmittedFixture::from_source(
        "schemaVersion: 1\nsteps:\n  first:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n    outputs:\n      one:\n        kind: file\n        from: path\n        path: one.bin\n        mediaType: application/octet-stream\n      two:\n        kind: file\n        from: path\n        path: two.bin\n        mediaType: application/octet-stream\n  second:\n    kind: cmd\n    dependsOn: [first]\n    command:\n      argv: [\"true\"]\nexports:\n  a:\n    ref: outputs.first.one\n  b:\n    ref: outputs.first.one\n  c:\n    ref: outputs.first.two\n",
    );
    let run_path = fixture.run_path("archive-alias-identity");
    let run = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    settle_as_succeeded(&run);
    retain_file_outputs_for_step(
        &run,
        "first",
        &[
            ("one", "application/octet-stream", b"x"),
            ("two", "application/octet-stream", b"x"),
        ],
    );
    let result_directory = publish_result_fixture(&fixture, &run);
    let metadata = serde_json::json!({
        "state": "available",
        "kind": "file",
        "mediaType": "application/octet-stream",
        "path": "exports/0001",
        "sizeBytes": 1,
        "digest": {
            "algorithm": "sha256",
            "value": "2d711642b726b04401627ca9fbac32f5c8530fb1903cc4db02258717921a4881"
        }
    });
    let mut valid = result_value(&result_directory);
    valid["exports"] = serde_json::json!({
        "a": metadata.clone(),
        "b": metadata.clone(),
        "c": {
            "state": "available",
            "kind": "file",
            "mediaType": "application/octet-stream",
            "path": "exports/0003",
            "sizeBytes": 1,
            "digest": {
                "algorithm": "sha256",
                "value": "2d711642b726b04401627ca9fbac32f5c8530fb1903cc4db02258717921a4881"
            }
        }
    });
    overwrite_result(&result_directory, valid.clone());
    fs::write(result_directory.join("exports/0001"), b"x").unwrap();
    fs::write(result_directory.join("exports/0003"), b"x").unwrap();
    let result_root = open_directory_path(&result_directory).unwrap();
    crate::workflow::artifact_set::read_and_validate(
        &result_root,
        crate::workflow::result_metadata::MAXIMUM_RESULT_JSON_BYTES,
    )
    .expect("the alias and equal-content carriers form a valid Artifact Set V1 result");
    load_local_archived_attempt(&run_path, None)
        .expect("aliases of one retained output must share their owner carrier");

    let mut non_owner = valid.clone();
    non_owner["exports"]["b"]["path"] = Value::String("exports/0002".to_owned());
    overwrite_result(&result_directory, non_owner);
    fs::write(result_directory.join("exports/0002"), b"x").unwrap();
    crate::workflow::artifact_set::read_and_validate(
        &result_root,
        crate::workflow::result_metadata::MAXIMUM_RESULT_JSON_BYTES,
    )
    .expect("separate carriers are portable without the retained source identities");
    assert_archive_operational(
        load_local_archived_attempt(&run_path, None).unwrap_err(),
        ArchivedAttemptOperationalErrorCode::ProjectionInvariant(ProjectionInvariant::Exports),
    );

    let mut shared_by_distinct_sources = valid;
    shared_by_distinct_sources["exports"]["c"] = metadata;
    overwrite_result(&result_directory, shared_by_distinct_sources);
    fs::remove_file(result_directory.join("exports/0002")).unwrap();
    fs::remove_file(result_directory.join("exports/0003")).unwrap();
    crate::workflow::artifact_set::read_and_validate(
        &result_root,
        crate::workflow::result_metadata::MAXIMUM_RESULT_JSON_BYTES,
    )
    .expect("equal bytes are portable without the retained source identities");
    assert_archive_operational(
        load_local_archived_attempt(&run_path, None).unwrap_err(),
        ArchivedAttemptOperationalErrorCode::ProjectionInvariant(ProjectionInvariant::Exports),
    );
}

#[test]
fn archived_attempt_enforces_stream_prefix_retention_invariants() {
    let fixture = AdmittedFixture::new();
    let run_path = fixture.run_path("archive-stream-retention");
    let run = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    settle_as_workflow_failed(&run);
    let result_directory = publish_result_fixture(&fixture, &run);
    let valid = result_value(&result_directory);

    let maximum = crate::workflow::MAXIMUM_RETAINED_BYTES_PER_STREAM;
    let retained = vec![b'x'; usize::try_from(maximum).unwrap()];
    let mut full_prefix = valid.clone();
    full_prefix["steps"][0]["commandOutput"]["stdout"] = serde_json::json!({
        "encoding": "base64",
        "data": BASE64_STANDARD.encode(&retained),
        "retainedBytes": maximum,
        "discardedBytes": 1,
        "truncated": true,
        "fullyDrained": true
    });
    overwrite_result(&result_directory, full_prefix);
    let archived = load_local_archived_attempt(&run_path, None).unwrap();
    assert_eq!(
        archived.projection.steps[0]
            .command_output
            .as_ref()
            .unwrap()
            .stdout
            .bytes
            .as_ref(),
        retained
    );

    let mut impossible = valid;
    impossible["steps"][0]["commandOutput"]["stdout"]["discardedBytes"] = Value::from(1);
    impossible["steps"][0]["commandOutput"]["stdout"]["truncated"] = Value::Bool(true);
    overwrite_result(&result_directory, impossible);
    assert_archive_operational(
        load_local_archived_attempt(&run_path, None).unwrap_err(),
        ArchivedAttemptOperationalErrorCode::ProjectionInvariant(ProjectionInvariant::ArtifactSet),
    );
}

// REQ-EVIDENCE-7, REQ-EVIDENCE-13, REQ-OUTPUTS-5
#[test]
fn archived_attempt_validates_failure_identities_against_the_retained_step() {
    let fixture = AdmittedFixture::from_source_with_inputs(
        "schemaVersion: 1\ninputs:\n  request: {kind: text}\nsteps:\n  first:\n    kind: cmd\n    inputs:\n      prompt:\n        ref: inputs.request\n    command:\n      argv: [\"true\"]\n    outputs:\n      artifact:\n        kind: file\n        from: path\n        path: artifact.txt\n        mediaType: text/plain\n  second:\n    kind: cmd\n    dependsOn: [first]\n    command:\n      argv: [\"true\"]\n",
        ResolvedInputs::new(BTreeMap::from([(
            "request".to_owned(),
            ResolvedInput::Text(Arc::from("retained request")),
        )])),
        1024,
    );
    let run_path = fixture.run_path("archive-failure-identities");
    let run = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    settle_as_workflow_failed(&run);
    let result_directory = publish_result_fixture(&fixture, &run);
    let original = result_value(&result_directory);
    let set_failure_detail = |detail: crate::workflow::evidence::FailureDetail| {
        run.state
            .update(|state| {
                current_attempt_mut(state)?.progress.steps[0].detail = Some(
                    crate::workflow::evidence::NodeDetail::Failed(detail.clone()),
                );
                Ok(())
            })
            .unwrap();
    };

    let mut invalid_name = original.clone();
    let invalid_name_detail = serde_json::json!({
        "phase": "start",
        "code": "input_invalid_name",
        "input": "../escape"
    });
    invalid_name["steps"][0]["detail"] = invalid_name_detail.clone();
    invalid_name["steps"][0]
        .as_object_mut()
        .unwrap()
        .remove("commandOutput");
    invalid_name["primaryIssue"] = serde_json::json!({
        "node": { "id": "first", "role": "step" },
        "state": "failed",
        "detail": invalid_name_detail
    });
    overwrite_result(&result_directory, invalid_name);
    assert_archive_operational(
        load_local_archived_attempt(&run_path, None).unwrap_err(),
        ArchivedAttemptOperationalErrorCode::ProjectionInvariant(ProjectionInvariant::Steps),
    );

    let mut input_failure = original.clone();
    let declared_input_detail = serde_json::json!({
        "phase": "start",
        "code": "input_value_size_limit",
        "input": "prompt"
    });
    input_failure["steps"][0]["detail"] = declared_input_detail.clone();
    input_failure["steps"][0]
        .as_object_mut()
        .unwrap()
        .remove("commandOutput");
    input_failure["primaryIssue"] = serde_json::json!({
        "node": { "id": "first", "role": "step" },
        "state": "failed",
        "detail": declared_input_detail
    });
    overwrite_result(&result_directory, input_failure.clone());
    set_failure_detail(
        crate::workflow::evidence::FailureDetail::new(
            crate::workflow::evidence::FailurePhase::Start,
            crate::workflow::evidence::FailureCode::InputValueSizeLimit,
            Some("prompt".to_owned()),
            None,
            None,
            None,
        )
        .unwrap(),
    );
    load_local_archived_attempt(&run_path, None).unwrap();

    input_failure["steps"][0]["detail"]["input"] = Value::String("fabricated".to_owned());
    input_failure["primaryIssue"]["detail"]["input"] = Value::String("fabricated".to_owned());
    overwrite_result(&result_directory, input_failure);
    assert_archive_operational(
        load_local_archived_attempt(&run_path, None).unwrap_err(),
        ArchivedAttemptOperationalErrorCode::ProjectionInvariant(ProjectionInvariant::Steps),
    );

    let mut indexed_scalar = original.clone();
    let indexed_detail = serde_json::json!({
        "phase": "start",
        "code": "input_collection_ordinal_limit",
        "input": "prompt",
        "collectionIndex": 0
    });
    indexed_scalar["steps"][0]["detail"] = indexed_detail.clone();
    indexed_scalar["steps"][0]
        .as_object_mut()
        .unwrap()
        .remove("commandOutput");
    indexed_scalar["primaryIssue"] = serde_json::json!({
        "node": { "id": "first", "role": "step" },
        "state": "failed",
        "detail": indexed_detail
    });
    overwrite_result(&result_directory, indexed_scalar);
    assert_archive_operational(
        load_local_archived_attempt(&run_path, None).unwrap_err(),
        ArchivedAttemptOperationalErrorCode::ProjectionInvariant(ProjectionInvariant::Steps),
    );

    let mut output_failure = original;
    let declared_output_detail = serde_json::json!({
        "phase": "output_capture",
        "code": "output_missing",
        "output": "artifact"
    });
    output_failure["steps"][0]["detail"] = declared_output_detail.clone();
    output_failure["primaryIssue"] = serde_json::json!({
        "node": { "id": "first", "role": "step" },
        "state": "failed",
        "detail": declared_output_detail
    });
    overwrite_result(&result_directory, output_failure.clone());
    set_failure_detail(
        crate::workflow::evidence::FailureDetail::new(
            crate::workflow::evidence::FailurePhase::OutputCapture,
            crate::workflow::evidence::FailureCode::OutputMissing,
            None,
            None,
            Some("artifact".to_owned()),
            None,
        )
        .unwrap(),
    );
    load_local_archived_attempt(&run_path, None).unwrap();

    output_failure["steps"][0]["detail"]["output"] = Value::String("fabricated".to_owned());
    output_failure["primaryIssue"]["detail"]["output"] = Value::String("fabricated".to_owned());
    overwrite_result(&result_directory, output_failure);
    assert_archive_operational(
        load_local_archived_attempt(&run_path, None).unwrap_err(),
        ArchivedAttemptOperationalErrorCode::ProjectionInvariant(ProjectionInvariant::Steps),
    );
}

#[test]
fn archived_attempt_rejects_impossible_outcomes_and_blocking_causes() {
    let fixture = AdmittedFixture::from_source(
        "schemaVersion: 1\nsteps:\n  first:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n  second:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n  third:\n    kind: cmd\n    dependsOn: [first, second]\n    command:\n      argv: [\"true\"]\n",
    );
    let run_path = fixture.run_path("archive-terminal-invariants");
    let run = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    run.state
        .update(|state| {
            let attempt = current_attempt_mut(state)?;
            let settled = attempt.created_at.clone();
            attempt.started_at = Some(settled.clone());
            attempt.settled_at = Some(settled);
            attempt.settlement_snapshot = Some(fixture_settlement_snapshot());
            attempt.state = AttemptStateV1::WorkflowFailed;
            attempt.progress.steps[0].state = AttemptStepStateV1::Failed;
            attempt.progress.steps[0].detail = Some(crate::workflow::evidence::NodeDetail::Failed(
                crate::workflow::evidence::FailureDetail::new(
                    crate::workflow::evidence::FailurePhase::Execution,
                    crate::workflow::evidence::FailureCode::CommandExit,
                    None,
                    None,
                    None,
                    Some(23),
                )
                .unwrap(),
            ));
            attempt.progress.steps[1].state = AttemptStepStateV1::Succeeded;
            attempt.progress.steps[2].state = AttemptStepStateV1::Blocked;
            attempt.progress.steps[2].detail =
                Some(crate::workflow::evidence::NodeDetail::Blocked(
                    crate::workflow::evidence::BlockedDetail::new([
                        crate::workflow::evidence::Prerequisite::control("first").unwrap(),
                    ])
                    .unwrap(),
                ));
            attempt.result = AttemptResultV1::NotPublished {
                reason: ResultAbsentReasonV1::PublicationPending,
            };
            Ok(())
        })
        .unwrap();
    let durable = read_state(run.root_handle()).unwrap();
    let attempt = durable.attempts.last().unwrap();
    let run_document = read_run(run.root_handle()).unwrap();
    let stream = serde_json::json!({
        "encoding": "base64",
        "data": "",
        "retainedBytes": 0,
        "discardedBytes": 0,
        "truncated": false,
        "fullyDrained": true
    });
    let command_output = serde_json::json!({
        "stdout": stream.clone(),
        "stderr": stream
    });
    let valid = serde_json::json!({
        "schemaVersion": 1,
        "attemptNumber": attempt.attempt_number,
        "workflow": {
            "path": fixture.admitted.workflow().source.workflow_path,
            "provenance": {
                "kind": "local",
                "sourceRoot": fixture.admitted.workflow().source.source_root
            },
            "digest": {
                "algorithm": run_document.workflow_digest.algorithm,
                "value": run_document.workflow_digest.value
            }
        },
        "execution": {
            "executionRoot": attempt.execution_root,
            "maximumParallelSteps": 2,
            "startedAt": "2026-08-02T12:01:44Z",
            "finishedAt": "2026-08-02T12:01:45Z",
            "durationMilliseconds": 1000
        },
        "commandOutputPolicy": {
            "encoding": "base64",
            "maximumRetainedBytesPerStream": crate::workflow::MAXIMUM_RETAINED_BYTES_PER_STREAM
        },
        "outcome": "failed",
        "forceAbort": null,
        "primaryIssue": {
            "node": { "id": "first", "role": "step" },
            "state": "failed",
            "detail": { "phase": "execution", "code": "command_exit", "exitCode": 23 }
        },
        "steps": [
            {
                "id": "first",
                "role": "step",
                "kind": "cmd",
                "failurePolicy": "required",
                "state": "failed",
                "startedAt": "2026-08-02T12:01:44Z",
                "durationMilliseconds": 100,
                "detail": {
                    "phase": "execution",
                    "code": "command_exit",
                    "exitCode": 23
                },
                "commandOutput": command_output.clone()
            },
            {
                "id": "second",
                "role": "step",
                "kind": "cmd",
                "failurePolicy": "required",
                "state": "succeeded",
                "startedAt": "2026-08-02T12:01:44Z",
                "durationMilliseconds": 100,
                "commandOutput": command_output
            },
            {
                "id": "third",
                "role": "step",
                "kind": "cmd",
                "failurePolicy": "required",
                "state": "blocked",
                "detail": {
                    "code": "prerequisites_unsatisfied",
                    "prerequisites": [{"kind": "control", "node": "first"}]
                }
            }
        ],
        "exports": {}
    });
    let result_directory = run
        .run_directory()
        .join(attempt_result_relative_path(attempt.attempt_number));
    fs::create_dir_all(result_directory.join("exports")).unwrap();
    overwrite_result(&result_directory, valid.clone());
    run.record_result_published().unwrap();
    load_local_archived_attempt(&run_path, None).unwrap();

    // Both roots can already be active when `second` fails first. The consumer records
    // that then-terminal prerequisite even if lexicographically lower `first` fails later.
    let mut historical_blocker = valid.clone();
    historical_blocker["primaryIssue"] = serde_json::json!({
        "node": { "id": "second", "role": "step" },
        "state": "failed",
        "detail": { "phase": "execution", "code": "command_exit", "exitCode": 29 }
    });
    historical_blocker["steps"][1]["state"] = Value::String("failed".to_owned());
    historical_blocker["steps"][1]["detail"] = serde_json::json!({
        "phase": "execution",
        "code": "command_exit",
        "exitCode": 29
    });
    historical_blocker["steps"][2]["detail"]["prerequisites"] =
        serde_json::json!([{"kind": "control", "node": "second"}]);
    run.state
        .update(|state| {
            let progress = &mut current_attempt_mut(state)?.progress.steps;
            progress[1].state = AttemptStepStateV1::Failed;
            progress[1].detail = Some(crate::workflow::evidence::NodeDetail::Failed(
                crate::workflow::evidence::FailureDetail::new(
                    crate::workflow::evidence::FailurePhase::Execution,
                    crate::workflow::evidence::FailureCode::CommandExit,
                    None,
                    None,
                    None,
                    Some(29),
                )
                .unwrap(),
            ));
            progress[2].detail = Some(crate::workflow::evidence::NodeDetail::Blocked(
                crate::workflow::evidence::BlockedDetail::new([
                    crate::workflow::evidence::Prerequisite::control("second").unwrap(),
                ])
                .unwrap(),
            ));
            Ok(())
        })
        .unwrap();
    overwrite_result(&result_directory, historical_blocker);
    load_local_archived_attempt(&run_path, None).unwrap();

    run.state
        .update(|state| {
            let progress = &mut current_attempt_mut(state)?.progress.steps;
            progress[1].state = AttemptStepStateV1::Succeeded;
            progress[1].detail = None;
            progress[2].detail = Some(crate::workflow::evidence::NodeDetail::Blocked(
                crate::workflow::evidence::BlockedDetail::new([
                    crate::workflow::evidence::Prerequisite::control("first").unwrap(),
                ])
                .unwrap(),
            ));
            Ok(())
        })
        .unwrap();

    let mut false_blocker = valid.clone();
    false_blocker["steps"][2]["detail"]["prerequisites"] =
        serde_json::json!([{"kind": "control", "node": "second"}]);
    overwrite_result(&result_directory, false_blocker);
    assert_archive_operational(
        load_local_archived_attempt(&run_path, None).unwrap_err(),
        ArchivedAttemptOperationalErrorCode::ProjectionInvariant(ProjectionInvariant::Steps),
    );

    let mut impossible_success = valid;
    impossible_success["outcome"] = Value::String("succeeded".to_owned());
    impossible_success
        .as_object_mut()
        .unwrap()
        .remove("primaryIssue");
    overwrite_result(&result_directory, impossible_success);
    run.state
        .update(|state| {
            current_attempt_mut(state)?.state = AttemptStateV1::Succeeded;
            Ok(())
        })
        .unwrap();
    assert_archive_operational(
        load_local_archived_attempt(&run_path, None).unwrap_err(),
        ArchivedAttemptOperationalErrorCode::ProjectionInvariant(ProjectionInvariant::ArtifactSet),
    );
}

#[test]
fn archived_attempt_rejects_not_run_step_with_failed_dependency() {
    let fixture = AdmittedFixture::new();
    let run_path = fixture.run_path("archive-impossible-not-run");
    let run = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    settle_as_workflow_failed(&run);
    let result_directory = publish_result_fixture(&fixture, &run);
    let mut result = result_value(&result_directory);
    result["steps"][1] = serde_json::json!({
        "id": "second",
        "role": "step",
        "kind": "cmd",
        "failurePolicy": "required",
        "state": "not_run",
        "detail": { "code": "failure_stop" }
    });
    overwrite_result(&result_directory, result);
    run.state
        .update(|state| {
            let step = &mut current_attempt_mut(state)?.progress.steps[1];
            step.state = AttemptStepStateV1::NotRun;
            step.detail = Some(crate::workflow::evidence::NodeDetail::NotRun(
                crate::workflow::evidence::NonExecutionDetail::failure_stop(),
            ));
            Ok(())
        })
        .unwrap();

    assert_archive_operational(
        load_local_archived_attempt(&run_path, None).unwrap_err(),
        ArchivedAttemptOperationalErrorCode::ProjectionInvariant(
            ProjectionInvariant::TerminalSteps,
        ),
    );
}

#[test]
fn archived_attempt_rejects_symlinks_and_does_not_adopt_replacements() {
    let fixture = AdmittedFixture::new();
    let run_path = fixture.run_path("archive-symlink");
    let run = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    settle_as_workflow_failed(&run);
    let result_directory = publish_result_fixture(&fixture, &run);
    let result = result_directory.join("result.json");
    fs::rename(&result, result_directory.join("result-real.json")).unwrap();
    symlink("result-real.json", &result).unwrap();
    assert_archive_operational(
        load_local_archived_attempt(&run_path, None).unwrap_err(),
        ArchivedAttemptOperationalErrorCode::PublishedResultUnavailable,
    );

    fs::remove_file(&result).unwrap();
    fs::rename(result_directory.join("result-real.json"), &result).unwrap();
    let replacement = fs::read(&result).unwrap();
    assert_archive_operational(
        load_local_archived_attempt_observed(
            &run_path,
            None,
            |_| {},
            |result_directory| {
                let result = result_directory.join("result.json");
                fs::rename(
                    &result,
                    result_directory.join("result-before-replacement.json"),
                )
                .unwrap();
                fs::write(result, &replacement).unwrap();
            },
        )
        .unwrap_err(),
        ArchivedAttemptOperationalErrorCode::PublishedResultUnavailable,
    );

    let fixture = AdmittedFixture::new();
    let run_path = fixture.run_path("archive-path-replacement");
    let run = InitialLocalRun::create(&run_path, &fixture.admitted).unwrap();
    settle_as_workflow_failed(&run);
    publish_result_fixture(&fixture, &run);
    let archived = load_local_archived_attempt_observed(
        &run_path,
        None,
        |run_directory| {
            let moved = run_directory.with_file_name("archive-path-original");
            fs::rename(run_directory, moved).unwrap();
            fs::create_dir(run_directory).unwrap();
        },
        |_| {},
    )
    .unwrap();
    assert_eq!(archived.projection.attempt_number, 1);
    assert!(durable_tree(&run_path).is_empty());
}

#[tokio::test]
async fn cloud_step_is_readable_while_a_real_downstream_command_is_running() {
    let fixture = AdmittedFixture::from_source(
        "schemaVersion: 1\nsteps:\n  first:\n    kind: cmd\n    command: {argv: [\"/bin/sh\", \"-c\", \"printf private-evidence > message.txt\"]}\n    outputs:\n      message: {kind: text, from: path, path: message.txt}\n  second:\n    kind: cmd\n    inputs:\n      prompt: {ref: outputs.first.message}\n    command: {argv: [\"/bin/sh\", \"-c\", \"printf ready > ready; read x < gate; IFS= read -r value < \\\"$UM_STEP_INPUTS/values/prompt\\\"; printf '%s' \\\"$value\\\" > consumed.txt\"]}\n",
    );
    let root = fs::canonicalize(&fixture.execution_root).unwrap();
    nix::unistd::mkfifo(
        &root.join("gate"),
        nix::sys::stat::Mode::S_IRUSR | nix::sys::stat::Mode::S_IWUSR,
    )
    .unwrap();
    let private = fs::canonicalize(fixture.run_path("incremental-private").parent().unwrap())
        .unwrap()
        .join("incremental-private");
    fs::create_dir(&private).unwrap();
    let artifacts = ArtifactStaging::create(fixture.admitted.execution(), &private).unwrap();
    let inputs =
        crate::workflow::input::InputStaging::create(fixture.admitted.execution(), &private)
            .unwrap();
    let attempt = "atm_01k0z6r1w8f4jy2m7q9v3x5abe";
    let execution = tokio::spawn({
        let admitted = fixture.admitted.clone();
        let artifacts = artifacts.clone();
        let private = private.clone();
        async move {
            crate::workflow::execution::execute_workflow(
                admitted,
                &artifacts,
                &inputs,
                &crate::workflow::diagnostic::StepDiagnosticLog::default(),
                crate::workflow::step_runtime::AgentExecution::disabled(),
                crate::workflow::test_support::step_clock::TestClock,
                crate::workflow::execution::CloudStepCommitPort::new(
                    private,
                    attempt.to_owned(),
                    1,
                    artifacts.clone(),
                    None,
                ),
                crate::workflow::observation::NoopExecutionObserver,
                crate::workflow::step_runtime::WorkflowExecutionStart::initial(
                    crate::workflow::process_group::ProcessGuardRegistry::default(),
                ),
            )
            .await
        }
    });
    // The external child exposes a file marker, with no in-process notification.
    // Its shell redirect creates the marker before writing it, so wait for the
    // complete value rather than existence. Poll only this OS boundary; the
    // watchdog bounds a missing readiness signal.
    let ready = root.join("ready");
    cloud_test_poll(|| fs::read(&ready).is_ok_and(|marker| marker == b"ready")).await;
    assert!(!execution.is_finished());
    assert!(!private.join("cloud-retained-v1/state.json").exists());
    assert!(!private.join("result.json").exists());
    let marker: serde_json::Value = serde_json::from_slice(
        &fs::read(private.join("cloud-retained-v1/commits/first.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(marker["steps"].as_object().unwrap().len(), 1);
    assert_eq!(marker["steps"]["first"]["state"], "succeeded");
    assert_eq!(
        marker["steps"]["first"]["references"][0]["attemptId"],
        attempt
    );
    // A crashed, unrenamed downstream checkpoint must not supply success.
    fs::write(private.join("cloud-retained-v1/.state.pending"), b"{broken").unwrap();
    fs::write(
        private.join("cloud-retained-v1/commits/.second.pending"),
        b"{broken",
    )
    .unwrap();
    assert!(matches!(
        load_cloud_continuation_seed(
            &fixture.admitted,
            &artifacts,
            &BTreeMap::from([(attempt.to_owned(), private.clone())]),
            attempt,
            1,
            &[json!({"id":"second","priorState":"succeeded","definitionChanged":false})],
            &[],
        ),
        Err(LocalRunDirectoryError::StateInvalid)
    ));
    let carrier = private.join("cloud-retained-v1/values/steps/first/message");
    assert_eq!(fs::read(carrier).unwrap(), b"private-evidence");
    let seed = load_cloud_continuation_seed(
        &fixture.admitted,
        &artifacts,
        &BTreeMap::from([(attempt.to_owned(), private.clone())]),
        attempt,
        1,
        &[json!({"id":"first","priorState":"succeeded","definitionChanged":false})],
        &["second".to_owned()],
    )
    .unwrap();
    assert_eq!(
        seed.inherited_step("first").unwrap().producers["message"].attempt_id,
        attempt
    );
    cloud_test_watchdog(async {
        cloud_test_poll(|| {
            match nix::fcntl::open(
                &root.join("gate"),
                nix::fcntl::OFlag::O_WRONLY | nix::fcntl::OFlag::O_NONBLOCK,
                nix::sys::stat::Mode::empty(),
            ) {
                Ok(fd) => {
                    File::from(fd).write_all(b"go\n").unwrap();
                    true
                }
                Err(nix::errno::Errno::ENXIO) => false,
                Err(error) => panic!("downstream gate could not open: {error}"),
            }
        })
        .await;
    })
    .await;
    let result = cloud_test_watchdog(execution).await.unwrap().unwrap();
    assert_eq!(
        result.outcome,
        crate::workflow::runtime::RunOutcome::Succeeded
    );
    retain_cloud_workflow_evidence(&private, attempt, 1, &result, &artifacts).unwrap();
    let terminal: serde_json::Value =
        serde_json::from_slice(&fs::read(private.join("cloud-retained-v1/state.json")).unwrap())
            .unwrap();
    assert_eq!(terminal["steps"].as_object().unwrap().len(), 2);
    assert_eq!(
        fs::read(fixture.execution_root.join("consumed.txt")).unwrap(),
        b"private-evidence"
    );
}

#[expect(
    clippy::disallowed_methods,
    reason = "bounded OS process readiness polling, not an assertion timer"
)]
async fn cloud_test_poll(mut ready: impl FnMut() -> bool) {
    cloud_test_watchdog(async {
        let mut poll = tokio::time::interval(Duration::from_millis(10));
        while !ready() {
            poll.tick().await;
        }
    })
    .await;
}

#[expect(
    clippy::disallowed_methods,
    reason = "real time only bounds a hung child process, never establishes readiness"
)]
async fn cloud_test_watchdog<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(15), future)
        .await
        .unwrap_or_else(|_| panic!("cloud checkpoint test process did not finish"))
}

#[tokio::test]
async fn failed_cloud_checkpoint_never_releases_step_success() {
    let fixture = AdmittedFixture::from_source(
        "schemaVersion: 1\nsteps:\n  first:\n    kind: cmd\n    command: {argv: [\"/bin/sh\", \"-c\", \"printf evidence > message.txt\"]}\n    outputs:\n      message: {kind: text, from: path, path: message.txt}\n  second:\n    kind: cmd\n    dependsOn: [first]\n    command: {argv: [\"/bin/sh\", \"-c\", \"printf started > downstream\"]}\n",
    );
    let private = fixture.run_path("failed-checkpoint");
    fs::create_dir(&private).unwrap();
    fs::create_dir_all(private.join("cloud-retained-v1/commits/first.json")).unwrap();
    let artifacts = ArtifactStaging::create(fixture.admitted.execution(), &private).unwrap();
    let inputs =
        crate::workflow::input::InputStaging::create(fixture.admitted.execution(), &private)
            .unwrap();
    let result = crate::workflow::execution::execute_workflow(
        fixture.admitted.clone(),
        &artifacts,
        &inputs,
        &crate::workflow::diagnostic::StepDiagnosticLog::default(),
        crate::workflow::step_runtime::AgentExecution::disabled(),
        crate::workflow::test_support::step_clock::TestClock,
        crate::workflow::execution::CloudStepCommitPort::new(
            private.clone(),
            "atm_01k0z6r1w8f4jy2m7q9v3x5ac0".into(),
            1,
            artifacts.clone(),
            None,
        ),
        crate::workflow::observation::NoopExecutionObserver,
        crate::workflow::step_runtime::WorkflowExecutionStart::initial(
            crate::workflow::process_group::ProcessGuardRegistry::default(),
        ),
    )
    .await;
    assert!(matches!(
        result,
        Err(crate::workflow::coordinator::CoordinationError::CommitFailed(_))
    ));
    assert!(!fixture.execution_root.join("downstream").exists());
    assert!(!private.join("cloud-retained-v1/state.json").exists());
}

#[tokio::test]
async fn cloud_checkpoint_accepts_carrier_free_git_and_rejects_unpublished_records() {
    let fixture = AdmittedFixture::from_source_with_git(
        "schemaVersion: 1\nsteps:\n  first:\n    kind: cmd\n    command: {argv: [\"git\", \"--version\"]}\n    outputs:\n      branch: {kind: git_branch, from: workspace}\n  second:\n    kind: cmd\n    dependsOn: [first]\n    command: {argv: [\"git\", \"--version\"]}\n",
        ResolvedInputs::default(),
        1024,
        EnvironmentSnapshot::new([("PATH", std::env::var_os("PATH").unwrap())]),
        true,
    );
    let private = fixture.run_path("git-checkpoint");
    fs::create_dir(&private).unwrap();
    let private = fs::canonicalize(private).unwrap();
    let artifacts = ArtifactStaging::create(fixture.admitted.execution(), &private).unwrap();
    let attempt = "atm_01k0z6r1w8f4jy2m7q9v3x5abf";
    let inputs =
        crate::workflow::input::InputStaging::create(fixture.admitted.execution(), &private)
            .unwrap();
    let commits = private.join("cloud-retained-v1/commits");
    fs::create_dir_all(&commits).unwrap();
    fs::write(commits.join(".first.pending"), b"{incomplete").unwrap();
    let roots = BTreeMap::from([(attempt.to_owned(), private.clone())]);
    let request = [json!({"id":"first","priorState":"succeeded","definitionChanged":false})];
    assert!(matches!(
        load_cloud_continuation_seed(
            &fixture.admitted,
            &artifacts,
            &roots,
            attempt,
            1,
            &request,
            &["second".to_owned()],
        ),
        Err(LocalRunDirectoryError::StateInvalid)
    ));
    let result = crate::workflow::execution::execute_workflow(
        fixture.admitted.clone(),
        &artifacts,
        &inputs,
        &crate::workflow::diagnostic::StepDiagnosticLog::default(),
        crate::workflow::step_runtime::AgentExecution::disabled(),
        crate::workflow::test_support::step_clock::TestClock,
        crate::workflow::execution::CloudStepCommitPort::new(
            private.clone(),
            attempt.to_owned(),
            1,
            artifacts.clone(),
            None,
        ),
        crate::workflow::observation::NoopExecutionObserver,
        crate::workflow::step_runtime::WorkflowExecutionStart::initial(
            crate::workflow::process_group::ProcessGuardRegistry::default(),
        ),
    )
    .await
    .unwrap();
    assert_eq!(
        result.outcome,
        crate::workflow::runtime::RunOutcome::Succeeded
    );
    let marker: serde_json::Value =
        serde_json::from_slice(&fs::read(commits.join("first.json")).unwrap()).unwrap();
    assert!(marker["steps"]["first"]["outputs"][0]["carrier"].is_null());
    let seed = load_cloud_continuation_seed(
        &fixture.admitted,
        &artifacts,
        &roots,
        attempt,
        1,
        &request,
        &["second".to_owned()],
    )
    .unwrap();
    assert!(seed.inherited_step("first").unwrap().outputs.is_empty());
    assert_eq!(
        marker["steps"]["first"]["references"][0]["attemptId"],
        attempt
    );
    // A renamed but incomplete commit is not evidence either.
    fs::set_permissions(commits.join("first.json"), Permissions::from_mode(0o600)).unwrap();
    fs::write(commits.join("first.json"), b"{incomplete").unwrap();
    assert!(matches!(
        load_cloud_continuation_seed(
            &fixture.admitted,
            &artifacts,
            &roots,
            attempt,
            1,
            &request,
            &["second".to_owned()],
        ),
        Err(LocalRunDirectoryError::StateInvalid)
    ));
}
