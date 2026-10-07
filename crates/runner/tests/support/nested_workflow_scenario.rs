use std::collections::BTreeSet;
use std::fs;
use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, anyhow, ensure};
use ring::digest::{SHA256, digest};
use time::OffsetDateTime;
use um_execution::{CaptureCancellation, resolve};
use um_runner_protocol::{
    ArtifactRegistrationOutcome, ArtifactRegistrationResponse, ArtifactResultRegistrationOutcome,
    ArtifactResultRegistrationResponse, ExecutionCapacityV1RunnerProjection, ExecutionLeaseGrant,
    ExecutionLeasePolicy, ExecutionLimitsV1RunnerProjection,
    PrimaryWorkspaceSourceV1RunnerProjection, WorkflowDefinitionSourceV1RunnerProjection,
    WorkflowSourceClosureDigestV1RunnerProjection,
};
use url::Url;

use super::*;
use crate::credential::Credential;
use crate::service::config::{AssignmentConfig, RepositoryUrlPolicy};
use crate::service::source::{
    CommitAvailability, CredentialBrokerFailure, ProviderCredential, ProviderSecret,
    SourceCredentialBroker, WorkflowGitRevocation,
};
use crate::service::workspace::{WorkRootLease, WorkspaceFilesystem};

const BOOT_ID: &str = "rbt_01k0z6r1w8f4jy2m7q9v3x5abe";
const EXPECTED_RESULT: &[u8] = b"nested portable result";

pub(crate) async fn run_nested_workflow_delivery_failure_scenario(
    fixture_arguments: &[String],
) -> anyhow::Result<()> {
    ensure!(
        !fixture_arguments.is_empty(),
        "nested workflow fixture command is empty"
    );
    let nested_arguments = serde_json::to_string(fixture_arguments)
        .context("encode nested workflow fixture command")?;
    let workflow = format!(
        "schemaVersion: 1\nsteps:\n  nested:\n    kind: cmd\n    command:\n      argv: {nested_arguments}\n    outputs:\n      result:\n        kind: file\n        from: path\n        path: delivery-rounds/0001/nested-result.txt\n        mediaType: text/plain\nexports:\n  nestedResult:\n    ref: outputs.nested.result\n"
    );
    let nested_workflow = "schemaVersion: 1\nsteps:\n  produce:\n    kind: cmd\n    command:\n      argv: [\"/bin/sh\", \"-c\", \"printf 'nested portable result' > nested.txt\"]\n    outputs:\n      result:\n        kind: file\n        from: path\n        path: nested.txt\n        mediaType: text/plain\n  consume:\n    kind: cmd\n    inputs:\n      payload:\n        ref: outputs.produce.result\n    command:\n      argv: [\"/bin/sh\", \"-c\", \"mkdir -p ../run/.private/workflow-retained/.inputs-retained; cp -a \\\"$UM_STEP_INPUTS\\\" ../run/.private/workflow-retained/.inputs-retained/view-retained\"]\nexports:\n  portable:\n    ref: outputs.produce.result\n";

    let temporary = tempfile::tempdir().context("create nested workflow scenario root")?;
    let source = temporary.path().join("source");
    let work = temporary.path().join("work");
    fs::create_dir(&source).context("create nested workflow source")?;
    fs::create_dir(&work).context("create nested workflow runner work root")?;
    fs::set_permissions(&work, fs::Permissions::from_mode(0o700))
        .context("protect nested workflow runner work root")?;
    fs::write(source.join("workflow.yaml"), workflow)
        .context("write assignment workflow fixture")?;
    fs::write(source.join("nested.yaml"), nested_workflow)
        .context("write nested workflow fixture")?;
    initialize_source_repository(&source)?;

    let assignment = AssignmentConfig::new(&work)?;
    let credential = Credential::from_enrolled_state(
        "rnr_01k0z6r1w8f4jy2m7q9v3x5abd",
        "rrc_01k0z6r1w8f4jy2m7q9v3x5abd",
        "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    )
    .map_err(|error| anyhow!(error))?;
    let config = Config::new(
        "wss://gateway.example.test/v1/runner/connect",
        credential,
        false,
        assignment,
        RepositoryUrlPolicy::with_file_repositories(true),
    )?;
    let work_root = WorkRootLease::acquire_with(
        config.assignment().work_root(),
        BOOT_ID,
        WorkspaceFilesystem::testing(),
    )?;
    let source_broker = Arc::new(FixtureSourceBroker::new(&source)?);
    let dependencies = AssignmentDependencies::new(
        Arc::clone(&work_root),
        Arc::new(crate::service::TokioSleeper),
        Some(Arc::clone(&source_broker) as Arc<dyn SourceCredentialBroker>),
        None,
        Arc::from("nested-workflow-test"),
        None,
        true,
    );
    let mut manager = AssignmentManager::new(&config, LeaseClock::system()?, dependencies);
    manager
        .retain_lease_policy(&lease_policy())
        .map_err(|error| anyhow!("retain nested workflow lease policy: {error:?}"))?;

    let mut offered = offer();
    align_offer_with_source(&source, &mut offered)?;
    let assignment_path = config
        .assignment()
        .work_root()
        .join(BOOT_ID)
        .join(&offered.assignment_id);
    manager
        .handle_offer(offered.clone())
        .map_err(|error| anyhow!("offer nested workflow assignment: {error:?}"))?;
    prepare_current(&mut manager, &offered).await?;
    spawn_execution(&mut manager, &offered)?;

    let pending = wait_for_carrier_registration(&mut manager).await?;
    let expected_sha256 = digest(&SHA256, EXPECTED_RESULT)
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    ensure!(
        pending.iter().any(|entry| matches!(
            &entry.observation,
            AssignmentObservation::Artifact {
                request: ArtifactRequest::RegisterCarrier {
                    portable_owner_path,
                    media_type,
                    size_bytes,
                    sha256,
                    ..
                },
                ..
            } if portable_owner_path == "exports/0001"
                && media_type == "text/plain"
                && *size_bytes == u64::try_from(EXPECTED_RESULT.len()).unwrap_or(u64::MAX)
                && sha256 == &expected_sha256
        )),
        "nested workflow export did not reach carrier registration"
    );
    ensure!(
        assignment_path.join("workspace").exists(),
        "assignment workspace disappeared before delivery failed"
    );
    ensure!(
        fail_pending_artifact_registrations(&mut manager, &pending)?,
        "nested workflow carrier registration was not failed"
    );

    let reports = wait_for_terminal(&mut manager).await?;
    ensure!(
        matches!(
            reports.last(),
            Some(ExecutionReport::Finished { outcome, .. })
                if outcome == &serde_json::json!({
                    "outcome": "succeeded",
                    "forceAbort": null,
                })
        ),
        "nested workflow assignment did not finish successfully: {reports:#?}"
    );
    acknowledge_terminal_and_settle(&mut manager).await?;
    ensure!(
        !manager.cleanup_failed,
        "nested workflow assignment cleanup failed"
    );
    ensure!(
        manager.slot.is_none(),
        "nested workflow slot remained occupied"
    );
    ensure!(
        assignment_path.exists(),
        "delivery failure did not retain the assignment root"
    );
    ensure!(
        assignment_path
            .join("private")
            .read_dir()
            .context("read retained assignment private directory")?
            .next()
            .is_some(),
        "delivery failure did not retain private assignment staging"
    );

    // The first attempt has no portable result. Offer a real successor on
    // the same boot; it must execute the nested binary in the retained tree.
    let mut successor = offered.clone();
    // A replacement has a larger effective reservation and executes against
    // the original Git baseline, without rerunning the nested CLI fixture.
    fs::write(
        source.join("workflow.yaml"),
        "schemaVersion: 1\nsteps:\n  nested:\n    kind: cmd\n    command: {argv: [\"/bin/sh\", \"-c\", \"test -r \\\"$SCHERZO_CONTINUATION_CONTEXT\\\"; test -f delivery-rounds/0001/nested-result.txt; printf continued > continued.txt\"]}\n    outputs:\n      result: {kind: text, from: path, path: continued.txt}\n  follow:\n    kind: cmd\n    inputs:\n      payload: {ref: outputs.nested.result}\n    command: {argv: [\"/bin/sh\", \"-c\", \"test -r \\\"$SCHERZO_CONTINUATION_CONTEXT\\\"; IFS= read -r value < \\\"$UM_STEP_INPUTS/values/payload\\\" || [ -n \\\"$value\\\" ] || exit 1; printf '%s' \\\"$value\\\" > followed.txt\"]}\nfinalizers:\n  clean:\n    kind: cmd\n    command: {argv: [\"/bin/sh\", \"-c\", \"test -r \\\"$SCHERZO_CONTINUATION_CONTEXT\\\"; printf finalized > finalized.txt\"]}\nexports:\n  nestedResult: {ref: outputs.nested.result}\n",
    )?;
    run_git(&source, &["add", "workflow.yaml"])?;
    run_git(&source, &["commit", "--quiet", "-m", "replacement"])?;
    // Change the connection URL without changing the retained baseline.
    let alternate_source = temporary.path().join("new-connection");
    std::os::unix::fs::symlink(&source, &alternate_source)?;
    let new_origin = Url::from_file_path(&alternate_source)
        .map_err(|()| anyhow!("alternate repository URL unavailable"))?
        .to_string();
    let old_origin = run_git(
        &assignment_path.join("workspace"),
        &["remote", "get-url", "origin"],
    )?;
    ensure!(
        new_origin != old_origin,
        "connection change did not change its URL"
    );
    source_broker.set_repository_url(&alternate_source)?;
    let original_commit = successor
        .execution_spec
        .primary_workspace_source
        .commit_oid
        .clone();
    align_offer_with_source(&source, &mut successor)?;
    successor.execution_spec.primary_workspace_source.commit_oid = original_commit.clone();
    ensure!(
        successor.execution_spec.capacity.encoded_outbox_bytes
            > offered.execution_spec.capacity.encoded_outbox_bytes,
        "replacement capacity did not grow"
    );
    successor.effect_id = "eff_01k0z6r1w8f4jy2m7q9v3x5abm".into();
    successor.assignment_id = "asn_01k0z6r1w8f4jy2m7q9v3x5abm".into();
    successor.attempt_id = "atm_01k0z6r1w8f4jy2m7q9v3x5abm".into();
    successor.attempt_number = 2;
    successor.execution_spec.execution_spec_id = "xsp_01k0z6r1w8f4jy2m7q9v3x5abm".into();
    successor.continuation = Some(Box::new(um_runner_protocol::ContinuationOffer {
        prior_assignment_id: offered.assignment_id.clone(),
        prior_attempt_id: offered.attempt_id.clone(),
        required_runner_boot_id: BOOT_ID.into(),
        execution_root: assignment_path
            .join("workspace")
            .to_string_lossy()
            .into_owned(),
        definition_source: successor.execution_spec.workflow_definition_source.clone(),
        effective_capacity: successor.execution_spec.capacity.clone(),
        prior_manifest_digest: offered
            .execution_spec
            .workflow_definition_source
            .workflow_source_closure_digest
            .clone(),
        request: serde_json::json!({"fromSteps":["nested"],"definition":{
            "replaced":{"commitOid":successor.execution_spec.workflow_definition_source.commit_oid,
                "workflowPath":"workflow.yaml"}}}),
        reexecuted_steps: vec!["nested".into(), "follow".into()],
        inherited_steps: Vec::new(),
        prior_settlement_snapshot: None,
    }));
    let mut invalid = successor.clone();
    invalid.effect_id = "eff_01k0z6r1w8f4jy2m7q9v3x5abv".into();
    invalid.assignment_id = "asn_01k0z6r1w8f4jy2m7q9v3x5abv".into();
    invalid.attempt_id = "atm_01k0z6r1w8f4jy2m7q9v3x5abv".into();
    invalid
        .execution_spec
        .workflow_definition_source
        .workflow_source_closure_digest
        .value = "0".repeat(64);
    manager
        .handle_offer(invalid.clone())
        .map_err(|error| anyhow!("offer corrupted digest: {error:?}"))?;
    let refused = manager.pending_observations(&BTreeSet::new(), 100);
    let rejection = refused
        .iter()
        .find(|entry| {
            matches!(&entry.observation,
        AssignmentObservation::Decision(AssignmentDecision::Rejected { assignment_id, .. })
            if assignment_id == &invalid.assignment_id)
        })
        .ok_or_else(|| anyhow!("corrupted digest was not rejected before preparation"))?;
    ensure!(
        !work.join(BOOT_ID).join(&invalid.assignment_id).exists(),
        "corrupted digest created a workspace before validation"
    );
    manager.acknowledge_observation(rejection.id);

    let mut understated = successor.clone();
    understated.effect_id = "eff_01k0z6r1w8f4jy2m7q9v3x5abx".into();
    understated.assignment_id = "asn_01k0z6r1w8f4jy2m7q9v3x5abx".into();
    understated.attempt_id = "atm_01k0z6r1w8f4jy2m7q9v3x5abx".into();
    understated.execution_spec.capacity.encoded_outbox_bytes -= 1;
    understated
        .continuation
        .as_mut()
        .ok_or_else(|| anyhow!("replacement offer omitted continuation"))?
        .effective_capacity = understated.execution_spec.capacity.clone();
    manager
        .handle_offer(understated.clone())
        .map_err(|error| anyhow!("offer understated capacity: {error:?}"))?;
    let refused = manager.pending_observations(&BTreeSet::new(), 100);
    let rejection = refused
        .iter()
        .find(|entry| {
            matches!(&entry.observation,
        AssignmentObservation::Decision(AssignmentDecision::Rejected { assignment_id, .. })
            if assignment_id == &understated.assignment_id)
        })
        .ok_or_else(|| anyhow!("understated capacity was not rejected before reservation"))?;
    ensure!(
        !work.join(BOOT_ID).join(&understated.assignment_id).exists(),
        "understated capacity created an assignment before validation"
    );
    manager.acknowledge_observation(rejection.id);
    ensure!(
        assignment_path.join("workspace").exists(),
        "rejected capacity removed the prior execution tree"
    );

    // A FIFO is a deterministic unsupported workspace entry: no race or
    // permissions assumption is needed to make the snapshot unavailable.
    let unsupported = assignment_path.join("workspace/nested.yaml");
    let saved = assignment_path.join("workspace/nested.yaml.saved");
    let original = fs::read(&unsupported)?;
    fs::rename(&unsupported, &saved)?;
    nix::unistd::mkfifo(
        &unsupported,
        nix::sys::stat::Mode::S_IRUSR | nix::sys::stat::Mode::S_IWUSR,
    )?;
    let mut snapshot_offer = successor.clone();
    snapshot_offer.effect_id = "eff_01k0z6r1w8f4jy2m7q9v3x5ab4".into();
    snapshot_offer.assignment_id = "asn_01k0z6r1w8f4jy2m7q9v3x5ab4".into();
    snapshot_offer.attempt_id = "atm_01k0z6r1w8f4jy2m7q9v3x5ab4".into();
    snapshot_offer.execution_spec.capacity.maximum_invocations -= 1;
    snapshot_offer
        .continuation
        .as_mut()
        .ok_or_else(|| anyhow!("snapshot offer omitted continuation"))?
        .effective_capacity = snapshot_offer.execution_spec.capacity.clone();
    manager
        .handle_offer(snapshot_offer.clone())
        .map_err(|error| anyhow!("offer snapshot-unavailable continuation: {error:?}"))?;
    let preparing = wait_for_observation(&mut manager, |observation| {
        matches!(observation,
        AssignmentObservation::Preparing { assignment_id, .. }
            if assignment_id == &snapshot_offer.assignment_id)
    })
    .await?;
    manager.acknowledge_observation(preparing.id);
    let expires = (manager.sleeper.utc_now() + time::Duration::minutes(15))
        .format(&time::format_description::well_known::Rfc3339)?;
    manager
        .handle_prepare(AssignmentPrepare {
            effect_id: "eff_01k0z6r1w8f4jy2m7q9v3x5ab5".into(),
            assignment_id: snapshot_offer.assignment_id.clone(),
            run_id: snapshot_offer.run_id.clone(),
            attempt_id: snapshot_offer.attempt_id.clone(),
            execution_spec_id: snapshot_offer.execution_spec.execution_spec_id.clone(),
            preparation_expires_at: expires,
        })
        .map_err(|error| anyhow!("prepare snapshot-unavailable continuation: {error:?}"))?;
    let ready = wait_for_observation(&mut manager, |observation| {
        matches!(observation,
        AssignmentObservation::ContinuationReady { assignment_id, .. }
            if assignment_id == &snapshot_offer.assignment_id)
    })
    .await?;
    let AssignmentObservation::ContinuationReady {
        start_snapshot,
        modified,
        ..
    } = &ready.observation
    else {
        anyhow::bail!("snapshot fixture did not produce readiness proof");
    };
    ensure!(
        start_snapshot["unavailable"] == "unsupported_entry" && modified == "unknown",
        "snapshot failure was not explicit with unknown modification: {start_snapshot:?}, {modified:?}"
    );
    manager.acknowledge_observation(ready.id);
    wait_for_manager_state(&mut manager, |manager| {
        manager.drain_events();
        !matches!(manager.slot, Some(LocalSlot::Preparing(_)))
    })
    .await;
    let pending = manager.pending_observations(&BTreeSet::new(), 100);
    let rejection = pending
        .iter()
        .find(|entry| {
            matches!(&entry.observation,
        AssignmentObservation::Decision(AssignmentDecision::Rejected { assignment_id, .. })
            if assignment_id == &snapshot_offer.assignment_id)
        })
        .ok_or_else(|| anyhow!("understated capacity passed after unavailable snapshot"))?;
    manager.acknowledge_observation(rejection.id);
    wait_for_manager_state(&mut manager, |manager| {
        manager.drain_events();
        manager.slot.is_none()
    })
    .await;
    fs::remove_file(&unsupported)?;
    fs::rename(&saved, &unsupported)?;
    ensure!(
        fs::read(&unsupported)? == original,
        "snapshot fixture did not restore tracked workspace bytes"
    );

    manager
        .handle_offer(successor.clone())
        .map_err(|error| anyhow!("offer retained successor: {error:?}"))?;
    prepare_current(&mut manager, &successor).await?;
    let retained_workspace = assignment_path.join("workspace");
    ensure!(
        run_git(&retained_workspace, &["remote", "get-url", "origin"])? == new_origin,
        "continued worktree still points at the prior connection"
    );
    ensure!(
        run_git(&retained_workspace, &["rev-parse", "HEAD"])? == original_commit,
        "origin rewiring changed the retained baseline"
    );
    let helper_key = format!("credential.{new_origin}.helper");
    ensure!(
        run_git(
            &retained_workspace,
            &["config", "--local", "--get", &helper_key]
        )?
        .contains(&successor.assignment_id),
        "replacement helper was not scoped to the new authorized URL"
    );
    run_git(&retained_workspace, &["fetch", "origin"])?;
    ensure!(
        matches!(&manager.slot, Some(LocalSlot::Accepted(accepted))
        if accepted.root.execution == assignment_path.join("workspace")),
        "successor did not adopt the exact retained workspace"
    );
    spawn_execution(&mut manager, &successor)?;
    let next_pending = wait_for_carrier_registration(&mut manager).await?;
    ensure!(
        matches!(
            &manager.slot,
            Some(LocalSlot::Running(_)) | Some(LocalSlot::Finishing(_))
        ),
        "replacement did not execute after admission"
    );
    ensure!(
        fail_pending_artifact_registrations(&mut manager, &next_pending)?,
        "retained successor did not register its result"
    );
    let next_reports = wait_for_terminal(&mut manager).await?;
    ensure!(
        matches!(next_reports.last(), Some(ExecutionReport::Finished { outcome, .. })
        if outcome["outcome"] == "succeeded"),
        "retained successor did not finish: {next_reports:#?}"
    );
    acknowledge_terminal_and_settle(&mut manager).await?;
    let context = work
        .join(BOOT_ID)
        .join(&successor.assignment_id)
        .join("private/continuation-context.json");
    ensure!(context.is_file(), "executed successor has no bound context");
    let document: serde_json::Value = serde_json::from_slice(&fs::read(&context)?)?;
    ensure!(
        document["continuation"]["workspace"]["preparation"] == "ready",
        "successor context did not carry ready proof"
    );
    ensure!(
        document["continuation"]["workspace"]["quiescence"]["groupsRecorded"]
            .as_u64()
            .is_some_and(|count| count > 0),
        "successor context omitted recorded process proof"
    );
    ensure!(
        document["continuation"]["workspace"]["startSnapshot"].is_object(),
        "successor context omitted start snapshot"
    );
    ensure!(
        assignment_path.join("workspace").exists(),
        "delivery failure removed the retained execution tree"
    );
    ensure!(
        fs::read(assignment_path.join("workspace/finalized.txt"))? == b"finalized",
        "replacement finalizer did not execute in retained workspace"
    );
    ensure!(
        fs::read(assignment_path.join("workspace/followed.txt"))? == b"continued",
        "replacement did not consume its fresh output"
    );

    // C inherits B's original producer, but the value becomes invalid only
    // after admission. The consumer must fail normally, not undo the claim.
    let producer = work
        .join(BOOT_ID)
        .join(&successor.assignment_id)
        .join("private/cloud-retained-v1/values/steps/nested/result");
    fs::set_permissions(&producer, fs::Permissions::from_mode(0o600))?;
    fs::write(&producer, b"corrupt")?;
    let mut third = successor.clone();
    third.effect_id = "eff_01k0z6r1w8f4jy2m7q9v3x5abr".into();
    third.assignment_id = "asn_01k0z6r1w8f4jy2m7q9v3x5abr".into();
    third.attempt_id = "atm_01k0z6r1w8f4jy2m7q9v3x5abr".into();
    third.attempt_number = 3;
    third.execution_spec.execution_spec_id = "xsp_01k0z6r1w8f4jy2m7q9v3x5abr".into();
    third.continuation = Some(Box::new(um_runner_protocol::ContinuationOffer {
        prior_assignment_id: successor.assignment_id.clone(),
        prior_attempt_id: successor.attempt_id.clone(),
        required_runner_boot_id: BOOT_ID.into(),
        execution_root: assignment_path
            .join("workspace")
            .to_string_lossy()
            .into_owned(),
        definition_source: third.execution_spec.workflow_definition_source.clone(),
        effective_capacity: third.execution_spec.capacity.clone(),
        prior_manifest_digest: successor
            .execution_spec
            .workflow_definition_source
            .workflow_source_closure_digest
            .clone(),
        request: serde_json::json!({"fromSteps":["follow"],"definition":"inherited"}),
        reexecuted_steps: vec!["follow".into()],
        inherited_steps: vec![serde_json::json!({"id":"nested","priorState":"succeeded",
            "definitionChanged":false})],
        prior_settlement_snapshot: None,
    }));
    manager
        .handle_offer(third.clone())
        .map_err(|error| anyhow!("offer inherited consumer: {error:?}"))?;
    prepare_current(&mut manager, &third).await?;
    spawn_execution(&mut manager, &third)?;
    let third_reports = wait_for_terminal(&mut manager).await?;
    ensure!(
        matches!(third_reports.last(), Some(ExecutionReport::Finished { outcome, .. })
        if outcome["outcome"] == "failed"
            && outcome["primaryIssue"]["detail"]["code"] == "inputs_unavailable"),
        "corrupt inherited consumer did not fail with its ordinary input outcome: {third_reports:#?}"
    );
    ensure!(
        third_reports.iter().any(|report| matches!(report,
        ExecutionReport::Transition { workflow_event, .. }
            if workflow_event["role"] == "finalizer" && workflow_event["to"] == "succeeded")),
        "fresh finalizer did not finish after consumer failure"
    );
    acknowledge_terminal_and_settle(&mut manager).await?;
    let context3 = work
        .join(BOOT_ID)
        .join(&third.assignment_id)
        .join("private/continuation-context.json");
    let document3: serde_json::Value = serde_json::from_slice(&fs::read(context3)?)?;
    ensure!(
        document3["inheritedOutputs"]["nested"]["result"]["attemptId"] == successor.attempt_id,
        "corrupt consumer lost the original-producer reference"
    );

    // The retaining boot has settled. A new boot can see the durable bytes,
    // but cannot claim the previous boot's workspace, even with exact IDs.
    drop(manager);
    drop(work_root);
    let replacement_boot = "rbt_01k0z6r1w8f4jy2m7q9v3x5abf";
    let other =
        WorkRootLease::acquire_with(&work, replacement_boot, WorkspaceFilesystem::testing())?;
    let later = "asn_01k0z6r1w8f4jy2m7q9v3x5abz";
    let mut candidate = other
        .create_assignment_for_attempt(
            later,
            &offered.run_id,
            "atm_01k0z6r1w8f4jy2m7q9v3x5abz",
            None,
        )
        .map_err(|error| anyhow!("create new-boot assignment: {error:?}"))?;
    let rejected = other.claim_retained_for_attempt(
        &mut candidate,
        crate::service::workspace::RetainedClaimRequest {
            assignment_id: later,
            run_id: &offered.run_id,
            attempt_id: "atm_01k0z6r1w8f4jy2m7q9v3x5abz",
            prior_assignment_id: &third.assignment_id,
            prior_attempt_id: &third.attempt_id,
            recorded_root: &assignment_path.join("workspace"),
        },
    );
    ensure!(
        matches!(
            rejected,
            Err(crate::service::workspace::AssignmentRootCreationError::OwnershipUnproven)
        ),
        "new boot claimed a predecessor's execution root: {rejected:?}"
    );
    ensure!(
        fs::read(assignment_path.join("workspace/finalized.txt"))? == b"finalized",
        "new boot modified prior retained bytes"
    );
    let dependencies = AssignmentDependencies::new(
        Arc::clone(&other),
        Arc::new(crate::service::TokioSleeper),
        Some(Arc::new(FixtureSourceBroker::new(&source)?)),
        None,
        Arc::from("nested-workflow-test"),
        None,
        true,
    );
    let mut next_boot = AssignmentManager::new(&config, LeaseClock::system()?, dependencies);
    next_boot
        .retain_lease_policy(&lease_policy())
        .map_err(|error| anyhow!("retain new-boot lease policy: {error:?}"))?;
    let mut stale = third.clone();
    stale.effect_id = "eff_01k0z6r1w8f4jy2m7q9v3x5abw".into();
    stale.assignment_id = "asn_01k0z6r1w8f4jy2m7q9v3x5abw".into();
    stale.attempt_id = "atm_01k0z6r1w8f4jy2m7q9v3x5abw".into();
    stale.attempt_number = 4;
    stale.execution_spec.execution_spec_id = "xsp_01k0z6r1w8f4jy2m7q9v3x5abw".into();
    let continuation = stale
        .continuation
        .as_mut()
        .ok_or_else(|| anyhow!("new-boot offer omitted continuation"))?;
    continuation.prior_assignment_id = third.assignment_id.clone();
    continuation.prior_attempt_id = third.attempt_id.clone();
    next_boot
        .handle_offer(stale.clone())
        .map_err(|error| anyhow!("offer stale-boot continuation: {error:?}"))?;
    let preparing = wait_for_observation(&mut next_boot, |observation| {
        matches!(observation,
        AssignmentObservation::Preparing { assignment_id, .. }
            if assignment_id == &stale.assignment_id)
    })
    .await?;
    next_boot.acknowledge_observation(preparing.id);
    let expires = (next_boot.sleeper.utc_now() + time::Duration::minutes(15))
        .format(&time::format_description::well_known::Rfc3339)?;
    next_boot
        .handle_prepare(AssignmentPrepare {
            effect_id: "eff_01k0z6r1w8f4jy2m7q9v3x5ab3".into(),
            assignment_id: stale.assignment_id.clone(),
            run_id: stale.run_id.clone(),
            attempt_id: stale.attempt_id.clone(),
            execution_spec_id: stale.execution_spec.execution_spec_id.clone(),
            preparation_expires_at: expires,
        })
        .map_err(|error| anyhow!("prepare stale-boot continuation: {error:?}"))?;
    wait_for_manager_state(&mut next_boot, |manager| {
        manager.drain_events();
        !matches!(manager.slot, Some(LocalSlot::Preparing(_)))
    })
    .await;
    let observations = next_boot.pending_observations(&BTreeSet::new(), 100);
    ensure!(
        observations.iter().any(|entry| matches!(&entry.observation,
        AssignmentObservation::Decision(AssignmentDecision::Rejected { assignment_id, .. })
            if assignment_id == &stale.assignment_id)),
        "new boot did not reject pinned continuation"
    );
    ensure!(
        !observations.iter().any(|entry| matches!(&entry.observation,
        AssignmentObservation::ContinuationReady { assignment_id, .. }
            | AssignmentObservation::Execution { assignment_id, .. }
            if assignment_id == &stale.assignment_id)),
        "new boot published readiness or executed stale continuation"
    );
    ensure!(
        fs::read(assignment_path.join("workspace/finalized.txt"))? == b"finalized",
        "new boot touched protected producer bytes"
    );
    Ok(())
}

struct FixtureSourceBroker {
    repository_url: std::sync::Mutex<Arc<str>>,
}

impl FixtureSourceBroker {
    fn new(repository: &Path) -> anyhow::Result<Self> {
        let repository_url = Url::from_file_path(repository)
            .map_err(|()| anyhow!("nested workflow source path cannot be represented as a URL"))?;
        Ok(Self {
            repository_url: std::sync::Mutex::new(Arc::from(repository_url.as_str())),
        })
    }

    fn set_repository_url(&self, repository: &Path) -> anyhow::Result<()> {
        let next = Self::new(repository)?;
        *self
            .repository_url
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = next
            .repository_url
            .into_inner()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Ok(())
    }

    fn credential(&self) -> ProviderCredential {
        ProviderCredential {
            repository_url: Arc::clone(
                &self
                    .repository_url
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner),
            ),
            token: ProviderSecret(b"fixture-provider-token".to_vec()),
            expires_at: OffsetDateTime::UNIX_EPOCH + Duration::from_secs(4_102_444_800),
        }
    }
}

impl SourceCredentialBroker for FixtureSourceBroker {
    fn issue(
        &self,
        _assignment_id: &str,
        cancellation: &CaptureCancellation,
    ) -> Result<ProviderCredential, CredentialBrokerFailure> {
        if cancellation.is_cancelled() {
            Err(CredentialBrokerFailure::Fenced)
        } else {
            Ok(self.credential())
        }
    }

    fn commit_availability(
        &self,
        _assignment_id: &str,
        cancellation: &CaptureCancellation,
    ) -> Result<CommitAvailability, CredentialBrokerFailure> {
        if cancellation.is_cancelled() {
            Err(CredentialBrokerFailure::Fenced)
        } else {
            Ok(CommitAvailability::CommitAvailable)
        }
    }

    fn issue_workflow_git(
        &self,
        _assignment_id: &str,
        _cancellation: &CaptureCancellation,
    ) -> Result<ProviderCredential, CredentialBrokerFailure> {
        Err(CredentialBrokerFailure::Unavailable)
    }

    fn revoke_workflow_git(
        &self,
        _assignment_id: &str,
        _token: &[u8],
    ) -> Result<WorkflowGitRevocation, CredentialBrokerFailure> {
        Err(CredentialBrokerFailure::Unavailable)
    }
}

fn initialize_source_repository(source: &Path) -> anyhow::Result<()> {
    run_git(source, &["init", "--quiet", "--object-format=sha1"])?;
    run_git(source, &["config", "user.name", "Scherzo Fixture"])?;
    run_git(source, &["config", "user.email", "fixture@scherzo.invalid"])?;
    // The nested fixture can inherit the host's global commit-signing policy.
    run_git(source, &["config", "commit.gpgsign", "false"])?;
    run_git(source, &["add", "."])?;
    run_git(source, &["commit", "--quiet", "-m", "nested fixture"])?;
    Ok(())
}

fn run_git(repository: &Path, arguments: &[&str]) -> anyhow::Result<String> {
    let output = Command::new("git")
        .current_dir(repository)
        .args(arguments)
        .output()
        .with_context(|| format!("run Git fixture command {arguments:?}"))?;
    ensure!(
        output.status.success(),
        "Git fixture command {arguments:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .context("Git fixture command emitted non-UTF-8 standard output")
        .map(|output| output.trim().to_owned())
}

fn lease_policy() -> ExecutionLeasePolicy {
    ExecutionLeasePolicy {
        schema_version: 2,
        force_stop_and_reap_budget_milliseconds: 5000,
        terminal_report_delivery_budget_milliseconds: 5000,
        renewal_delivery_budget_milliseconds: 5000,
        lease_duration_milliseconds: 371_000,
        fencing_margin_milliseconds: 11_000,
    }
}

fn offer() -> AssignmentOffer {
    AssignmentOffer {
        effect_id: "eff_01k0z6r1w8f4jy2m7q9v3x5abg".to_owned(),
        assignment_id: "asn_01k0z6r1w8f4jy2m7q9v3x5abg".to_owned(),
        run_id: "run_01k0z6r1w8f4jy2m7q9v3x5abg".to_owned(),
        project_id: "prj_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
        attempt_id: "atm_01k0z6r1w8f4jy2m7q9v3x5abg".to_owned(),
        attempt_number: 1,
        execution_spec: um_runner_protocol::ExecutionSpecV1RunnerProjection {
            execution_spec_id: "xsp_01k0z6r1w8f4jy2m7q9v3x5abg".to_owned(),
            schema_version: 1,
            execution_limits: ExecutionLimitsV1RunnerProjection {
                maximum_parallel_steps: 1,
                cancellation_grace_seconds: 1,
            },
            source_branch: "main".to_owned(),
            source_display_snapshot: None,
            workflow_definition_source: WorkflowDefinitionSourceV1RunnerProjection {
                repository_connection_id: "rpc_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                object_format: "sha1".to_owned(),
                commit_oid: "0123456789abcdef0123456789abcdef01234567".to_owned(),
                workflow_path: "workflow.yaml".to_owned(),
                workflow_source_closure_digest: source_digest_fixture(),
            },
            primary_workspace_source: PrimaryWorkspaceSourceV1RunnerProjection {
                kind: "connected_repository".to_owned(),
                provider_kind: "github".to_owned(),
                repository_connection_id: "rpc_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                object_format: "sha1".to_owned(),
                commit_oid: "0123456789abcdef0123456789abcdef01234567".to_owned(),
                materialization_contract: "git_full_clone_v1".to_owned(),
            },
            capacity: ExecutionCapacityV1RunnerProjection {
                execution_contract: "workflow_v1_cloud_inputs_artifacts@1".to_owned(),
                source_closure_digest: source_digest_fixture(),
                general_maximum_transitions: 8,
                selected_maximum_transitions: 7,
                maximum_invocations: 1,
                maximum_retained_bytes_per_invocation: 4_194_304,
                diagnostic_retention_bytes: 8_388_608,
                native_session_retention_bytes: 4_194_304,
                aggregate_retention_bytes: 12_582_912,
                condition_transition_count: 0,
                aggregate_condition_transition_bytes: 0,
                terminal_result_structure_bytes: 67_108_864,
                presentation_result_bytes: 0,
                portable_result_bytes: 202_027_692,
                encoded_outbox_bytes: 85_458_944,
            },
            run_inputs: None,
        },
        continuation: None,
    }
}

fn source_digest_fixture() -> WorkflowSourceClosureDigestV1RunnerProjection {
    WorkflowSourceClosureDigestV1RunnerProjection {
        algorithm: "sha256".to_owned(),
        value: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_owned(),
    }
}

fn align_offer_with_source(source: &Path, offered: &mut AssignmentOffer) -> anyhow::Result<()> {
    let commit_oid = run_git(source, &["rev-parse", "HEAD"])?;
    offered.execution_spec.workflow_definition_source.commit_oid = commit_oid.clone();
    offered.execution_spec.primary_workspace_source.commit_oid = commit_oid;
    let workflow = resolve(source, Path::new("workflow.yaml"))
        .map_err(|error| anyhow!("resolve assignment workflow fixture: {error:?}"))?;
    let digest = &workflow.capacity.source_closure_digest;
    let requirements = workflow.capacity.requirements;
    let projected_digest = WorkflowSourceClosureDigestV1RunnerProjection {
        algorithm: digest.algorithm.as_str().to_owned(),
        value: digest.value.clone(),
    };
    offered
        .execution_spec
        .workflow_definition_source
        .workflow_source_closure_digest = projected_digest.clone();
    offered.execution_spec.capacity = ExecutionCapacityV1RunnerProjection {
        execution_contract: "workflow_v1_cloud_inputs_artifacts@1".to_owned(),
        source_closure_digest: projected_digest,
        general_maximum_transitions: requirements.general_maximum_transitions,
        selected_maximum_transitions: requirements.cloud_maximum_transitions,
        maximum_invocations: requirements.maximum_invocations,
        maximum_retained_bytes_per_invocation: requirements.maximum_retained_bytes_per_invocation,
        diagnostic_retention_bytes: requirements.diagnostic_retention_bytes,
        native_session_retention_bytes: requirements.native_session_retention_bytes,
        aggregate_retention_bytes: requirements.aggregate_retention_bytes,
        condition_transition_count: requirements.condition_transition_count,
        aggregate_condition_transition_bytes: requirements.aggregate_condition_transition_bytes,
        terminal_result_structure_bytes: requirements.terminal_result_structure_bytes,
        presentation_result_bytes: requirements.presentation_result_bytes,
        portable_result_bytes: requirements.portable_result_bytes,
        encoded_outbox_bytes: requirements.encoded_outbox_bytes,
    };
    Ok(())
}

async fn prepare_current(
    manager: &mut AssignmentManager,
    offered: &AssignmentOffer,
) -> anyhow::Result<()> {
    let preparation_id = wait_for_observation(manager, |observation| {
        matches!(observation, AssignmentObservation::Preparing { .. })
    })
    .await?
    .id;
    manager.acknowledge_observation(preparation_id);
    let preparation_expires_at = (manager.sleeper.utc_now() + time::Duration::minutes(15))
        .format(&time::format_description::well_known::Rfc3339)
        .context("format assignment preparation deadline")?;
    manager
        .handle_prepare(AssignmentPrepare {
            effect_id: if offered.attempt_number == 1 {
                "eff_01k0z6r1w8f4jy2m7q9v3x5acz".to_owned()
            } else if offered.attempt_number == 2 {
                "eff_01k0z6r1w8f4jy2m7q9v3x5abn".to_owned()
            } else {
                "eff_01k0z6r1w8f4jy2m7q9v3x5abs".to_owned()
            },
            assignment_id: offered.assignment_id.clone(),
            run_id: offered.run_id.clone(),
            attempt_id: offered.attempt_id.clone(),
            execution_spec_id: offered.execution_spec.execution_spec_id.clone(),
            preparation_expires_at,
        })
        .map_err(|error| anyhow!("prepare nested workflow assignment: {error:?}"))?;
    if offered.continuation.is_some() {
        let ready = wait_for_observation(manager, |observation| {
            matches!(observation, AssignmentObservation::ContinuationReady { assignment_id, .. }
                if assignment_id == &offered.assignment_id)
        })
        .await?;
        manager.acknowledge_observation(ready.id);
    }
    wait_for_manager_state(manager, |manager| {
        manager.drain_events();
        !matches!(manager.slot, Some(LocalSlot::Preparing(_)))
    })
    .await;
    let pending = manager.pending_observations(&BTreeSet::new(), 10);
    ensure!(
        matches!(manager.slot, Some(LocalSlot::Accepted(_))),
        "nested workflow assignment preparation was not accepted: {pending:#?}"
    );
    for id in pending.into_iter().filter_map(|pending| {
        matches!(
            pending.observation,
            AssignmentObservation::PreparationProgress { .. }
        )
        .then_some(pending.id)
    }) {
        manager.acknowledge_observation(id);
    }
    Ok(())
}

fn spawn_execution(
    manager: &mut AssignmentManager,
    offered: &AssignmentOffer,
) -> anyhow::Result<()> {
    let job = manager
        .handle_start(AssignmentStart {
            effect_id: if offered.attempt_number == 1 {
                "eff_01k0z6r1w8f4jy2m7q9v3x5abh".to_owned()
            } else if offered.attempt_number == 2 {
                "eff_01k0z6r1w8f4jy2m7q9v3x5abp".to_owned()
            } else {
                "eff_01k0z6r1w8f4jy2m7q9v3x5abt".to_owned()
            },
            assignment_id: offered.assignment_id.clone(),
            run_id: offered.run_id.clone(),
            attempt_id: offered.attempt_id.clone(),
            execution_spec_id: offered.execution_spec.execution_spec_id.clone(),
            lease: ExecutionLeaseGrant { sequence: 1 },
        })
        .map_err(|error| anyhow!("start nested workflow assignment: {error:?}"))?
        .ok_or_else(|| anyhow!("nested workflow assignment start did not dispatch execution"))?;
    manager
        .handle_start_authorized(AssignmentStartAuthorization {
            effect_id: if offered.attempt_number == 1 {
                "eff_01k0z6r1w8f4jy2m7q9v3x5abk".to_owned()
            } else if offered.attempt_number == 2 {
                "eff_01k0z6r1w8f4jy2m7q9v3x5abq".to_owned()
            } else {
                "eff_01k0z6r1w8f4jy2m7q9v3x5abu".to_owned()
            },
            assignment_id: offered.assignment_id.clone(),
            run_id: offered.run_id.clone(),
            attempt_id: offered.attempt_id.clone(),
        })
        .map_err(|error| anyhow!("authorize nested workflow assignment: {error:?}"))?;
    job.spawn();
    Ok(())
}

async fn wait_for_observation(
    manager: &mut AssignmentManager,
    matches: impl Fn(&AssignmentObservation) -> bool,
) -> anyhow::Result<PendingAssignmentObservation> {
    let notification = manager.notification();
    loop {
        let notified = notification.notified();
        tokio::pin!(notified);
        // The outbox calls notify_waiters, which can lose a wakeup before registration.
        notified.as_mut().enable();
        manager.drain_events();
        let pending = manager.pending_observations(&BTreeSet::new(), 100);
        if let Some(observation) = pending.iter().find(|entry| matches(&entry.observation)) {
            return Ok(observation.clone());
        }
        if pending.iter().any(|entry| {
            matches!(
                entry.observation,
                AssignmentObservation::Decision(AssignmentDecision::Rejected { .. })
            )
        }) {
            anyhow::bail!("assignment rejected while awaiting fixture observation: {pending:#?}");
        }
        notified.await;
    }
}

async fn wait_for_manager_state(
    manager: &mut AssignmentManager,
    mut reached: impl FnMut(&mut AssignmentManager) -> bool,
) {
    let notification = manager.notification();
    loop {
        let notified = notification.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if reached(manager) {
            return;
        }
        notified.await;
    }
}

async fn wait_for_carrier_registration(
    manager: &mut AssignmentManager,
) -> anyhow::Result<Vec<PendingAssignmentObservation>> {
    let notification = manager.notification();
    loop {
        let notified = notification.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let pending = manager.pending_observations(&BTreeSet::new(), 100);
        if pending.iter().any(|entry| {
            matches!(
                entry.observation,
                AssignmentObservation::Artifact {
                    request: ArtifactRequest::RegisterCarrier { .. },
                    ..
                }
            )
        }) {
            return Ok(pending);
        }
        if let Some(terminal) = pending.iter().find(|entry| entry.observation.is_terminal()) {
            anyhow::bail!(
                "nested workflow assignment ended before carrier registration: {:?}",
                terminal.observation
            );
        }
        notified.await;
    }
}

fn fail_pending_artifact_registrations(
    manager: &mut AssignmentManager,
    pending: &[PendingAssignmentObservation],
) -> anyhow::Result<bool> {
    let registrations = pending
        .iter()
        .filter_map(|entry| match &entry.observation {
            AssignmentObservation::Artifact {
                delivery_id,
                request: ArtifactRequest::RegisterCarrier { .. },
            } => Some((entry.id, *delivery_id, false)),
            AssignmentObservation::Artifact {
                delivery_id,
                request: ArtifactRequest::RegisterResult { .. },
            } => Some((entry.id, *delivery_id, true)),
            _ => None,
        })
        .collect::<Vec<_>>();
    for (observation_id, delivery_id, is_result) in &registrations {
        let response = if *is_result {
            ArtifactCloudResponse::ResultRegistration(ArtifactResultRegistrationResponse {
                request_message_id: "rmsg_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                outcome: ArtifactResultRegistrationOutcome::Failed {
                    code: "storage_quota_exceeded".to_owned(),
                },
            })
        } else {
            ArtifactCloudResponse::CarrierRegistration(ArtifactRegistrationResponse {
                request_message_id: "rmsg_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
                outcome: ArtifactRegistrationOutcome::Failed {
                    code: "storage_quota_exceeded".to_owned(),
                },
            })
        };
        manager
            .handle_artifact_response(*observation_id, *delivery_id, response)
            .map_err(|error| anyhow!("fail nested workflow artifact registration: {error:?}"))?;
    }
    Ok(!registrations.is_empty())
}

async fn wait_for_terminal(
    manager: &mut AssignmentManager,
) -> anyhow::Result<Vec<ExecutionReport>> {
    let notification = manager.notification();
    loop {
        let notified = notification.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let pending = manager.pending_observations(&BTreeSet::new(), 100);
        if fail_pending_artifact_registrations(manager, &pending)? {
            continue;
        }
        if pending
            .iter()
            .any(|pending| pending.observation.is_terminal())
        {
            return Ok(pending
                .into_iter()
                .filter_map(|pending| match pending.observation {
                    AssignmentObservation::Execution { report, .. } => Some(report),
                    AssignmentObservation::Preparing { .. }
                    | AssignmentObservation::PreparationProgress { .. }
                    | AssignmentObservation::ContinuationReady { .. }
                    | AssignmentObservation::Decision(_)
                    | AssignmentObservation::CancellationApplied(_)
                    | AssignmentObservation::LeaseRenewalRequested { .. }
                    | AssignmentObservation::WorkspaceRetention { .. }
                    | AssignmentObservation::Artifact { .. } => None,
                })
                .collect());
        }
        notified.await;
    }
}

async fn acknowledge_terminal_and_settle(manager: &mut AssignmentManager) -> anyhow::Result<()> {
    wait_for_manager_state(manager, |manager| {
        manager.drain_events();
        !matches!(manager.slot, Some(LocalSlot::Running(_)))
    })
    .await;
    let terminal_id = manager
        .pending_observations(&BTreeSet::new(), 100)
        .into_iter()
        .find(|entry| entry.observation.is_terminal())
        .ok_or_else(|| anyhow!("nested workflow terminal observation is missing"))?
        .id;
    manager.acknowledge_observation(terminal_id);
    wait_for_manager_state(manager, |manager| {
        manager.drain_events();
        !matches!(manager.slot, Some(LocalSlot::Releasing(_)))
    })
    .await;
    Ok(())
}
