use serde_json::{Value, json};

use super::*;

fn result_fixture() -> Value {
    let export = json!({
        "state": "available",
        "kind": "file",
        "mediaType": "application/octet-stream",
        "path": "exports/0001",
        "sizeBytes": 4,
        "digest": {
            "algorithm": "sha256",
            "value": "0".repeat(64)
        }
    });
    json!({
        "schemaVersion": 1,
        "attemptNumber": 1,
        "workflow": {
            "path": "workflow.yaml",
            "provenance": {
                "kind": "local",
                "sourceRoot": "/tmp/source"
            },
            "digest": {
                "algorithm": "sha256",
                "value": "1".repeat(64)
            }
        },
        "execution": {
            "executionRoot": "/tmp/execution",
            "maximumParallelSteps": 1,
            "startedAt": "2026-08-02T12:01:44Z",
            "finishedAt": "2026-08-02T12:01:45Z",
            "durationMilliseconds": 1000
        },
        "commandOutputPolicy": {
            "encoding": "base64",
            "maximumRetainedBytesPerStream": super::super::MAXIMUM_RETAINED_BYTES_PER_STREAM
        },
        "outcome": "succeeded",
        "forceAbort": null,
        "steps": [{
            "id": "produce",
            "role": "step",
            "kind": "agent",
            "failurePolicy": "required",
            "state": "succeeded",
            "startedAt": "2026-08-02T12:01:44Z",
            "durationMilliseconds": 1000
        }],
        "exports": {
            "first": export.clone(),
            "second": export
        }
    })
}

fn finalized_result_fixture() -> Value {
    let mut result = result_fixture();
    result["exports"] = json!({});
    result["finalization"] = json!({
        "trigger": "succeeded",
        "finalizers": [{
            "id": "cleanup",
            "role": "finalizer",
            "kind": "agent",
            "failurePolicy": "required",
            "state": "succeeded",
            "startedAt": "2026-08-02T12:01:45Z",
            "durationMilliseconds": 100
        }],
        "issues": [],
        "forceAbort": false
    });
    result
}

fn cloud_result_fixture() -> Value {
    let mut result = result_fixture();
    result["workflow"]["provenance"] = json!({
        "kind": "cloud",
        "projectId": "prj_01k0z6r1w8f4jy2m7q9v3x5abc",
        "repositoryConnectionId": "rpc_01k0z6r1w8f4jy2m7q9v3x5abc",
        "objectFormat": "sha1",
        "commitOid": "0123456789abcdef0123456789abcdef01234567"
    });
    result["execution"]
        .as_object_mut()
        .unwrap()
        .remove("executionRoot");
    result["execution"]["capacity"] = json!({
        "executionContract": "workflow_v1_cloud_inputs_artifacts@1",
        "sourceClosureDigest": { "algorithm": "sha256", "value": "1".repeat(64) },
        "generalMaximumTransitions": 8,
        "selectedMaximumTransitions": 7,
        "maximumInvocations": 1,
        "maximumRetainedBytesPerInvocation": 4_194_304,
        "diagnosticRetentionBytes": 8_388_608,
        "nativeSessionRetentionBytes": 4_194_304,
        "aggregateRetentionBytes": 12_582_912,
        "conditionTransitionCount": 0,
        "aggregateConditionTransitionBytes": 0,
        "terminalResultStructureBytes": 67_108_864,
        "portableResultBytes": 202_027_692,
        "encodedOutboxBytes": 85_458_944
    });
    result
}

// The Coordinator integration harness supplies this Run API response from a
// real Gateway-admitted run; preparation deliberately cannot execute GitLab
// yet, so construct only a portable result reader fixture from its retained
// identity rather than pretending that the runner produced an output.
#[test]
#[ignore = "requires the admitted Run API response supplied by the cross-component harness"]
fn admitted_gitlab_snapshot_survives_portable_result_reader() {
    let file = std::env::var("UM_GITLAB_ADMITTED_API_FILE").unwrap();
    let run: Value = serde_json::from_slice(&std::fs::read(file).unwrap()).unwrap();
    let mut result = cloud_result_fixture();
    result["workflow"]["provenance"]["projectId"] = run["projectId"].clone();
    result["workflow"]["provenance"]["repositoryConnectionId"] =
        run["primaryWorkspaceSource"]["repositoryConnectionId"].clone();
    result["workflow"]["provenance"]["commitOid"] =
        run["primaryWorkspaceSource"]["commitOid"].clone();
    result["workflow"]["provenance"]["sourceDisplaySnapshot"] =
        run["sourceDisplaySnapshot"].clone();
    let decoded = decode(&encode(&result)).unwrap();
    let super::super::publication::WorkflowProvenanceV1::Cloud {
        source_display_snapshot: Some(snapshot),
        ..
    } = decoded.workflow.provenance
    else {
        panic!("portable reader lost the admitted source snapshot");
    };
    assert_eq!(
        snapshot.repository.full_name,
        run["sourceDisplaySnapshot"]["repository"]["fullName"]
            .as_str()
            .unwrap()
    );
}

// REQ-WORKSPACE-30, REQ-WORKSPACE-31
#[test]
fn cloud_gitlab_snapshot_round_trips_exact_boundary_and_rejects_malformed_names() {
    for length in [256, 8192] {
        let name = format!("group/{}project", "n".repeat(length - 13));
        let mut result = cloud_result_fixture();
        result["workflow"]["provenance"]["sourceDisplaySnapshot"] = json!({
            "organizationDisplayName": "Example Organization",
            "projectName": "example-project",
            "repository": {
                "providerKind": "gitlab", "instanceUrl": "https://gitlab.com",
                "fullName": name
            }
        });
        assert_eq!(name.len(), length);
        let parsed = decode(&encode(&result)).unwrap();
        let super::super::publication::WorkflowProvenanceV1::Cloud {
            source_display_snapshot: Some(snapshot),
            ..
        } = parsed.workflow.provenance
        else {
            panic!("GitLab snapshot was not retained");
        };
        assert_eq!(snapshot.repository.full_name, name);
        for invalid in ["group/../repo", "group/%2f/repo"] {
            let mut altered = result.clone();
            altered["workflow"]["provenance"]["sourceDisplaySnapshot"]["repository"]["fullName"] =
                json!(invalid);
            assert!(decode(&encode(&altered)).is_err());
        }
        if length == 8192 {
            let mut too_long = result.clone();
            too_long["workflow"]["provenance"]["sourceDisplaySnapshot"]["repository"]["fullName"] =
                json!(format!("{name}x"));
            assert!(decode(&encode(&too_long)).is_err());
        }
    }
}

fn cloud_metadata_only_result_fixture() -> Value {
    let mut result = cloud_result_fixture();
    result["exports"] = json!({});
    result
}

fn continuation_result_fixture() -> Value {
    let mut result = result_fixture();
    result["attemptNumber"] = json!(2);
    result["steps"] = json!([
        {
            "id": "produce",
            "role": "step",
            "kind": "agent",
            "failurePolicy": "required",
            "state": "inherited",
            "detail": {
                "priorAttemptId": "00000000-0000-0000-0000-000000000001",
                "priorAttemptNumber": 1,
                "priorState": "succeeded",
                "definitionChanged": false
            }
        },
        {
            "id": "rerun",
            "role": "step",
            "kind": "agent",
            "failurePolicy": "required",
            "state": "succeeded",
            "startedAt": "2026-08-02T12:01:44Z",
            "durationMilliseconds": 1000
        }
    ]);
    result["outputProducers"] = json!({
        "produce": {
            "message": {
                "attemptId": "00000000-0000-0000-0000-000000000001",
                "attemptNumber": 1,
                "node": "produce",
                "output": "message"
            }
        }
    });
    result["continuation"] = json!({
        "request": {
            "fromSteps": ["rerun"],
            "definition": "inherited"
        },
        "fromSteps": ["rerun"],
        "reexecutedSteps": ["rerun"],
        "inheritedSteps": [{
            "id": "produce",
            "priorState": "succeeded",
            "definitionChanged": false
        }],
        "definitionSource": {
            "kind": "inherited",
            "manifestDigest": {"algorithm": "sha256", "value": "2".repeat(64)},
            "priorManifestDigest": {"algorithm": "sha256", "value": "2".repeat(64)}
        },
        "workspace": {
            "executionRoot": "/tmp/execution",
            "priorExecutionRoot": "/tmp/execution",
            "startSnapshot": {
                "algorithm": "git_worktree_sha256_v1",
                "value": "3".repeat(64),
                "takenAt": "2026-08-02T12:01:43Z"
            },
            "priorSettlementSnapshot": {
                "algorithm": "git_worktree_sha256_v1",
                "value": "3".repeat(64),
                "takenAt": "2026-08-02T12:01:40Z",
                "settledBy": "engine"
            },
            "modified": false,
            "quiescence": {
                "groupsRecorded": 2,
                "groupsTerminated": 1,
                "groupsAbsent": 1,
                "provenAt": "2026-08-02T12:01:42Z"
            }
        }
    });
    result["exports"] = json!({});
    result
}

fn failed_result_fixture(phase: &str, cause: Value) -> Value {
    let mut result = result_fixture();
    let mut detail = cause;
    detail["phase"] = Value::String(phase.to_owned());
    result["outcome"] = Value::String("failed".to_owned());
    result["primaryIssue"] = json!({
        "node": { "id": "produce", "role": "step" },
        "state": "failed",
        "detail": detail.clone()
    });
    result["steps"][0]["state"] = Value::String("failed".to_owned());
    result["steps"][0]["detail"] = detail;
    result
}

fn encode(value: &Value) -> Vec<u8> {
    let mut bytes = serde_json::to_vec_pretty(value).unwrap();
    bytes.push(b'\n');
    bytes
}

// INV-ENGINE-14
#[test]
fn cloud_result_recovery_decisions_are_closed_and_legacy_bytes_stay_absent() {
    let legacy = cloud_metadata_only_result_fixture();
    let legacy_bytes = encode(&legacy);
    let parsed: WorkflowResultV1 = serde_json::from_slice(&legacy_bytes).unwrap();
    assert_eq!(validate_with_invariant(&parsed), Ok(()));
    assert!(
        serde_json::to_value(parsed)
            .unwrap()
            .get("workspaceRecovery")
            .is_none()
    );

    let mut seeded = legacy.clone();
    seeded["workspaceRecovery"] = json!({
        "schemaVersion": 1, "status": "not_established", "reason": "registration_rejected"
    });
    let parsed: WorkflowResultV1 = serde_json::from_value(seeded.clone()).unwrap();
    assert_eq!(validate_with_invariant(&parsed), Ok(()));
    seeded["workspaceRecovery"]["reason"] = Value::Null;
    let invalid: WorkflowResultV1 = serde_json::from_value(seeded.clone()).unwrap();
    assert_eq!(
        validate_with_invariant(&invalid),
        Err(RunResultInvariant::ResultStructure)
    );
    seeded["workspaceRecovery"]
        .as_object_mut()
        .unwrap()
        .remove("reason");
    assert!(serde_json::from_value::<WorkflowResultV1>(seeded).is_err());

    let mut local = result_fixture();
    local["workspaceRecovery"] = json!({"schemaVersion":1,"status":"established","reason":null});
    let local: WorkflowResultV1 = serde_json::from_value(local).unwrap();
    assert_eq!(
        validate_with_invariant(&local),
        Err(RunResultInvariant::ResultStructure)
    );
}

// INV-ENGINE-14
#[test]
fn seeded_cloud_continuation_recovery_reader_keeps_legacy_and_rejects_unbound_claims() {
    let mut seeded = continuation_result_fixture();
    let cloud = cloud_result_fixture();
    seeded["workflow"]["provenance"] = cloud["workflow"]["provenance"].clone();
    seeded["execution"] = cloud["execution"].clone();
    let prior_id = "atm_01k0z6r1w8f4jy2m7q9v3x5abc";
    seeded["steps"][0]["detail"]["priorAttemptId"] = json!(prior_id);
    seeded["outputProducers"]["produce"]["message"]["attemptId"] = json!(prior_id);
    let legacy: WorkflowResultV1 = serde_json::from_value(seeded.clone()).unwrap();
    assert_eq!(validate_with_invariant(&legacy), Ok(()));
    assert!(
        serde_json::to_value(legacy).unwrap()["continuation"]
            .get("schemaVersion")
            .is_none()
    );

    seeded["continuation"]["schemaVersion"] = json!(2);
    seeded["continuation"]["workspace"]["recovery"] = json!({
        "schemaVersion":1,
        "workspaceId":"rws_01k0z6r1w8f4jy2m7q9v3x5abc",
        "workRootId":"rwr_01k0z6r1w8f4jy2m7q9v3x5abc",
        "runnerId":"rnr_01k0z6r1w8f4jy2m7q9v3x5abc",
        "originRunnerBootId":"rbt_01k0z6r1w8f4jy2m7q9v3x5abc",
        "targetRunnerBootId":"rbt_01k0z6r1w8f4jy2m7q9v3x5abd",
        "claimingAssignmentId":"asn_01k0z6r1w8f4jy2m7q9v3x5abc",
        "claimGeneration":1,"failure":null
    });
    let parsed: WorkflowResultV1 = serde_json::from_value(seeded.clone()).unwrap();
    assert_eq!(validate_with_invariant(&parsed), Ok(()));
    seeded["continuation"]["workspace"]["recovery"]["claimGeneration"] = Value::Null;
    let unbound: WorkflowResultV1 = serde_json::from_value(seeded.clone()).unwrap();
    assert_eq!(
        validate_with_invariant(&unbound),
        Err(RunResultInvariant::Continuation)
    );
    seeded["continuation"]["workspace"]["recovery"]
        .as_object_mut()
        .unwrap()
        .remove("failure");
    assert!(serde_json::from_value::<WorkflowResultV1>(seeded).is_err());
}

#[test]
fn inherited_result_metadata_validates_producers_and_workspace_comparison() {
    let valid: WorkflowResultV1 = serde_json::from_value(continuation_result_fixture()).unwrap();
    assert_eq!(validate_with_invariant(&valid), Ok(()));

    let mut invalid_producer = valid.clone();
    invalid_producer
        .output_producers
        .get_mut("produce")
        .unwrap()
        .get_mut("message")
        .unwrap()
        .attempt_number = 2;
    assert_eq!(
        validate_with_invariant(&invalid_producer),
        Err(RunResultInvariant::Continuation)
    );

    let mut invalid_comparison = valid;
    invalid_comparison
        .continuation
        .as_mut()
        .unwrap()
        .workspace
        .modified = super::super::publication::WorkspaceModifiedV1::Known(true);
    assert_eq!(
        validate_with_invariant(&invalid_comparison),
        Err(RunResultInvariant::Continuation)
    );

    let mut stale_direct_producer = continuation_result_fixture();
    stale_direct_producer["attemptNumber"] = json!(3);
    stale_direct_producer["steps"][0]["detail"]["priorAttemptId"] =
        json!("00000000-0000-0000-0000-000000000002");
    stale_direct_producer["steps"][0]["detail"]["priorAttemptNumber"] = json!(2);
    let stale_direct_result: WorkflowResultV1 =
        serde_json::from_value(stale_direct_producer.clone()).unwrap();
    assert_eq!(
        validate_with_invariant(&stale_direct_result),
        Err(RunResultInvariant::Continuation)
    );

    let mut valid_chain = stale_direct_producer;
    valid_chain["steps"][0]["detail"]["priorState"] = json!("inherited");
    valid_chain["continuation"]["inheritedSteps"][0]["priorState"] = json!("inherited");
    let valid_chain_result: WorkflowResultV1 = serde_json::from_value(valid_chain.clone()).unwrap();
    assert_eq!(validate_with_invariant(&valid_chain_result), Ok(()));

    let mut unflattened_chain = valid_chain;
    unflattened_chain["outputProducers"]["produce"]["message"]["attemptId"] =
        json!("00000000-0000-0000-0000-000000000002");
    unflattened_chain["outputProducers"]["produce"]["message"]["attemptNumber"] = json!(2);
    let unflattened_chain: WorkflowResultV1 = serde_json::from_value(unflattened_chain).unwrap();
    assert_eq!(
        validate_with_invariant(&unflattened_chain),
        Err(RunResultInvariant::Continuation)
    );
}

#[test]
fn staged_workspace_evidence_does_not_claim_preparation_or_inherited_bytes() {
    let mut document = continuation_result_fixture();
    // Historical local results had no preparation field and remain readable.
    assert!(decode(&encode(&document)).is_ok());
    document["continuation"]["workspace"]["preparation"] = json!("ready");
    assert!(decode(&encode(&document)).is_ok());

    let mut pending = document.clone();
    pending["continuation"]["workspace"]["preparation"] = json!("pending");
    pending["continuation"]["workspace"]["startSnapshot"] = Value::Null;
    pending["continuation"]["workspace"]["quiescence"] = Value::Null;
    pending["continuation"]["workspace"]["modified"] = json!("unknown");
    let pending_record: super::super::publication::ContinuationRecordV1 =
        serde_json::from_value(pending["continuation"].clone()).unwrap();
    assert!(validate_continuation_record(&pending_record));
    // A portable result cannot precede an authoritative engine result.
    assert_eq!(
        validate_with_invariant(&serde_json::from_value(pending.clone()).unwrap()),
        Err(RunResultInvariant::Continuation)
    );
    pending["continuation"]["workspace"]["preparation"] = json!("unavailable");
    let unavailable: super::super::publication::ContinuationRecordV1 =
        serde_json::from_value(pending["continuation"].clone()).unwrap();
    assert!(validate_continuation_record(&unavailable));
    pending["continuation"]["workspace"]["modified"] = json!(false);
    let false_claim: super::super::publication::ContinuationRecordV1 =
        serde_json::from_value(pending["continuation"].clone()).unwrap();
    assert!(!validate_continuation_record(&false_claim));

    // An unexported original producer remains a reference, not a carrier inventory.
    document["continuation"]["workspace"]["priorSettlementSnapshot"] = Value::Null;
    document["continuation"]["workspace"]["modified"] = json!("unknown");
    assert!(decode(&encode(&document)).is_ok());
    assert_eq!(
        document["outputProducers"]["produce"]["message"]["attemptNumber"],
        1
    );
    assert_eq!(document["exports"], json!({}));
}

#[test]
fn unavailable_inherited_exports_require_a_resolved_skipped_source() {
    let mut document = continuation_result_fixture();
    document["outputProducers"] = json!({});
    document["exportSources"] = json!({
        "message": {
            "node": {"id": "produce", "role": "step"},
            "output": "message"
        }
    });
    document["exports"] = json!({
        "message": {
            "state": "unavailable",
            "reason": "source_skipped"
        }
    });
    let direct_succeeded: WorkflowResultV1 = serde_json::from_value(document.clone()).unwrap();
    assert_eq!(
        validate_with_invariant(&direct_succeeded),
        Err(RunResultInvariant::ExportMetadata)
    );

    document["steps"][0]["detail"]["priorState"] = json!("skipped");
    document["continuation"]["inheritedSteps"][0]["priorState"] = json!("skipped");
    assert!(decode(&encode(&document)).is_ok());

    let mut chained_skipped = document.clone();
    chained_skipped["attemptNumber"] = json!(3);
    chained_skipped["steps"][0]["detail"]["priorAttemptId"] =
        json!("00000000-0000-0000-0000-000000000002");
    chained_skipped["steps"][0]["detail"]["priorAttemptNumber"] = json!(2);
    chained_skipped["steps"][0]["detail"]["priorState"] = json!("inherited");
    chained_skipped["continuation"]["inheritedSteps"][0]["priorState"] = json!("inherited");
    assert!(decode(&encode(&chained_skipped)).is_ok());

    chained_skipped["outputProducers"] = json!({
        "produce": {
            "other": {
                "attemptId": "00000000-0000-0000-0000-000000000001",
                "attemptNumber": 1,
                "node": "produce",
                "output": "other"
            }
        }
    });
    let resolved_succeeded: WorkflowResultV1 = serde_json::from_value(chained_skipped).unwrap();
    assert_eq!(
        validate_with_invariant(&resolved_succeeded),
        Err(RunResultInvariant::ExportMetadata)
    );

    let producer = json!({
        "attemptId": "00000000-0000-0000-0000-000000000001",
        "attemptNumber": 1,
        "node": "produce",
        "output": "message"
    });
    document["outputProducers"] = json!({
        "produce": { "message": producer.clone() }
    });
    document["exports"]["message"] = json!({
        "state": "available",
        "kind": "text",
        "mediaType": "text/plain; charset=utf-8",
        "path": "exports/0001",
        "sizeBytes": 4,
        "digest": {"algorithm": "sha256", "value": "0".repeat(64)},
        "provenance": "inherited",
        "producer": producer
    });
    let invalid: WorkflowResultV1 = serde_json::from_value(document).unwrap();
    assert_eq!(
        validate_with_invariant(&invalid),
        Err(RunResultInvariant::Continuation)
    );
}

#[test]
fn cloud_continuation_preserves_typed_attempt_ids_and_export_producers() {
    let mut document = continuation_result_fixture();
    let cloud = cloud_result_fixture();
    document["workflow"]["provenance"] = cloud["workflow"]["provenance"].clone();
    document["execution"] = cloud["execution"].clone();
    let attempt_id = "atm_01k0z6r1w8f4jy2m7q9v3x5abc";
    document["steps"][0]["detail"]["priorAttemptId"] = json!(attempt_id);
    document["outputProducers"]["produce"]["message"]["attemptId"] = json!(attempt_id);
    document["exportSources"] = json!({
        "message": {"node": {"id": "produce", "role": "step"}, "output": "message"}
    });
    document["exports"] = json!({
        "message": {
            "state": "available", "kind": "text", "mediaType": "text/plain; charset=utf-8",
            "path": "exports/0001", "sizeBytes": 4,
            "digest": {"algorithm": "sha256", "value": "0".repeat(64)},
            "provenance": "inherited",
            "producer": document["outputProducers"]["produce"]["message"].clone()
        }
    });
    let valid: WorkflowResultV1 = serde_json::from_value(document.clone()).unwrap();
    assert_eq!(validate_with_invariant(&valid), Ok(()));

    for invalid_id in [
        "00000000-0000-0000-0000-000000000001",
        "run_01k0z6r1w8f4jy2m7q9v3x5abc",
        "atm_invalid",
    ] {
        let mut invalid = valid.clone();
        let Some(NodeDetail::Inherited(detail)) = &mut invalid.steps[0].detail else {
            panic!("missing inherited step");
        };
        detail.prior_attempt_id = invalid_id.to_owned();
        assert_eq!(
            validate_with_invariant(&invalid),
            Err(RunResultInvariant::StepMetadata)
        );

        let mut invalid = valid.clone();
        invalid
            .output_producers
            .get_mut("produce")
            .unwrap()
            .get_mut("message")
            .unwrap()
            .attempt_id = invalid_id.to_owned();
        assert_eq!(
            validate_with_invariant(&invalid),
            Err(RunResultInvariant::Continuation)
        );
    }
    document["exports"]["message"]["producer"]["attemptId"] =
        json!("atm_01k0z6r1w8f4jy2m7q9v3x5abd");
    let mismatched: WorkflowResultV1 = serde_json::from_value(document).unwrap();
    assert_eq!(
        validate_with_invariant(&mismatched),
        Err(RunResultInvariant::ExportMetadata)
    );

    let mut local: WorkflowResultV1 =
        serde_json::from_value(continuation_result_fixture()).unwrap();
    let Some(NodeDetail::Inherited(detail)) = &mut local.steps[0].detail else {
        panic!("missing inherited step");
    };
    detail.prior_attempt_id = attempt_id.to_owned();
    assert_eq!(
        validate_with_invariant(&local),
        Err(RunResultInvariant::StepMetadata)
    );
}

#[test]
fn inherited_export_provenance_requires_its_direct_producer() {
    let mut document = continuation_result_fixture();
    document["exportSources"] = json!({
        "message": {
            "node": {"id": "produce", "role": "step"},
            "output": "message"
        }
    });
    document["exports"] = json!({
        "message": {
            "state": "available",
            "kind": "text",
            "mediaType": "text/plain; charset=utf-8",
            "path": "exports/0001",
            "sizeBytes": 4,
            "digest": {"algorithm": "sha256", "value": "0".repeat(64)},
            "provenance": "inherited",
            "producer": document["outputProducers"]["produce"]["message"].clone()
        }
    });
    let valid: WorkflowResultV1 = serde_json::from_value(document.clone()).unwrap();
    assert_eq!(validate_with_invariant(&valid), Ok(()));

    let mut mismatched = document.clone();
    mismatched["exports"]["message"]["producer"]["attemptId"] =
        json!("00000000-0000-0000-0000-000000000002");
    let mismatched: WorkflowResultV1 = serde_json::from_value(mismatched).unwrap();
    assert_eq!(
        validate_with_invariant(&mismatched),
        Err(RunResultInvariant::ExportMetadata)
    );

    document["exports"]["message"]
        .as_object_mut()
        .unwrap()
        .remove("producer");
    let invalid: WorkflowResultV1 = serde_json::from_value(document.clone()).unwrap();
    assert_eq!(
        validate_with_invariant(&invalid),
        Err(RunResultInvariant::ExportMetadata)
    );

    document["exports"]["message"]
        .as_object_mut()
        .unwrap()
        .remove("provenance");
    let stripped: WorkflowResultV1 = serde_json::from_value(document).unwrap();
    assert_eq!(
        validate_with_invariant(&stripped),
        Err(RunResultInvariant::ExportMetadata)
    );
}

#[test]
fn invalid_result_metadata_reports_a_closed_field_category() {
    let valid: WorkflowResultV1 = serde_json::from_value(result_fixture()).unwrap();

    let mut invalid_execution = valid.clone();
    invalid_execution.execution.execution_root = Some("relative".to_owned());
    assert_eq!(
        validate_with_invariant(&invalid_execution),
        Err(RunResultInvariant::ExecutionMetadata)
    );

    let mut invalid_step = valid;
    invalid_step.steps[0].id.clear();
    assert_eq!(
        validate_with_invariant(&invalid_step),
        Err(RunResultInvariant::StepMetadata)
    );
}

#[test]
fn finalization_metadata_rejects_role_issue_and_force_mismatches() {
    let valid = finalized_result_fixture();
    assert!(decode(&encode(&valid)).is_ok());

    let mut wrong_role = valid.clone();
    wrong_role["finalization"]["finalizers"][0]["role"] = Value::String("step".to_owned());
    assert_eq!(decode(&encode(&wrong_role)), Err(ResultMetadataError));

    let mut false_issue = valid.clone();
    false_issue["finalization"]["issues"] = json!([{
        "node": { "id": "cleanup", "role": "finalizer" },
        "impact": "required"
    }]);
    assert_eq!(decode(&encode(&false_issue)), Err(ResultMetadataError));

    let mut impossible_force_abort = valid;
    impossible_force_abort["finalization"]["forceAbort"] = Value::Bool(true);
    assert_eq!(
        decode(&encode(&impossible_force_abort)),
        Err(ResultMetadataError)
    );
}

#[test]
fn force_abort_after_graceful_cancellation_accepts_authoritative_terminal_reason() {
    let mut result = finalized_result_fixture();
    result["outcome"] = json!("cancelled");
    result["finalization"]["cancellation"] = json!({
        "reason": "runner_shutdown",
        "forceStopDeadline": "2026-08-02T12:01:46Z"
    });
    result["forceAbort"] = json!({
        "reason": "force_abort",
        "phase": "finalization"
    });
    result["finalization"]["forceAbort"] = json!(true);
    result["finalization"]["finalizers"][0]["state"] = json!("cancelled");
    result["finalization"]["finalizers"][0]["detail"] = json!({
        "code": "force_abort"
    });

    assert!(decode(&encode(&result)).is_ok());
}

#[test]
fn finalization_force_abort_cannot_rewrite_an_ordinary_node() {
    let mut result = finalized_result_fixture();
    result["outcome"] = json!("cancelled");
    result["cancellation"] = json!({
        "reason": "user_request",
        "forceStopDeadline": "2026-08-02T12:01:46Z"
    });
    result["forceAbort"] = json!({
        "reason": "force_abort",
        "phase": "finalization"
    });
    result["steps"][0]["state"] = json!("cancelled");
    result["steps"][0]["detail"] = json!({ "code": "force_abort" });
    result["finalization"]["trigger"] = json!("cancelled");
    result["finalization"]["cancellation"] = json!({ "reason": "force_abort" });
    result["finalization"]["forceAbort"] = json!(true);
    result["finalization"]["finalizers"][0]["state"] = json!("cancelled");
    result["finalization"]["finalizers"][0]["detail"] = json!({ "code": "force_abort" });

    assert_eq!(decode(&encode(&result)), Err(ResultMetadataError));

    result["steps"][0]["detail"] = json!({ "code": "user_request" });
    assert!(decode(&encode(&result)).is_ok());
}

#[test]
fn ordinary_force_abort_requires_force_finalization_cancellation() {
    let mut result = finalized_result_fixture();
    result["outcome"] = json!("cancelled");
    result["forceAbort"] = json!({
        "reason": "force_abort",
        "phase": "ordinary"
    });
    result["steps"][0]["state"] = json!("cancelled");
    result["steps"][0]["detail"] = json!({ "code": "force_abort" });
    result["finalization"]["trigger"] = json!("cancelled");
    result["finalization"]["cancellation"] = json!({
        "reason": "runner_shutdown",
        "forceStopDeadline": "2026-08-02T12:01:46Z"
    });
    result["finalization"]["forceAbort"] = json!(true);
    result["finalization"]["finalizers"][0]["state"] = json!("cancelled");
    result["finalization"]["finalizers"][0]["detail"] = json!({ "code": "force_abort" });

    assert_eq!(decode(&encode(&result)), Err(ResultMetadataError));

    result["finalization"]["cancellation"] = json!({ "reason": "force_abort" });
    assert!(decode(&encode(&result)).is_ok());
}

#[test]
fn ordinary_force_abort_accepts_only_trigger_ineligible_finalizers() {
    let mut result = finalized_result_fixture();
    result["outcome"] = json!("cancelled");
    result["forceAbort"] = json!({
        "reason": "force_abort",
        "phase": "ordinary"
    });
    result["steps"][0]["state"] = json!("cancelled");
    result["steps"][0]["detail"] = json!({ "code": "force_abort" });
    result["finalization"]["trigger"] = json!("cancelled");
    result["finalization"]["cancellation"] = json!({
        "reason": "force_abort"
    });
    result["finalization"]["forceAbort"] = json!(true);
    result["finalization"]["finalizers"][0]["state"] = json!("not_run");
    result["finalization"]["finalizers"][0]["detail"] =
        json!({ "code": "finalizer_trigger_not_selected" });
    let finalizer = result["finalization"]["finalizers"][0]
        .as_object_mut()
        .unwrap();
    finalizer.remove("startedAt");
    finalizer.remove("durationMilliseconds");

    assert!(decode(&encode(&result)).is_ok());

    result["finalization"]["finalizers"][0] = json!({
        "id": "cleanup",
        "role": "finalizer",
        "kind": "agent",
        "failurePolicy": "required",
        "state": "succeeded",
        "startedAt": "2026-08-02T12:01:45Z",
        "durationMilliseconds": 100
    });
    assert_eq!(decode(&encode(&result)), Err(ResultMetadataError));
}

#[test]
fn admits_exact_local_and_cloud_origin_profiles() {
    assert!(decode(&encode(&result_fixture())).is_ok());
    assert!(decode(&encode(&cloud_result_fixture())).is_ok());
}

#[test]
fn rejects_unknown_mixed_or_malformed_cloud_origin_profiles() {
    let mut invalid_results = Vec::new();

    let mut unknown_kind = cloud_metadata_only_result_fixture();
    unknown_kind["workflow"]["provenance"]["kind"] = Value::String("remote".to_owned());
    invalid_results.push(unknown_kind);

    let mut mixed_cloud = cloud_metadata_only_result_fixture();
    mixed_cloud["workflow"]["provenance"]["sourceRoot"] = Value::String("/runner".to_owned());
    invalid_results.push(mixed_cloud);

    let mut mixed_local = result_fixture();
    mixed_local["workflow"]["provenance"]["projectId"] =
        Value::String("prj_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned());
    invalid_results.push(mixed_local);

    let mut malformed_project = cloud_metadata_only_result_fixture();
    malformed_project["workflow"]["provenance"]["projectId"] =
        Value::String("prj_81k0z6r1w8f4jy2m7q9v3x5abc".to_owned());
    invalid_results.push(malformed_project);

    let mut malformed_connection = cloud_metadata_only_result_fixture();
    malformed_connection["workflow"]["provenance"]["repositoryConnectionId"] =
        Value::String("rpc_01K0z6r1w8f4jy2m7q9v3x5abc".to_owned());
    invalid_results.push(malformed_connection);

    let mut malformed_commit = cloud_metadata_only_result_fixture();
    malformed_commit["workflow"]["provenance"]["commitOid"] =
        Value::String("0123456789abcdef0123456789abcdef0123456G".to_owned());
    invalid_results.push(malformed_commit);

    let mut runner_execution_root = cloud_metadata_only_result_fixture();
    runner_execution_root["execution"]["executionRoot"] = Value::String("/runner/work".to_owned());
    invalid_results.push(runner_execution_root);

    for field in [
        "runnerPath",
        "runnerId",
        "organizationName",
        "repositoryUrl",
        "bucket",
        "objectKey",
        "credential",
        "destinationPublicationState",
    ] {
        let mut extra_origin = cloud_metadata_only_result_fixture();
        extra_origin["workflow"]["provenance"][field] = Value::String("forbidden".to_owned());
        invalid_results.push(extra_origin);
    }

    for result in invalid_results {
        assert_eq!(decode(&encode(&result)), Err(ResultMetadataError));
    }
}

#[test]
fn local_and_cloud_profiles_share_result_invariants() {
    for mut result in [result_fixture(), cloud_result_fixture()] {
        result["exports"]["second"]["digest"]["value"] = Value::String("2".repeat(64));
        assert_eq!(decode(&encode(&result)), Err(ResultMetadataError));
    }

    for mut result in [result_fixture(), cloud_metadata_only_result_fixture()] {
        result["steps"][0]["state"] = Value::String("failed".to_owned());
        assert_eq!(decode(&encode(&result)), Err(ResultMetadataError));
    }
}

#[test]
fn accepts_stream_at_shared_retention_cap_and_rejects_larger_claim() {
    let maximum = super::super::MAXIMUM_RETAINED_BYTES_PER_STREAM;
    let bytes = vec![b'x'; usize::try_from(maximum).unwrap()];
    let stream = json!({
        "encoding": "base64",
        "data": BASE64_STANDARD.encode(bytes),
        "retainedBytes": maximum,
        "discardedBytes": 0,
        "truncated": false,
        "fullyDrained": true
    });
    let mut result = result_fixture();
    result["steps"][0]["kind"] = Value::String("cmd".to_owned());
    result["steps"][0]["commandOutput"] = json!({
        "stdout": stream,
        "stderr": {
            "encoding": "base64",
            "data": "",
            "retainedBytes": 0,
            "discardedBytes": 0,
            "truncated": false,
            "fullyDrained": true
        }
    });

    assert!(decode(&encode(&result)).is_ok());

    let oversized = vec![b'x'; usize::try_from(maximum + 1).unwrap()];
    result["steps"][0]["commandOutput"]["stdout"]["data"] =
        Value::String(BASE64_STANDARD.encode(&oversized));
    result["steps"][0]["commandOutput"]["stdout"]["retainedBytes"] = Value::from(maximum + 1);
    assert_eq!(decode(&encode(&result)), Err(ResultMetadataError));
}

#[test]
fn accepts_stream_truncated_at_the_declared_admitted_limit() {
    let maximum = 1_024_u64;
    let retained = vec![b'x'; usize::try_from(maximum).unwrap()];
    let stream = json!({
        "encoding": "base64",
        "data": BASE64_STANDARD.encode(&retained),
        "retainedBytes": maximum,
        "discardedBytes": 1,
        "truncated": true,
        "fullyDrained": true
    });
    let mut result = result_fixture();
    result["commandOutputPolicy"]["maximumRetainedBytesPerStream"] = json!(maximum);
    result["steps"][0]["kind"] = json!("cmd");
    result["steps"][0]["commandOutput"] = json!({
        "stdout": stream,
        "stderr": {
            "encoding": "base64",
            "data": "",
            "retainedBytes": 0,
            "discardedBytes": 0,
            "truncated": false,
            "fullyDrained": true
        }
    });

    assert!(decode(&encode(&result)).is_ok());

    let mut underfilled = result.clone();
    underfilled["steps"][0]["commandOutput"]["stdout"]["data"] =
        json!(BASE64_STANDARD.encode(&retained[..retained.len() - 1]));
    underfilled["steps"][0]["commandOutput"]["stdout"]["retainedBytes"] = json!(maximum - 1);
    assert_eq!(decode(&encode(&underfilled)), Err(ResultMetadataError));

    result["commandOutputPolicy"]["maximumRetainedBytesPerStream"] =
        json!(super::super::MAXIMUM_RETAINED_BYTES_PER_STREAM + 1);
    assert_eq!(decode(&encode(&result)), Err(ResultMetadataError));
}

// REQ-WFRECOVERY-41
#[test]
fn recovery_version_dispatch_precedes_nested_interpretation() {
    let mut result = result_fixture();
    result["steps"][0]["recovery"] = json!({
        "schemaVersion": 2,
        "futureNestedShape": {"not": "schema one"}
    });

    assert_eq!(
        dispatch_recovery_summary_versions(&result),
        Err(RecoverySummaryVersionError::Unsupported)
    );
    assert_eq!(decode(&encode(&result)), Err(ResultMetadataError));
}

fn recovered_result_fixture() -> Value {
    let mut result = result_fixture();
    result["steps"][0]["recovery"] = json!({
        "schemaVersion": 1,
        "configuredRetries": 1,
        "rounds": [{
            "number": 1,
            "failedExecution": {
                "executionNumber": 1,
                "invocationId": 1,
                "failure": {
                    "phase": "execution",
                    "cause": { "code": "harness_failed" }
                }
            }
        }],
        "termination": {
            "kind": "recovered",
            "executionNumber": 2
        }
    });
    result["steps"][0]["invocations"] = json!([
        {
            "invocationId": 1,
            "role": "target",
            "targetExecution": 1,
            "state": "settled",
            "startedAt": "2026-08-02T12:01:44Z",
            "finishedAt": "2026-08-02T12:01:44.1Z",
            "durationMilliseconds": 100,
            "usage": { "inputTokens": 1, "outputTokens": 1 }
        },
        {
            "invocationId": 3,
            "role": "target",
            "targetExecution": 2,
            "state": "settled",
            "startedAt": "2026-08-02T12:01:44.2Z",
            "finishedAt": "2026-08-02T12:01:44.3Z",
            "durationMilliseconds": 100,
            "usage": { "inputTokens": 1, "outputTokens": 1 }
        }
    ]);
    result
}

#[test]
fn recovery_duration_uses_monotonic_time_even_when_wall_clock_moves() {
    let mut result = recovered_result_fixture();
    // The runner records UTC timestamps and elapsed time from separate clocks.
    result["steps"][0]["invocations"][0]["finishedAt"] = json!("2026-08-02T12:01:43.9Z");

    let decoded = decode(&encode(&result)).unwrap();
    assert_eq!(validate_with_invariant(&decoded), Ok(()));
    assert_eq!(decoded.steps[0].invocations[0].duration_milliseconds, 100);
}

#[test]
fn launch_failure_with_terminal_recovery_validates_for_publication() {
    for termination in ["gave_up", "handler_failed"] {
        let mut result = recovered_result_fixture();
        let detail = json!({ "phase": "start", "code": "harness_start_failed" });
        result["outcome"] = json!("failed");
        result["exports"] = json!({});
        result["steps"][0]["state"] = json!("failed");
        result["steps"][0]["detail"] = detail.clone();
        result["primaryIssue"] = json!({
            "node": { "id": "produce", "role": "step" },
            "state": "failed",
            "detail": detail
        });
        result["steps"][0]["recovery"]["handlerKind"] = json!("cmd");
        result["steps"][0]["recovery"]["rounds"][0]["failedExecution"]["failure"] = json!({
            "phase": "start",
            "cause": {
                "code": "harness_start_failed"
            }
        });
        result["steps"][0]["invocations"]
            .as_array_mut()
            .unwrap()
            .pop();
        result["steps"][0]["invocations"]
            .as_array_mut()
            .unwrap()
            .push(handler_invocation_fixture());
        if termination == "gave_up" {
            result["steps"][0]["recovery"]["rounds"][0]["handler"] = json!({
                "kind": "cmd", "invocationId": 2, "outcome": "gave_up",
                "summary": "No repair.", "reason": "Cannot recheck."
            });
            result["steps"][0]["recovery"]["termination"] =
                json!({ "kind": "gave_up", "round": 1 });
        } else {
            let failure = json!({ "phase": "start", "cause": { "code": "context_unavailable" } });
            result["steps"][0]["recovery"]["rounds"][0]["handler"] = json!({
                "kind": "cmd", "invocationId": 2, "outcome": "failed",
                "failure": failure
            });
            result["steps"][0]["recovery"]["termination"] = json!({
                "kind": "handler_failed", "round": 1, "handlerFailure": failure
            });
        }

        let decoded = decode(&encode(&result)).expect(termination);
        assert_eq!(validate_with_invariant(&decoded), Ok(()));
    }
}

fn handler_invocation_fixture() -> Value {
    json!({
        "invocationId": 2,
        "role": "recovery_handler",
        "recoveryRound": 1,
        "state": "settled",
        "startedAt": "2026-08-02T12:01:44.1Z",
        "finishedAt": "2026-08-02T12:01:44.2Z",
        "durationMilliseconds": 100,
        "usage": { "inputTokens": 0, "outputTokens": 0 }
    })
}

#[test]
fn recovered_summary_accepts_schema_length_non_ascii_text() {
    let mut result = recovered_result_fixture();
    result["steps"][0]["recovery"]["handlerKind"] = json!("cmd");
    result["steps"][0]["recovery"]["rounds"][0]["handler"] = json!({
        "kind": "cmd",
        "invocationId": 2,
        "outcome": "recheck",
        "summary": "é".repeat(3_000),
        "reason": "Verify the repair."
    });
    result["steps"][0]["invocations"]
        .as_array_mut()
        .unwrap()
        .insert(1, handler_invocation_fixture());

    assert!(decode(&encode(&result)).is_ok());
}

// REQ-WFRECOVERY-42
#[test]
fn recovered_summary_rejects_a_handler_that_gave_up() {
    let mut result = recovered_result_fixture();
    result["steps"][0]["recovery"]["handlerKind"] = json!("cmd");
    result["steps"][0]["recovery"]["rounds"][0]["handler"] = json!({
        "kind": "cmd",
        "invocationId": 2,
        "outcome": "gave_up",
        "summary": "No repair was made.",
        "reason": "The handler refused to recheck."
    });
    result["steps"][0]["invocations"]
        .as_array_mut()
        .unwrap()
        .insert(1, handler_invocation_fixture());

    assert_eq!(
        decode(&encode(&result)),
        Err(ResultMetadataError),
        "gave_up is terminal and cannot authorize the target execution claimed to recover"
    );
}

// REQ-WFRECOVERY-42
#[test]
fn handlerless_summary_rejects_a_phantom_handler_invocation() {
    let mut result = recovered_result_fixture();
    result["steps"][0]["invocations"]
        .as_array_mut()
        .unwrap()
        .insert(1, handler_invocation_fixture());

    assert_eq!(decode(&encode(&result)), Err(ResultMetadataError));
}

#[test]
fn accepts_consistent_alias_metadata_owned_by_the_lowest_ordinal() {
    let decoded = decode(&encode(&result_fixture())).unwrap();

    assert_eq!(decoded.exports["first"], decoded.exports["second"]);
}

#[test]
fn carrier_aliases_allow_independent_presentation_but_require_matching_digests() {
    for git_branch in [false, true] {
        let mut result = result_fixture();
        if git_branch {
            let file = result["exports"]["first"].clone();
            let branch = json!({
                "state": "available", "kind": "git_branch", "artifactVersion": 1,
                "objectFormat": "sha1", "baseOid": "a".repeat(40),
                "headOid": "b".repeat(40), "treeOid": "c".repeat(40),
                "carrier": {
                    "path": file["path"], "sizeBytes": file["sizeBytes"],
                    "mediaType": "application/vnd.git.bundle", "digest": file["digest"]
                }
            });
            result["exports"]["first"] = branch.clone();
            result["exports"]["second"] = branch;
        }
        for name in ["first", "second"] {
            result["exports"][name]["presentation"] = json!({
                "title": {"state": "available", "value": name}
            });
        }
        assert!(decode(&encode(&result)).is_ok());
        let alias = &mut result["exports"]["second"];
        let metadata = if git_branch {
            &mut alias["carrier"]
        } else {
            alias
        };
        metadata["digest"]["value"] = Value::from("f".repeat(64));
        assert_eq!(decode(&encode(&result)), Err(ResultMetadataError));
    }
}

#[test]
fn rejects_removed_step_fields_and_duplicate_object_members() {
    let mut removed_field = result_fixture();
    removed_field["steps"][0]["committedOutputCount"] = Value::from(0);
    assert_eq!(decode(&encode(&removed_field)), Err(ResultMetadataError));

    let duplicate = String::from_utf8(encode(&result_fixture()))
        .unwrap()
        .replacen(
            "\"schemaVersion\": 1,",
            "\"schemaVersion\": 1,\n  \"schemaVersion\": 1,",
            1,
        );
    assert_eq!(decode(duplicate.as_bytes()), Err(ResultMetadataError));
}

#[test]
fn rejects_failures_with_impossible_phases_or_cause_fields() {
    let valid = failed_result_fixture(
        "execution",
        json!({ "code": "command_exit", "exitCode": 23 }),
    );
    assert!(decode(&encode(&valid)).is_ok());

    for invalid in [
        failed_result_fixture("start", json!({ "code": "command_exit", "exitCode": 23 })),
        failed_result_fixture(
            "execution",
            json!({ "code": "command_exit", "exitCode": 0 }),
        ),
        failed_result_fixture(
            "execution",
            json!({ "code": "command_exit", "input": "payload" }),
        ),
        failed_result_fixture(
            "execution",
            json!({ "code": "input_invalid_name", "input": "payload" }),
        ),
        failed_result_fixture(
            "output_capture",
            json!({ "code": "output_missing", "output": "" }),
        ),
    ] {
        assert_eq!(decode(&encode(&invalid)), Err(ResultMetadataError));
    }
}

#[test]
fn advisory_issue_is_valid_on_success_but_cannot_be_primary() {
    let mut advisory = result_fixture();
    advisory["steps"][0]["failurePolicy"] = Value::String("advisory".to_owned());
    advisory["steps"][0]["state"] = Value::String("failed".to_owned());
    advisory["steps"][0]["detail"] = json!({
        "phase": "execution",
        "code": "harness_failed"
    });
    assert!(decode(&encode(&advisory)).is_ok());

    let mut required = advisory.clone();
    required["steps"][0]["failurePolicy"] = Value::String("required".to_owned());
    assert_eq!(decode(&encode(&required)), Err(ResultMetadataError));

    advisory["outcome"] = Value::String("failed".to_owned());
    advisory["primaryIssue"] = json!({
        "node": { "id": "produce", "role": "step" },
        "state": "failed",
        "detail": { "phase": "execution", "code": "harness_failed" }
    });
    assert_eq!(decode(&encode(&advisory)), Err(ResultMetadataError));
}

#[test]
fn rejects_multiply_owned_alias_metadata() {
    let mut non_owner = result_fixture();
    non_owner["exports"]["first"]["path"] = Value::String("exports/0002".to_owned());
    non_owner["exports"]["second"]["path"] = Value::String("exports/0002".to_owned());
    assert_eq!(decode(&encode(&non_owner)), Err(ResultMetadataError));
}

fn strip_timing(node: &mut Value) {
    let node = node.as_object_mut().unwrap();
    node.remove("startedAt");
    node.remove("durationMilliseconds");
}

fn add_timing(node: &mut Value) {
    node["startedAt"] = json!("2026-08-02T12:01:44Z");
    node["durationMilliseconds"] = json!(1000);
}

fn empty_command_output() -> Value {
    let stream = json!({
        "encoding": "base64",
        "data": "",
        "retainedBytes": 0,
        "discardedBytes": 0,
        "truncated": false,
        "fullyDrained": true
    });
    json!({ "stdout": stream.clone(), "stderr": stream })
}

fn condition_failure_detail() -> Value {
    serde_json::to_value(
        super::super::evidence::FailureDetail::json_pointer_missing(
            "outputs.upstream.ready",
            "/ready",
        )
        .unwrap(),
    )
    .unwrap()
}

/// `produce` fails during execution; `consume` is blocked on it and `later` never runs.
fn stopped_downstream_fixture() -> Value {
    let mut result = failed_result_fixture("execution", json!({ "code": "harness_failed" }));
    let steps = result["steps"].as_array_mut().unwrap();
    steps.push(json!({
        "id": "consume",
        "role": "step",
        "kind": "cmd",
        "failurePolicy": "required",
        "state": "blocked",
        "detail": {
            "code": "prerequisites_unsatisfied",
            "prerequisites": [{ "kind": "control", "node": "produce" }]
        }
    }));
    steps.push(json!({
        "id": "later",
        "role": "step",
        "kind": "cmd",
        "failurePolicy": "required",
        "state": "not_run",
        "detail": { "code": "failure_stop" }
    }));
    result
}

fn skipped_fixture() -> Value {
    let mut result = result_fixture();
    result["steps"].as_array_mut().unwrap().push(json!({
        "id": "optional",
        "role": "step",
        "kind": "cmd",
        "failurePolicy": "required",
        "state": "skipped",
        "detail": {
            "code": "condition_false",
            "evaluatedPredicates": [{ "path": "", "result": false }]
        }
    }));
    result
}

fn advisory_failure_fixture() -> Value {
    let mut result = result_fixture();
    result["steps"][0]["failurePolicy"] = json!("advisory");
    result["steps"][0]["state"] = json!("failed");
    result["steps"][0]["detail"] = json!({ "phase": "execution", "code": "harness_failed" });
    result
}

fn cancelled_fixture(reason: &str) -> Value {
    let mut result = result_fixture();
    result["exports"] = json!({});
    result["outcome"] = json!("cancelled");
    result["cancellation"] = json!({
        "reason": reason,
        "forceStopDeadline": "2026-08-02T12:01:46Z"
    });
    result["steps"][0]["state"] = json!("cancelled");
    result["steps"][0]["detail"] = json!({ "code": reason });
    result
}

// REQ-EVIDENCE-1
#[test]
fn only_nodes_that_started_record_timing_and_command_output() {
    let started = stopped_downstream_fixture();
    assert!(decode(&encode(&started)).is_ok());

    let mut untimed_success = result_fixture();
    strip_timing(&mut untimed_success["steps"][0]);
    assert_eq!(decode(&encode(&untimed_success)), Err(ResultMetadataError));

    let mut untimed_execution_failure = started.clone();
    strip_timing(&mut untimed_execution_failure["steps"][0]);
    assert_eq!(
        decode(&encode(&untimed_execution_failure)),
        Err(ResultMetadataError)
    );

    let timed_condition_failure = failed_result_fixture("condition", condition_failure_detail());
    assert_eq!(
        decode(&encode(&timed_condition_failure)),
        Err(ResultMetadataError)
    );
    let mut condition_failure = timed_condition_failure;
    strip_timing(&mut condition_failure["steps"][0]);
    assert!(decode(&encode(&condition_failure)).is_ok());
    condition_failure["steps"][0]["kind"] = json!("cmd");
    assert!(decode(&encode(&condition_failure)).is_ok());
    condition_failure["steps"][0]["commandOutput"] = empty_command_output();
    assert_eq!(
        decode(&encode(&condition_failure)),
        Err(ResultMetadataError)
    );

    for (fixture, never_started) in [
        (started.clone(), 1),
        (started, 2),
        (skipped_fixture(), 1),
        (continuation_result_fixture(), 0),
    ] {
        assert!(decode(&encode(&fixture)).is_ok());
        let mut timed = fixture.clone();
        add_timing(&mut timed["steps"][never_started]);
        assert_eq!(decode(&encode(&timed)), Err(ResultMetadataError));
        if fixture["steps"][never_started]["kind"] == "cmd" {
            let mut with_output = fixture;
            with_output["steps"][never_started]["commandOutput"] = empty_command_output();
            assert_eq!(decode(&encode(&with_output)), Err(ResultMetadataError));
        }
    }
}

// REQ-EVIDENCE-4
#[test]
fn node_detail_is_present_exactly_for_states_that_need_explaining() {
    let mut explained_success = result_fixture();
    explained_success["steps"][0]["detail"] = json!({ "code": "failure_stop" });
    assert_eq!(
        decode(&encode(&explained_success)),
        Err(ResultMetadataError)
    );

    for (fixture, explained, state) in [
        (advisory_failure_fixture(), 0, "failed"),
        (stopped_downstream_fixture(), 1, "blocked"),
        (stopped_downstream_fixture(), 2, "not_run"),
        (skipped_fixture(), 1, "skipped"),
        (cancelled_fixture("user_request"), 0, "cancelled"),
        (continuation_result_fixture(), 0, "inherited"),
    ] {
        assert_eq!(fixture["steps"][explained]["state"], state);
        assert!(decode(&encode(&fixture)).is_ok(), "{state}");
        let mut unexplained = fixture;
        unexplained["steps"][explained]
            .as_object_mut()
            .unwrap()
            .remove("detail");
        assert_eq!(
            decode(&encode(&unexplained)),
            Err(ResultMetadataError),
            "{state}"
        );
    }
}

// REQ-EVIDENCE-8
#[test]
fn cancelled_node_detail_is_the_closed_authoritative_stop_reason() {
    for reason in [
        "user_request",
        "termination_request",
        "caller_output_failure",
        "runner_shutdown",
        "execution_lease_expired",
    ] {
        let cancelled = cancelled_fixture(reason);
        assert!(decode(&encode(&cancelled)).is_ok(), "{reason}");
        let mut other_reason = cancelled;
        other_reason["steps"][0]["detail"]["code"] = json!(if reason == "user_request" {
            "runner_shutdown"
        } else {
            "user_request"
        });
        assert_eq!(
            decode(&encode(&other_reason)),
            Err(ResultMetadataError),
            "{reason}"
        );
    }

    let mut forced = cancelled_fixture("user_request");
    forced["steps"][0]["detail"]["code"] = json!("force_abort");
    assert_eq!(decode(&encode(&forced)), Err(ResultMetadataError));
    forced["forceAbort"] = json!({ "reason": "force_abort", "phase": "ordinary" });
    assert!(decode(&encode(&forced)).is_ok());

    for unknown in ["cancelled", "timeout", "operator_request"] {
        let mut unknown_reason = cancelled_fixture("user_request");
        unknown_reason["steps"][0]["detail"]["code"] = json!(unknown);
        assert_eq!(
            decode(&encode(&unknown_reason)),
            Err(ResultMetadataError),
            "{unknown}"
        );
    }
}
