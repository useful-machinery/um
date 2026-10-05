use super::*;

// Workflow V1 capacity ceilings from contracts/fixtures/workflow/v1/capacity-contract.json.
pub(super) const MAXIMUM_GENERAL_TRANSITIONS: u64 = 1_286;
pub(super) const MAXIMUM_SELECTED_TRANSITIONS: u64 = 1_030;
pub(super) const MAXIMUM_INVOCATIONS: u64 = 488;
pub(super) const MAXIMUM_RETAINED_BYTES_PER_INVOCATION: u64 = 4_194_304;
pub(super) const MAXIMUM_DIAGNOSTIC_RETENTION_BYTES: u64 = 134_217_728;
pub(super) const MAXIMUM_NATIVE_SESSION_RETENTION_BYTES: u64 = 67_108_864;
pub(super) const MAXIMUM_AGGREGATE_RETENTION_BYTES: u64 = 201_326_592;
const MAXIMUM_WORKFLOW_PATH_CHARACTERS: usize = 4096;
const MAXIMUM_SOURCE_BRANCH_CHARACTERS: usize = 1024;
const SHA1_HEX_CHARACTERS: usize = 40;
const SHA256_HEX_CHARACTERS: usize = 64;

pub(in crate::service) fn validate_lease_policy(
    policy: &ExecutionLeasePolicy,
) -> Result<(), WelcomePolicyFailure> {
    if policy.schema_version != 2 {
        return Err(WelcomePolicyFailure::Invalid);
    }
    let force_stop = nonnegative(policy.force_stop_and_reap_budget_milliseconds)?;
    let terminal_report = nonnegative(policy.terminal_report_delivery_budget_milliseconds)?;
    let renewal_delivery = nonnegative(policy.renewal_delivery_budget_milliseconds)?;
    if policy.lease_duration_milliseconds == 0 || policy.fencing_margin_milliseconds == 0 {
        return Err(WelcomePolicyFailure::Invalid);
    }
    let fencing_required = force_stop
        .checked_add(terminal_report)
        .ok_or(WelcomePolicyFailure::Invalid)?;
    if policy.fencing_margin_milliseconds < fencing_required {
        return Err(WelcomePolicyFailure::Invalid);
    }
    let maximum_cancellation_grace_milliseconds =
        u64::try_from(MAXIMUM_CANCELLATION_GRACE.as_millis())
            .map_err(|_| WelcomePolicyFailure::Invalid)?;
    let cancellation_window = policy
        .lease_duration_milliseconds
        .checked_sub(policy.fencing_margin_milliseconds)
        .and_then(|value| value.checked_sub(maximum_cancellation_grace_milliseconds))
        .ok_or(WelcomePolicyFailure::Invalid)?;
    let minimum_renewal_headroom_milliseconds = u64::try_from(MINIMUM_RENEWAL_HEADROOM.as_millis())
        .map_err(|_| WelcomePolicyFailure::Invalid)?;
    let renewal_lead = renewal_delivery.max(minimum_renewal_headroom_milliseconds);
    if cancellation_window / 2 < renewal_lead {
        return Err(WelcomePolicyFailure::Invalid);
    }
    Ok(())
}

pub(in crate::service) fn nonnegative(value: i64) -> Result<u64, WelcomePolicyFailure> {
    u64::try_from(value).map_err(|_| WelcomePolicyFailure::Invalid)
}

pub(in crate::service) fn validate_execution_spec(
    execution_spec: &ExecutionSpecV1RunnerProjection,
) -> Result<(), AssignmentDecline> {
    if !matches!(execution_spec.schema_version, 1 | 2) {
        return Err(AssignmentDecline::ExecutionSpecInvalid(
            ExecutionSpecInvalidReason::UnsupportedSchemaVersion,
        ));
    }
    if execution_spec
        .run_inputs
        .as_ref()
        .is_some_and(|projection| {
            super::super::run_inputs::validate_projection(projection).is_err()
        })
    {
        return Err(AssignmentDecline::ExecutionSpecInvalid(
            ExecutionSpecInvalidReason::InvalidInputProjection,
        ));
    }
    let maximum_parallel_steps =
        usize::try_from(execution_spec.execution_limits.maximum_parallel_steps)
            .map_err(|_| invalid_execution_limits())?;
    let cancellation_grace =
        Duration::from_secs(execution_spec.execution_limits.cancellation_grace_seconds);
    if !(1..=MAXIMUM_PARALLEL_STEPS).contains(&maximum_parallel_steps)
        || !(MINIMUM_CANCELLATION_GRACE..=MAXIMUM_CANCELLATION_GRACE).contains(&cancellation_grace)
    {
        return Err(invalid_execution_limits());
    }
    let workflow = &execution_spec.workflow_definition_source;
    let primary = &execution_spec.primary_workspace_source;
    let capacity = &execution_spec.capacity;
    if workflow.object_format != "sha1" || primary.object_format != "sha1" {
        return Err(AssignmentDecline::ExecutionSpecInvalid(
            ExecutionSpecInvalidReason::UnsupportedSourceObjectFormat,
        ));
    }
    let valid_workflow_connection =
        um_runner_protocol::valid_repository_connection_id(&workflow.repository_connection_id);
    let valid_primary_connection =
        um_runner_protocol::valid_repository_connection_id(&primary.repository_connection_id);
    let valid_path = !workflow.workflow_path.is_empty()
        && workflow.workflow_path.chars().count() <= MAXIMUM_WORKFLOW_PATH_CHARACTERS
        && !workflow.workflow_path.starts_with('/')
        && !workflow.workflow_path.contains('\0')
        && workflow
            .workflow_path
            .split('/')
            .all(|component| !matches!(component, "" | "." | ".."));
    if execution_spec.source_branch.is_empty()
        || execution_spec.source_branch.chars().count() > MAXIMUM_SOURCE_BRANCH_CHARACTERS
        || primary.kind != "connected_repository"
        || primary.provider_kind != "github"
        || primary.materialization_contract != "git_full_clone_v1"
        || !valid_workflow_connection
        || !valid_primary_connection
        || !validate_source_identity_pair(workflow, primary)
        || !lowercase_hex(&workflow.commit_oid, SHA1_HEX_CHARACTERS)
        || !lowercase_hex(&primary.commit_oid, SHA1_HEX_CHARACTERS)
        || !valid_path
        || workflow.workflow_source_closure_digest.algorithm != "sha256"
        || !lowercase_hex(
            &workflow.workflow_source_closure_digest.value,
            SHA256_HEX_CHARACTERS,
        )
        || capacity.execution_contract != "workflow_v1_cloud_inputs_artifacts@1"
        || capacity.source_closure_digest != workflow.workflow_source_closure_digest
        || capacity.general_maximum_transitions == 0
        || capacity.general_maximum_transitions > MAXIMUM_GENERAL_TRANSITIONS
        || capacity.selected_maximum_transitions == 0
        || capacity.selected_maximum_transitions > MAXIMUM_SELECTED_TRANSITIONS
        || capacity.maximum_invocations == 0
        || capacity.maximum_invocations > MAXIMUM_INVOCATIONS
        || capacity.maximum_retained_bytes_per_invocation == 0
        || capacity.maximum_retained_bytes_per_invocation > MAXIMUM_RETAINED_BYTES_PER_INVOCATION
        || capacity.diagnostic_retention_bytes < capacity.maximum_retained_bytes_per_invocation
        || capacity.diagnostic_retention_bytes > MAXIMUM_DIAGNOSTIC_RETENTION_BYTES
        || capacity.native_session_retention_bytes < capacity.maximum_retained_bytes_per_invocation
        || capacity.native_session_retention_bytes > MAXIMUM_NATIVE_SESSION_RETENTION_BYTES
        || capacity
            .diagnostic_retention_bytes
            .checked_add(capacity.native_session_retention_bytes)
            != Some(capacity.aggregate_retention_bytes)
        || capacity.aggregate_retention_bytes > MAXIMUM_AGGREGATE_RETENTION_BYTES
        || !valid_condition_capacity(ConditionCapacityBounds::from_parts(
            capacity.selected_maximum_transitions,
            (
                capacity.condition_transition_count,
                capacity.aggregate_condition_transition_bytes,
            ),
            (
                capacity.terminal_result_structure_bytes,
                capacity.presentation_result_bytes,
                capacity.portable_result_bytes,
                capacity.encoded_outbox_bytes,
            ),
        ))
    {
        return Err(AssignmentDecline::ExecutionSpecInvalid(
            ExecutionSpecInvalidReason::InvalidSourceProjection,
        ));
    }
    Ok(())
}

pub(in crate::service) fn validate_source_identity_pair(
    workflow: &WorkflowDefinitionSourceV1RunnerProjection,
    primary: &PrimaryWorkspaceSourceV1RunnerProjection,
) -> bool {
    workflow.repository_connection_id == primary.repository_connection_id
        && workflow.object_format == primary.object_format
        && workflow.commit_oid == primary.commit_oid
}

pub(in crate::service) fn lowercase_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .as_bytes()
            .iter()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
}
