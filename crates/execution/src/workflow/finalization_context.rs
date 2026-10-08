use std::sync::Arc;

use serde::Serialize;

use super::admission::CancellationReason;
use super::document::{FailurePolicy, FinalizationTrigger};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OrdinaryIssueDisposition {
    Failed,
    Blocked,
}

impl OrdinaryIssueDisposition {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Failed => "failed",
            Self::Blocked => "blocked",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OrdinaryIssue {
    pub(crate) step_id: String,
    pub(crate) failure_policy: FailurePolicy,
    pub(crate) disposition: OrdinaryIssueDisposition,
    pub(crate) failure: Option<super::evidence::FailureDetail>,
    pub(crate) blocked_code: Option<super::evidence::BlockedCode>,
}

pub(crate) struct FinalizationContext<'a> {
    pub(crate) run_id: &'a str,
    pub(crate) attempt_id: &'a str,
    pub(crate) trigger: FinalizationTrigger,
    pub(crate) primary_issue_step_id: Option<&'a str>,
    pub(crate) cancellation_reason: Option<CancellationReason>,
    pub(crate) ordinary_issues: &'a [OrdinaryIssue],
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SerializedContext<'a> {
    schema_version: u8,
    run_id: &'a str,
    attempt_id: &'a str,
    trigger: &'static str,
    primary_issue_step_id: Option<&'a str>,
    cancellation_reason: Option<&'static str>,
    ordinary_issues: Vec<SerializedIssue<'a>>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SerializedIssue<'a> {
    step_id: &'a str,
    failure_policy: &'static str,
    disposition: &'static str,
    // Canonical node evidence is authoritative. Do not serialize arbitrary causes or logs.
    failure: Option<FailureFact<'a>>,
    blocked_code: Option<super::evidence::BlockedCode>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct FailureFact<'a> {
    phase: &'static str,
    code: super::evidence::FailureCode,
    exit_code: Option<i32>,
    input: Option<&'a str>,
    output: Option<&'a str>,
    // Only the classified, bounded runner sidecar is safe to project. Other
    // diagnostics stay in the authoritative result/observation surfaces.
    diagnostic: Option<serde_json::Value>,
}

#[expect(
    clippy::expect_used,
    reason = "the closed serializer contains no fallible map keys or custom serializers"
)]
pub(crate) fn serialize(context: FinalizationContext<'_>) -> Arc<[u8]> {
    let mut ordinary_issues = context
        .ordinary_issues
        .iter()
        .map(|issue| SerializedIssue {
            step_id: &issue.step_id,
            failure_policy: match issue.failure_policy {
                FailurePolicy::Required => "required",
                FailurePolicy::Advisory => "advisory",
            },
            disposition: issue.disposition.as_str(),
            failure: issue.failure.as_ref().map(|detail| FailureFact {
                phase: detail.phase.as_str(),
                code: detail.code,
                exit_code: detail.exit_code,
                input: detail.input.as_deref().filter(|value| value.len() <= 128),
                output: detail.output.as_deref().filter(|value| value.len() <= 128),
                diagnostic: detail.runner_diagnostic(),
            }),
            blocked_code: issue.blocked_code,
        })
        .collect::<Vec<_>>();
    ordinary_issues.sort_by(|left, right| left.step_id.as_bytes().cmp(right.step_id.as_bytes()));
    let value = SerializedContext {
        schema_version: 2,
        run_id: context.run_id,
        attempt_id: context.attempt_id,
        trigger: context.trigger.as_str(),
        primary_issue_step_id: context.primary_issue_step_id,
        cancellation_reason: context.cancellation_reason.map(CancellationReason::as_str),
        ordinary_issues,
    };
    Arc::from(
        serde_json::to_vec(&value)
            .expect("the closed finalization context contains only infallible JSON values"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classified_runner_diagnostic_is_projected_without_raw_cause() {
        use super::super::agent::{
            AgentFailure, AgentFailureCause, AgentHarnessFailureDetail, CanonicalHarnessFailure,
            HarnessFailureDiagnostic,
        };
        use super::super::evidence::{FailurePhase, failure_detail};
        use super::super::step_runtime::{StepExecutionFailure, StepFailureCause};
        let cause = StepFailureCause::Execution(StepExecutionFailure::Agent(AgentFailure::new(
            AgentFailureCause::HarnessFailed {
                detail: AgentHarnessFailureDetail::Classified {
                    canonical: CanonicalHarnessFailure::ModelError,
                    diagnostic: HarnessFailureDiagnostic::from_code("rate_limit_error", Some(429)),
                },
            },
        )));
        let failure = failure_detail(FailurePhase::Execution, &cause).unwrap();
        let issues = [OrdinaryIssue {
            step_id: "work".to_owned(),
            failure_policy: FailurePolicy::Required,
            disposition: OrdinaryIssueDisposition::Failed,
            failure: Some(failure),
            blocked_code: None,
        }];
        let bytes = serialize(FinalizationContext {
            run_id: "run-1",
            attempt_id: "attempt-1",
            trigger: FinalizationTrigger::Failed,
            primary_issue_step_id: Some("work"),
            cancellation_reason: None,
            ordinary_issues: &issues,
        });
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            value["ordinaryIssues"][0]["failure"]["diagnostic"],
            serde_json::json!({"harnessError": "rate_limited", "httpStatus": 429})
        );
        assert!(!String::from_utf8_lossy(&bytes).contains("rate_limit_error"));
    }

    #[test]
    fn dedicated_serializer_preserves_the_context_abi_order() {
        use super::super::evidence::{BlockedCode, FailureCode, FailureDetail, FailurePhase};
        let issues = [
            OrdinaryIssue {
                step_id: "zeta".to_owned(),
                failure_policy: FailurePolicy::Required,
                disposition: OrdinaryIssueDisposition::Blocked,
                failure: None,
                blocked_code: Some(BlockedCode::PrerequisitesUnsatisfied),
            },
            OrdinaryIssue {
                step_id: "lint".to_owned(),
                failure_policy: FailurePolicy::Advisory,
                disposition: OrdinaryIssueDisposition::Failed,
                failure: Some(
                    FailureDetail::new(
                        FailurePhase::Execution,
                        FailureCode::CommandWaitFailed,
                        None,
                        None,
                        None,
                        None,
                    )
                    .unwrap(),
                ),
                blocked_code: None,
            },
        ];

        assert_eq!(
            serialize(FinalizationContext {
                run_id: "run-1",
                attempt_id: "attempt-1",
                trigger: FinalizationTrigger::Succeeded,
                primary_issue_step_id: None,
                cancellation_reason: None,
                ordinary_issues: &issues,
            })
            .as_ref(),
            br#"{"schemaVersion":2,"runId":"run-1","attemptId":"attempt-1","trigger":"succeeded","primaryIssueStepId":null,"cancellationReason":null,"ordinaryIssues":[{"stepId":"lint","failurePolicy":"advisory","disposition":"failed","failure":{"phase":"execution","code":"command_wait_failed","exitCode":null,"input":null,"output":null,"diagnostic":null},"blockedCode":null},{"stepId":"zeta","failurePolicy":"required","disposition":"blocked","failure":null,"blockedCode":"prerequisites_unsatisfied"}]}"#
        );
    }
}
