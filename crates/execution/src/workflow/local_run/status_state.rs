use std::io::{self, Write};
use std::time::Duration;

use serde::Serialize;
use serde_json::Value;
use time::OffsetDateTime;

use super::super::archived_presentation::safe_text;
use super::super::evidence::{FailureDetail, NodeDetail};
use super::super::publication::{
    RecoveryHandlerOutcomeV1, RecoveryInvocationRoleV1, RecoveryTerminationV1, WorkflowResultV1,
    WorkspaceModifiedV1,
};
use super::{AttemptFinalizationV1, AttemptStepV1, DurableStepRecoveryV1, LocalRunStateV1};

// Keep the durable state typed at the status boundary. The separately serialized
// state document in LocalRunStatusSnapshot is only used for the JSON response.
#[derive(Clone, Debug, PartialEq)]
pub struct LocalStatusStateView {
    pub(super) state: LocalRunStateV1,
}

/// A recorded start time and the time elapsed since it at a caller's `now`, zero for a start
/// recorded after `now` (REQ-LOCALRUN-189).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalStatusStart {
    pub at: String,
    pub elapsed: Duration,
}

/// The current attempt's start and its in-flight nodes (REQ-LOCALRUN-189). A start the state
/// does not record, as in a state an earlier `um` wrote (REQ-LOCALRUN-201), is `None`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalStatusProgress {
    pub started: Option<LocalStatusStart>,
    pub nodes: Vec<LocalStatusNode>,
}

/// A step or finalizer of the current attempt that is `starting`, `running`,
/// `capturing_outputs` or `cancelling`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalStatusNode {
    pub id: String,
    pub role: String,
    pub state: String,
    pub started: Option<LocalStatusStart>,
}

impl LocalStatusStateView {
    /// Whether the current attempt is a successful repair started after the parent's wait.
    /// Compare parsed timestamps because canonical UTC permits varying fractional precision.
    pub fn succeeded_after(&self, since: &str) -> io::Result<bool> {
        let attempt = self.current()?;
        let parse = super::super::schema_common::parse_canonical_utc_timestamp;
        Ok(attempt.state == super::AttemptStateV1::Succeeded
            && attempt
                .started_at
                .as_deref()
                .and_then(parse)
                .zip(parse(since))
                .is_some_and(|(started, waited)| started > waited))
    }

    /// The current attempt's start and its in-flight nodes, the steps first, then the
    /// finalizers while finalization is in progress, with the time since each start at `now`.
    pub fn in_flight(&self, now: OffsetDateTime) -> io::Result<LocalStatusProgress> {
        let attempt = self.current()?;
        let finalizers = match &attempt.finalization {
            Some(AttemptFinalizationV1::Progress(progress)) => progress.finalizers.as_slice(),
            _ => &[],
        };
        let nodes = attempt
            .progress
            .steps
            .iter()
            .chain(finalizers)
            .filter(|node| node.state.is_in_flight())
            .map(|node: &AttemptStepV1| {
                Ok(LocalStatusNode {
                    id: node.id.clone(),
                    role: label(node.role)?,
                    state: label(node.state)?,
                    started: start(node.started_at.as_deref(), now),
                })
            })
            .collect::<io::Result<_>>()?;
        Ok(LocalStatusProgress {
            started: start(attempt.started_at.as_deref(), now),
            nodes,
        })
    }

    fn current(&self) -> io::Result<&super::LocalAttemptV1> {
        self.state
            .attempts
            .last()
            .ok_or_else(|| io::Error::other("status has no current attempt"))
    }

    pub fn workspace_modified(&self) -> io::Result<Value> {
        let modified = self
            .current()?
            .continuation
            .as_ref()
            .map(|continuation| &continuation.workspace.modified);
        match modified {
            Some(modified) => serde_json::to_value(modified).map_err(io::Error::other),
            None => serde_json::to_value(WorkspaceModifiedV1::Unknown(
                super::super::publication::WorkspaceModifiedUnknownV1::Unknown,
            ))
            .map_err(io::Error::other),
        }
    }

    pub fn write_recovery(
        &self,
        writer: &mut impl Write,
        archived_result: Option<&WorkflowResultV1>,
    ) -> io::Result<()> {
        let progress = &self.current()?.progress;
        for step in &progress.steps {
            let Some(recovery) = &step.recovery else {
                continue;
            };
            let failure = archived_result
                .and_then(|result| result.steps.iter().find(|archived| archived.id == step.id))
                .and_then(|step| match step.detail.as_ref() {
                    Some(NodeDetail::Failed(failure)) => Some(failure),
                    _ => None,
                });
            let detail = recovery_detail(recovery, failure)?;
            writeln!(
                writer,
                "step recovery: {} · {detail} · rounds {}/{}",
                safe_text(&step.id),
                recovery.rounds.len(),
                recovery.configured_retries
            )?;
        }
        let accounting = &progress.accounting;
        if accounting.observed_invocations != 0 {
            writeln!(
                writer,
                "invocations: {} observed · {} settled · usage input {} output {}",
                accounting.observed_invocations,
                accounting.settled_invocations,
                accounting.input_tokens,
                accounting.output_tokens
            )?;
        }
        Ok(())
    }
}

/// `at` with the time since it at `now`; validation admits only a canonical UTC timestamp.
fn start(at: Option<&str>, now: OffsetDateTime) -> Option<LocalStatusStart> {
    let at = at?;
    let parsed = super::super::schema_common::parse_canonical_utc_timestamp(at)?;
    Some(LocalStatusStart {
        at: at.to_owned(),
        elapsed: Duration::try_from(now - parsed).unwrap_or(Duration::ZERO),
    })
}

fn label(value: impl Serialize) -> io::Result<String> {
    let value = serde_json::to_value(value).map_err(io::Error::other)?;
    value
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| io::Error::other("status state label is not a string"))
}

fn recovery_detail(
    recovery: &DurableStepRecoveryV1,
    archived: Option<&FailureDetail>,
) -> io::Result<String> {
    if let Some(active) = &recovery.active {
        let mut detail = match active.role {
            RecoveryInvocationRoleV1::Target => {
                let number = active
                    .target_execution
                    .ok_or_else(|| io::Error::other("active target has no execution number"))?;
                format!("target execution {number}")
            }
            RecoveryInvocationRoleV1::RecoveryHandler => {
                let round = active
                    .recovery_round
                    .ok_or_else(|| io::Error::other("active recovery handler has no round"))?;
                let kind = recovery
                    .handler_kind
                    .ok_or_else(|| io::Error::other("active recovery handler has no kind"))?;
                // Cancellation can leave an active handler invocation after its
                // starting/running state has been cleared. Keep that valid
                // durable snapshot readable, as the live view does.
                let state = active
                    .handler_state
                    .map(label)
                    .transpose()?
                    .unwrap_or_else(|| "active".to_owned());
                format!(
                    "recovery_handler {} {} · round {round}",
                    label(kind)?,
                    state
                )
            }
        };
        if let Some(decision) = active.decision {
            detail.push_str(&format!(" · decision {}", label(decision)?));
        }
        return Ok(detail);
    }
    let Some(termination) = &recovery.termination else {
        return Ok("incomplete".to_owned());
    };
    let mut detail = match termination {
        RecoveryTerminationV1::Recovered { execution_number } => format!(
            "recovered · target execution {execution_number} · output owner target execution {execution_number}"
        ),
        RecoveryTerminationV1::Exhausted { execution_number } => {
            format!("exhausted · target execution {execution_number} · no output owner")
        }
        RecoveryTerminationV1::GaveUp { round } => {
            format!("gave_up · round {round} · no output owner")
        }
        RecoveryTerminationV1::HandlerFailed { round, .. } => {
            format!("handler_failed · round {round} · no output owner")
        }
        RecoveryTerminationV1::Cancelled {
            round,
            active_role,
            execution_number,
        } => {
            let mut text = format!("cancelled · active role {}", label(*active_role)?);
            if let Some(number) = execution_number {
                text.push_str(&format!(" · target execution {number}"));
            }
            text.push_str(&format!(" · round {round} · no output owner"));
            text
        }
    };
    let latest = recovery.rounds.last();
    if let Some(handler) = latest.and_then(|round| round.handler.as_ref()) {
        let prefix = if matches!(
            handler.outcome,
            RecoveryHandlerOutcomeV1::Recheck | RecoveryHandlerOutcomeV1::GaveUp
        ) {
            " · decision "
        } else {
            " · handler outcome "
        };
        detail.push_str(prefix);
        detail.push_str(&label(handler.outcome)?);
    }
    if let Some(failure) = archived {
        detail.push_str(&format!(
            " · latest target failure {} · {}",
            label(failure.phase)?,
            label(failure.code)?
        ));
        if let Some(exit) = failure.exit_code {
            detail.push_str(&format!(" · exit {exit}"));
        }
    } else if let Some(failure) = latest.map(|round| &round.failed_execution.failure) {
        detail.push_str(&format!(
            " · latest target failure {} · {}",
            label(failure.phase)?,
            label(failure.cause.code)?
        ));
        if let Some(exit) = failure.cause.exit_code {
            detail.push_str(&format!(" · exit {exit}"));
        }
    }
    Ok(detail)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflow::local_run::{DurableRecoveryActiveV1, DurableRecoveryHandlerKindV1};

    #[test]
    fn active_recovery_handler_without_handler_state_remains_readable() {
        let recovery = DurableStepRecoveryV1 {
            schema_version: 1,
            configured_retries: 1,
            handler_kind: Some(DurableRecoveryHandlerKindV1::Agent),
            rounds: Vec::new(),
            active: Some(DurableRecoveryActiveV1 {
                role: RecoveryInvocationRoleV1::RecoveryHandler,
                target_execution: None,
                recovery_round: Some(1),
                invocation_id: 1,
                handler_state: None,
                decision: Some(RecoveryHandlerOutcomeV1::Recheck),
            }),
            termination: None,
        };

        assert!(recovery_detail(&recovery, None).is_ok());
    }
}
