//! A run a runner read, drawn by the run interface's step graph and inspector: the step
//! answers the shared widgets from the words it was given, which the execution crate reads
//! here, and a word this release does not know is drawn as received
//! (REQ-RUNNERENROLL-135, REQ-RUNNERENROLL-146).

use std::borrow::Cow;

use super::*;
use crate::workflow::run_view_model::WorkflowRunElapsed;

/// The role word of a finalizer.
const FINALIZER_ROLE: &str = "finalizer";

/// Every step state the engine has, each read against a state word through its `as_str`,
/// which spells what `state.json` holds.
const STEP_STATE_KINDS: [StepStateKind; 13] = [
    StepStateKind::Pending,
    StepStateKind::Starting,
    StepStateKind::Running,
    StepStateKind::CapturingOutputs,
    StepStateKind::Recovering,
    StepStateKind::Cancelling,
    StepStateKind::Succeeded,
    StepStateKind::Inherited,
    StepStateKind::Failed,
    StepStateKind::Blocked,
    StepStateKind::Skipped,
    StepStateKind::NotRun,
    StepStateKind::Cancelled,
];

/// The step state a state word spells, or `None` for a word this release does not know.
pub(super) fn step_state_kind(word: &str) -> Option<StepStateKind> {
    STEP_STATE_KINDS
        .into_iter()
        .find(|kind| kind.as_str() == word)
}

/// A run's finalization phase starts at its first finalizer; the answer names no trigger.
pub(super) fn phase_boundary(steps: &[RunnerWatchRunStep]) -> Option<StepPhaseBoundary> {
    steps
        .iter()
        .position(|step| step.role == FINALIZER_ROLE)
        .map(|finalization_start| StepPhaseBoundary {
            finalization_start,
            trigger: None,
        })
}

impl StepProjection for RunnerWatchRunStep {
    fn header(&self) -> StepHeader<'_> {
        let state = step_state_kind(&self.state);
        let timing = self
            .started_at
            .zip(self.duration)
            .map(|(started_at, duration)| WorkflowRunElapsed {
                started_at,
                duration,
                frozen: !state.is_some_and(step_state_is_active),
            });
        let work = match &self.work {
            RunnerWatchRunWork::Command { cwd, .. } => StepWork::Command {
                cwd: cwd.as_deref(),
            },
            RunnerWatchRunWork::Agent {
                profile,
                harness,
                model,
                effort,
            } => StepWork::Agent {
                profile,
                harness: StepHarness::Words {
                    harness,
                    model: model.as_deref(),
                    effort: effort.as_deref(),
                },
            },
            RunnerWatchRunWork::Workflow { file } => StepWork::Workflow { file },
            RunnerWatchRunWork::Unknown => StepWork::Unknown,
        };
        StepHeader {
            id: &self.id,
            kind: &self.kind,
            failure_policy: &self.failure_policy,
            direct_dependencies: &self.direct_dependencies,
            work,
            state: state.unwrap_or(StepStateKind::Pending),
            timing,
        }
    }

    fn unknown_state(&self) -> Option<&str> {
        step_state_kind(&self.state)
            .is_none()
            .then_some(self.state.as_str())
    }

    fn dag_detail(&self) -> Option<String> {
        if step_state_kind(&self.state) == Some(StepStateKind::Succeeded) {
            return Some(crate::workflow::render_style::success_detail(
                matches!(self.work, RunnerWatchRunWork::Command { .. }),
                self.outputs
                    .iter()
                    .filter(|output| output.committed)
                    .count(),
            ));
        }
        self.child_run_summary()
    }

    fn inspector_command(&self) -> Option<String> {
        let RunnerWatchRunWork::Command { argv, .. } = &self.work else {
            return None;
        };
        let command = argv
            .iter()
            .map(|argument| shell_quote(argument))
            .collect::<Vec<_>>()
            .join(" ");
        Some(if self.role == FINALIZER_ROLE {
            format!("finalizer · {command}")
        } else {
            command
        })
    }

    fn inspector_fact(&self) -> Option<InspectorField> {
        self.child_run_summary()
            .map(|summary| InspectorField::new("child runs", summary, Tone::Neutral))
    }

    fn inspector_outputs(&self) -> Vec<InspectorOutput> {
        let state = step_state_kind(&self.state);
        self.outputs
            .iter()
            .map(|output| {
                let (disposition, tone, marker, marker_tone) =
                    output_disposition(run_output_disposition(output.committed, state));
                InspectorOutput {
                    marker,
                    marker_tone,
                    name: visible_text(&output.name),
                    kind: Cow::Owned(visible_text(&output.kind)),
                    detail: "—".to_owned(),
                    disposition: Some(disposition),
                    tone,
                }
            })
            .collect()
    }

    fn show_empty_outputs(&self) -> bool {
        false
    }
}

impl RunnerWatchRunStep {
    /// A `workflow` step's child runs: their count and the newest one's state and outcome.
    fn child_run_summary(&self) -> Option<String> {
        let newest = self.child_runs.last()?;
        let count = u64::try_from(self.child_runs.len())
            .unwrap_or(u64::MAX)
            .saturating_add(self.child_runs_omitted);
        let mut summary = if count == 1 {
            "1 child run".to_owned()
        } else {
            format!("{count} child runs")
        };
        summary.push_str(" · ");
        summary.push_str(&newest.state);
        if let Some(outcome) = &newest.outcome {
            summary.push_str(" · ");
            summary.push_str(outcome);
        }
        Some(summary)
    }
}

/// An output as the run interface reads it: committed is captured, and otherwise the
/// step's state says why it is not, or that it may yet be.
fn run_output_disposition(
    committed: bool,
    state: Option<StepStateKind>,
) -> WorkflowRunOutputDisposition {
    if committed {
        return WorkflowRunOutputDisposition::Committed;
    }
    let reason = match state {
        Some(StepStateKind::Failed) => WorkflowRunOutputUnavailableReason::Failed,
        Some(StepStateKind::Blocked) => WorkflowRunOutputUnavailableReason::Blocked,
        Some(StepStateKind::Skipped) => WorkflowRunOutputUnavailableReason::Skipped,
        Some(StepStateKind::NotRun) => WorkflowRunOutputUnavailableReason::NotRun,
        Some(StepStateKind::Cancelled) => WorkflowRunOutputUnavailableReason::Cancelled,
        Some(StepStateKind::Succeeded | StepStateKind::Inherited) => {
            WorkflowRunOutputUnavailableReason::NotCommitted
        }
        Some(
            StepStateKind::Pending
            | StepStateKind::Starting
            | StepStateKind::Running
            | StepStateKind::CapturingOutputs
            | StepStateKind::Recovering
            | StepStateKind::Cancelling,
        )
        | None => return WorkflowRunOutputDisposition::Pending,
    };
    WorkflowRunOutputDisposition::Unavailable(reason)
}
