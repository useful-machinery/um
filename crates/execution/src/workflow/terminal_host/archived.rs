use crate::workflow::publication::{WorkflowOutcomeV1, WorkflowStepStateV1};
use std::io;

use super::*;
mod interaction;
#[cfg(test)]
mod tests;
mod widgets;
use self::interaction::ArchivedHostInteraction;
use self::interaction::*;
use self::widgets::*;
use crate::workflow::archived_attempt::{
    ArchivedDiagnosticStream, ArchivedStep, ArchivedStepDetail, LocalArchivedAttempt,
};
use crate::workflow::archived_presentation::{
    archived_cancellation_reason, archived_failure_detail, archived_finalization_trigger,
    blocked_detail, condition_false_detail, safe_path, safe_text,
};
use crate::workflow::evidence::{NodeDetail, PrimaryIssueDetail};
use crate::workflow::presentation_feed::{
    NormalizedRetainedRecord, normalize_retained_prefix, normalize_terminal_shell_argument,
};

const ARCHIVED_WORKFLOW_COLUMN_PERCENTAGE: u16 = 52;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArchivedTerminalHostExit {
    Quit,
    Interrupted,
    Terminated,
}

#[derive(Clone)]
pub struct ArchivedTerminalExitRequest {
    exit: tokio::sync::mpsc::UnboundedSender<ArchivedTerminalHostExit>,
}

impl ArchivedTerminalExitRequest {
    pub fn request(&self, exit: ArchivedTerminalHostExit) {
        let _ = self.exit.send(exit);
    }
}

pub struct ArchivedWorkflowTerminalHost {
    exit: tokio::sync::mpsc::UnboundedSender<ArchivedTerminalHostExit>,
    task: Option<tokio::task::JoinHandle<Result<ArchivedTerminalHostExit, PresentationFailure>>>,
}

impl ArchivedWorkflowTerminalHost {
    pub fn start(attempt: LocalArchivedAttempt, color: bool) -> Result<Self, PresentationFailure> {
        Self::start_with_boundary(attempt, color, SystemTerminalBoundary::new())
    }

    fn start_with_boundary<Boundary>(
        attempt: LocalArchivedAttempt,
        color: bool,
        boundary: Boundary,
    ) -> Result<Self, PresentationFailure>
    where
        Boundary: ArchivedTerminalBoundary,
    {
        let view = ArchivedTerminalView::new(attempt);
        let mut terminal = RestoringTerminal::new(boundary);
        let setup = std::panic::catch_unwind(AssertUnwindSafe(|| {
            let area = terminal.boundary.setup().map_err(|error| {
                presentation_failure(PresentationFailureOperation::TerminalSetup, &error)
            })?;
            let mut interaction = ArchivedHostInteraction {
                terminal_area: area,
                ..ArchivedHostInteraction::default()
            };
            terminal
                .boundary
                .draw_archived(&view, &mut interaction, color)
                .map_err(|error| {
                    presentation_failure(PresentationFailureOperation::TerminalDraw, &error)
                })?;
            Ok::<_, PresentationFailure>(interaction)
        }));
        let interaction = match setup {
            Ok(Ok(interaction)) => interaction,
            Ok(Err(failure)) => {
                let _ = terminal.restore();
                return Err(failure);
            }
            Err(payload) => {
                let _ = terminal.restore();
                return Err(report_terminal_panic(payload));
            }
        };
        let _ = terminal
            .boundary
            .notify_lifecycle(TerminalLifecycleEvent::QuitEligible);

        let (exit, exit_receiver) = tokio::sync::mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            match AssertUnwindSafe(run_archived_terminal(
                terminal,
                view,
                color,
                exit_receiver,
                interaction,
            ))
            .catch_unwind()
            .await
            {
                Ok(result) => result,
                // Unwinding dropped the restoring terminal before reporting the panic.
                Err(payload) => Err(report_terminal_panic(payload)),
            }
        });
        Ok(Self {
            exit,
            task: Some(task),
        })
    }

    pub fn exit_request(&self) -> ArchivedTerminalExitRequest {
        ArchivedTerminalExitRequest {
            exit: self.exit.clone(),
        }
    }

    pub async fn wait(mut self) -> Result<ArchivedTerminalHostExit, PresentationFailure> {
        archived_join_result(self.take_task()?.await)
    }

    fn take_task(
        &mut self,
    ) -> Result<
        tokio::task::JoinHandle<Result<ArchivedTerminalHostExit, PresentationFailure>>,
        PresentationFailure,
    > {
        self.task.take().ok_or_else(|| {
            PresentationFailure::operation(PresentationFailureOperation::TerminalTask)
        })
    }
}

impl Drop for ArchivedWorkflowTerminalHost {
    fn drop(&mut self) {
        let _ = self.exit.send(ArchivedTerminalHostExit::Terminated);
    }
}

fn archived_join_result(
    result: Result<Result<ArchivedTerminalHostExit, PresentationFailure>, tokio::task::JoinError>,
) -> Result<ArchivedTerminalHostExit, PresentationFailure> {
    match result {
        Ok(result) => result,
        Err(error) if error.is_panic() => Err(report_terminal_panic(error.into_panic())),
        Err(_) => Err(PresentationFailure::operation(
            PresentationFailureOperation::TerminalTask,
        )),
    }
}

trait ArchivedTerminalBoundary: TerminalBoundary {
    fn draw_archived(
        &mut self,
        view: &ArchivedTerminalView,
        interaction: &mut ArchivedHostInteraction,
        color: bool,
    ) -> io::Result<()>;
}

impl ArchivedTerminalBoundary for SystemTerminalBoundary {
    fn draw_archived(
        &mut self,
        view: &ArchivedTerminalView,
        interaction: &mut ArchivedHostInteraction,
        color: bool,
    ) -> io::Result<()> {
        self.surface_mut()?.draw_archived(view, interaction, color)
    }
}

impl TerminalSurface {
    fn draw_archived(
        &mut self,
        view: &ArchivedTerminalView,
        interaction: &mut ArchivedHostInteraction,
        color: bool,
    ) -> io::Result<()> {
        clamp_step_selection(&mut interaction.selected, view.steps.len());
        let graph = self
            .graph
            .get_or_insert_with(|| DagLayout::for_steps(&view.steps));
        self.terminal
            .draw(|frame| render_archived(frame, view, graph, interaction, color))?;
        Ok(())
    }
}

async fn run_archived_terminal<Boundary: ArchivedTerminalBoundary>(
    mut terminal: RestoringTerminal<Boundary>,
    view: ArchivedTerminalView,
    color: bool,
    mut requested_exit: tokio::sync::mpsc::UnboundedReceiver<ArchivedTerminalHostExit>,
    mut interaction: ArchivedHostInteraction,
) -> Result<ArchivedTerminalHostExit, PresentationFailure> {
    loop {
        tokio::select! {
            biased;
            exit = requested_exit.recv() => {
                return restore_archived_terminal(
                    &mut terminal,
                    exit.unwrap_or(ArchivedTerminalHostExit::Terminated),
                );
            }
            event = terminal.boundary.next_event() => {
                let event = match event {
                    Ok(event) => event,
                    Err(error) => {
                        return fail_archived_terminal(
                            &mut terminal,
                            PresentationFailureOperation::TerminalInput,
                            &error,
                        );
                    }
                };
                if event == TerminalInputEvent::Resize {
                    match terminal.boundary.resize() {
                        Ok(area) => interaction.terminal_area = area,
                        Err(error) => {
                            return fail_archived_terminal(
                                &mut terminal,
                                PresentationFailureOperation::TerminalDraw,
                                &error,
                            );
                        }
                    }
                } else if let Some(exit) = interaction.handle_key(event, &view) {
                    return restore_archived_terminal(&mut terminal, exit);
                }
                if event == TerminalInputEvent::Help && interaction.help_visible {
                    let _ = terminal
                        .boundary
                        .notify_lifecycle(TerminalLifecycleEvent::HelpOpened);
                }
                if let Err(error) = terminal
                    .boundary
                    .draw_archived(&view, &mut interaction, color)
                {
                    return fail_archived_terminal(
                        &mut terminal,
                        PresentationFailureOperation::TerminalDraw,
                        &error,
                    );
                }
            }
        }
    }
}

fn restore_archived_terminal<Boundary: TerminalBoundary>(
    terminal: &mut RestoringTerminal<Boundary>,
    exit: ArchivedTerminalHostExit,
) -> Result<ArchivedTerminalHostExit, PresentationFailure> {
    terminal.restore().map_or_else(
        |error| {
            Err(presentation_failure(
                PresentationFailureOperation::TerminalRestore,
                &error,
            ))
        },
        |()| Ok(exit),
    )
}

fn fail_archived_terminal<Boundary: TerminalBoundary>(
    terminal: &mut RestoringTerminal<Boundary>,
    operation: PresentationFailureOperation,
    error: &io::Error,
) -> Result<ArchivedTerminalHostExit, PresentationFailure> {
    let failure = presentation_failure(operation, error);
    let _ = terminal.restore();
    Err(failure)
}

struct ArchivedTerminalView {
    summary: Vec<ArchivedSummaryLine>,
    steps: Vec<ArchivedTerminalStepView>,
    phase_boundary: Option<StepPhaseBoundary>,
}

struct ArchivedSummaryLine {
    text: String,
    tone: Tone,
}

struct ArchivedTerminalStepView {
    id: String,
    role: crate::workflow::validated::WorkflowNodeRole,
    definition: WorkflowPresentationStep,
    command: Option<String>,
    state: StepStateKind,
    timing: Option<super::super::run_view_model::WorkflowRunElapsed>,
    detail: ArchivedStepDetail,
    output: ArchivedCommandOutputView,
    document: Vec<ArchivedOutputRow>,
    maximum_document_width: usize,
    recovery: Option<super::super::publication::StepRecoverySummaryV1>,
    invocations: Vec<super::super::publication::RecoveryInvocationV1>,
}

#[derive(Clone)]
enum ArchivedCommandOutputView {
    Missing,
    Present {
        stdout: ArchivedStreamView,
        stderr: ArchivedStreamView,
    },
}

#[derive(Clone)]
struct ArchivedStreamView {
    source: ArchivedStreamSource,
    records: Vec<NormalizedRetainedRecord>,
    unterminated: bool,
    retained_bytes: u64,
    discarded_bytes: u64,
    truncated: bool,
    fully_drained: bool,
}

#[derive(Clone, Copy)]
enum ArchivedStreamSource {
    StandardOutput,
    StandardError,
}

impl ArchivedTerminalView {
    fn new(attempt: LocalArchivedAttempt) -> Self {
        let summary = archived_summary(&attempt);
        let phase_boundary = attempt
            .workflow
            .finalization_start
            .zip(attempt.finalization.as_ref())
            .map(|(finalization_start, finalization)| StepPhaseBoundary {
                finalization_start,
                trigger: Some(archived_finalization_trigger(finalization.trigger)),
            });
        let mut definitions = attempt.workflow.steps;
        let steps = attempt
            .steps
            .into_iter()
            .filter_map(|step| {
                let definition = definitions.remove(&step.id)?;
                Some(ArchivedTerminalStepView::new(step, definition))
            })
            .collect::<Vec<_>>();
        Self {
            summary,
            steps,
            phase_boundary,
        }
    }
}

impl ArchivedTerminalStepView {
    fn new(step: ArchivedStep, definition: WorkflowPresentationStep) -> Self {
        let command = archived_command(&definition);
        let timing = step
            .started_at
            .zip(step.duration)
            .map(
                |(started_at, duration)| super::super::run_view_model::WorkflowRunElapsed {
                    started_at,
                    duration,
                    frozen: true,
                },
            );
        let output =
            step.command_output
                .as_ref()
                .map_or(ArchivedCommandOutputView::Missing, |output| {
                    ArchivedCommandOutputView::Present {
                        stdout: ArchivedStreamView::new(
                            ArchivedStreamSource::StandardOutput,
                            &output.stdout,
                        ),
                        stderr: ArchivedStreamView::new(
                            ArchivedStreamSource::StandardError,
                            &output.stderr,
                        ),
                    }
                });
        let document = output_document(&output);
        let maximum_document_width = document
            .iter()
            .map(|row| display_width(&row.text))
            .max()
            .unwrap_or(0);
        Self {
            document,
            maximum_document_width,
            id: safe_text(&step.id),
            role: step.role,
            definition: safe_definition(definition),
            command,
            state: archived_step_state(step.state),
            timing,
            recovery: step.recovery,
            invocations: step.invocations,
            detail: step.detail,
            output,
        }
    }
}

impl ArchivedStreamView {
    fn new(source: ArchivedStreamSource, stream: &ArchivedDiagnosticStream) -> Self {
        let normalized = normalize_retained_prefix(&stream.bytes);
        Self {
            source,
            records: normalized.records,
            unterminated: normalized.unterminated,
            retained_bytes: stream.retained_bytes,
            discarded_bytes: stream.discarded_bytes,
            truncated: stream.truncated,
            fully_drained: stream.fully_drained,
        }
    }
}

impl StepProjection for ArchivedTerminalStepView {
    // These accessors deliberately keep the archived terminal projection separate from
    // the live observation-backed view model while sharing read-only renderer geometry.
    fn header(&self) -> StepHeader<'_> {
        StepHeader::new(&self.id, &self.definition, self.state, self.timing.as_ref())
    }

    fn dag_detail(&self) -> Option<String> {
        match &self.detail {
            ArchivedStepDetail::Succeeded => Some(self.with_recovery_detail(
                crate::workflow::render_style::success_detail(
                    matches!(self.definition, WorkflowPresentationStep::Command { .. }),
                    self.definition.outputs().len(),
                ),
            )),
            ArchivedStepDetail::Evidence(NodeDetail::Inherited(detail)) => {
                Some(crate::workflow::render_style::inherited_detail(detail))
            }
            ArchivedStepDetail::Evidence(NodeDetail::Failed(failure)) => {
                Some(self.with_recovery_detail(issue_detail_for_step(
                    archived_failure_detail(failure),
                    &self.definition,
                    self.state,
                )))
            }
            ArchivedStepDetail::Evidence(NodeDetail::Blocked(detail)) => Some(
                issue_detail_for_step(blocked_detail(detail), &self.definition, self.state),
            ),
            ArchivedStepDetail::Evidence(NodeDetail::Skipped(detail)) => {
                Some(condition_false_detail(detail))
            }
            ArchivedStepDetail::Evidence(NodeDetail::NotRun(detail)) => {
                Some(crate::workflow::presentation::snake_case_debug(detail.code))
            }
            ArchivedStepDetail::Evidence(NodeDetail::Cancellation(detail)) => Some(
                self.with_recovery_detail(crate::workflow::presentation::snake_case_debug(
                    detail.code,
                )),
            ),
        }
    }

    fn inspector_command(&self) -> Option<String> {
        self.command.clone().map(|command| {
            if self.role == crate::workflow::validated::WorkflowNodeRole::Finalizer {
                format!("finalizer · {command}")
            } else {
                command
            }
        })
    }

    fn inspector_fact(&self) -> Option<InspectorField> {
        if self.recovery.is_some() {
            return Some(InspectorField::new(
                "recovery",
                self.with_recovery_detail(String::new()),
                match self.state {
                    StepStateKind::Succeeded => Tone::Success,
                    StepStateKind::Failed => Tone::Failure,
                    StepStateKind::Cancelled => Tone::Blocked,
                    _ => Tone::Neutral,
                },
            ));
        }
        match &self.detail {
            ArchivedStepDetail::Succeeded => Some(InspectorField::new(
                "outputs",
                output_count_detail(self.definition.outputs().len()),
                Tone::Success,
            )),
            ArchivedStepDetail::Evidence(NodeDetail::Inherited(detail)) => {
                Some(InspectorField::new(
                    "inheritance",
                    crate::workflow::render_style::inherited_detail(detail),
                    Tone::Neutral,
                ))
            }
            ArchivedStepDetail::Evidence(NodeDetail::Failed(failure)) => Some(InspectorField::new(
                "failure",
                archived_failure_detail(failure),
                Tone::Failure,
            )),
            ArchivedStepDetail::Evidence(NodeDetail::Blocked(detail)) => Some(InspectorField::new(
                "prerequisites",
                blocked_detail(detail),
                Tone::Blocked,
            )),
            ArchivedStepDetail::Evidence(NodeDetail::Skipped(detail)) => Some(InspectorField::new(
                "condition",
                condition_false_detail(detail),
                Tone::Muted,
            )),
            ArchivedStepDetail::Evidence(NodeDetail::NotRun(detail)) => Some(InspectorField::new(
                "not run",
                crate::workflow::presentation::snake_case_debug(detail.code),
                Tone::Muted,
            )),
            ArchivedStepDetail::Evidence(NodeDetail::Cancellation(detail)) => {
                Some(InspectorField::new(
                    "cancellation",
                    crate::workflow::presentation::snake_case_debug(detail.code),
                    Tone::Blocked,
                ))
            }
        }
    }

    fn inspector_outputs(&self) -> Vec<InspectorOutput> {
        self.definition
            .outputs()
            .iter()
            .map(|(name, output)| {
                let (kind, detail) = archived_output_description(output);
                InspectorOutput::declaration(safe_text(name), kind, detail)
            })
            .collect()
    }

    fn show_empty_outputs(&self) -> bool {
        false
    }
}

impl ArchivedTerminalStepView {
    fn with_recovery_detail(&self, base: String) -> String {
        let Some(recovery) = &self.recovery else {
            return base;
        };
        let terminal_failure = match &self.detail {
            ArchivedStepDetail::Evidence(NodeDetail::Failed(failure)) => {
                Some(archived_failure_detail(failure))
            }
            _ => None,
        };
        let termination = super::super::presentation::terminal_recovery_detail(
            recovery,
            terminal_failure.as_deref(),
        );
        let usage = super::super::publication::total_recovery_usage(&self.invocations);
        let recovery_detail = format!(
            "{termination} · {} invocations · usage input {} output {}",
            self.invocations.len(),
            usage.input_tokens,
            usage.output_tokens
        );
        if base.is_empty() {
            recovery_detail
        } else {
            format!("{base} · {recovery_detail}")
        }
    }
}

fn archived_summary(attempt: &LocalArchivedAttempt) -> Vec<ArchivedSummaryLine> {
    let selection = if attempt.attempt_number == attempt.current_attempt_number {
        "current at snapshot"
    } else {
        "historical"
    };
    let trigger = match attempt.trigger {
        crate::workflow::local_run::AttemptTriggerV1::Initial => "initial",
        crate::workflow::local_run::AttemptTriggerV1::ExplicitRetry => "explicit retry",
        crate::workflow::local_run::AttemptTriggerV1::Continuation => "continuation",
    };
    let attempt_state = match attempt.state {
        crate::workflow::local_run::AttemptStateV1::Succeeded => "succeeded",
        crate::workflow::local_run::AttemptStateV1::WorkflowFailed => "workflow_failed",
        crate::workflow::local_run::AttemptStateV1::Cancelled => "cancelled",
        crate::workflow::local_run::AttemptStateV1::Created => "created",
        crate::workflow::local_run::AttemptStateV1::Running => "running",
        crate::workflow::local_run::AttemptStateV1::Cancelling => "cancelling",
        crate::workflow::local_run::AttemptStateV1::Interrupted => "interrupted",
        crate::workflow::local_run::AttemptStateV1::Rejected => "rejected",
    };
    let (outcome, outcome_tone) = archived_outcome_status(attempt.outcome);
    let mut lines = vec![
        ArchivedSummaryLine {
            text: format!("run {}", safe_path(&attempt.run_directory)),
            tone: Tone::Primary,
        },
        ArchivedSummaryLine {
            text: format!(
                "attempt {} of {} · {selection} · {trigger}",
                attempt.attempt_number, attempt.current_attempt_number
            ),
            tone: Tone::Neutral,
        },
        ArchivedSummaryLine {
            text: format!(
                "workflow {} · {} {} · concurrency {}",
                safe_text(&attempt.workflow_path),
                attempt.steps.len(),
                if attempt.steps.len() == 1 {
                    "step"
                } else {
                    "steps"
                },
                attempt.execution.maximum_parallel_steps,
            ),
            tone: Tone::Neutral,
        },
        ArchivedSummaryLine {
            text: format!(
                "attempt state {attempt_state} · outcome {outcome} · result {}",
                safe_path(&attempt.result_directory)
            ),
            tone: outcome_tone,
        },
        ArchivedSummaryLine {
            text: format!(
                "created {} · started {} · settled {}",
                header_timestamp(attempt.created_at),
                attempt
                    .started_at
                    .map_or_else(|| "—".to_owned(), header_timestamp),
                header_timestamp(attempt.settled_at),
            ),
            tone: Tone::Muted,
        },
        ArchivedSummaryLine {
            text: format!(
                "execution {} → {} · {}",
                header_timestamp(attempt.execution.started_at),
                header_timestamp(attempt.execution.finished_at),
                human_duration(attempt.execution.duration),
            ),
            tone: Tone::Muted,
        },
    ];
    let modified = match attempt.workspace_modified {
        crate::workflow::publication::WorkspaceModifiedV1::Known(true) => "true",
        crate::workflow::publication::WorkspaceModifiedV1::Known(false) => "false",
        crate::workflow::publication::WorkspaceModifiedV1::Unknown(_) => "unknown",
    };
    lines.push(ArchivedSummaryLine {
        text: format!("workspace modified {modified}"),
        tone: Tone::Neutral,
    });
    if let Some(continuation) = &attempt.continuation {
        lines.push(ArchivedSummaryLine {
            text: format!(
                "continuation definition {:?}",
                continuation.definition_source
            ),
            tone: Tone::Neutral,
        });
    }
    if let Some(primary) = &attempt.primary_issue {
        let detail = match &primary.detail {
            PrimaryIssueDetail::Failed(detail) => archived_failure_detail(detail),
            PrimaryIssueDetail::Blocked(detail) => blocked_detail(detail),
        };
        lines.push(ArchivedSummaryLine {
            text: format!(
                "primary issue {} {} · {:?} · {}",
                match primary.node.role {
                    crate::workflow::validated::WorkflowNodeRole::Step => "step",
                    crate::workflow::validated::WorkflowNodeRole::Finalizer => "finalizer",
                },
                safe_text(&primary.node.id),
                primary.state,
                detail,
            ),
            tone: Tone::Failure,
        });
    }
    if let Some(cancellation) = &attempt.cancellation {
        lines.push(ArchivedSummaryLine {
            text: format!(
                "cancellation {} · requested {} · force-stop {}",
                archived_cancellation_reason(cancellation.reason),
                header_timestamp(cancellation.requested_at),
                header_timestamp(cancellation.force_stop_deadline),
            ),
            tone: Tone::Blocked,
        });
    }
    if let Some(force_abort) = attempt.force_abort {
        let phase = match force_abort.phase {
            crate::workflow::publication::ForceAbortPhaseV1::Ordinary => "ordinary",
            crate::workflow::publication::ForceAbortPhaseV1::Finalization => "finalization",
        };
        lines.push(ArchivedSummaryLine {
            text: format!(
                "force abort {} · phase {phase}",
                archived_cancellation_reason(force_abort.reason),
            ),
            tone: Tone::Failure,
        });
    }
    if let Some(finalization) = &attempt.finalization {
        let trigger = match finalization.trigger {
            crate::workflow::publication::FinalizationTriggerV1::Succeeded => "succeeded",
            crate::workflow::publication::FinalizationTriggerV1::Failed => "failed",
            crate::workflow::publication::FinalizationTriggerV1::Cancelled => "cancelled",
        };
        let cleanup = if finalization.force_abort {
            "incomplete · force abort accepted".to_owned()
        } else if let Some(cancellation) = &finalization.cancellation {
            format!(
                "incomplete · cancelled {}",
                archived_cancellation_reason(cancellation.reason)
            )
        } else {
            "complete".to_owned()
        };
        lines.push(ArchivedSummaryLine {
            text: format!(
                "finalization trigger {trigger} · {} issues · cleanup {cleanup}",
                finalization.issues.len()
            ),
            tone: if finalization.force_abort || finalization.cancellation.is_some() {
                Tone::Blocked
            } else if finalization.issues.is_empty() {
                Tone::Success
            } else {
                Tone::Failure
            },
        });
    }
    lines
}

fn archived_outcome_status(outcome: WorkflowOutcomeV1) -> (&'static str, Tone) {
    match outcome {
        WorkflowOutcomeV1::Succeeded => ("succeeded", Tone::Success),
        WorkflowOutcomeV1::Failed => ("failed", Tone::Failure),
        WorkflowOutcomeV1::Cancelled => ("cancelled", Tone::Blocked),
    }
}

fn archived_step_state(state: WorkflowStepStateV1) -> StepStateKind {
    match state {
        WorkflowStepStateV1::Succeeded => StepStateKind::Succeeded,
        WorkflowStepStateV1::Inherited => StepStateKind::Inherited,
        WorkflowStepStateV1::Failed => StepStateKind::Failed,
        WorkflowStepStateV1::Blocked => StepStateKind::Blocked,
        WorkflowStepStateV1::Skipped => StepStateKind::Skipped,
        WorkflowStepStateV1::NotRun => StepStateKind::NotRun,
        WorkflowStepStateV1::Cancelled => StepStateKind::Cancelled,
    }
}

fn archived_output_description(output: &WorkflowOutput) -> (&'static str, String) {
    (super::semantic_output_kind(output), "—".to_owned())
}

fn archived_command(definition: &WorkflowPresentationStep) -> Option<String> {
    let WorkflowPresentationStep::Command { argv, .. } = definition else {
        return None;
    };
    Some(
        argv.iter()
            .map(|argument| {
                shell_quote_visible_argument(&normalize_terminal_shell_argument(
                    argument.as_bytes(),
                ))
            })
            .collect::<Vec<_>>()
            .join(" "),
    )
}

fn safe_definition(mut definition: WorkflowPresentationStep) -> WorkflowPresentationStep {
    match &mut definition {
        WorkflowPresentationStep::Command {
            argv,
            cwd,
            direct_dependencies,
            outputs,
            ..
        } => {
            for argument in argv {
                *argument = safe_text(argument);
            }
            if let Some(cwd) = cwd {
                *cwd = safe_text(cwd);
            }
            for dependency in direct_dependencies {
                *dependency = safe_text(dependency);
            }
            normalize_outputs(outputs);
        }
        WorkflowPresentationStep::Agent {
            profile,
            harness,
            direct_dependencies,
            outputs,
            ..
        } => {
            *profile = safe_text(profile);
            match harness {
                AgentPresentationHarness::Pi { model, .. }
                | AgentPresentationHarness::ClaudeCode { model, .. } => {
                    *model = safe_text(model);
                }
                AgentPresentationHarness::Codex { model, effort } => {
                    *model = safe_text(model);
                    *effort = safe_text(effort);
                }
            }
            for dependency in direct_dependencies {
                *dependency = safe_text(dependency);
            }
            normalize_outputs(outputs);
        }
    }
    definition
}

fn normalize_outputs(outputs: &mut std::collections::BTreeMap<String, WorkflowOutput>) {
    for output in outputs.values_mut() {
        match output {
            WorkflowOutput::TextAgentResponse | WorkflowOutput::GitBranchWorkspace => {}
            WorkflowOutput::TextPath { path } => *path = safe_text(path),
            WorkflowOutput::JsonPath { path, schema } => {
                *path = safe_text(path);
                *schema = safe_text(schema);
            }
            WorkflowOutput::JsonAgentResult { schema } => *schema = safe_text(schema),
            WorkflowOutput::FilePath {
                path, media_type, ..
            } => {
                *path = safe_text(path);
                *media_type = safe_text(media_type);
            }
        }
    }
}
