use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum LifecycleControl {
    Cancel,
    Quit,
    None,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum FinalizationSignalAction {
    Graceful,
    ForceAbort,
    Inert,
}

pub(super) fn finalization_signal_action(
    snapshot: &WorkflowRunViewSnapshot,
) -> FinalizationSignalAction {
    match &snapshot.workflow {
        WorkflowState::Executing {
            gate: SchedulingGate::Open | SchedulingGate::FailureStopped { .. },
        }
        | WorkflowState::Finalizing {
            gate: crate::workflow::runtime::FinalizationGate::Open,
            ..
        } => FinalizationSignalAction::Graceful,
        WorkflowState::Finalizing {
            gate:
                crate::workflow::runtime::FinalizationGate::Cancelling {
                    force_abort: false, ..
                },
            ..
        } => FinalizationSignalAction::ForceAbort,
        WorkflowState::Executing {
            gate: SchedulingGate::Cancelling { .. },
        }
        | WorkflowState::Finalizing {
            gate:
                crate::workflow::runtime::FinalizationGate::Cancelling {
                    force_abort: true, ..
                },
            ..
        }
        | WorkflowState::Succeeded
        | WorkflowState::Failed { .. }
        | WorkflowState::Cancelled { .. } => FinalizationSignalAction::Inert,
    }
}

pub(super) fn cancellation_available(snapshot: &WorkflowRunViewSnapshot) -> bool {
    finalization_signal_action(snapshot) != FinalizationSignalAction::Inert
}

pub(super) fn lifecycle_control(snapshot: &WorkflowRunViewSnapshot) -> LifecycleControl {
    if snapshot.quit_eligible {
        LifecycleControl::Quit
    } else if cancellation_available(snapshot) {
        LifecycleControl::Cancel
    } else {
        LifecycleControl::None
    }
}

#[derive(Default)]
pub(super) struct StepCounts {
    pub(super) pending: usize,
    pub(super) active: usize,
    pub(super) succeeded: usize,
    pub(super) failed: usize,
    pub(super) blocked: usize,
    pub(super) skipped: usize,
    pub(super) not_run: usize,
    pub(super) cancelled: usize,
}

pub(super) fn step_counts(snapshot: &WorkflowRunViewSnapshot) -> StepCounts {
    let mut counts = StepCounts::default();
    for step in &snapshot.steps {
        match step.state {
            StepStateKind::Pending => counts.pending += 1,
            StepStateKind::Starting
            | StepStateKind::Running
            | StepStateKind::CapturingOutputs
            | StepStateKind::Recovering
            | StepStateKind::Cancelling => counts.active += 1,
            StepStateKind::Succeeded | StepStateKind::Inherited => counts.succeeded += 1,
            StepStateKind::Failed => counts.failed += 1,
            StepStateKind::Blocked => counts.blocked += 1,
            StepStateKind::Skipped => counts.skipped += 1,
            StepStateKind::NotRun => counts.not_run += 1,
            StepStateKind::Cancelled => counts.cancelled += 1,
        }
    }
    counts
}

pub(super) fn step_count_summary(counts: &StepCounts, total: usize) -> String {
    let step_label = if total == 1 { "step" } else { "steps" };
    let mut parts = vec![format!("{total} {step_label}")];
    for (count, label) in [
        (counts.succeeded, "ok"),
        (counts.active, "running"),
        (counts.failed, "failed"),
        (counts.blocked, "blocked"),
        (counts.skipped, "skipped"),
        (counts.pending, "pending"),
        (counts.not_run, "not-run"),
        (counts.cancelled, "cancelled"),
    ] {
        if count != 0 {
            parts.push(format!("{count} {label}"));
        }
    }
    parts.join(" · ")
}

pub(super) fn workflow_header_status(snapshot: &WorkflowRunViewSnapshot) -> (&'static str, Tone) {
    match (&snapshot.publication, snapshot.cleanup) {
        (WorkflowRunPublicationState::Publishing, _) => ("publishing", Tone::Active),
        (WorkflowRunPublicationState::Completed(WorkflowRunPublicationResult::Failed(_)), _) => {
            ("publication failed", Tone::Failure)
        }
        (
            WorkflowRunPublicationState::Completed(WorkflowRunPublicationResult::Succeeded {
                ..
            }),
            WorkflowRunCleanupState::Completed(WorkflowRunCleanupResult::Failed),
        ) => ("cleanup failed", Tone::Failure),
        (
            WorkflowRunPublicationState::Completed(WorkflowRunPublicationResult::Succeeded {
                ..
            }),
            WorkflowRunCleanupState::NotStarted | WorkflowRunCleanupState::Cleaning,
        ) => ("cleaning", Tone::Active),
        _ if snapshot.force_abort.is_some() => ("force aborted", Tone::Failure),
        _ => (
            workflow_status(&snapshot.workflow),
            workflow_tone(&snapshot.workflow),
        ),
    }
}

pub(super) fn workflow_status<Deadline>(workflow: &WorkflowState<Deadline>) -> &'static str {
    match workflow {
        WorkflowState::Executing {
            gate: SchedulingGate::Open,
        } => "running",
        WorkflowState::Executing {
            gate: SchedulingGate::FailureStopped { .. },
        } => "failing",
        WorkflowState::Executing {
            gate: SchedulingGate::Cancelling { .. },
        } => "cancelling",
        WorkflowState::Finalizing { .. } => "finalizing",
        WorkflowState::Succeeded => "succeeded",
        WorkflowState::Failed { .. } => "failed",
        WorkflowState::Cancelled { .. } => "cancelled",
    }
}

pub(super) fn workflow_tone<Deadline>(workflow: &WorkflowState<Deadline>) -> Tone {
    match workflow {
        WorkflowState::Succeeded => Tone::Success,
        WorkflowState::Failed { .. }
        | WorkflowState::Executing {
            gate: SchedulingGate::FailureStopped { .. },
        } => Tone::Failure,
        WorkflowState::Cancelled { .. }
        | WorkflowState::Executing {
            gate: SchedulingGate::Cancelling { .. },
        } => Tone::Blocked,
        WorkflowState::Executing {
            gate: SchedulingGate::Open,
        }
        | WorkflowState::Finalizing { .. } => Tone::Active,
    }
}

pub(super) fn step_state_glyph<Step: StepProjection>(step: &Step) -> &'static str {
    match step.state() {
        StepStateKind::Pending => "○",
        StepStateKind::Starting => "◔",
        StepStateKind::Running => {
            let elapsed = step
                .timing()
                .map_or(Duration::ZERO, |timing| timing.duration);
            let frame = elapsed.as_millis() / REDRAW_INTERVAL.as_millis();
            let index = usize::try_from(frame).unwrap_or(0) % RUNNING_INDICATOR_FRAMES.len();
            RUNNING_INDICATOR_FRAMES[index]
        }
        StepStateKind::CapturingOutputs => "◕",
        StepStateKind::Recovering => "◑",
        StepStateKind::Cancelling => "◒",
        StepStateKind::Succeeded | StepStateKind::Inherited => "✓",
        StepStateKind::Failed => "×",
        StepStateKind::Blocked => "◐",
        StepStateKind::Skipped => "↷",
        StepStateKind::NotRun => "–",
        StepStateKind::Cancelled => "⊘",
    }
}

pub(super) fn step_state_label(state: StepStateKind) -> &'static str {
    match state {
        StepStateKind::Pending => "pending",
        StepStateKind::Starting => "starting",
        StepStateKind::Running => "running",
        StepStateKind::CapturingOutputs => "capturing",
        StepStateKind::Recovering => "recovering",
        StepStateKind::Cancelling => "cancelling",
        StepStateKind::Succeeded => "succeeded",
        StepStateKind::Inherited => "inherited",
        StepStateKind::Failed => "failed",
        StepStateKind::Blocked => "blocked",
        StepStateKind::Skipped => "skipped",
        StepStateKind::NotRun => "not-run",
        StepStateKind::Cancelled => "cancelled",
    }
}

pub(super) fn step_state_style(state: StepStateKind, color: bool) -> Style {
    tone_style(color, step_state_tone(state)).add_modifier(Modifier::BOLD)
}

pub(super) fn step_state_tone(state: StepStateKind) -> Tone {
    match state {
        StepStateKind::Starting
        | StepStateKind::Running
        | StepStateKind::CapturingOutputs
        | StepStateKind::Recovering => Tone::Active,
        StepStateKind::Succeeded | StepStateKind::Inherited => Tone::Success,
        StepStateKind::Failed => Tone::Failure,
        StepStateKind::Cancelling | StepStateKind::Blocked | StepStateKind::Cancelled => {
            Tone::Blocked
        }
        StepStateKind::Pending | StepStateKind::Skipped | StepStateKind::NotRun => Tone::Muted,
    }
}

#[derive(Clone, Copy)]
pub(super) enum Tone {
    Primary,
    Neutral,
    Muted,
    Active,
    Success,
    Failure,
    Blocked,
}

pub(super) fn tone_style(color: bool, tone: Tone) -> Style {
    if !color {
        return Style::default();
    }
    let foreground = match tone {
        Tone::Primary => crate::workflow::render_style::PRIMARY,
        Tone::Neutral => crate::workflow::render_style::NEUTRAL,
        Tone::Muted => crate::workflow::render_style::MUTED,
        Tone::Active => crate::workflow::render_style::ACTIVE,
        Tone::Success => crate::workflow::render_style::SUCCESS,
        Tone::Failure => crate::workflow::render_style::FAILURE,
        Tone::Blocked => crate::workflow::render_style::BLOCKED,
    };
    Style::default().fg(theme_color(foreground))
}

pub(super) fn theme_color((red, green, blue): (u8, u8, u8)) -> Color {
    Color::Rgb(red, green, blue)
}

pub(super) fn command_accent_style(color: bool) -> Style {
    fixed_color_style(color, theme_color(crate::workflow::render_style::ACCENT))
}

pub(super) fn footer_key_style(color: bool) -> Style {
    fixed_color_style(
        color,
        theme_color(crate::workflow::render_style::FOOTER_KEY),
    )
}

pub(super) fn help_key_style(color: bool) -> Style {
    fixed_color_style(color, theme_color(crate::workflow::render_style::HELP_KEY))
}

pub(super) fn footer_separator_style(color: bool) -> Style {
    fixed_color_style(color, theme_color(crate::workflow::render_style::SELECTION))
}

pub(super) fn separator_style(color: bool) -> Style {
    fixed_color_style(color, theme_color(crate::workflow::render_style::SEPARATOR))
}

pub(super) fn fixed_color_style(color: bool, foreground: Color) -> Style {
    if color {
        Style::default().fg(foreground)
    } else {
        Style::default()
    }
}
