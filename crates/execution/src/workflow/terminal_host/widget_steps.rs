use super::*;

pub(super) fn render_split_steps<Step: StepProjection>(
    frame: &mut Frame<'_>,
    layout: SplitBodyLayout,
    steps: &[Step],
    graph: &DagLayout,
    phase_boundary: Option<StepPhaseBoundary>,
    selected: usize,
    color: bool,
) {
    render_projected_steps(
        frame,
        layout.dag,
        steps,
        graph,
        selected,
        color,
        StepPanel {
            borders: layout.dag_borders(),
            show_title: false,
            phase_boundary,
        },
    );
}

pub(super) fn render_projected_steps<Step: StepProjection>(
    frame: &mut Frame<'_>,
    area: Rect,
    steps: &[Step],
    graph: &DagLayout,
    selected_step: usize,
    color: bool,
    panel: StepPanel,
) {
    let mut block = Block::default()
        .borders(panel.borders)
        .border_style(separator_style(color));
    if panel.show_title {
        block = block.title(format!(" Steps ({}) ", steps.len()));
    }
    let available_width = usize::from(block.inner(area).width);
    let columns = StepColumns::for_steps(available_width, graph.gutter_width(), steps);
    let connector_style = graph_connector_style(color);
    let phase_boundary = panel
        .phase_boundary
        .filter(|boundary| boundary.finalization_start < steps.len());
    let mut items = Vec::with_capacity(steps.len() + usize::from(phase_boundary.is_some()) * 2);
    if phase_boundary.is_some() {
        items.push(ListItem::new(Line::from(Span::styled(
            "  ordinary phase",
            tone_style(color, Tone::Muted).add_modifier(Modifier::BOLD),
        ))));
    }
    for (index, (step, graph_row)) in steps.iter().zip(graph.rows()).enumerate() {
        if phase_boundary.is_some_and(|boundary| boundary.finalization_start == index) {
            let trigger = phase_boundary
                .and_then(|boundary| boundary.trigger)
                .map_or_else(String::new, |trigger| format!(" · trigger {trigger}"));
            items.push(ListItem::new(Line::from(Span::styled(
                format!("  finalization phase{trigger}"),
                tone_style(color, Tone::Muted).add_modifier(Modifier::BOLD),
            ))));
        }
        let selected = index == selected_step;
        let marker = if selected { "▏ " } else { "  " };
        let id = padded_text(&visible_text(step.id()), columns.id_width);
        let duration = step
            .timing()
            .map(|timing| human_duration(timing.duration))
            .unwrap_or_else(|| "-".to_owned());
        let mut spans = vec![
            Span::styled(marker, selection_marker_style(color)),
            Span::styled(graph_row.before_node.clone(), connector_style),
            Span::styled(
                step_state_glyph(step),
                step_state_style(step.state(), color),
            ),
            Span::styled(graph_row.after_node.clone(), connector_style),
            Span::raw(" "),
            Span::styled(id, step_identity_style(step.state(), color)),
        ];
        if columns.kind {
            spans.push(Span::raw("  "));
            spans.push(Span::styled(
                format!("{:<KIND_COLUMN_WIDTH$}", step_kind(step.definition())),
                tone_style(color, Tone::Muted),
            ));
        }
        spans.push(Span::raw("  "));
        spans.push(Span::styled(
            padded_text(&duration, columns.duration_width),
            step_duration_style(step.state(), color),
        ));
        if columns.detail {
            spans.push(Span::raw("  "));
            if let Some(detail) = step.dag_detail() {
                spans.push(Span::styled(
                    fit_text(&visible_text(&detail), columns.detail_width),
                    tone_style(color, Tone::Muted),
                ));
            }
        }
        let mut node_line = Line::from(spans);
        if selected {
            node_line = node_line.style(step_selection_style(color));
        }
        let connector_line = Line::from(vec![
            Span::raw("  "),
            Span::styled(graph_row.below_node.clone(), connector_style),
        ]);
        items.push(ListItem::new(vec![node_line, connector_line]));
    }
    let list = List::new(items).block(block);
    let mut state = ListState::default();
    if !steps.is_empty() {
        let phase_rows_before_selection = phase_boundary.map_or(0, |boundary| {
            1 + usize::from(selected_step >= boundary.finalization_start)
        });
        state.select(Some(selected_step + phase_rows_before_selection));
    }
    frame.render_stateful_widget(list, area, &mut state);
}

pub(super) fn step_state_is_active(state: StepStateKind) -> bool {
    matches!(
        state,
        StepStateKind::Starting
            | StepStateKind::Running
            | StepStateKind::CapturingOutputs
            | StepStateKind::Cancelling
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct StepColumns {
    pub(super) id_width: usize,
    pub(super) kind: bool,
    pub(super) duration_width: usize,
    pub(super) detail: bool,
    pub(super) detail_width: usize,
}

impl StepColumns {
    pub(super) fn for_steps<Step: StepProjection>(
        available: usize,
        gutter_width: usize,
        steps: &[Step],
    ) -> Self {
        let id_width = steps
            .iter()
            .map(|step| display_width(&visible_text(step.id())))
            .max()
            .unwrap_or(0);
        let duration_width = steps
            .iter()
            .map(|step| {
                step.timing()
                    .map_or(1, |timing| display_width(&human_duration(timing.duration)))
            })
            .max()
            .unwrap_or(1);
        let prefix_width = 2_usize.saturating_add(gutter_width).saturating_add(1);
        let exact_with_kind = prefix_width
            .saturating_add(id_width)
            .saturating_add(2)
            .saturating_add(KIND_COLUMN_WIDTH)
            .saturating_add(2)
            .saturating_add(duration_width);
        let detail = available
            >= exact_with_kind
                .saturating_add(2)
                .saturating_add(MINIMUM_DETAIL_WIDTH);
        if detail {
            return Self {
                id_width,
                kind: true,
                duration_width,
                detail: true,
                detail_width: available.saturating_sub(exact_with_kind.saturating_add(2)),
            };
        }
        if available >= exact_with_kind {
            return Self {
                id_width,
                kind: true,
                duration_width,
                detail: false,
                detail_width: 0,
            };
        }
        let fixed_width = prefix_width
            .saturating_add(2)
            .saturating_add(duration_width);
        Self {
            id_width: id_width.min(available.saturating_sub(fixed_width)),
            kind: false,
            duration_width,
            detail: false,
            detail_width: 0,
        }
    }
}

pub(in crate::workflow) fn live_step_detail(step: &WorkflowRunStepView) -> Option<String> {
    if let Some(detail) = &step.inherited {
        return Some(crate::workflow::render_style::inherited_detail(detail));
    }
    match &step.fact {
        Some(ObservedStepTransition::Recovery {
            active,
            configured_rounds,
            handler_kind,
            handler_state,
            decision,
            ..
        }) => Some(recovery_progress_detail(
            *active,
            *configured_rounds,
            *handler_kind,
            *handler_state,
            *decision,
        )),
        Some(ObservedStepTransition::OutputsCommitted { outputs }) => {
            Some(output_count_detail(outputs.len()))
        }
        Some(ObservedStepTransition::Failed { detail }) => Some(issue_detail_for_step(
            canonical_failure_detail(detail),
            &step.definition,
            step.state,
        )),
        Some(ObservedStepTransition::Blocked { detail }) => Some(issue_detail_for_step(
            canonical_blocked_detail(detail),
            &step.definition,
            step.state,
        )),
        Some(ObservedStepTransition::Skipped { detail }) => {
            Some(crate::workflow::archived_presentation::condition_false_detail(detail))
        }
        Some(ObservedStepTransition::NotRun { detail }) => {
            Some(crate::workflow::presentation::snake_case_debug(detail.code))
        }
        Some(ObservedStepTransition::Cancelling { detail })
        | Some(ObservedStepTransition::Cancelled { detail }) => {
            Some(cancellation_reason(detail.code).to_owned())
        }
        None if step.state == StepStateKind::Succeeded => {
            let committed_outputs = step
                .outputs
                .values()
                .filter(|disposition| **disposition == WorkflowRunOutputDisposition::Committed)
                .count();
            Some(crate::workflow::render_style::success_detail(
                matches!(step.definition, WorkflowPresentationStep::Command { .. }),
                committed_outputs,
            ))
        }
        None => None,
    }
}

pub(super) fn failure_policy_name(policy: FailurePolicy) -> &'static str {
    match policy {
        FailurePolicy::Required => "required",
        FailurePolicy::Advisory => "advisory",
    }
}

pub(super) fn is_advisory_issue(
    definition: &WorkflowPresentationStep,
    state: StepStateKind,
) -> bool {
    definition.failure_policy() == FailurePolicy::Advisory
        && matches!(state, StepStateKind::Failed | StepStateKind::Blocked)
}

pub(super) fn issue_detail_for_step(
    detail: String,
    definition: &WorkflowPresentationStep,
    state: StepStateKind,
) -> String {
    if is_advisory_issue(definition, state) {
        format!("{detail} · advisory")
    } else {
        detail
    }
}

pub(super) fn output_count_detail(count: usize) -> String {
    if count == 1 {
        "1 output committed".to_owned()
    } else {
        format!("{count} outputs committed")
    }
}

pub(super) fn padded_text(value: &str, width: usize) -> String {
    let fitted = fit_text(value, width);
    let padding = width.saturating_sub(display_width(&fitted));
    format!("{fitted}{}", " ".repeat(padding))
}

pub(super) fn graph_connector_style(color: bool) -> Style {
    let style = if color {
        Style::default().fg(theme_color(crate::workflow::render_style::MUTED))
    } else {
        Style::default()
    };
    style.add_modifier(Modifier::DIM)
}

pub(super) fn selection_marker_style(color: bool) -> Style {
    if color {
        tone_style(true, Tone::Active)
    } else {
        Style::default()
    }
}

pub(super) fn step_selection_style(color: bool) -> Style {
    if color {
        Style::default().bg(theme_color(crate::workflow::render_style::SELECTION))
    } else {
        Style::default().add_modifier(Modifier::REVERSED)
    }
}

pub(super) fn step_identity_style(state: StepStateKind, color: bool) -> Style {
    let tone = match state {
        StepStateKind::Pending
        | StepStateKind::Blocked
        | StepStateKind::NotRun
        | StepStateKind::Cancelled => Tone::Muted,
        _ => Tone::Primary,
    };
    tone_style(color, tone)
}

pub(super) fn step_duration_style(state: StepStateKind, color: bool) -> Style {
    let tone = if step_state_is_active(state) {
        Tone::Active
    } else {
        Tone::Muted
    };
    tone_style(color, tone)
}
