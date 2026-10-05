use super::*;

// Reserve space for the right-side status before drawing the left header content.
pub(super) fn header_status_areas(area: Rect, status: &Line<'_>) -> (u16, Rect) {
    let status_width = u16::try_from(status.width())
        .unwrap_or(u16::MAX)
        .min(area.width);
    (
        area.width.saturating_sub(status_width.saturating_add(2)),
        Rect::new(
            area.right().saturating_sub(status_width),
            area.y,
            status_width,
            1,
        ),
    )
}

pub(super) fn render(
    frame: &mut Frame<'_>,
    snapshot: &WorkflowRunViewSnapshot,
    graph: &DagLayout,
    interaction: &mut HostInteraction,
    color: bool,
) {
    let area = frame.area();
    interaction.terminal_area = area;
    frame.render_widget(Clear, area);
    if !operational_area(area) {
        render_too_small(frame, area, snapshot, color);
        return;
    }

    let sections =
        Layout::vertical([Constraint::Min(0), Constraint::Length(FOOTER_HEIGHT)]).split(area);
    if interaction.surface == HostSurface::FullLog {
        let selected_step = snapshot.steps.get(interaction.selected);
        if let Some(step) = selected_step {
            let (width, rows) = full_log_record_dimensions(area, step);
            interaction.prepare_filtered_log(&step.log, interaction.selected, snapshot.generation);
            interaction
                .full_log
                .synchronize(&interaction.filtered_log, width, rows);
        }
        let full_log_sections =
            inspector_and_log_areas(sections[0], inspector_desired_height(selected_step));
        render_inspector(
            frame,
            full_log_sections[0],
            selected_step,
            color,
            Borders::NONE,
        );
        render_full_log(
            frame,
            full_log_sections[1],
            selected_step,
            &interaction.full_log,
            &interaction.filtered_log,
            interaction.log_filters,
            color,
        );
        render_contextual_footer(
            frame,
            sections[1],
            snapshot,
            color,
            "LOG",
            &FULL_LOG_FOOTER_OPTIONS,
        );
    } else {
        render_split_body(frame, sections[0], snapshot, graph, interaction, color);
        render_contextual_footer(
            frame,
            sections[1],
            snapshot,
            color,
            "DAG",
            &SPLIT_FOOTER_OPTIONS,
        );
        render_split_footer_junction(
            frame,
            sections[0],
            sections[1].y,
            interaction.help_visible,
            color,
            wide_split_columns(sections[0]),
        );
    }

    if interaction.help_visible {
        render_help_overlay(
            frame,
            sections[0],
            interaction.surface,
            lifecycle_control(snapshot),
            color,
        );
    }
}

#[derive(Clone, Copy)]
pub(super) struct SplitBodyLayout {
    pub(super) summary: Rect,
    pub(super) dag: Rect,
    pub(super) inspector: Rect,
    pub(super) output: Rect,
    pub(super) wide: bool,
}

impl SplitBodyLayout {
    pub(super) fn summary_borders(self) -> Borders {
        if self.wide {
            Borders::BOTTOM | Borders::RIGHT
        } else {
            Borders::BOTTOM
        }
    }

    pub(super) fn dag_borders(self) -> Borders {
        if self.wide {
            Borders::RIGHT
        } else {
            Borders::NONE
        }
    }

    pub(super) fn inspector_borders(self) -> Borders {
        if self.wide {
            Borders::NONE
        } else {
            Borders::TOP
        }
    }
}

pub(super) fn split_body_layout(
    area: Rect,
    summary_height: u16,
    desired_inspector_height: u16,
    wide_columns: [Rect; 2],
) -> SplitBodyLayout {
    if area.width >= WIDE_LAYOUT_WIDTH {
        let left = Layout::vertical([Constraint::Length(summary_height), Constraint::Min(0)])
            .split(wide_columns[0]);
        let right = inspector_and_log_areas(wide_columns[1], desired_inspector_height);
        SplitBodyLayout {
            summary: left[0],
            dag: left[1],
            inspector: right[0],
            output: right[1],
            wide: true,
        }
    } else {
        let body_height = area.height.saturating_sub(summary_height);
        let dag_height = (body_height / 3).clamp(5, 10);
        let remaining_height = body_height.saturating_sub(dag_height);
        let inspector_height = bounded_inspector_height(remaining_height, desired_inspector_height);
        let rows = Layout::vertical([
            Constraint::Length(summary_height),
            Constraint::Length(dag_height),
            Constraint::Length(inspector_height),
            Constraint::Min(MINIMUM_LOG_HEIGHT),
        ])
        .split(area);
        SplitBodyLayout {
            summary: rows[0],
            dag: rows[1],
            inspector: rows[2],
            output: rows[3],
            wide: false,
        }
    }
}

pub(super) fn render_split_body(
    frame: &mut Frame<'_>,
    area: Rect,
    snapshot: &WorkflowRunViewSnapshot,
    graph: &DagLayout,
    interaction: &mut HostInteraction,
    color: bool,
) {
    let selected_step = snapshot.steps.get(interaction.selected);
    let layout = split_body_layout(
        area,
        WORKFLOW_SUMMARY_HEIGHT,
        inspector_desired_height(selected_step),
        wide_split_columns(area),
    );
    render_workflow_summary(
        frame,
        layout.summary,
        snapshot,
        color,
        layout.summary_borders(),
    );
    render_split_steps(
        frame,
        layout,
        &snapshot.steps,
        graph,
        live_step_phase_boundary(snapshot),
        interaction.selected,
        color,
    );
    render_inspector(
        frame,
        layout.inspector,
        selected_step,
        color,
        layout.inspector_borders(),
    );
    render_log(
        frame,
        layout.output,
        snapshot,
        interaction,
        color,
        Borders::TOP,
    );
    render_split_body_junctions(frame, layout, color);
}

pub(super) fn render_split_body_junctions(
    frame: &mut Frame<'_>,
    layout: SplitBodyLayout,
    color: bool,
) {
    if layout.wide {
        let divider_x = layout.inspector.x.saturating_sub(1);
        render_junction(
            frame,
            divider_x,
            layout.summary.bottom().saturating_sub(1),
            "┼",
            color,
        );
        render_junction(frame, divider_x, layout.output.y, "├", color);
    }
}

pub(super) fn render_split_footer_junction(
    frame: &mut Frame<'_>,
    body: Rect,
    footer_y: u16,
    help_visible: bool,
    color: bool,
    wide_columns: [Rect; 2],
) {
    if body.width >= WIDE_LAYOUT_WIDTH && !help_visible {
        render_junction(
            frame,
            wide_columns[1].x.saturating_sub(1),
            footer_y,
            "┴",
            color,
        );
    }
}

pub(super) fn wide_split_columns(area: Rect) -> [Rect; 2] {
    let columns =
        Layout::horizontal([Constraint::Ratio(1, 3), Constraint::Ratio(2, 3)]).split(area);
    [columns[0], columns[1]]
}

pub(super) fn render_junction(
    frame: &mut Frame<'_>,
    x: u16,
    y: u16,
    symbol: &'static str,
    color: bool,
) {
    frame.render_widget(
        Paragraph::new(Span::styled(symbol, separator_style(color))),
        Rect::new(x, y, 1, 1),
    );
}

pub(super) fn inspector_and_log_areas(area: Rect, desired_inspector_height: u16) -> [Rect; 2] {
    let inspector_height = bounded_inspector_height(area.height, desired_inspector_height);
    let rows = Layout::vertical([
        Constraint::Length(inspector_height),
        Constraint::Min(MINIMUM_LOG_HEIGHT),
    ])
    .split(area);
    [rows[0], rows[1]]
}

pub(super) fn bounded_inspector_height(available_height: u16, desired_height: u16) -> u16 {
    let maximum_height = available_height.saturating_sub(MINIMUM_LOG_HEIGHT);
    let minimum_height = MINIMUM_INSPECTOR_HEIGHT.min(maximum_height);
    desired_height.clamp(minimum_height, maximum_height)
}

pub(super) fn inspector_desired_height<Step: StepProjection>(step: Option<&Step>) -> u16 {
    let body_height = step.map_or(1, |step| {
        inspector_detail_row_count(step)
            .saturating_add(1)
            .saturating_add(inspector_outputs_desired_height(step))
    });
    u16::try_from(body_height)
        .unwrap_or(u16::MAX)
        .saturating_add(INSPECTOR_HEADER_HEIGHT)
}

pub(super) fn inspector_outputs_desired_height<Step: StepProjection>(step: &Step) -> usize {
    let outputs = step.inspector_outputs();
    if outputs.is_empty() {
        usize::from(step.show_empty_outputs()) * 4
    } else {
        inspector_outputs_desired_height_for_count(outputs.len())
    }
}

pub(super) fn section_block(borders: Borders, color: bool) -> Block<'static> {
    let has_side_border = borders.contains(Borders::LEFT) || borders.contains(Borders::RIGHT);
    let padding = u16::from(!has_side_border);
    Block::default()
        .borders(borders)
        .border_style(separator_style(color))
        .padding(Padding::horizontal(padding))
}

pub(super) fn render_too_small(
    frame: &mut Frame<'_>,
    area: Rect,
    snapshot: &WorkflowRunViewSnapshot,
    color: bool,
) {
    let mut lines = vec![
        Line::from(Span::styled(
            "Terminal too small",
            tone_style(color, Tone::Failure),
        )),
        Line::from(format!(
            "Resize to at least {MINIMUM_WIDTH}x{MINIMUM_HEIGHT}."
        )),
    ];
    match lifecycle_control(snapshot) {
        LifecycleControl::Cancel => lines.push(Line::from("Ctrl-C cancels the workflow.")),
        LifecycleControl::Quit => lines.push(Line::from("Press q to quit.")),
        LifecycleControl::None => lines.push(Line::from("Finishing workflow lifecycle…")),
    }
    frame.render_widget(
        Paragraph::new(lines).block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(separator_style(color))
                .title(" Scherzo workflow run "),
        ),
        area,
    );
}
