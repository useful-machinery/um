use super::*;

pub(super) struct InspectorField {
    pub(super) label: &'static str,
    pub(super) value: String,
    pub(super) tone: Tone,
}

impl InspectorField {
    pub(super) fn new(label: &'static str, value: impl AsRef<str>, tone: Tone) -> Self {
        Self {
            label,
            value: visible_text(value.as_ref()),
            tone,
        }
    }
}

pub(super) struct InspectorOutput {
    pub(super) marker: &'static str,
    pub(super) marker_tone: Tone,
    pub(super) name: String,
    pub(super) kind: &'static str,
    pub(super) detail: String,
    pub(super) disposition: Option<String>,
    pub(super) tone: Tone,
}

impl InspectorOutput {
    pub(super) fn declaration(name: String, kind: &'static str, detail: String) -> Self {
        Self {
            marker: "·",
            marker_tone: Tone::Muted,
            name,
            kind,
            detail,
            disposition: None,
            tone: Tone::Muted,
        }
    }
}

impl StepProjection for WorkflowRunStepView {
    fn header(&self) -> StepHeader<'_> {
        StepHeader::new(&self.id, &self.definition, self.state, self.timing.as_ref())
    }

    fn dag_detail(&self) -> Option<String> {
        live_step_detail(self)
    }

    fn inspector_command(&self) -> Option<String> {
        let WorkflowPresentationStep::Command { argv, .. } = &self.definition else {
            return None;
        };
        let command = argv
            .iter()
            .map(|argument| shell_quote(argument))
            .collect::<Vec<_>>()
            .join(" ");
        Some(
            if self.role == crate::workflow::validated::WorkflowNodeRole::Finalizer {
                format!("finalizer · {command}")
            } else {
                command
            },
        )
    }

    fn inspector_fact(&self) -> Option<InspectorField> {
        self.inherited.as_ref().map_or_else(
            || live_inspector_fact(self.fact.as_ref()),
            |detail| {
                Some(InspectorField::new(
                    "inheritance",
                    crate::workflow::render_style::inherited_detail(detail),
                    Tone::Neutral,
                ))
            },
        )
    }

    fn inspector_outputs(&self) -> Vec<InspectorOutput> {
        live_inspector_outputs(self)
    }

    fn show_empty_outputs(&self) -> bool {
        true
    }
}

pub(super) fn render_inspector<Step: StepProjection>(
    frame: &mut Frame<'_>,
    area: Rect,
    step: Option<&Step>,
    color: bool,
    borders: Borders,
) {
    let sections = Layout::vertical([
        Constraint::Length(INSPECTOR_HEADER_HEIGHT),
        Constraint::Min(0),
    ])
    .split(area);
    let mut header_borders = Borders::BOTTOM;
    for border in [Borders::TOP, Borders::LEFT, Borders::RIGHT] {
        if borders.contains(border) {
            header_borders |= border;
        }
    }

    let header =
        section_block(header_borders, color).padding(Padding::horizontal(INSPECTOR_PANEL_PADDING));
    let header_content = header.inner(sections[0]);
    frame.render_widget(header, sections[0]);
    if let Some(step) = step {
        render_selected_step_header(frame, header_content, step, color);
    } else {
        frame.render_widget(
            Paragraph::new(Span::styled(
                "Selected step",
                tone_style(color, Tone::Primary).add_modifier(Modifier::BOLD),
            )),
            header_content,
        );
    }

    let mut output_borders = Borders::NONE;
    for border in [Borders::LEFT, Borders::RIGHT, Borders::BOTTOM] {
        if borders.contains(border) {
            output_borders |= border;
        }
    }
    let Some(step) = step else {
        let block = section_block(output_borders, color)
            .padding(Padding::horizontal(INSPECTOR_PANEL_PADDING));
        frame.render_widget(
            Paragraph::new("No workflow steps.").block(block),
            sections[1],
        );
        return;
    };

    let outputs = step.inspector_outputs();
    let output_panel_visible = !outputs.is_empty() || step.show_empty_outputs();
    let available_body_height = sections[1].height;
    let desired_detail_height = u16::try_from(inspector_detail_row_count(step))
        .unwrap_or(u16::MAX)
        .saturating_add(u16::from(output_panel_visible));
    let reserved_output_height = if output_panel_visible {
        MINIMUM_OUTPUT_PANEL_HEIGHT.min(available_body_height)
    } else {
        0
    };
    let maximum_detail_height = available_body_height.saturating_sub(reserved_output_height);
    let detail_height = desired_detail_height.min(maximum_detail_height);
    let body_sections = Layout::vertical([Constraint::Length(detail_height), Constraint::Min(0)])
        .split(sections[1]);

    if detail_height != 0 {
        let mut detail_borders = if output_panel_visible {
            Borders::BOTTOM
        } else {
            Borders::NONE
        };
        for border in [Borders::LEFT, Borders::RIGHT] {
            if borders.contains(border) {
                detail_borders |= border;
            }
        }
        render_inspector_panel(
            frame,
            body_sections[0],
            color,
            detail_borders,
            |content_area| inspector_detail_lines(step, content_area, color),
        );
        if output_panel_visible && body_sections[0].x != 0 && !borders.contains(Borders::LEFT) {
            render_junction(
                frame,
                body_sections[0].x.saturating_sub(1),
                body_sections[0].bottom().saturating_sub(1),
                "├",
                color,
            );
        }
    }
    if output_panel_visible {
        render_inspector_panel(
            frame,
            body_sections[1],
            color,
            output_borders,
            |content_area| {
                inspector_output_lines(&outputs, content_area.width, content_area.height, color)
            },
        );
    }
}

pub(super) fn render_inspector_panel(
    frame: &mut Frame<'_>,
    area: Rect,
    color: bool,
    borders: Borders,
    content: impl FnOnce(Rect) -> Vec<Line<'static>>,
) {
    let block = section_block(borders, color).padding(Padding::horizontal(INSPECTOR_PANEL_PADDING));
    let content_area = block.inner(area);
    frame.render_widget(Paragraph::new(content(content_area)).block(block), area);
}

pub(super) fn inspector_detail_lines<Step: StepProjection>(
    step: &Step,
    content_area: Rect,
    color: bool,
) -> Vec<Line<'static>> {
    let total_rows = inspector_detail_row_count(step);
    let total_items = inspector_fixed_field_count(step);
    let available_rows = usize::from(content_area.height);
    let overflowing = total_rows > available_rows;
    let regular_row_limit = if overflowing {
        available_rows.saturating_sub(1)
    } else {
        total_rows
    };
    let fields = inspector_fields_for_rows(step, content_area.width, regular_row_limit);
    let rendered_items = fields.len();
    let mut lines = fields
        .iter()
        .map(|field| inspector_field_line(field, content_area.width, color))
        .collect::<Vec<_>>();
    if overflowing && available_rows != 0 {
        let omitted = total_items.saturating_sub(rendered_items);
        lines.push(Line::from(Span::styled(
            format!("+{omitted} more"),
            tone_style(color, Tone::Muted),
        )));
    }
    lines
}

pub(super) fn render_selected_step_header<Step: StepProjection>(
    frame: &mut Frame<'_>,
    area: Rect,
    step: &Step,
    color: bool,
) {
    if area.is_empty() {
        return;
    }
    let status = selected_step_status_title(step, color);
    let (title_width, status_area) = header_status_areas(area, &status);
    if title_width != 0 {
        frame.render_widget(
            Paragraph::new(selected_step_title(step, color, usize::from(title_width))),
            Rect::new(area.x, area.y, title_width, 1),
        );
    }
    frame.render_widget(
        Paragraph::new(status).alignment(Alignment::Right),
        status_area,
    );
}

pub(super) fn selected_step_title<Step: StepProjection>(
    step: &Step,
    color: bool,
    maximum_width: usize,
) -> Line<'static> {
    let badge = format!(" {} ", step_kind(step.definition()));
    let fixed_width = 5_usize.saturating_add(display_width(&badge));
    let id = ellipsize(
        &visible_text(step.id()),
        maximum_width.saturating_sub(fixed_width),
    );
    Line::from(vec![
        Span::styled(
            step_state_glyph(step),
            step_state_style(step.state(), color),
        ),
        Span::raw("  "),
        Span::styled(
            id,
            tone_style(color, Tone::Primary).add_modifier(Modifier::BOLD),
        ),
        Span::raw("  "),
        Span::styled(badge, step_kind_badge_style(color)),
    ])
}

pub(super) fn step_kind_badge_style(color: bool) -> Style {
    let style = tone_style(color, Tone::Muted);
    if color {
        style.bg(theme_color(crate::workflow::render_style::SELECTION))
    } else {
        style
    }
}

pub(super) fn selected_step_status_title<Step: StepProjection>(
    step: &Step,
    color: bool,
) -> Line<'static> {
    let style = tone_style(color, step_state_tone(step.state()));
    let mut spans = vec![Span::styled(step_state_label(step.state()), style)];
    spans.push(Span::styled(
        format!(
            " · {}",
            failure_policy_name(step.definition().failure_policy())
        ),
        style,
    ));
    if let Some(timing) = step.timing() {
        let duration = if timing.frozen && step_state_is_active(step.state()) {
            format!("{} interrupted", human_duration(timing.duration))
        } else {
            human_duration(timing.duration)
        };
        spans.push(Span::styled(" · ", style));
        spans.push(Span::styled(duration, style));
    }
    Line::from(spans)
}

pub(super) fn inspector_detail_row_count<Step: StepProjection>(step: &Step) -> usize {
    inspector_fixed_field_count(step)
}

pub(super) fn inspector_fixed_field_count<Step: StepProjection>(step: &Step) -> usize {
    let mut count = 3;
    if inspector_timing(step).is_some() {
        count += 1;
    }
    if step.inspector_fact().is_some() {
        count += 1;
    }
    count
}

pub(super) fn inspector_fields_for_rows<Step: StepProjection>(
    step: &Step,
    width: u16,
    maximum_rows: usize,
) -> Vec<InspectorField> {
    let maximum_fields = maximum_rows.min(inspector_fixed_field_count(step));
    inspector_fields(step, usize::from(width), maximum_fields)
}

pub(super) fn live_inspector_outputs(step: &WorkflowRunStepView) -> Vec<InspectorOutput> {
    let declarations = step.definition.outputs();
    step.outputs
        .iter()
        .map(|(name, disposition)| {
            let (disposition, tone, marker, marker_tone) = output_disposition(*disposition);
            let (kind, detail) = declarations
                .get(name)
                .map_or(("output", "—".to_owned()), output_description);
            InspectorOutput {
                marker,
                marker_tone,
                name: visible_text(name),
                kind,
                detail,
                disposition: Some(disposition),
                tone,
            }
        })
        .collect()
}

pub(super) fn inspector_fields<Step: StepProjection>(
    step: &Step,
    content_width: usize,
    maximum_fields: usize,
) -> Vec<InspectorField> {
    let mut fields = Vec::with_capacity(maximum_fields);
    let direct_dependencies = match step.definition() {
        WorkflowPresentationStep::Command {
            cwd,
            direct_dependencies,
            ..
        } => {
            if let Some(command) = step.inspector_command() {
                push_inspector_field(&mut fields, maximum_fields, || {
                    InspectorField::new("command", command, Tone::Neutral)
                });
            }
            push_inspector_field(&mut fields, maximum_fields, || {
                InspectorField::new("cwd", cwd.as_deref().unwrap_or("."), Tone::Neutral)
            });
            direct_dependencies
        }
        WorkflowPresentationStep::Agent {
            profile,
            harness,
            direct_dependencies,
            ..
        } => {
            push_inspector_field(&mut fields, maximum_fields, || {
                InspectorField::new("profile", profile, Tone::Neutral)
            });
            push_inspector_field(&mut fields, maximum_fields, || {
                InspectorField::new("harness", harness_description(harness), Tone::Neutral)
            });
            direct_dependencies
        }
    };

    let timing = inspector_timing(step);
    if let Some(timing) = timing {
        push_inspector_field(&mut fields, maximum_fields, || {
            InspectorField::new(
                "started",
                header_timestamp(timing.started_at),
                Tone::Neutral,
            )
        });
    }
    push_inspector_field(&mut fields, maximum_fields, || {
        let dependency_width = content_width.saturating_sub(INSPECTOR_LABEL_WIDTH);
        InspectorField::new(
            "depends on",
            summarize_repeated_values(direct_dependencies, dependency_width),
            Tone::Neutral,
        )
    });
    if fields.len() < maximum_fields
        && let Some(fact) = step.inspector_fact()
    {
        fields.push(fact);
    }
    fields
}

pub(super) fn harness_description(harness: &AgentPresentationHarness) -> String {
    match harness {
        AgentPresentationHarness::Pi { model, thinking } => {
            let thinking = format!("{thinking:?}").to_ascii_lowercase();
            format!("pi · {} · thinking={thinking}", visible_text(model))
        }
        AgentPresentationHarness::ClaudeCode { model, effort } => format!(
            "claude code · {} · effort={}",
            visible_text(model),
            effort.as_str()
        ),
        AgentPresentationHarness::Codex { model, effort } => {
            format!(
                "codex · {} · effort={}",
                visible_text(model),
                visible_text(effort)
            )
        }
    }
}

pub(super) fn push_inspector_field(
    fields: &mut Vec<InspectorField>,
    maximum_fields: usize,
    field: impl FnOnce() -> InspectorField,
) {
    if fields.len() < maximum_fields {
        fields.push(field());
    }
}

pub(super) fn inspector_timing<Step: StepProjection>(
    step: &Step,
) -> Option<&crate::workflow::run_view_model::WorkflowRunElapsed> {
    if matches!(
        step.state(),
        StepStateKind::Pending
            | StepStateKind::Blocked
            | StepStateKind::Skipped
            | StepStateKind::NotRun
    ) {
        None
    } else {
        step.timing()
    }
}

pub(super) fn live_inspector_fact(fact: Option<&ObservedStepTransition>) -> Option<InspectorField> {
    match fact? {
        ObservedStepTransition::Recovery {
            active,
            configured_rounds,
            handler_kind,
            handler_state,
            decision,
            ..
        } => Some(InspectorField::new(
            "recovery",
            recovery_progress_detail(
                *active,
                *configured_rounds,
                *handler_kind,
                *handler_state,
                *decision,
            ),
            Tone::Active,
        )),
        ObservedStepTransition::Failed { detail } => Some(InspectorField::new(
            "failure",
            canonical_failure_detail(detail),
            Tone::Failure,
        )),
        ObservedStepTransition::Blocked { detail } => Some(InspectorField::new(
            "prerequisites",
            canonical_blocked_detail(detail),
            Tone::Blocked,
        )),
        ObservedStepTransition::Skipped { detail } => Some(InspectorField::new(
            "condition",
            crate::workflow::archived_presentation::condition_false_detail(detail),
            Tone::Muted,
        )),
        ObservedStepTransition::NotRun { detail } => Some(InspectorField::new(
            "not run",
            crate::workflow::presentation::snake_case_debug(detail.code),
            Tone::Muted,
        )),
        ObservedStepTransition::Cancelling { detail }
        | ObservedStepTransition::Cancelled { detail } => Some(InspectorField::new(
            "cancellation",
            cancellation_reason(detail.code),
            Tone::Blocked,
        )),
        ObservedStepTransition::OutputsCommitted { .. } => None,
    }
}

pub(super) fn output_disposition(
    disposition: WorkflowRunOutputDisposition,
) -> (String, Tone, &'static str, Tone) {
    match disposition {
        WorkflowRunOutputDisposition::Pending => {
            ("pending".to_owned(), Tone::Muted, "○", Tone::Muted)
        }
        WorkflowRunOutputDisposition::Committed => {
            ("captured".to_owned(), Tone::Success, "✓", Tone::Success)
        }
        WorkflowRunOutputDisposition::Unavailable(reason) => {
            let reason = match reason {
                WorkflowRunOutputUnavailableReason::Failed => "failed",
                WorkflowRunOutputUnavailableReason::Blocked => "blocked",
                WorkflowRunOutputUnavailableReason::Skipped => "skipped",
                WorkflowRunOutputUnavailableReason::NotRun => "not-run",
                WorkflowRunOutputUnavailableReason::Cancelled => "cancelled",
            };
            (
                format!("unavailable ({reason})"),
                Tone::Blocked,
                "–",
                Tone::Blocked,
            )
        }
    }
}

pub(super) fn output_description(output: &WorkflowOutput) -> (&'static str, String) {
    (semantic_output_kind(output), "—".to_owned())
}

pub(super) fn semantic_output_kind(output: &WorkflowOutput) -> &'static str {
    match output {
        WorkflowOutput::TextPath { .. } | WorkflowOutput::TextAgentResponse => "text",
        WorkflowOutput::JsonPath { .. } | WorkflowOutput::JsonAgentResult { .. } => "json",
        WorkflowOutput::FilePath { .. } => "file",
        WorkflowOutput::GitBranchWorkspace => "git_branch",
    }
}

pub(super) fn summarize_repeated_values(values: &[String], maximum_width: usize) -> String {
    let Some(first) = values.first() else {
        return "none".to_owned();
    };
    if values.len() == 1 {
        return visible_text(first);
    }

    let mut complete = String::new();
    let mut complete_width = 0_usize;
    let mut prefix_boundaries = Vec::new();
    for (index, value) in values.iter().enumerate() {
        let value = visible_text(value);
        if index != 0 {
            complete.push_str(", ");
            complete_width = complete_width.saturating_add(2);
        }
        complete.push_str(&value);
        complete_width = complete_width.saturating_add(display_width(&value));
        prefix_boundaries.push((complete.len(), complete_width));
        if complete_width > maximum_width {
            break;
        }
        if index + 1 == values.len() {
            return complete;
        }
    }

    for included_count in (1..prefix_boundaries.len()).rev() {
        let (byte_length, prefix_width) = prefix_boundaries[included_count - 1];
        let suffix = format!(", +{} more", values.len() - included_count);
        if prefix_width.saturating_add(display_width(&suffix)) <= maximum_width {
            complete.truncate(byte_length);
            complete.push_str(&suffix);
            return complete;
        }
    }

    let suffix = format!(", +{} more", values.len() - 1);
    let first = visible_text(first);
    let prefix_width = maximum_width.saturating_sub(display_width(&suffix));
    format!("{}{suffix}", ellipsize(&first, prefix_width))
}

pub(super) fn inspector_field_line(
    field: &InspectorField,
    width: u16,
    color: bool,
) -> Line<'static> {
    Line::from(inspector_field_spans(field, usize::from(width), color))
}

pub(super) fn inspector_field_spans(
    field: &InspectorField,
    maximum_width: usize,
    color: bool,
) -> Vec<Span<'static>> {
    let label_width = INSPECTOR_LABEL_WIDTH.min(maximum_width);
    let label = padded_text(field.label, label_width);
    let value = if label_width < maximum_width {
        ellipsize(&field.value, maximum_width - label_width)
    } else {
        String::new()
    };
    let mut spans = vec![Span::styled(label, tone_style(color, Tone::Muted))];
    if !value.is_empty() {
        spans.push(Span::styled(value, tone_style(color, field.tone)));
    }
    spans
}

pub(super) fn inspector_output_lines(
    outputs: &[InspectorOutput],
    width: u16,
    height: u16,
    color: bool,
) -> Vec<Line<'static>> {
    let available_rows = usize::from(height);
    if available_rows == 0 {
        return Vec::new();
    }

    let mut lines = vec![Line::from(Span::styled(
        "OUTPUTS",
        tone_style(color, Tone::Muted),
    ))];
    if outputs.is_empty() {
        if available_rows >= 3 {
            lines.push(Line::default());
        }
        if lines.len() < available_rows {
            lines.push(Line::from(Span::styled(
                "·  —  none declared",
                tone_style(color, Tone::Muted),
            )));
        }
        if lines.len() < available_rows {
            lines.push(Line::default());
        }
        return lines;
    }

    if available_rows >= 4 {
        lines.push(Line::default());
    }
    let remaining_rows = available_rows.saturating_sub(lines.len());
    if remaining_rows == 1 {
        if outputs.len() == 1 {
            lines.push(inspector_output_summary_line(&outputs[0], width, color));
        } else {
            lines.push(inspector_outputs_omitted_line(outputs.len(), color));
        }
        return lines;
    }

    let include_gaps = inspector_outputs_desired_height_for_count(outputs.len()) <= available_rows;
    let all_fit = include_gaps || outputs.len().saturating_mul(2) <= remaining_rows;
    let rendered_count = if all_fit {
        outputs.len()
    } else {
        remaining_rows.saturating_sub(1) / 2
    };
    for (index, output) in outputs.iter().take(rendered_count).enumerate() {
        if include_gaps && index != 0 {
            lines.push(Line::default());
        }
        lines.push(inspector_output_summary_line(output, width, color));
        lines.push(inspector_output_detail_line(output, width, color));
    }
    if rendered_count < outputs.len() && lines.len() < available_rows {
        lines.push(inspector_outputs_omitted_line(
            outputs.len() - rendered_count,
            color,
        ));
    } else if lines.len() < available_rows {
        lines.push(Line::default());
    }
    lines
}

pub(super) fn inspector_outputs_desired_height_for_count(output_count: usize) -> usize {
    3_usize
        .saturating_add(output_count.saturating_mul(2))
        .saturating_add(output_count.saturating_sub(1))
}

pub(super) fn inspector_outputs_omitted_line(omitted: usize, color: bool) -> Line<'static> {
    Line::from(Span::styled(
        format!("+{omitted} more outputs"),
        tone_style(color, Tone::Muted),
    ))
}

pub(super) fn inspector_output_summary_line(
    output: &InspectorOutput,
    width: u16,
    color: bool,
) -> Line<'static> {
    let available_width = usize::from(width);
    let disposition_width = output.disposition.as_deref().map_or(0, display_width);
    if disposition_width != 0 && available_width <= disposition_width {
        return Line::from(Span::styled(
            ellipsize(
                output.disposition.as_deref().unwrap_or_default(),
                available_width,
            ),
            tone_style(color, output.tone),
        ));
    }

    let gap_width = usize::from(disposition_width != 0)
        .saturating_mul(2)
        .min(available_width.saturating_sub(disposition_width));
    let summary_width = available_width.saturating_sub(disposition_width + gap_width);
    let marker = format!("{}  ", output.marker);
    let marker_width = display_width(&marker).min(summary_width);
    let mut spans = vec![Span::styled(
        ellipsize(&marker, marker_width),
        tone_style(color, output.marker_tone),
    )];
    let mut used_width = marker_width;
    if used_width < summary_width {
        let kind_width = display_width(output.kind);
        let remaining_width = summary_width - used_width;
        let name_width = if remaining_width > kind_width.saturating_add(2) {
            remaining_width.saturating_sub(kind_width.saturating_add(2))
        } else {
            remaining_width
        };
        let name = ellipsize(&output.name, name_width);
        used_width = used_width.saturating_add(display_width(&name));
        spans.push(Span::styled(name, tone_style(color, Tone::Primary)));
        if summary_width.saturating_sub(used_width) > kind_width.saturating_add(1) {
            spans.push(Span::raw("  "));
            spans.push(Span::styled(output.kind, tone_style(color, Tone::Muted)));
            used_width = used_width.saturating_add(kind_width.saturating_add(2));
        }
    }
    if used_width < summary_width {
        spans.push(Span::raw(" ".repeat(summary_width - used_width)));
    }
    if let Some(disposition) = &output.disposition {
        spans.push(Span::raw(" ".repeat(gap_width)));
        spans.push(Span::styled(
            disposition.clone(),
            tone_style(color, output.tone),
        ));
    }
    Line::from(spans)
}

pub(super) fn inspector_output_detail_line(
    output: &InspectorOutput,
    width: u16,
    color: bool,
) -> Line<'static> {
    let prefix = "   ";
    let detail_width = usize::from(width).saturating_sub(display_width(prefix));
    Line::from(vec![
        Span::raw(prefix),
        Span::styled(
            ellipsize(&output.detail, detail_width),
            tone_style(color, Tone::Muted),
        ),
    ])
}
