use super::*;

pub(super) fn render_log(
    frame: &mut Frame<'_>,
    area: Rect,
    snapshot: &WorkflowRunViewSnapshot,
    interaction: &mut HostInteraction,
    color: bool,
    borders: Borders,
) {
    let Some(step) = snapshot.steps.get(interaction.selected) else {
        render_missing_step_log(frame, area, borders, color);
        return;
    };
    interaction.prepare_filtered_log(&step.log, interaction.selected, snapshot.generation);
    let log = &interaction.filtered_log;
    let records_area = render_log_surface(
        frame,
        area,
        borders,
        Some(LogHeaderState {
            step,
            status: LogTitleStatus::Following,
            filters: interaction.log_filters,
            hidden_records: log.hidden_records,
        }),
        color,
    );
    let lines = log_tail_lines(
        step,
        log,
        usize::from(records_area.width),
        usize::from(records_area.height),
        color,
    );
    frame.render_widget(Paragraph::new(Text::from(lines)), records_area);
}

pub(super) fn render_missing_step_log(
    frame: &mut Frame<'_>,
    area: Rect,
    borders: Borders,
    color: bool,
) {
    let records_area = render_log_surface(frame, area, borders, None, color);
    frame.render_widget(Paragraph::new("No workflow steps."), records_area);
}

pub(super) fn render_full_log(
    frame: &mut Frame<'_>,
    area: Rect,
    step: Option<&WorkflowRunStepView>,
    interaction: &FullLogInteraction,
    log: &FilteredLog,
    filters: LogFilterState,
    color: bool,
) {
    let Some(step) = step else {
        render_missing_step_log(frame, area, Borders::ALL, color);
        return;
    };
    let status = if interaction.follow {
        LogTitleStatus::Following
    } else {
        LogTitleStatus::Paused {
            lines_behind: interaction.lines_behind(log),
        }
    };
    let mut records_area = render_log_surface(
        frame,
        area,
        Borders::TOP,
        Some(LogHeaderState {
            step,
            status,
            filters,
            hidden_records: log.hidden_records,
        }),
        color,
    );

    if step.log.discarded_records != 0 && records_area.height != 0 {
        let marker_area = Rect::new(records_area.x, records_area.y, records_area.width, 1);
        frame.render_widget(
            Paragraph::new(log_eviction_line(
                step.log.discarded_records,
                step.log.discarded_bytes,
                interaction.anchor_clamped,
                color,
            )),
            marker_area,
        );
        records_area.y = records_area.y.saturating_add(1);
        records_area.height = records_area.height.saturating_sub(1);
    }
    if records_area.is_empty() {
        return;
    }
    if log.records.is_empty() {
        frame.render_widget(
            Paragraph::new(Line::from(Span::styled(
                filtered_empty_log_message(step, log.hidden_records),
                tone_style(color, Tone::Muted),
            ))),
            records_area,
        );
        return;
    }

    let available_width = usize::from(records_area.width);
    let top = interaction.top_index(log);
    let lines = log
        .records
        .iter()
        .skip(top)
        .take(usize::from(records_area.height))
        .map(|record| log_record_line(record, available_width, color))
        .collect::<Vec<_>>();
    let horizontal_offset = u16::try_from(interaction.horizontal_offset).unwrap_or(u16::MAX);
    frame.render_widget(
        Paragraph::new(Text::from(lines)).scroll((0, horizontal_offset)),
        records_area,
    );
}

#[derive(Clone, Copy)]
pub(super) enum LogTitleStatus {
    Following,
    Paused { lines_behind: usize },
}

#[derive(Clone, Copy)]
pub(super) struct LogHeaderState<'a> {
    pub(super) step: &'a WorkflowRunStepView,
    pub(super) status: LogTitleStatus,
    pub(super) filters: LogFilterState,
    pub(super) hidden_records: usize,
}

pub(super) fn render_log_surface(
    frame: &mut Frame<'_>,
    area: Rect,
    borders: Borders,
    header: Option<LogHeaderState<'_>>,
    color: bool,
) -> Rect {
    let block = log_block(borders, color);
    let content_area = block.inner(area);
    frame.render_widget(block, area);
    let sections = log_content_areas(content_area);
    render_log_header(frame, sections[0], header, color);
    sections[1]
}

pub(super) fn log_block(borders: Borders, color: bool) -> Block<'static> {
    section_block(borders, color).padding(Padding::horizontal(INSPECTOR_PANEL_PADDING))
}

pub(super) fn log_content_areas(area: Rect) -> [Rect; 2] {
    let rows =
        Layout::vertical([Constraint::Length(LOG_HEADER_HEIGHT), Constraint::Min(0)]).split(area);
    [rows[0], rows[1]]
}

pub(super) fn render_log_header(
    frame: &mut Frame<'_>,
    area: Rect,
    header: Option<LogHeaderState<'_>>,
    color: bool,
) {
    if area.is_empty() {
        return;
    }
    let Some(LogHeaderState {
        step,
        status,
        filters,
        hidden_records,
    }) = header
    else {
        frame.render_widget(
            Paragraph::new(Span::styled("LOG", tone_style(color, Tone::Muted))),
            Rect::new(area.x, area.y, area.width, 1),
        );
        return;
    };

    let full_channels = log_channel_title(step, filters, false, color);
    let compact_channels = log_channel_title(step, filters, true, color);
    let full_status = log_status_title(step, status, hidden_records, false, color);
    let compact_status = log_status_title(step, status, hidden_records, true, color);
    let available_width = usize::from(area.width);
    let fits = |channels: &Line<'_>, status: &Line<'_>| {
        channels
            .width()
            .saturating_add(2)
            .saturating_add(status.width())
            <= available_width
    };
    let (channels, status) = if fits(&full_channels, &full_status) {
        (full_channels, full_status)
    } else if fits(&compact_channels, &full_status) {
        (compact_channels, full_status)
    } else {
        (compact_channels, compact_status)
    };

    let (channel_width, status_area) = header_status_areas(area, &status);
    if channel_width >= 3 {
        frame.render_widget(
            Paragraph::new(channels),
            Rect::new(area.x, area.y, channel_width, 1),
        );
    }
    frame.render_widget(Paragraph::new(status), status_area);
}

pub(super) fn log_channel_title(
    step: &WorkflowRunStepView,
    filters: LogFilterState,
    compact: bool,
    color: bool,
) -> Line<'static> {
    let mut spans = vec![Span::styled("LOG", tone_style(color, Tone::Muted))];
    let separator = if compact { " " } else { "  " };
    for option in log_channel_options(step) {
        let enabled = filters.includes(option.channel);
        let style = if enabled {
            tone_style(color, Tone::Neutral).add_modifier(Modifier::UNDERLINED)
        } else {
            tone_style(color, Tone::Muted).add_modifier(Modifier::DIM)
        };
        let label = if compact {
            option.compact_label
        } else {
            option.label
        };
        spans.push(Span::raw(separator));
        spans.push(Span::styled(format!("{} {label}", option.key), style));
    }
    Line::from(spans)
}

pub(super) fn log_status_title(
    step: &WorkflowRunStepView,
    status: LogTitleStatus,
    hidden_records: usize,
    compact: bool,
    color: bool,
) -> Line<'static> {
    let tone = match status {
        LogTitleStatus::Following => Tone::Active,
        LogTitleStatus::Paused { .. } => Tone::Blocked,
    };
    let text = if compact {
        if hidden_records != 0 {
            match status {
                LogTitleStatus::Following => format!("● {hidden_records} hidden"),
                LogTitleStatus::Paused { lines_behind } => {
                    format!("● {lines_behind} back · {hidden_records} hidden")
                }
            }
        } else if step.log.retained_records != step.log.observed_records {
            format!(
                "● {}/{} kept",
                step.log.retained_records, step.log.observed_records
            )
        } else {
            match status {
                LogTitleStatus::Following => {
                    let line_label = if step.log.observed_records == 1 {
                        "line"
                    } else {
                        "lines"
                    };
                    format!("● {} {line_label}", step.log.observed_records)
                }
                LogTitleStatus::Paused { lines_behind } => {
                    format!("● {lines_behind} behind")
                }
            }
        }
    } else {
        let status = match status {
            LogTitleStatus::Following => "following".to_owned(),
            LogTitleStatus::Paused { lines_behind } => {
                let line_label = if lines_behind == 1 { "line" } else { "lines" };
                format!("paused · {lines_behind} {line_label} behind")
            }
        };
        let count = if hidden_records != 0 {
            format!("{hidden_records} hidden")
        } else if step.log.retained_records == step.log.observed_records {
            let line_label = if step.log.observed_records == 1 {
                "line"
            } else {
                "lines"
            };
            format!("{} {line_label}", step.log.observed_records)
        } else {
            format!(
                "{} retained / {} total",
                step.log.retained_records, step.log.observed_records
            )
        };
        format!("● {status} · {count}")
    };
    Line::from(Span::styled(text, tone_style(color, tone)))
}

pub(super) fn log_tail_lines(
    step: &WorkflowRunStepView,
    log: &FilteredLog,
    available_width: usize,
    available_rows: usize,
    color: bool,
) -> Vec<Line<'static>> {
    if available_rows == 0 {
        return Vec::new();
    }

    let mut lines = Vec::new();
    let tail_rows = if step.log.discarded_records == 0 {
        available_rows
    } else {
        lines.push(log_eviction_line(
            step.log.discarded_records,
            step.log.discarded_bytes,
            false,
            color,
        ));
        available_rows.saturating_sub(1)
    };
    if tail_rows == 0 {
        return lines;
    }

    if log.records.is_empty() {
        lines.push(Line::from(Span::styled(
            filtered_empty_log_message(step, log.hidden_records),
            tone_style(color, Tone::Muted),
        )));
        return lines;
    }

    let mut remaining_rows = tail_rows;
    let mut newest_first_record_lines = Vec::new();
    for record in log.records.iter().rev() {
        let record_lines = log_record_tail_lines(record, available_width, remaining_rows, color);
        remaining_rows = remaining_rows.saturating_sub(record_lines.len());
        newest_first_record_lines.push(record_lines);
        if remaining_rows == 0 {
            break;
        }
    }
    for record_lines in newest_first_record_lines.into_iter().rev() {
        lines.extend(record_lines);
    }
    lines
}

pub(super) fn log_eviction_line(
    discarded_records: u64,
    discarded_bytes: u64,
    anchor_clamped: bool,
    color: bool,
) -> Line<'static> {
    let line_label = if discarded_records == 1 {
        "line"
    } else {
        "lines"
    };
    let byte_label = if discarded_bytes == 1 {
        "byte"
    } else {
        "bytes"
    };
    let clamp_notice = if anchor_clamped {
        " | clamped to retained top"
    } else {
        ""
    };
    Line::from(Span::styled(
        format!(
            "↑ {discarded_records} older {line_label} / {discarded_bytes} {byte_label} discarded{clamp_notice}"
        ),
        tone_style(color, Tone::Muted),
    ))
}

pub(super) fn filtered_empty_log_message(
    step: &WorkflowRunStepView,
    hidden_records: usize,
) -> &'static str {
    if hidden_records != 0 {
        "All log channels hidden."
    } else {
        empty_log_message(step.state)
    }
}

pub(super) fn empty_log_message(state: StepStateKind) -> &'static str {
    match state {
        StepStateKind::Pending => "Waiting for this step to start.",
        StepStateKind::Starting
        | StepStateKind::Running
        | StepStateKind::CapturingOutputs
        | StepStateKind::Recovering
        | StepStateKind::Cancelling => "Waiting for output…",
        StepStateKind::Succeeded
        | StepStateKind::Inherited
        | StepStateKind::Failed
        | StepStateKind::Blocked
        | StepStateKind::Skipped
        | StepStateKind::NotRun
        | StepStateKind::Cancelled => "No output received.",
    }
}

pub(super) fn log_record_tail_lines(
    record: &WorkflowRunLogRecord,
    available_width: usize,
    maximum_rows: usize,
    color: bool,
) -> Vec<Line<'static>> {
    let gutter = LogGutter::for_width(available_width);
    let content_width = available_width.saturating_sub(gutter.width()).max(1);
    wrap_log_payload_tail(&record.payload, content_width, maximum_rows)
        .into_iter()
        .enumerate()
        .map(|(visible_index, (is_first_line, payload))| {
            let row_kind = if is_first_line {
                LogRowKind::for_record(record)
            } else if visible_index == 0 {
                LogRowKind::ClippedVisualContinuation
            } else {
                LogRowKind::VisualContinuation
            };
            log_line(record, payload, gutter, row_kind, color)
        })
        .collect()
}

pub(super) fn log_record_line(
    record: &WorkflowRunLogRecord,
    available_width: usize,
    color: bool,
) -> Line<'static> {
    log_line(
        record,
        record.payload.to_string(),
        LogGutter::for_width(available_width),
        LogRowKind::for_record(record),
        color,
    )
}

pub(super) fn log_line(
    record: &WorkflowRunLogRecord,
    payload: String,
    gutter: LogGutter,
    row_kind: LogRowKind,
    color: bool,
) -> Line<'static> {
    let mut spans = gutter.spans(record, row_kind, color);
    spans.push(Span::styled(
        payload,
        log_payload_style(record.source, color),
    ));
    Line::from(spans)
}

pub(super) fn wrap_log_payload(payload: &str, maximum_width: usize) -> Vec<String> {
    let mut lines = Vec::new();
    for_each_wrapped_log_payload(payload, maximum_width, |_, line| lines.push(line));
    lines
}

pub(super) fn wrap_log_payload_tail(
    payload: &str,
    maximum_width: usize,
    maximum_rows: usize,
) -> VecDeque<(bool, String)> {
    if maximum_rows == 0 {
        return VecDeque::new();
    }

    let mut lines = VecDeque::with_capacity(maximum_rows);
    for_each_wrapped_log_payload(payload, maximum_width, |is_first_line, line| {
        if lines.len() == maximum_rows {
            lines.pop_front();
        }
        lines.push_back((is_first_line, line));
    });
    lines
}

pub(super) fn for_each_wrapped_log_payload(
    payload: &str,
    maximum_width: usize,
    mut emit: impl FnMut(bool, String),
) {
    if payload.is_empty() {
        emit(true, String::new());
        return;
    }

    let mut line = String::new();
    let mut line_width = 0_usize;
    let mut is_first_line = true;
    for grapheme in payload.graphemes(true) {
        let grapheme_width = display_width(grapheme);
        if !line.is_empty() && line_width.saturating_add(grapheme_width) > maximum_width {
            emit(is_first_line, std::mem::take(&mut line));
            is_first_line = false;
            line_width = 0;
        }
        line.push_str(grapheme);
        line_width = line_width.saturating_add(grapheme_width);
    }
    if !line.is_empty() {
        emit(is_first_line, line);
    }
}

#[derive(Clone, Copy)]
pub(super) enum LogRowKind {
    Record,
    SafetyContinuation,
    VisualContinuation,
    ClippedVisualContinuation,
}

impl LogRowKind {
    pub(super) const fn for_record(record: &WorkflowRunLogRecord) -> Self {
        if record.continuation {
            Self::SafetyContinuation
        } else {
            Self::Record
        }
    }
}

#[derive(Clone, Copy)]
pub(super) struct LogSourcePresentation {
    pub(super) label: &'static str,
    pub(super) source_tone: Tone,
    pub(super) payload_tone: Tone,
    pub(super) dim_payload: bool,
}

pub(super) const fn log_source_presentation(source: WorkflowRunLogSource) -> LogSourcePresentation {
    let (label, source_tone, payload_tone, dim_payload) = match source {
        WorkflowRunLogSource::Command(CommandOutputSource::StandardOutput) => {
            ("stdout", Tone::Muted, Tone::Neutral, false)
        }
        WorkflowRunLogSource::Command(CommandOutputSource::StandardError) => {
            ("stderr", Tone::Blocked, Tone::Neutral, false)
        }
        WorkflowRunLogSource::Agent(AgentPresentationObservationKind::Assistant) => {
            ("agent", Tone::Active, Tone::Primary, false)
        }
        WorkflowRunLogSource::Agent(AgentPresentationObservationKind::Reasoning) => {
            ("reason", Tone::Muted, Tone::Muted, true)
        }
        WorkflowRunLogSource::Agent(AgentPresentationObservationKind::ToolCall) => {
            ("tool", Tone::Active, Tone::Neutral, false)
        }
        WorkflowRunLogSource::Agent(AgentPresentationObservationKind::ToolResult) => {
            ("result", Tone::Muted, Tone::Muted, true)
        }
        WorkflowRunLogSource::Agent(AgentPresentationObservationKind::Diagnostic) => {
            ("diag", Tone::Blocked, Tone::Neutral, false)
        }
        WorkflowRunLogSource::Agent(AgentPresentationObservationKind::Usage) => {
            ("usage", Tone::Muted, Tone::Muted, true)
        }
        WorkflowRunLogSource::Agent(AgentPresentationObservationKind::Model) => {
            ("model", Tone::Muted, Tone::Muted, true)
        }
        WorkflowRunLogSource::Agent(AgentPresentationObservationKind::Lifecycle) => {
            ("life", Tone::Muted, Tone::Muted, true)
        }
        WorkflowRunLogSource::Agent(AgentPresentationObservationKind::ValueRejected) => {
            ("reject", Tone::Failure, Tone::Neutral, false)
        }
        WorkflowRunLogSource::Agent(AgentPresentationObservationKind::HarnessEvent) => {
            ("event", Tone::Muted, Tone::Muted, true)
        }
    };
    LogSourcePresentation {
        label,
        source_tone,
        payload_tone,
        dim_payload,
    }
}

pub(super) fn log_payload_style(source: WorkflowRunLogSource, color: bool) -> Style {
    let presentation = log_source_presentation(source);
    let style = tone_style(color, presentation.payload_tone);
    if presentation.dim_payload {
        style.add_modifier(Modifier::DIM)
    } else {
        style
    }
}

pub(super) fn log_source_style(source: WorkflowRunLogSource, color: bool) -> Style {
    tone_style(color, log_source_presentation(source).source_tone)
}

#[derive(Clone, Copy)]
pub(super) struct LogGutter {
    pub(super) timestamp: bool,
}

impl LogGutter {
    pub(super) fn for_width(available_width: usize) -> Self {
        Self {
            timestamp: available_width
                >= LOG_TIMESTAMPED_GUTTER_WIDTH + MINIMUM_TIMESTAMPED_LOG_CONTENT_WIDTH,
        }
    }

    pub(super) const fn width(self) -> usize {
        if self.timestamp {
            LOG_TIMESTAMPED_GUTTER_WIDTH
        } else {
            LOG_SOURCE_GUTTER_WIDTH
        }
    }

    pub(super) fn spans(
        self,
        record: &WorkflowRunLogRecord,
        row_kind: LogRowKind,
        color: bool,
    ) -> Vec<Span<'static>> {
        let mut spans = Vec::new();
        let visual_continuation = matches!(
            row_kind,
            LogRowKind::VisualContinuation | LogRowKind::ClippedVisualContinuation
        );
        if self.timestamp {
            let timestamp = if visual_continuation {
                " ".repeat(LOG_TIMESTAMP_WIDTH)
            } else {
                log_timestamp(record.observed_at)
            };
            spans.push(Span::styled(timestamp, tone_style(color, Tone::Muted)));
            spans.push(Span::raw(" "));
        }
        let source = log_source_presentation(record.source);
        let source_style = log_source_style(record.source, color);
        let source_label = if matches!(row_kind, LogRowKind::VisualContinuation) {
            " ".repeat(LOG_SOURCE_WIDTH)
        } else {
            format!("{:<LOG_SOURCE_WIDTH$}", source.label)
        };
        spans.push(Span::styled(source_label, source_style));
        let marker = match row_kind {
            LogRowKind::Record => " │ ",
            LogRowKind::SafetyContinuation => " ↪ ",
            LogRowKind::VisualContinuation | LogRowKind::ClippedVisualContinuation => " ↳ ",
        };
        spans.push(Span::styled(marker, source_style));
        spans
    }
}

pub(super) fn log_timestamp(observed_at: time::OffsetDateTime) -> String {
    let utc = observed_at.to_offset(UtcOffset::UTC);
    format!(
        "{:02}:{:02}:{:02}.{:03}",
        utc.hour(),
        utc.minute(),
        utc.second(),
        utc.millisecond()
    )
}
