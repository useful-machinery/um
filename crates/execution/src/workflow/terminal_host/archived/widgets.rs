use super::*;

pub(super) fn render_archived(
    frame: &mut Frame<'_>,
    view: &ArchivedTerminalView,
    graph: &DagLayout,
    interaction: &mut ArchivedHostInteraction,
    color: bool,
) {
    let area = frame.area();
    interaction.terminal_area = area;
    clamp_step_selection(&mut interaction.selected, view.steps.len());
    frame.render_widget(Clear, area);
    if !operational_area(area) {
        render_archived_too_small(frame, area, color);
        return;
    }

    let sections =
        Layout::vertical([Constraint::Min(0), Constraint::Length(FOOTER_HEIGHT)]).split(area);
    if interaction.surface == HostSurface::FullLog {
        let selected_step = view.steps.get(interaction.selected);
        if selected_step.is_some() {
            interaction.synchronize_output(view);
        }
        let full_sections =
            inspector_and_log_areas(sections[0], inspector_desired_height(selected_step));
        render_inspector(frame, full_sections[0], selected_step, color, Borders::NONE);
        render_archived_full_output(
            frame,
            full_sections[1],
            selected_step,
            &interaction.output,
            color,
        );
        render_archived_footer(
            frame,
            sections[1],
            color,
            "OUTPUT",
            &FULL_LOG_FOOTER_OPTIONS,
        );
    } else {
        render_archived_split(frame, sections[0], view, graph, interaction, color);
        render_archived_footer(frame, sections[1], color, "DAG", &SPLIT_FOOTER_OPTIONS);
        render_split_footer_junction(
            frame,
            sections[0],
            sections[1].y,
            interaction.help_visible,
            color,
            archived_wide_split_columns(sections[0]),
        );
    }

    if interaction.help_visible {
        render_help_overlay_groups(
            frame,
            sections[0],
            archived_help_groups(interaction.surface),
            color,
        );
    }
}

pub(super) fn archived_wide_split_columns(area: Rect) -> [Rect; 2] {
    let columns = Layout::horizontal([
        Constraint::Percentage(ARCHIVED_WORKFLOW_COLUMN_PERCENTAGE),
        Constraint::Percentage(100 - ARCHIVED_WORKFLOW_COLUMN_PERCENTAGE),
    ])
    .split(area);
    [columns[0], columns[1]]
}

pub(super) fn render_archived_split(
    frame: &mut Frame<'_>,
    area: Rect,
    view: &ArchivedTerminalView,
    graph: &DagLayout,
    interaction: &ArchivedHostInteraction,
    color: bool,
) {
    let selected_step = view.steps.get(interaction.selected);
    let layout = split_body_layout(
        area,
        archived_summary_height(view),
        inspector_desired_height(selected_step),
        archived_wide_split_columns(area),
    );
    render_archived_summary(frame, layout.summary, view, color, layout.summary_borders());
    render_split_steps(
        frame,
        layout,
        &view.steps,
        graph,
        view.phase_boundary,
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
    render_archived_output_preview(frame, layout.output, selected_step, color, Borders::TOP);
    render_split_body_junctions(frame, layout, color);
}

pub(super) fn archived_summary_height(view: &ArchivedTerminalView) -> u16 {
    u16::try_from(view.summary.len())
        .unwrap_or(u16::MAX)
        .saturating_add(1)
}

pub(super) fn render_archived_summary(
    frame: &mut Frame<'_>,
    area: Rect,
    view: &ArchivedTerminalView,
    color: bool,
    borders: Borders,
) {
    let block = summary_block(borders, color);
    let content = block.inner(area);
    let lines = view
        .summary
        .iter()
        .map(|line| {
            Line::from(Span::styled(
                ellipsize(&line.text, usize::from(content.width)),
                tone_style(color, line.tone),
            ))
        })
        .collect::<Vec<_>>();
    frame.render_widget(Paragraph::new(lines).block(block), area);
}

pub(super) fn render_archived_too_small(frame: &mut Frame<'_>, area: Rect, color: bool) {
    frame.render_widget(
        Paragraph::new(vec![
            Line::from(Span::styled(
                "Terminal too small",
                tone_style(color, Tone::Failure),
            )),
            Line::from(format!(
                "Resize to at least {MINIMUM_WIDTH}x{MINIMUM_HEIGHT}."
            )),
            Line::from("Press q to quit or Ctrl-C to interrupt the viewer."),
        ])
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(separator_style(color))
                .title(" Scherzo archived workflow attempt "),
        ),
        area,
    );
}

pub(super) fn render_archived_output_preview(
    frame: &mut Frame<'_>,
    area: Rect,
    step: Option<&ArchivedTerminalStepView>,
    color: bool,
    borders: Borders,
) {
    let block = section_block(borders, color).padding(Padding::horizontal(INSPECTOR_PANEL_PADDING));
    let content = block.inner(area);
    frame.render_widget(block, area);
    let Some(step) = step else {
        frame.render_widget(Paragraph::new("No workflow steps."), content);
        return;
    };
    match &step.output {
        ArchivedCommandOutputView::Missing => {
            frame.render_widget(
                Paragraph::new(vec![
                    Line::from(Span::styled(
                        "RETAINED OUTPUT",
                        tone_style(color, Tone::Muted),
                    )),
                    Line::default(),
                    Line::from("No durable command-stream prefixes exist."),
                ]),
                content,
            );
        }
        ArchivedCommandOutputView::Present { stdout, stderr } => {
            let regions =
                Layout::vertical([Constraint::Percentage(50), Constraint::Percentage(50)])
                    .split(content);
            render_archived_stream_preview(frame, regions[0], stdout, color);
            render_archived_stream_preview(frame, regions[1], stderr, color);
        }
    }
}

pub(super) fn render_archived_stream_preview(
    frame: &mut Frame<'_>,
    area: Rect,
    stream: &ArchivedStreamView,
    color: bool,
) {
    if area.is_empty() {
        return;
    }
    let mut lines = vec![Line::from(Span::styled(
        ellipsize(
            &archived_stream_summary(stream, area.width < 100),
            usize::from(area.width),
        ),
        tone_style(color, archived_stream_tone(stream)),
    ))];
    let payload_rows = usize::from(area.height).saturating_sub(1);
    if payload_rows != 0 {
        if stream.records.is_empty() {
            lines.push(Line::from(Span::styled(
                "empty retained prefix",
                tone_style(color, Tone::Muted),
            )));
        } else {
            let payload_width = usize::from(area.width).max(1);
            lines.extend(
                stream
                    .records
                    .iter()
                    .flat_map(|record| wrap_archived_record(record, payload_width))
                    .take(payload_rows)
                    .map(|payload| {
                        Line::from(Span::styled(payload, tone_style(color, Tone::Neutral)))
                    }),
            );
        }
    }
    frame.render_widget(Paragraph::new(lines), area);
}

pub(super) fn wrap_archived_record(record: &NormalizedRetainedRecord, width: usize) -> Vec<String> {
    let prefix = if record.continuation { "↪ " } else { "" };
    let payload_width = width.saturating_sub(display_width(prefix)).max(1);
    wrap_log_payload(&record.payload, payload_width)
        .into_iter()
        .enumerate()
        .map(|(index, payload)| {
            if index == 0 {
                format!("{prefix}{payload}")
            } else {
                format!("↳ {payload}")
            }
        })
        .collect()
}

pub(super) fn archived_stream_summary(stream: &ArchivedStreamView, compact: bool) -> String {
    let source = match (stream.source, compact) {
        (ArchivedStreamSource::StandardOutput, false) => "STDOUT",
        (ArchivedStreamSource::StandardError, false) => "STDERR",
        (ArchivedStreamSource::StandardOutput, true) => "OUT",
        (ArchivedStreamSource::StandardError, true) => "ERR",
    };
    if compact {
        format!(
            "{source} r={}B d={}B trunc={} drain={}",
            stream.retained_bytes,
            stream.discarded_bytes,
            yes_no(stream.truncated),
            if stream.fully_drained {
                "EOF"
            } else {
                "incomplete"
            },
        )
    } else {
        format!(
            "{source} · retained {} B · discarded {} B · truncated {} · fully drained {}{}",
            stream.retained_bytes,
            stream.discarded_bytes,
            yes_no(stream.truncated),
            yes_no(stream.fully_drained),
            if stream.fully_drained {
                ""
            } else {
                " (incomplete drain)"
            },
        )
    }
}

pub(super) fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}

pub(super) fn archived_stream_tone(stream: &ArchivedStreamView) -> Tone {
    if stream.truncated || !stream.fully_drained {
        Tone::Blocked
    } else {
        Tone::Muted
    }
}

pub(super) struct ArchivedOutputRow {
    pub(super) text: String,
    pub(super) tone: Tone,
}

pub(super) fn output_document(output: &ArchivedCommandOutputView) -> Vec<ArchivedOutputRow> {
    let ArchivedCommandOutputView::Present { stdout, stderr } = output else {
        return vec![ArchivedOutputRow {
            text: "No durable command-stream prefixes exist for this step.".to_owned(),
            tone: Tone::Muted,
        }];
    };
    let mut rows = vec![ArchivedOutputRow {
        text: "Stdout is shown before stderr for layout only; cross-stream order is unavailable."
            .to_owned(),
        tone: Tone::Blocked,
    }];
    append_stream_document(&mut rows, stdout);
    rows.push(ArchivedOutputRow {
        text: String::new(),
        tone: Tone::Muted,
    });
    append_stream_document(&mut rows, stderr);
    rows
}

pub(super) fn append_stream_document(
    rows: &mut Vec<ArchivedOutputRow>,
    stream: &ArchivedStreamView,
) {
    rows.push(ArchivedOutputRow {
        text: match stream.source {
            ArchivedStreamSource::StandardOutput => "RETAINED STDOUT PREFIX".to_owned(),
            ArchivedStreamSource::StandardError => "RETAINED STDERR PREFIX".to_owned(),
        },
        tone: Tone::Primary,
    });
    rows.push(ArchivedOutputRow {
        text: archived_stream_summary(stream, false),
        tone: archived_stream_tone(stream),
    });
    if stream.records.is_empty() {
        rows.push(ArchivedOutputRow {
            text: "empty retained prefix".to_owned(),
            tone: Tone::Muted,
        });
    } else {
        rows.extend(stream.records.iter().map(|record| ArchivedOutputRow {
            text: format!(
                "{}{}",
                if record.continuation { "↪ " } else { "" },
                record.payload
            ),
            tone: Tone::Neutral,
        }));
    }
    if stream.unterminated {
        rows.push(ArchivedOutputRow {
            text: "⟂ retained-prefix boundary (final fragment has no line ending)".to_owned(),
            tone: Tone::Muted,
        });
    }
}

pub(super) fn archived_output_dimensions(
    area: Rect,
    step: &ArchivedTerminalStepView,
) -> (usize, usize) {
    let body =
        archived_output_block(Borders::TOP, false).inner(selected_lower_panel_area(area, step));
    let rows = archived_output_areas(body)[1];
    (usize::from(rows.width), usize::from(rows.height))
}

pub(super) fn render_archived_full_output(
    frame: &mut Frame<'_>,
    area: Rect,
    step: Option<&ArchivedTerminalStepView>,
    interaction: &ArchivedOutputInteraction,
    color: bool,
) {
    let block = archived_output_block(Borders::TOP, color);
    let content = block.inner(area);
    frame.render_widget(block, area);
    let sections = archived_output_areas(content);
    let title = step.map_or_else(
        || "RETAINED OUTPUT".to_owned(),
        |step| format!("RETAINED OUTPUT · {}", step.id),
    );
    frame.render_widget(
        Paragraph::new(Span::styled(
            ellipsize(&title, usize::from(sections[0].width)),
            tone_style(color, Tone::Muted),
        )),
        sections[0],
    );
    let Some(step) = step else {
        frame.render_widget(Paragraph::new("No workflow steps."), sections[1]);
        return;
    };
    let lines = step
        .document
        .iter()
        .skip(interaction.top)
        .take(usize::from(sections[1].height))
        .map(|row| Line::from(Span::styled(row.text.clone(), tone_style(color, row.tone))))
        .collect::<Vec<_>>();
    frame.render_widget(
        Paragraph::new(lines).scroll((
            0,
            u16::try_from(interaction.horizontal_offset).unwrap_or(u16::MAX),
        )),
        sections[1],
    );
}

pub(super) fn archived_output_block(borders: Borders, color: bool) -> Block<'static> {
    section_block(borders, color).padding(Padding::horizontal(INSPECTOR_PANEL_PADDING))
}

pub(super) fn archived_output_areas(area: Rect) -> [Rect; 2] {
    let rows =
        Layout::vertical([Constraint::Length(LOG_HEADER_HEIGHT), Constraint::Min(0)]).split(area);
    [rows[0], rows[1]]
}

pub(super) fn render_archived_footer(
    frame: &mut Frame<'_>,
    area: Rect,
    color: bool,
    label: &'static str,
    options: &[&[HelpCommand]],
) {
    let options = options
        .iter()
        .map(|commands| {
            let mut commands = commands
                .iter()
                .copied()
                .filter(|command| command.keys != "F")
                .collect::<Vec<_>>();
            commands.push(help("q", "quit"));
            commands.push(help("?", "help"));
            commands
        })
        .collect::<Vec<_>>();
    let reserved_width = u16::try_from(display_width(label).saturating_add(4)).unwrap_or(u16::MAX);
    let commands = fitting_footer(options, area.width.saturating_sub(reserved_width));
    render_footer_text(frame, area, label, commands, color);
}

pub(super) fn archived_help_groups(surface: HostSurface) -> Vec<HelpGroup> {
    let mut groups = surface_help_groups(surface, OutputHelpMode::Archived);
    groups.push(HelpGroup {
        title: "VIEWER",
        commands: vec![
            HelpCommand {
                keys: "q",
                description: "quit",
            },
            HelpCommand {
                keys: "^C",
                description: "interrupt",
            },
        ],
    });
    groups
}
