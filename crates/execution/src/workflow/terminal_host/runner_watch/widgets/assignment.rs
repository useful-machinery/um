//! An opened assignment's surface (REQ-RUNNERENROLL-146): the status, facts and URL rows
//! above the run interface's split view, whose header names the open run, and the log
//! pane's preview.

use super::super::assignment::{AssignmentScreen, OpenAssignment, ShownLog};
use super::super::log_document::LogDocument;
use super::super::run_steps::step_state_kind;
use super::*;

/// The run's header in the summary's place: one row and the block's rule. Beside the
/// inspector of the wide layout it takes the inspector's title's rows, so that their rules
/// meet.
const RUN_HEADER_HEIGHT: u16 = 2;
/// The phases in which an assignment's run has ended and the runner reports it.
const ENDING_PHASES: [&str; 2] = ["finishing", "reporting"];
const URL_LABEL: &str = "cloud ";

pub(super) fn render_assignment(
    frame: &mut Frame<'_>,
    screen: &RunnerWatchScreen,
    open: &mut OpenAssignment,
    shown: &AssignmentScreen,
    help_visible: bool,
    color: bool,
) {
    if open.full_log.is_some() {
        render_full_log(frame, screen, open, shown, help_visible, color);
        return;
    }
    let sections = Layout::vertical([Constraint::Min(0), Constraint::Length(FOOTER_HEIGHT)])
        .split(frame.area());
    let body = sections[0];
    let width = usize::from(body.width.saturating_sub(2));
    let mut head = status_line(
        screen,
        ending_line(screen, shown, width, color),
        width,
        color,
    )
    .into_iter()
    .collect::<Vec<_>>();
    head.push(facts_line(&shown.entry, width, color));
    head.extend(url_line(&shown.entry, width, color));
    // With the facts row alone the panes have the run interface's rows at this size.
    let panes = Rect::new(
        body.x,
        body.y.saturating_add(1),
        body.width,
        body.height.saturating_sub(1),
    );
    let further = u16::try_from(head.len().saturating_sub(1)).unwrap_or(u16::MAX);
    let path = open.path();
    let wanted = open.wanted_step(shown);
    let further = match shown.run_at(&path) {
        Some(drawn) => {
            let steps = &drawn.run.steps;
            let selected = open.selected_index(&drawn.run);
            let selected_step = steps.get(selected);
            let columns = wide_split_columns(panes);
            let header_height = if panes.width >= WIDE_LAYOUT_WIDTH {
                INSPECTOR_HEADER_HEIGHT
            } else {
                RUN_HEADER_HEIGHT
            };
            let mut layout = split_body_layout(
                panes,
                header_height,
                inspector_desired_height(selected_step),
                columns,
            );
            let further = yield_rows(&mut layout, further);
            render_run_header(
                frame,
                layout.summary,
                layout.summary_borders(),
                shown,
                Some(&drawn.run),
                &path,
                color,
            );
            render_split_steps(
                frame,
                layout,
                steps,
                &drawn.graph,
                drawn.phase_boundary,
                selected,
                color,
            );
            render_inspector(
                frame,
                layout.inspector,
                selected_step,
                color,
                layout.inspector_borders(),
            );
            let (heading, document) = log_heading(
                shown,
                &path,
                wanted.as_deref(),
                wanted_in_flight(wanted.as_deref(), selected_step),
            );
            render_log_preview(frame, layout.output, &heading, document, color);
            render_split_body_junctions(frame, layout, color);
            render_split_footer_junction(frame, panes, sections[1].y, help_visible, color, columns);
            further
        }
        None => {
            // A run never yet given: its header, that its steps are not available, and the
            // log pane when the answer has a log.
            let rows = Layout::vertical([
                Constraint::Length(further),
                Constraint::Length(RUN_HEADER_HEIGHT),
                Constraint::Length(1),
                Constraint::Min(0),
            ])
            .split(panes);
            render_run_header(frame, rows[1], Borders::BOTTOM, shown, None, &path, color);
            frame.render_widget(
                Paragraph::new(Span::styled(
                    "  the run's steps are not available",
                    tone_style(color, Tone::Muted),
                )),
                rows[2],
            );
            let (heading, document) = log_heading(shown, &path, None, false);
            if document.is_some() {
                render_log_preview(frame, rows[3], &heading, document, color);
            }
            further
        }
    };
    let head_height = further.saturating_add(1).min(body.height);
    frame.render_widget(
        Paragraph::new(head),
        Rect::new(
            body.x.saturating_add(1),
            body.y,
            body.width.saturating_sub(2),
            head_height,
        ),
    );
    render_fitting_footer(
        frame,
        sections[1],
        "RUN",
        assignment_footer_options(),
        color,
    );
    if help_visible {
        render_help_overlay_groups(frame, body, assignment_help_groups(), color);
    }
}

/// The log pane full-screen: the status row when there is one, then the selected step's
/// inspector above the log as in the run interface, or the log alone when the run has no
/// steps. The facts and URL rows give way.
fn render_full_log(
    frame: &mut Frame<'_>,
    screen: &RunnerWatchScreen,
    open: &mut OpenAssignment,
    shown: &AssignmentScreen,
    help_visible: bool,
    color: bool,
) {
    let sections = Layout::vertical([Constraint::Min(0), Constraint::Length(FOOTER_HEIGHT)])
        .split(frame.area());
    let mut body = sections[0];
    let width = usize::from(body.width.saturating_sub(2));
    if let Some(status) = status_line(
        screen,
        ending_line(screen, shown, width, color),
        width,
        color,
    ) {
        frame.render_widget(
            Paragraph::new(status),
            Rect::new(
                body.x.saturating_add(1),
                body.y,
                body.width.saturating_sub(2),
                body.height.min(1),
            ),
        );
        body.y = body.y.saturating_add(1);
        body.height = body.height.saturating_sub(1);
    }
    let wanted = open.wanted_step(shown);
    let selected_step = open.selected_step(shown);
    let log_area = match selected_step {
        Some(step) => {
            let areas = inspector_and_log_areas(body, inspector_desired_height(Some(step)));
            render_inspector(frame, areas[0], Some(step), color, Borders::NONE);
            areas[1]
        }
        None => body,
    };
    let (title, document) = log_heading(
        shown,
        &open.path(),
        wanted.as_deref(),
        wanted_in_flight(wanted.as_deref(), selected_step),
    );
    let rows = document.map_or(&[][..], |document| document.rows.as_slice());
    let mut pinned = document.map_or_else(Vec::new, |document| {
        document
            .stream_headings
            .iter()
            .filter_map(|row| document.rows.get(*row))
            .collect()
    });
    let mut dimensions = output_document_dimensions(log_area, pinned.len());
    // The stream headings are rows of the document too: where keeping them in view would
    // leave it no row, as under an inspector that takes every row it may at the minimum
    // size, they move with it and the log keeps the row the run interface's has.
    if dimensions.1 == 0 && !pinned.is_empty() {
        pinned.clear();
        dimensions = output_document_dimensions(log_area, 0);
    }
    let (columns, height) = dimensions;
    let Some(viewport) = &mut open.full_log else {
        return;
    };
    let document_width = document.map_or(0, |document| document.width);
    viewport.synchronize(&document, document_width, columns, height);
    let state = if viewport.following {
        ("● following".to_owned(), Tone::Active)
    } else {
        (
            format!("● paused · {} below", viewport.rows_below(rows.len())),
            Tone::Blocked,
        )
    };
    let heading = DocumentHeading {
        title,
        state: Some(state),
        pinned,
    };
    render_output_document(frame, log_area, heading, rows, viewport, color);
    render_fitting_footer(
        frame,
        sections[1],
        "LOG",
        viewer_footer_options(&FULL_LOG_FOOTER_OPTIONS, true),
        color,
    );
    if help_visible {
        render_help_overlay_groups(
            frame,
            sections[0],
            viewer_help_groups(HostSurface::FullLog, OutputHelpMode::FollowedRows),
            color,
        );
    }
}

/// Whether the step the log pane wants is in flight, beside which the run log says the
/// step's own output is kept when it settles.
fn wanted_in_flight(wanted: Option<&str>, selected_step: Option<&RunnerWatchRunStep>) -> bool {
    wanted.is_some()
        && selected_step
            .is_some_and(|step| step_state_kind(&step.state).is_some_and(step_state_is_active))
}

/// Takes the rows above the panes beyond the facts row out of the inspector, which keeps
/// its title, and moves the panes above it down. Answers the rows taken.
fn yield_rows(layout: &mut SplitBodyLayout, rows: u16) -> u16 {
    let rows = rows.min(
        layout
            .inspector
            .height
            .saturating_sub(INSPECTOR_HEADER_HEIGHT),
    );
    layout.summary.y = layout.summary.y.saturating_add(rows);
    layout.dag.y = layout.dag.y.saturating_add(rows);
    if layout.wide {
        layout.dag.height = layout.dag.height.saturating_sub(rows);
    }
    layout.inspector.y = layout.inspector.y.saturating_add(rows);
    layout.inspector.height = layout.inspector.height.saturating_sub(rows);
    rows
}

/// The facts row: the run's subject or run ID, the assignment's ID, the attempt, the phase
/// with its mark, Cloud's grant and whether the attempt continues an earlier one, the
/// least needed left out first.
fn facts_line(entry: &RunnerWatchAssignment, width: usize, color: bool) -> Line<'static> {
    fact_line(
        Vec::new(),
        vec![
            title_fact(&entry.title).tone(Tone::Primary),
            Fact::id("", &entry.assignment_id, 60),
            Fact::new(format!("attempt {}", entry.attempt_number), 20),
            Fact::new(
                format!("{} {}", mark_glyph(entry.mark), visible_text(&entry.phase)),
                ALWAYS,
            )
            .tone(mark_tone(entry.mark)),
            entry.lease.as_ref().map_or_else(Fact::absent, |lease| {
                Fact::new(
                    format!(
                        "lease {} {}",
                        visible_text(&lease.health),
                        short_duration(lease.time_left)
                    ),
                    40,
                )
            }),
            if entry.continues_earlier_attempt {
                Fact::new("continues an earlier attempt".to_owned(), 10)
            } else {
                Fact::absent()
            },
        ],
        " · ",
        width,
        color,
    )
}

/// The run's Cloud URL whole, behind its label when both fit; a row narrower than the URL
/// says the width that shows it and never part of it.
fn url_line(entry: &RunnerWatchAssignment, width: usize, color: bool) -> Option<Line<'static>> {
    let url = visible_text(entry.run_url.as_deref()?);
    let url_width = display_width(&url);
    let muted = tone_style(color, Tone::Muted);
    Some(if url_width.saturating_add(URL_LABEL.len()) <= width {
        Line::from(vec![Span::styled(URL_LABEL, muted), Span::raw(url)])
    } else if url_width <= width {
        Line::from(url)
    } else {
        Line::from(Span::styled(
            format!(
                "the run's Cloud URL shows at {} columns",
                url_width.saturating_add(2)
            ),
            muted,
        ))
    })
}

/// That the assignment ended: the phase while it finishes or reports, or after the runner
/// said it holds it no longer, the result from the overview's recent entry of the same
/// assignment.
fn ending_line(
    screen: &RunnerWatchScreen,
    shown: &AssignmentScreen,
    width: usize,
    color: bool,
) -> Option<Line<'static>> {
    let entry = &shown.entry;
    if !shown.not_held {
        return ENDING_PHASES.contains(&entry.phase.as_str()).then(|| {
            fact_line(
                vec![Span::styled("◌ ", tone_style(color, Tone::Blocked))],
                vec![Fact::new(format!("assignment {}", entry.phase), ALWAYS).tone(Tone::Blocked)],
                " · ",
                width,
                color,
            )
        });
    }
    let recent = screen
        .overview
        .recent
        .iter()
        .find(|recent| recent.assignment_id == entry.assignment_id);
    let Some(recent) = recent else {
        return Some(fact_line(
            vec![Span::styled("· ", tone_style(color, Tone::Muted))],
            vec![Fact::new(
                "the runner holds this assignment no longer".to_owned(),
                ALWAYS,
            )],
            " · ",
            width,
            color,
        ));
    };
    let (mark, tone) = if recent.succeeded {
        ("✓ ", Tone::Success)
    } else {
        ("✗ ", Tone::Failure)
    };
    Some(fact_line(
        vec![Span::styled(mark, tone_style(color, tone))],
        vec![
            Fact::new("assignment ended".to_owned(), ALWAYS).tone(tone),
            Fact::new(visible_text(&recent.result), ALWAYS).tone(tone),
            recent
                .detail
                .as_ref()
                .map_or_else(Fact::absent, |detail| Fact::new(visible_text(detail), 30)),
            Fact::new(format!("held {}", short_duration(recent.duration)), 3),
        ],
        " · ",
        width,
        color,
    ))
}

/// The open run's header: the root workflow's name and the child run's path from it, and
/// at its right the run's state words. A short row keeps the state and gives up the path's
/// leading elements, keeping the deepest.
fn render_run_header(
    frame: &mut Frame<'_>,
    area: Rect,
    borders: Borders,
    shown: &AssignmentScreen,
    run: Option<&RunnerWatchRun>,
    path: &[RunnerWatchRunElement],
    color: bool,
) {
    let block = summary_block(borders, color);
    let content = block.inner(area);
    frame.render_widget(block, area);
    let row = Rect::new(content.x, content.y, content.width, content.height.min(1));
    let state = run.map(run_state).unwrap_or_default();
    let state_width = display_width(&state);
    let room = usize::from(row.width)
        .saturating_sub(state_width)
        .saturating_sub(if state.is_empty() { 0 } else { 2 });
    let mut parts = vec![workflow_display_name(&shown.entry.workflow_path)];
    parts.extend(path.iter().map(path_element));
    let mut name = parts.join(STEP_SEPARATOR);
    for skipped in 1..parts.len() {
        if display_width(&name) <= room {
            break;
        }
        name = format!(
            "…{STEP_SEPARATOR}{}",
            parts
                .get(skipped..)
                .unwrap_or_default()
                .join(STEP_SEPARATOR)
        );
    }
    frame.render_widget(
        Paragraph::new(Span::styled(
            ellipsize(&name, room),
            tone_style(color, Tone::Primary).add_modifier(Modifier::BOLD),
        )),
        row,
    );
    frame.render_widget(
        Paragraph::new(Span::styled(state, tone_style(color, Tone::Muted)))
            .alignment(Alignment::Right),
        row,
    );
}

/// A run's state word, its outcome once it has one, what it waits for while it waits and
/// its attempt number past the first.
fn run_state(run: &RunnerWatchRun) -> String {
    let mut words = vec![visible_text(&run.state)];
    words.extend(run.outcome.as_deref().map(visible_text));
    words.extend(
        run.wait
            .as_ref()
            .map(|wait| format!("waits for {}", visible_text(&wait.kind))),
    );
    if run.attempt_number > 1 {
        words.push(format!("attempt {}", run.attempt_number));
    }
    words.join(" · ")
}

/// A child run's path element: its step, a fan-out's element in brackets and the ordinal
/// of a child that is not its step's first.
fn path_element(element: &RunnerWatchRunElement) -> String {
    let mut text = visible_text(&element.step);
    if let Some(item) = element.item {
        text.push_str(&format!("[{item}]"));
    }
    if element.ordinal > 1 {
        text.push_str(&format!("#{}", element.ordinal));
    }
    text
}

/// The log pane: its first row, which says what it shows and what was left out, then the
/// last rows of the log that fit, soft-wrapped.
fn render_log_preview(
    frame: &mut Frame<'_>,
    area: Rect,
    heading: &str,
    document: Option<&LogDocument>,
    color: bool,
) {
    let block =
        section_block(Borders::TOP, color).padding(Padding::horizontal(INSPECTOR_PANEL_PADDING));
    let content = block.inner(area);
    frame.render_widget(block, area);
    if content.is_empty() {
        return;
    }
    let width = usize::from(content.width);
    let mut lines = vec![Line::from(Span::styled(
        ellipsize(heading, width),
        tone_style(color, Tone::Muted),
    ))];
    if let Some(document) = document {
        let rows = usize::from(content.height).saturating_sub(1);
        let payload_width = width.saturating_sub(2).max(1);
        let mut tail = Vec::new();
        for row in document.rows.iter().rev() {
            let remaining = rows.saturating_sub(tail.len());
            if remaining == 0 {
                break;
            }
            for (first, text) in wrap_log_payload_tail(&row.text, payload_width, remaining)
                .into_iter()
                .rev()
            {
                let text = if first { text } else { format!("↳ {text}") };
                tail.push(Line::from(Span::styled(text, tone_style(color, row.tone))));
            }
        }
        lines.extend(tail.into_iter().rev());
    }
    frame.render_widget(Paragraph::new(lines), content);
}

/// The log pane's first row and the document under it. The pane shows a log only from an
/// answer whose read named the step it wants in the run at `path`: what the log is and what
/// was left out, or that the assignment has no run log yet. Until such an answer comes it
/// says the step is asked for, or, once the runner holds the assignment no longer and no
/// read will come, that it was not read. Beside the run log, a step in flight says its own
/// output is kept when it settles.
fn log_heading<'a>(
    shown: &'a AssignmentScreen,
    path: &[RunnerWatchRunElement],
    wanted: Option<&str>,
    step_in_flight: bool,
) -> (String, Option<&'a LogDocument>) {
    let subject = wanted.map_or_else(
        || "RUN LOG".to_owned(),
        |step| format!("OUTPUT {}", visible_text(step)),
    );
    match shown.log_for(path, wanted) {
        None if shown.not_held => (
            format!("{subject} · not read before the assignment ended"),
            None,
        ),
        None => (format!("{subject} · asked for"), None),
        Some(ShownLog { document: None, .. }) => ("no run log yet".to_owned(), None),
        Some(ShownLog {
            document: Some(document),
            ..
        }) => {
            let heading = if step_in_flight && document.run_log {
                format!(
                    "{} · the step's own output is kept when it settles",
                    document.heading
                )
            } else {
                document.heading.clone()
            };
            (heading, Some(document))
        }
    }
}

fn assignment_footer_options() -> Vec<Vec<HelpCommand>> {
    [
        ["up", "down", "open", "output", "back", "quit", "help"],
        ["", "", "open", "output", "back", "quit", "help"],
        [""; 7],
    ]
    .map(|descriptions| {
        ["↑/k", "↓/j", "↵", "o", "Esc", "q", "?"]
            .into_iter()
            .zip(descriptions)
            .map(|(keys, description)| help(keys, description))
            .collect()
    })
    .into()
}

fn assignment_help_groups() -> Vec<HelpGroup> {
    vec![
        HelpGroup {
            title: "MOVE",
            commands: vec![help("↑/k", "previous step"), help("↓/j", "next step")],
        },
        HelpGroup {
            title: "VIEW",
            commands: vec![
                help("↵", "open child run or log"),
                help("o", "open log"),
                help("Esc", "back"),
            ],
        },
        viewer_help_group(),
    ]
}
