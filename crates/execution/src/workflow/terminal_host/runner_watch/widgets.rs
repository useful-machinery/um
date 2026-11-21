use super::interaction::RunnerWatchSurface;
use super::*;
use crate::format_bytes;

mod assignment;

const RUNNER_WATCH_TITLE: &str = " Useful Machinery runner ";
/// The banner's label while the newest request failed (REQ-RUNNERENROLL-141).
pub(super) const LAST_KNOWN_LABEL: &str = "last known";
/// The note left when an answer's boot ID differs from the one shown.
pub(super) const RESTARTED_LABEL: &str = "runner restarted";
/// The operation notice's word while the runner has not yet answered (REQ-RUNNERENROLL-142).
pub(super) const IN_FLIGHT_LABEL: &str = "waiting for the runner";
/// A free text is not ellipsized below this width before a less needed fact is left out.
const MINIMUM_FREE_TEXT_WIDTH: usize = 8;
const LOGIN_ROWS: usize = 2;
const NOTICE_ROWS: usize = 3;
const STEP_SEPARATOR: &str = " › ";
/// The widest a run's title, a workflow's name and a wait's texts are drawn.
const TITLE_COLUMN: usize = 28;
const WORKFLOW_COLUMN: usize = 20;
const WAIT_COLUMN: usize = 24;
/// A fact the row keeps whatever else it leaves out.
const ALWAYS: u8 = u8::MAX;

pub(super) fn render_runner_watch(
    frame: &mut Frame<'_>,
    screen: &RunnerWatchScreen,
    interaction: &mut RunnerWatchInteraction,
    color: bool,
) {
    let area = frame.area();
    interaction.terminal_area = area;
    frame.render_widget(Clear, area);
    if !operational_area(area) {
        render_viewer_too_small(frame, area, RUNNER_WATCH_TITLE, color);
        return;
    }
    if let (RunnerWatchSurface::Assignment(open), Some(shown)) =
        (&mut interaction.surface, &screen.assignment)
    {
        assignment::render_assignment(frame, screen, open, shown, interaction.help_visible, color);
        return;
    }
    let sections =
        Layout::vertical([Constraint::Min(0), Constraint::Length(FOOTER_HEIGHT)]).split(area);
    let body = sections[0];
    let content = Rect::new(
        body.x.saturating_add(1),
        body.y,
        body.width.saturating_sub(2),
        body.height,
    );
    let width = usize::from(content.width);
    let wide = area.width >= WIDE_LAYOUT_WIDTH;

    let mut head = status_line(screen, None, width, color)
        .into_iter()
        .collect::<Vec<_>>();
    head.extend(health_lines(&screen.overview, width, color));
    let head_height = u16::try_from(head.len())
        .unwrap_or(u16::MAX)
        .min(content.height);
    frame.render_widget(
        Paragraph::new(head),
        Rect::new(content.x, content.y, content.width, head_height),
    );

    // The notice sits directly above the footer and takes its rows from the lists.
    let notice = notice_lines(screen, width, color);
    let notice_height = u16::try_from(notice.len())
        .unwrap_or(u16::MAX)
        .min(content.height.saturating_sub(head_height));
    let notice_area = Rect::new(
        content.x,
        content.bottom().saturating_sub(notice_height),
        content.width,
        notice_height,
    );
    frame.render_widget(Paragraph::new(notice), notice_area);
    let middle_height = content
        .height
        .saturating_sub(head_height)
        .saturating_sub(notice_height);

    let overview = &screen.overview;
    let entry_height = if wide { 1 } else { 2 };
    let shown = list_rows(
        usize::from(middle_height),
        entry_height,
        wide,
        [
            overview.assignments.len(),
            overview.recent.len(),
            overview.events.len(),
        ],
    );
    interaction.scroll(overview.assignments.len(), shown.assignments);
    let assignment_lines = assignment_lines(overview, interaction, wide, width, color);
    let assignments_height = u16::try_from(assignment_lines.len()).unwrap_or(u16::MAX);
    let assignments_area = Rect::new(
        content.x,
        content.y.saturating_add(head_height),
        content.width,
        assignments_height.min(middle_height),
    );
    frame.render_widget(Paragraph::new(assignment_lines), assignments_area);

    let lists_area = Rect::new(
        content.x,
        assignments_area.bottom(),
        content.width,
        notice_area.y.saturating_sub(assignments_area.bottom()),
    );
    let (recent_area, events_area) = if wide {
        let columns = Layout::horizontal([Constraint::Percentage(50), Constraint::Percentage(50)])
            .spacing(2)
            .split(lists_area);
        (columns[0], columns[1])
    } else {
        let recent_height = u16::try_from(shown.recent.saturating_add(1)).unwrap_or(u16::MAX);
        let rows = Layout::vertical([Constraint::Length(recent_height), Constraint::Min(0)])
            .split(lists_area);
        (rows[0], rows[1])
    };
    frame.render_widget(
        Paragraph::new(recent_lines(
            overview,
            shown.recent,
            usize::from(recent_area.width),
            color,
        )),
        recent_area,
    );
    frame.render_widget(
        Paragraph::new(event_lines(
            screen,
            shown.events,
            usize::from(events_area.width),
            color,
        )),
        events_area,
    );

    let operations = !screen.operation_in_flight();
    render_runner_watch_footer(frame, sections[1], operations, color);
    if interaction.help_visible {
        render_help_overlay_groups(frame, body, runner_watch_help_groups(operations), color);
    }
}

/// How many entries of each list are shown.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ListRows {
    pub(super) assignments: usize,
    pub(super) recent: usize,
    pub(super) events: usize,
}

/// Shares `rows` among the three lists: each keeps its heading and, when it is not empty,
/// one entry; the assignments are then served up to what they need, and Recent and Events
/// split what is left, Events taking the odd row and a list that needs fewer passing its
/// rows on. Side by side, Recent and Events share their rows.
pub(super) fn list_rows(
    rows: usize,
    entry_height: usize,
    wide: bool,
    [assignments, recent, events]: [usize; 3],
) -> ListRows {
    let mut shown = ListRows {
        assignments: assignments.min(1),
        recent: recent.min(1),
        events: events.min(1),
    };
    let lists = if wide {
        shown.recent.max(shown.events).saturating_add(1)
    } else {
        shown.recent.saturating_add(shown.events).saturating_add(2)
    };
    let mut spare = rows
        .saturating_sub(1)
        .saturating_sub(shown.assignments.saturating_mul(entry_height))
        .saturating_sub(lists);
    let more = assignments
        .saturating_sub(shown.assignments)
        .min(spare / entry_height.max(1));
    shown.assignments = shown.assignments.saturating_add(more);
    spare = spare.saturating_sub(more.saturating_mul(entry_height));
    let recent_wants = recent.saturating_sub(shown.recent);
    let events_wants = events.saturating_sub(shown.events);
    if wide {
        shown.recent = shown.recent.saturating_add(recent_wants.min(spare));
        shown.events = shown.events.saturating_add(events_wants.min(spare));
    } else {
        let recent_share = spare / 2;
        let events_extra =
            events_wants.min(spare.div_ceil(2) + recent_share.saturating_sub(recent_wants));
        let recent_extra = recent_wants.min(spare.saturating_sub(events_extra));
        shown.recent = shown.recent.saturating_add(recent_extra);
        shown.events = shown.events.saturating_add(events_extra);
    }
    shown
}

/// The status row: the last-known banner while the newest request failed, else the ending
/// an opened assignment gives, else the note that the runner restarted.
fn status_line(
    screen: &RunnerWatchScreen,
    ending: Option<Line<'static>>,
    width: usize,
    color: bool,
) -> Option<Line<'static>> {
    if let Some(failure) = screen.failure {
        let cause = match failure {
            RunnerWatchSourceError::NotReachable => {
                vec![
                    "the runner did not answer".to_owned(),
                    "no answer".to_owned(),
                ]
            }
            RunnerWatchSourceError::Unreadable => vec![
                "its answer could not be read".to_owned(),
                "unreadable answer".to_owned(),
            ],
            RunnerWatchSourceError::EarlierRelease => vec![
                "the runner's release does not answer this".to_owned(),
                "earlier release".to_owned(),
            ],
        };
        return Some(fact_line(
            vec![Span::styled("✗ ", tone_style(color, Tone::Failure))],
            vec![
                Fact::new(
                    format!(
                        "{LAST_KNOWN_LABEL} · answered {} ago",
                        short_duration(screen.since_last_answer())
                    ),
                    ALWAYS,
                )
                .tone(Tone::Failure),
                Fact::forms(cause, ALWAYS).tone(Tone::Failure),
            ],
            " · ",
            width,
            color,
        ));
    }
    if ending.is_some() {
        return ending;
    }
    screen.restarted_at.map(|restarted_at| {
        fact_line(
            vec![Span::styled("↻ ", tone_style(color, Tone::Blocked))],
            vec![
                Fact::new(RESTARTED_LABEL.to_owned(), ALWAYS).tone(Tone::Blocked),
                Fact::new(format!("at {}", health_time(restarted_at)), 2),
                Fact::new("selection reset".to_owned(), 1),
            ],
            " · ",
            width,
            color,
        )
    })
}

fn health_lines(overview: &RunnerWatchOverview, width: usize, color: bool) -> Vec<Line<'static>> {
    let low_disk = overview.disk.filter(|disk| disk.low);
    // A disk that is not low is one more fact of the identity line; a low one is its own line.
    let disk = overview
        .disk
        .filter(|disk| !disk.low)
        .map_or_else(Fact::absent, |disk| {
            let available = format!("disk {} free", format_bytes(disk.available_bytes));
            Fact::forms(
                vec![
                    format!("{available} of {}", format_bytes(disk.total_bytes)),
                    available,
                ],
                1,
            )
        });
    let mut lines = vec![fact_line(
        Vec::new(),
        vec![
            Fact::id("", &overview.runner_id, ALWAYS).tone(Tone::Primary),
            Fact::new(format!("v{}", visible_text(&overview.version)), 2),
            Fact::new(format!("up {}", short_duration(overview.uptime)), 1),
            disk,
            Fact::id("boot ", &overview.boot_id, 3),
        ],
        "  ",
        width,
        color,
    )];
    if let Some(disk) = low_disk {
        lines.push(fact_line(
            vec![Span::styled("✗ ", tone_style(color, Tone::Failure))],
            vec![
                Fact::new("disk low".to_owned(), ALWAYS).tone(Tone::Failure),
                Fact::new(
                    format!(
                        "{} free of {}",
                        format_bytes(disk.available_bytes),
                        format_bytes(disk.total_bytes)
                    ),
                    3,
                ),
                Fact::new(
                    format!("minimum {}", format_bytes(disk.minimum_free_bytes)),
                    2,
                ),
            ],
            " · ",
            width,
            color,
        ));
    }
    lines.extend([
        connection_line(overview, width, color),
        fact_line(
            Vec::new(),
            vec![
                overview.current_credential_id.as_deref().map_or_else(
                    || Fact::new("credential none".to_owned(), ALWAYS),
                    |id| Fact::id("credential ", id, ALWAYS),
                ),
                overview
                    .pending_credential_id
                    .as_deref()
                    .map_or_else(Fact::absent, |id| Fact::id("pending ", id, 4))
                    .tone(Tone::Blocked),
            ],
            "  ",
            width,
            color,
        ),
    ]);
    lines.extend(login_lines(&overview.logins, width, color));
    if let Some(left) = overview.stop_time_left {
        lines.push(fact_line(
            Vec::new(),
            vec![
                Fact::new("stopping".to_owned(), ALWAYS).tone(Tone::Blocked),
                Fact::new(format!("{} left", short_duration(left)), 3),
            ],
            " · ",
            width,
            color,
        ));
    }
    lines
}

fn connection_line(overview: &RunnerWatchOverview, width: usize, color: bool) -> Line<'static> {
    let connection = &overview.connection;
    let tone = mark_tone(connection.mark);
    let connected_at = connection.last_connected_at.map(|at| {
        if connection.connected {
            Fact::new(format!("since {}", health_time(at)), 4)
        } else {
            Fact::new(format!("last connected {}", health_time(at)), 2)
        }
    });
    let reports = match overview.reports_waiting {
        0 => Fact::absent(),
        1 => Fact::new("1 report waiting".to_owned(), 5),
        count => Fact::new(format!("{count} reports waiting"), 5),
    };
    fact_line(
        vec![Span::styled(
            format!("{} ", mark_glyph(connection.mark)),
            tone_style(color, tone),
        )],
        vec![
            Fact::new(visible_text(&connection.state), ALWAYS).tone(tone),
            connected_at.unwrap_or_else(Fact::absent),
            connection
                .last_failure
                .as_deref()
                .map_or_else(Fact::absent, |failure| {
                    Fact::new(format!("last failure {}", visible_text(failure)), 2)
                }),
            connection.attempts.map_or_else(Fact::absent, |attempts| {
                Fact::new(format!("attempt {attempts}"), 3)
            }),
            connection
                .next_attempt_in
                .map_or_else(Fact::absent, |next| {
                    Fact::new(format!("next in {}", short_duration(next)), 3)
                }),
            reports,
        ],
        " · ",
        width,
        color,
    )
}

/// One entry per provider, the ones that are not ready first, whole entries only on at most
/// two rows, the rest counted.
fn login_lines(logins: &[RunnerWatchLogin], width: usize, color: bool) -> Vec<Line<'static>> {
    const LABEL: &str = "logins  ";
    let available = width.saturating_sub(display_width(LABEL)).max(1);
    let mut entries = logins.iter().collect::<Vec<_>>();
    entries.sort_by_key(|login| login.ready);
    let mut entries = entries
        .into_iter()
        .map(|login| {
            let provider = visible_text(&login.provider);
            if login.ready {
                (format!("{provider} ✓"), Tone::Success)
            } else {
                let reason = login
                    .reason
                    .as_deref()
                    .map_or_else(String::new, |reason| format!(" {}", visible_text(reason)));
                (
                    ellipsize(&format!("{provider} ✗{reason}"), available),
                    Tone::Failure,
                )
            }
        })
        .collect::<Vec<_>>();
    if entries.is_empty() {
        entries.push(("none reported".to_owned(), Tone::Muted));
    }
    whole_entry_lines(
        Span::styled(LABEL, tone_style(color, Tone::Muted)),
        entries,
        LOGIN_ROWS,
        width,
        color,
    )
}

/// The operation notice: what is in flight and for how long, or the outcome's facts, whole
/// on at most three rows with the rest counted (REQ-RUNNERENROLL-142).
fn notice_lines(screen: &RunnerWatchScreen, width: usize, color: bool) -> Vec<Line<'static>> {
    match &screen.notice {
        None => Vec::new(),
        Some(RunnerWatchNotice::InFlight { operation, since }) => vec![fact_line(
            vec![Span::styled("◌ ", tone_style(color, Tone::Blocked))],
            vec![
                Fact::new(operation_label(*operation).to_owned(), ALWAYS).tone(Tone::Blocked),
                Fact::new(IN_FLIGHT_LABEL.to_owned(), ALWAYS),
                Fact::new(
                    short_duration(screen.now.monotonic.saturating_duration_since(*since)),
                    1,
                ),
            ],
            " · ",
            width,
            color,
        )],
        Some(RunnerWatchNotice::Answered { operation, outcome }) => {
            let (mark, tone, word) = match outcome {
                RunnerWatchOutcome::Done(_) => ("✓ ", Tone::Success, "done"),
                RunnerWatchOutcome::Refused(_) => ("✗ ", Tone::Failure, "not done"),
            };
            let mut entries = vec![(format!("{} · {word}", operation_label(*operation)), tone)];
            entries.extend(
                outcome
                    .facts()
                    .iter()
                    .map(|fact| (visible_text(fact), Tone::Neutral)),
            );
            whole_entry_lines(
                Span::styled(mark, tone_style(color, tone)),
                entries,
                NOTICE_ROWS,
                width,
                color,
            )
        }
    }
}

fn operation_label(operation: RunnerWatchOperation) -> &'static str {
    match operation {
        RunnerWatchOperation::ReloadCredential => "reload credential",
        RunnerWatchOperation::SetUpAgentLogins => "set up agent logins",
    }
}

/// Lays entries out whole after `prefix` on at most `max_rows` rows, as many as fit in
/// order, with the count of the rest at the end. An entry wider than a row is counted.
fn whole_entry_lines(
    prefix: Span<'static>,
    entries: Vec<(String, Tone)>,
    max_rows: usize,
    width: usize,
    color: bool,
) -> Vec<Line<'static>> {
    let prefix_width = display_width(&prefix.content);
    let available = width.saturating_sub(prefix_width).max(1);
    let (entries, too_wide): (Vec<_>, Vec<_>) = entries
        .into_iter()
        .partition(|(text, _)| display_width(text) <= available);
    let rows = (0..=entries.len())
        .rev()
        .find_map(|placed| whole_rows(&entries, placed, too_wide.len(), available, max_rows))
        .unwrap_or_default();
    rows.into_iter()
        .enumerate()
        .map(|(index, row)| {
            let mut spans = vec![if index == 0 {
                prefix.clone()
            } else {
                Span::raw(" ".repeat(prefix_width))
            }];
            for (position, (text, tone)) in row.into_iter().enumerate() {
                if position != 0 {
                    spans.push(Span::raw("  "));
                }
                spans.push(Span::styled(text, tone_style(color, tone)));
            }
            Line::from(spans)
        })
        .collect()
}

/// Lays the first `placed` entries out on at most `max_rows` rows, with the count of the
/// rest and of `left_out` more at the end, or answers `None` when they do not fit.
fn whole_rows(
    entries: &[(String, Tone)],
    placed: usize,
    left_out: usize,
    width: usize,
    max_rows: usize,
) -> Option<Vec<Vec<(String, Tone)>>> {
    let mut items = entries.iter().take(placed).cloned().collect::<Vec<_>>();
    let rest = entries
        .len()
        .saturating_sub(placed)
        .saturating_add(left_out);
    if rest != 0 {
        items.push((format!("+{rest} more"), Tone::Muted));
    }
    let mut rows: Vec<Vec<(String, Tone)>> = vec![Vec::new()];
    let mut used = 0_usize;
    for item in items {
        let item_width = display_width(&item.0);
        let needed = if used == 0 {
            item_width
        } else {
            used.saturating_add(2).saturating_add(item_width)
        };
        if needed <= width {
            used = needed;
            rows.last_mut()?.push(item);
        } else if item_width <= width && rows.len() < max_rows {
            used = item_width;
            rows.push(vec![item]);
        } else {
            return None;
        }
    }
    Some(rows)
}

fn assignment_lines(
    overview: &RunnerWatchOverview,
    interaction: &RunnerWatchInteraction,
    wide: bool,
    width: usize,
    color: bool,
) -> Vec<Line<'static>> {
    let counts = overview.counts;
    let mut phases = [
        (counts.running, "working"),
        (counts.waiting, "waiting"),
        (counts.preparing, "preparing"),
        (counts.accepted, "accepted"),
        (counts.finishing, "finishing"),
        (counts.reporting, "reporting"),
    ]
    .into_iter()
    .filter(|(count, _)| *count != 0)
    .map(|(count, phase)| format!("{count} {phase}"))
    .collect::<Vec<_>>()
    .join(" · ");
    if phases.is_empty() {
        phases = "none held".to_owned();
    }
    let mut lines = vec![heading_line(
        "ASSIGNMENTS",
        phases,
        interaction.visible,
        overview.assignments.len(),
        overview.assignments_omitted,
        width,
        color,
    )];
    for (index, assignment) in overview
        .assignments
        .iter()
        .enumerate()
        .skip(interaction.top)
        .take(interaction.visible)
    {
        let selected = index == interaction.selected;
        let style = if selected {
            Style::default().add_modifier(Modifier::REVERSED)
        } else {
            Style::default()
        };
        let tone = mark_tone(assignment.mark);
        let prefix = vec![
            Span::styled(if selected { "▸ " } else { "  " }, style),
            Span::styled(
                format!("{} ", mark_glyph(assignment.mark)),
                tone_style(color, tone).patch(style),
            ),
        ];
        let mut facts = vec![
            title_fact(&assignment.title).tone(Tone::Primary),
            Fact::free(workflow_display_name(&assignment.workflow_path), 20)
                .column(WORKFLOW_COLUMN),
            Fact::new(visible_text(&assignment.phase), ALWAYS).tone(tone),
            Fact::new(short_duration(assignment.held), 30),
            assignment
                .lease
                .as_ref()
                .map_or_else(Fact::absent, |lease| {
                    Fact::new(
                        format!(
                            "lease {} {}",
                            visible_text(&lease.health),
                            short_duration(lease.time_left)
                        ),
                        40,
                    )
                }),
        ];
        let detail = detail_facts(&assignment.detail);
        if wide {
            // On one row the detail is left out before any fact of the entry itself.
            facts.extend(detail.into_iter().map(|fact| Fact {
                need: fact.need / 16,
                ..fact
            }));
            lines.push(fact_line(prefix, facts, "  ", width, color).patch_style(style));
        } else {
            lines.push(fact_line(prefix, facts, "  ", width, color).patch_style(style));
            lines.push(
                fact_line(vec![Span::raw("      ")], detail, "  ", width, color).patch_style(style),
            );
        }
    }
    lines
}

fn detail_facts(detail: &RunnerWatchAssignmentDetail) -> Vec<Fact> {
    match detail {
        RunnerWatchAssignmentDetail::None => Vec::new(),
        RunnerWatchAssignmentDetail::Wait(wait) => vec![
            Fact::free(format!("waits for {}", visible_text(&wait.kind)), ALWAYS)
                .column(WAIT_COLUMN),
            if wait.step.is_empty() {
                Fact::absent()
            } else {
                Fact::free(format!("at {}", step_path(&wait.step)), 32).column(WAIT_COLUMN)
            },
            wait.since.map_or_else(Fact::absent, |since| {
                Fact::new(format!("since {}", health_time(since)), 16)
            }),
        ],
        RunnerWatchAssignmentDetail::Steps { steps, omitted } => {
            let paths = steps
                .iter()
                .map(|step| step_path(&step.path))
                .collect::<Vec<_>>();
            let total = u64::try_from(paths.len())
                .unwrap_or(u64::MAX)
                .saturating_add(*omitted);
            let forms = (1..=paths.len())
                .rev()
                .map(|shown| {
                    let listed = paths
                        .iter()
                        .take(shown)
                        .cloned()
                        .collect::<Vec<_>>()
                        .join(" · ");
                    let rest = total.saturating_sub(u64::try_from(shown).unwrap_or(u64::MAX));
                    if rest == 0 {
                        listed
                    } else {
                        format!("{listed}  +{rest} more")
                    }
                })
                .collect::<Vec<_>>();
            if forms.is_empty() {
                if *omitted == 0 {
                    Vec::new()
                } else {
                    vec![Fact::new(format!("{omitted} steps running"), ALWAYS)]
                }
            } else {
                vec![Fact {
                    free: true,
                    ..Fact::forms(forms, ALWAYS)
                }]
            }
        }
    }
}

fn recent_lines(
    overview: &RunnerWatchOverview,
    shown: usize,
    width: usize,
    color: bool,
) -> Vec<Line<'static>> {
    let mut lines = vec![heading_line(
        "RECENT",
        String::new(),
        shown,
        overview.recent.len(),
        overview.recent_omitted,
        width,
        color,
    )];
    lines.extend(overview.recent.iter().take(shown).map(|recent| {
        let (mark, tone) = if recent.succeeded {
            ("✓ ", Tone::Success)
        } else {
            ("✗ ", Tone::Failure)
        };
        let result = visible_text(&recent.result);
        let result = match &recent.detail {
            Some(detail) => Fact::forms(
                vec![format!("{result} · {}", visible_text(detail)), result],
                ALWAYS,
            ),
            None => Fact::new(result, ALWAYS),
        };
        fact_line(
            vec![Span::styled(mark, tone_style(color, tone))],
            vec![
                title_fact(&recent.title).tone(Tone::Primary),
                Fact::free(workflow_display_name(&recent.workflow_path), 2).column(WORKFLOW_COLUMN),
                result.tone(tone),
                Fact::new(short_duration(recent.duration), 3),
                Fact::new(format!("ended {}", health_time(recent.ended_at)), 1),
            ],
            "  ",
            width,
            color,
        )
    }));
    lines
}

fn event_lines(
    screen: &RunnerWatchScreen,
    shown: usize,
    width: usize,
    color: bool,
) -> Vec<Line<'static>> {
    let overview = &screen.overview;
    let today = screen.now.utc.to_offset(UtcOffset::UTC).date();
    let mut lines = vec![heading_line(
        "EVENTS (UTC)",
        String::new(),
        shown,
        overview.events.len(),
        overview.events_omitted,
        width,
        color,
    )];
    lines.extend(overview.events.iter().take(shown).map(|event| {
        let at = event.at.to_offset(UtcOffset::UTC);
        let time = format!("{:02}:{:02}:{:02}", at.hour(), at.minute(), at.second());
        let time = if at.date() == today {
            time
        } else {
            format!("{} {time}", at.date())
        };
        fact_line(
            Vec::new(),
            vec![
                Fact::new(time, ALWAYS).tone(Tone::Muted),
                Fact::free(visible_text(&event.text), ALWAYS).tone(mark_tone(event.mark)),
                if event.count > 1 {
                    Fact::new(format!("×{}", event.count), 3)
                } else {
                    Fact::absent()
                },
            ],
            "  ",
            width,
            color,
        )
    }));
    lines
}

/// A list's heading: its name, what it counts and, when the list shows part of itself or
/// the answer left entries out, how many of them it shows.
fn heading_line(
    name: &'static str,
    summary: String,
    shown: usize,
    listed: usize,
    omitted: u64,
    width: usize,
    color: bool,
) -> Line<'static> {
    let shown = shown.min(listed);
    let total = u64::try_from(listed)
        .unwrap_or(u64::MAX)
        .saturating_add(omitted);
    let partial = u64::try_from(shown).unwrap_or(u64::MAX) < total;
    fact_line(
        vec![
            Span::styled(
                name,
                tone_style(color, Tone::Primary).add_modifier(Modifier::BOLD),
            ),
            Span::raw("  "),
        ],
        vec![
            if summary.is_empty() {
                Fact::absent()
            } else {
                Fact::new(summary, 3)
            },
            if partial {
                Fact::new(format!("{shown} of {total}"), ALWAYS)
            } else {
                Fact::absent()
            },
        ],
        "  ",
        width,
        color,
    )
}

/// The footer, with the two operation keys while none is in flight.
fn render_runner_watch_footer(frame: &mut Frame<'_>, area: Rect, operations: bool, color: bool) {
    let options = [
        ["up", "down", "open", "reload", "logins", "quit", "help"],
        ["", "", "open", "reload", "logins", "quit", "help"],
        [""; 7],
    ]
    .map(|descriptions| {
        ["↑/k", "↓/j", "↵", "C", "L", "q", "?"]
            .into_iter()
            .zip(descriptions)
            .filter(|(keys, _)| operations || !matches!(*keys, "C" | "L"))
            .map(|(keys, description)| help(keys, description))
            .collect::<Vec<_>>()
    });
    render_fitting_footer(frame, area, "RUNNER", Vec::from(options), color);
}

fn runner_watch_help_groups(operations: bool) -> Vec<HelpGroup> {
    let mut groups = vec![
        HelpGroup {
            title: "MOVE",
            commands: vec![
                help("↑/k", "previous"),
                help("↓/j", "next"),
                help("g", "first"),
                help("G", "last"),
            ],
        },
        HelpGroup {
            title: "VIEW",
            commands: vec![help("↵", "open assignment"), help("Esc", "dismiss")],
        },
    ];
    if operations {
        groups.push(HelpGroup {
            title: "RUNNER",
            commands: vec![
                help("C", "reload credential"),
                help("L", "set up agent logins"),
            ],
        });
    }
    groups.push(viewer_help_group());
    groups
}

fn title_fact(title: &RunnerWatchTitle) -> Fact {
    match title {
        RunnerWatchTitle::Subject(subject) => {
            Fact::free(visible_text(subject), ALWAYS).column(TITLE_COLUMN)
        }
        RunnerWatchTitle::RunId(run_id) => Fact::id("", run_id, ALWAYS),
    }
}

fn step_path(parts: &[String]) -> String {
    parts
        .iter()
        .map(|part| visible_text(part))
        .collect::<Vec<_>>()
        .join(STEP_SEPARATOR)
}

pub(super) fn mark_glyph(mark: RunnerWatchMark) -> &'static str {
    match mark {
        RunnerWatchMark::Active => "●",
        RunnerWatchMark::Pending => "◌",
        RunnerWatchMark::Success => "✓",
        RunnerWatchMark::Failure => "✗",
        RunnerWatchMark::Neutral => "·",
    }
}

fn mark_tone(mark: RunnerWatchMark) -> Tone {
    match mark {
        RunnerWatchMark::Active => Tone::Active,
        RunnerWatchMark::Pending => Tone::Blocked,
        RunnerWatchMark::Success => Tone::Success,
        RunnerWatchMark::Failure => Tone::Failure,
        RunnerWatchMark::Neutral => Tone::Muted,
    }
}

/// A duration in at most two units: `42s`, `18m`, `3h12m`, `3d4h`.
pub(super) fn short_duration(duration: Duration) -> String {
    let seconds = duration.as_secs();
    let (minutes, hours, days) = (seconds / 60, seconds / 3_600, seconds / 86_400);
    if minutes == 0 {
        format!("{seconds}s")
    } else if hours == 0 {
        format!("{minutes}m")
    } else if days == 0 {
        format!("{hours}h{:02}m", minutes % 60)
    } else {
        format!("{days}d{}h", hours % 24)
    }
}

/// A time of day in UTC as the health rows show it, `HH:MM:SSZ`.
fn health_time(at: time::OffsetDateTime) -> String {
    let at = at.to_offset(UtcOffset::UTC);
    format!("{:02}:{:02}:{:02}Z", at.hour(), at.minute(), at.second())
}

/// An ID shortened to its type prefix, an ellipsis and its last six characters, or `None`
/// when that would not shorten it.
pub(super) fn short_id(id: &str) -> Option<String> {
    let (prefix, rest) = id.split_once('_').unwrap_or(("", id));
    let tail = rest.chars().rev().take(6).collect::<Vec<_>>();
    if rest.chars().count() <= tail.len().saturating_add(1) {
        return None;
    }
    let tail = tail.into_iter().rev().collect::<String>();
    let separator = if prefix.is_empty() { "" } else { "_" };
    Some(format!("{prefix}{separator}…{tail}"))
}

/// One fact of a row: its forms, longest first, how much the row needs it, and whether it is
/// a free text that may end in an ellipsis.
#[derive(Clone)]
struct Fact {
    forms: Vec<String>,
    need: u8,
    tone: Tone,
    free: bool,
    /// An ID, whose shorter form the row takes before it leaves any fact out.
    id: bool,
}

impl Fact {
    fn new(text: String, need: u8) -> Self {
        Self::forms(vec![text], need)
    }

    fn forms(forms: Vec<String>, need: u8) -> Self {
        Self {
            forms,
            need,
            tone: Tone::Neutral,
            free: false,
            id: false,
        }
    }

    fn free(text: String, need: u8) -> Self {
        Self {
            free: true,
            ..Self::new(text, need)
        }
    }

    fn id(label: &str, id: &str, need: u8) -> Self {
        let id = visible_text(id);
        let mut forms = vec![format!("{label}{id}")];
        forms.extend(short_id(&id).map(|short| format!("{label}{short}")));
        Self {
            id: true,
            ..Self::forms(forms, need)
        }
    }

    fn form(&self, form: usize) -> String {
        self.forms.get(form).cloned().unwrap_or_default()
    }

    /// A fact the entry does not have.
    fn absent() -> Self {
        Self::forms(Vec::new(), 0)
    }

    /// Ends a free text in an ellipsis at its column's width.
    fn column(mut self, width: usize) -> Self {
        for form in &mut self.forms {
            *form = ellipsize(form, width);
        }
        self
    }

    fn tone(self, tone: Tone) -> Self {
        Self { tone, ..self }
    }
}

struct PlacedFact {
    fact: Fact,
    /// Where the fact stands among the row's facts.
    position: usize,
    form: usize,
    text: String,
}

/// Lays a row's facts out within `width`. An ID is shortened first. Then, least needed
/// first, a fact takes its shorter form or is left out whole, and a free text ends in an
/// ellipsis down to a minimum width, until the row fits; a row that still does not fit
/// ends its free texts sooner. When a fact shortened later left the room, the facts left
/// out come back whole, the most needed first and none after one that still does not fit,
/// so a row never shows a fact beside the absence of one it needs more.
fn fit_facts(facts: Vec<Fact>, separator: &str, width: usize) -> Vec<(String, Tone)> {
    let mut placed = facts
        .into_iter()
        .enumerate()
        .filter_map(|(position, fact)| {
            let text = fact.forms.first()?.clone();
            Some(PlacedFact {
                fact,
                position,
                form: 0,
                text,
            })
        })
        .collect::<Vec<_>>();
    let mut left_out = Vec::new();
    let separator_width = display_width(separator);
    let total = |placed: &[PlacedFact]| {
        placed
            .iter()
            .map(|placed| display_width(&placed.text))
            .sum::<usize>()
            .saturating_add(separator_width.saturating_mul(placed.len().saturating_sub(1)))
    };
    while total(&placed) > width {
        let Some(id) = placed
            .iter_mut()
            .filter(|placed| placed.fact.id && placed.has_shorter_form())
            .min_by_key(|placed| placed.fact.need)
        else {
            break;
        };
        id.shorten();
    }
    while total(&placed) > width {
        let overflow = total(&placed).saturating_sub(width);
        let reducible = placed
            .iter()
            .enumerate()
            .filter(|(_, placed)| placed.has_shorter_form() || placed.fact.need != ALWAYS)
            .min_by_key(|(_, placed)| placed.fact.need)
            .map(|(index, placed)| (index, placed.fact.need));
        let squeezable = placed
            .iter()
            .enumerate()
            .filter(|(_, placed)| {
                placed.fact.free
                    && !placed.has_shorter_form()
                    && display_width(&placed.text) > MINIMUM_FREE_TEXT_WIDTH
            })
            .min_by_key(|(_, placed)| placed.fact.need)
            .map(|(index, placed)| (index, placed.fact.need));
        match (reducible, squeezable) {
            (_, Some((index, need)))
                if reducible.is_none_or(|(_, reducible)| need <= reducible) =>
            {
                if let Some(free) = placed.get_mut(index) {
                    free.squeeze(overflow, MINIMUM_FREE_TEXT_WIDTH);
                }
            }
            (Some((index, _)), _) => match placed.get_mut(index) {
                Some(fact) if fact.has_shorter_form() => fact.shorten(),
                _ => left_out.push(placed.remove(index)),
            },
            _ => break,
        }
    }
    while total(&placed) > width {
        let overflow = total(&placed).saturating_sub(width);
        let Some(widest) = placed
            .iter_mut()
            .filter(|placed| placed.fact.free && display_width(&placed.text) > 1)
            .max_by_key(|placed| display_width(&placed.text))
        else {
            break;
        };
        widest.squeeze(overflow, 1);
    }
    left_out.sort_by_key(|fact| std::cmp::Reverse(fact.fact.need));
    for mut fact in left_out {
        let room = width
            .saturating_sub(total(&placed))
            .saturating_sub(if placed.is_empty() {
                0
            } else {
                separator_width
            });
        let Some(form) = fact
            .fact
            .forms
            .iter()
            .position(|form| display_width(form) <= room)
        else {
            break;
        };
        fact.form = form;
        fact.text = fact.fact.form(form);
        let at = placed.partition_point(|placed| placed.position < fact.position);
        placed.insert(at, fact);
    }
    placed
        .into_iter()
        .map(|placed| (placed.text, placed.fact.tone))
        .collect()
}

impl PlacedFact {
    fn has_shorter_form(&self) -> bool {
        self.form.saturating_add(1) < self.fact.forms.len()
    }

    fn shorten(&mut self) {
        self.form = self.form.saturating_add(1);
        self.text = self.fact.form(self.form);
    }

    /// Ends the text in an ellipsis `overflow` columns sooner, not below `minimum`.
    fn squeeze(&mut self, overflow: usize, minimum: usize) {
        let target = display_width(&self.text)
            .saturating_sub(overflow)
            .max(minimum);
        self.text = ellipsize(&self.text, target);
    }
}

fn fact_line(
    mut spans: Vec<Span<'static>>,
    facts: Vec<Fact>,
    separator: &'static str,
    width: usize,
    color: bool,
) -> Line<'static> {
    let prefix_width = spans
        .iter()
        .map(|span| display_width(&span.content))
        .sum::<usize>();
    for (index, (text, tone)) in fit_facts(facts, separator, width.saturating_sub(prefix_width))
        .into_iter()
        .enumerate()
    {
        if index != 0 {
            spans.push(Span::raw(separator));
        }
        spans.push(Span::styled(text, tone_style(color, tone)));
    }
    Line::from(spans)
}
