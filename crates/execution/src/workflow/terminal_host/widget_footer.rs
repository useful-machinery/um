use super::*;

pub(super) const fn help(keys: &'static str, description: &'static str) -> HelpCommand {
    HelpCommand { keys, description }
}

pub(super) const SPLIT_FOOTER_OPTIONS: [&[HelpCommand]; 3] = [
    &[help("↑/k", "up"), help("↓/j", "down"), help("↵", "open")],
    &[help("↑/k", "up"), help("↓/j", "down"), help("↵", "open")],
    &[help("↑/k", ""), help("↓/j", ""), help("↵", "")],
];

pub(super) const FULL_LOG_FOOTER_OPTIONS: [&[HelpCommand]; 3] = [
    &[
        help("Esc", "back"),
        help("↑/k", "up"),
        help("↓/j", "down"),
        help("PgUp/b", "page-up"),
        help("PgDn/f", "page-down"),
        help("←/h", "left"),
        help("→/l", "right"),
        help("F", "follow"),
    ],
    &[
        help("Esc", "back"),
        help("↑/k", ""),
        help("↓/j", ""),
        help("PgUp/b", ""),
        help("PgDn/f", ""),
        help("F", "follow"),
    ],
    &[
        help("Esc", ""),
        help("↑/k", ""),
        help("↓/j", ""),
        help("F", ""),
    ],
];

pub(super) fn render_contextual_footer(
    frame: &mut Frame<'_>,
    area: Rect,
    snapshot: &WorkflowRunViewSnapshot,
    color: bool,
    label: &'static str,
    command_options: &[&[HelpCommand]],
) {
    let lifecycle = lifecycle_control(snapshot);
    let options = command_options
        .iter()
        .enumerate()
        .map(|(index, commands)| {
            footer_option(
                commands,
                lifecycle,
                index.saturating_add(1) == command_options.len(),
            )
        })
        .collect();
    let reserved_width = u16::try_from(display_width(label).saturating_add(4)).unwrap_or(u16::MAX);
    let commands = fitting_footer(options, area.width.saturating_sub(reserved_width));
    render_footer_text(frame, area, label, commands, color);
}

pub(super) fn footer_option(
    commands: &[HelpCommand],
    lifecycle: LifecycleControl,
    abbreviate_lifecycle: bool,
) -> Vec<HelpCommand> {
    let mut parts = commands.to_vec();
    let lifecycle = match (lifecycle, abbreviate_lifecycle) {
        (LifecycleControl::Cancel, false) => Some(help("^C", "cancel run")),
        (LifecycleControl::Cancel, true) => Some(help("^C", "")),
        (LifecycleControl::Quit, _) => Some(help("q", "quit")),
        (LifecycleControl::None, _) => None,
    };
    if let Some(lifecycle) = lifecycle {
        parts.push(lifecycle);
    }
    parts.push(help("?", "help"));
    parts
}

pub(super) fn footer_width(commands: &[HelpCommand]) -> usize {
    commands
        .iter()
        .map(|command| {
            display_width(command.keys)
                + if command.description.is_empty() {
                    0
                } else {
                    1 + display_width(command.description)
                }
        })
        .sum::<usize>()
        + commands.len().saturating_sub(1) * 2
}

pub(super) fn fitting_footer(options: Vec<Vec<HelpCommand>>, width: u16) -> Vec<HelpCommand> {
    let available = usize::from(width);
    options
        .iter()
        .find(|option| footer_width(option) <= available)
        .cloned()
        .unwrap_or_else(|| vec![help("?", "help")])
}

pub(super) fn render_footer_text(
    frame: &mut Frame<'_>,
    area: Rect,
    label: &'static str,
    commands: Vec<HelpCommand>,
    color: bool,
) {
    let mut spans = vec![
        Span::styled(
            label,
            command_accent_style(color).add_modifier(Modifier::BOLD),
        ),
        Span::raw("  "),
    ];
    for (index, command) in commands.iter().enumerate() {
        if index != 0 {
            spans.push(Span::raw("  "));
        }
        let key_style = if command.keys == "?" {
            command_accent_style(color)
        } else {
            footer_key_style(color)
        };
        spans.push(Span::styled(command.keys, key_style));
        if !command.description.is_empty() {
            spans.push(Span::styled(
                format!(" {}", command.description),
                tone_style(color, Tone::Muted),
            ));
        }
    }
    frame.render_widget(
        Paragraph::new(Line::from(spans)).block(
            section_block(Borders::TOP, color)
                .border_style(footer_separator_style(color))
                .padding(Padding::horizontal(INSPECTOR_PANEL_PADDING)),
        ),
        area,
    );
}

#[derive(Clone, Copy)]
pub(super) struct HelpCommand {
    pub(super) keys: &'static str,
    pub(super) description: &'static str,
}

pub(super) struct HelpGroup {
    pub(super) title: &'static str,
    pub(super) commands: Vec<HelpCommand>,
}

pub(super) fn render_help_overlay(
    frame: &mut Frame<'_>,
    area: Rect,
    surface: HostSurface,
    lifecycle: LifecycleControl,
    color: bool,
) {
    render_help_overlay_groups(frame, area, help_groups(surface, lifecycle), color);
}

pub(super) fn render_help_overlay_groups(
    frame: &mut Frame<'_>,
    area: Rect,
    groups: Vec<HelpGroup>,
    color: bool,
) {
    let column_count = help_column_count(area.width);
    let grid_height = help_grid_height(&groups, column_count);
    let desired_height = u16::try_from(grid_height)
        .unwrap_or(u16::MAX)
        .saturating_add(3);
    let panel_height = desired_height.min(area.height);
    let panel_area = Rect::new(
        area.x,
        area.bottom().saturating_sub(panel_height),
        area.width,
        panel_height,
    );
    let block =
        section_block(Borders::TOP, color).padding(Padding::horizontal(INSPECTOR_PANEL_PADDING));
    let content_area = block.inner(panel_area);

    frame.render_widget(Clear, panel_area);
    frame.render_widget(block, panel_area);
    if content_area.is_empty() {
        return;
    }
    render_help_heading(frame, content_area, color);
    let grid_area = Rect::new(
        content_area.x,
        content_area.y.saturating_add(2),
        content_area.width,
        content_area.height.saturating_sub(2),
    );
    render_help_groups(frame, grid_area, &groups, column_count, color);
}

#[derive(Clone, Copy)]
pub(super) enum OutputHelpMode {
    Live,
    Archived,
}

pub(super) fn surface_help_groups(surface: HostSurface, mode: OutputHelpMode) -> Vec<HelpGroup> {
    match surface {
        HostSurface::Split => vec![
            HelpGroup {
                title: "MOVE",
                commands: vec![
                    HelpCommand {
                        keys: "↑/k",
                        description: "previous step",
                    },
                    HelpCommand {
                        keys: "↓/j",
                        description: "next step",
                    },
                ],
            },
            HelpGroup {
                title: "OPEN",
                commands: vec![HelpCommand {
                    keys: "↵",
                    description: match mode {
                        OutputHelpMode::Live => "open step log",
                        OutputHelpMode::Archived => "open retained output",
                    },
                }],
            },
            HelpGroup {
                title: "VIEW",
                commands: vec![
                    HelpCommand {
                        keys: "?",
                        description: "this help",
                    },
                    HelpCommand {
                        keys: "Esc",
                        description: "dismiss",
                    },
                ],
            },
        ],
        HostSurface::FullLog => {
            let noun = match mode {
                OutputHelpMode::Live => "record",
                OutputHelpMode::Archived => "row",
            };
            let mut view_commands = Vec::new();
            if matches!(mode, OutputHelpMode::Live) {
                view_commands.push(HelpCommand {
                    keys: "F",
                    description: "follow latest",
                });
            }
            view_commands.extend([
                HelpCommand {
                    keys: "Esc",
                    description: "back / dismiss",
                },
                HelpCommand {
                    keys: "?",
                    description: "this help",
                },
            ]);
            vec![
                HelpGroup {
                    title: "MOVE",
                    commands: vec![
                        HelpCommand {
                            keys: "↑/k",
                            description: if noun == "record" {
                                "one record up"
                            } else {
                                "one row up"
                            },
                        },
                        HelpCommand {
                            keys: "↓/j",
                            description: if noun == "record" {
                                "one record down"
                            } else {
                                "one row down"
                            },
                        },
                        HelpCommand {
                            keys: "PgUp/b",
                            description: "one page up",
                        },
                        HelpCommand {
                            keys: "PgDn/f/Space",
                            description: "one page down",
                        },
                        HelpCommand {
                            keys: "u/^U",
                            description: "half page up",
                        },
                        HelpCommand {
                            keys: "d/^D",
                            description: "half page down",
                        },
                    ],
                },
                HelpGroup {
                    title: "JUMP",
                    commands: vec![
                        HelpCommand {
                            keys: "g",
                            description: match mode {
                                OutputHelpMode::Live => "first record",
                                OutputHelpMode::Archived => "top",
                            },
                        },
                        HelpCommand {
                            keys: "G",
                            description: match mode {
                                OutputHelpMode::Live => "retained bottom",
                                OutputHelpMode::Archived => "bottom",
                            },
                        },
                        HelpCommand {
                            keys: "←/h",
                            description: "pan left",
                        },
                        HelpCommand {
                            keys: "→/l",
                            description: "pan right",
                        },
                    ],
                },
                HelpGroup {
                    title: "VIEW",
                    commands: view_commands,
                },
            ]
        }
    }
}

pub(super) fn help_groups(surface: HostSurface, lifecycle: LifecycleControl) -> Vec<HelpGroup> {
    let mut groups = surface_help_groups(surface, OutputHelpMode::Live);
    groups.push(HelpGroup {
        title: "FILTER",
        commands: vec![HelpCommand {
            keys: "1…n",
            description: "toggle log channels",
        }],
    });
    let command = match lifecycle {
        LifecycleControl::Cancel => Some(HelpCommand {
            keys: "^C",
            description: "cancel run",
        }),
        LifecycleControl::Quit => Some(HelpCommand {
            keys: "q",
            description: "quit",
        }),
        LifecycleControl::None => None,
    };
    if let Some(command) = command {
        groups.push(HelpGroup {
            title: "RUN",
            commands: vec![command],
        });
    }
    groups
}

pub(super) fn help_column_count(width: u16) -> usize {
    if width >= WIDE_LAYOUT_WIDTH { 4 } else { 2 }
}

pub(super) fn help_grid_height(groups: &[HelpGroup], column_count: usize) -> usize {
    groups
        .chunks(column_count)
        .enumerate()
        .map(|(index, groups)| {
            usize::from(index != 0).saturating_add(1).saturating_add(
                groups
                    .iter()
                    .map(|group| group.commands.len())
                    .max()
                    .unwrap_or(0),
            )
        })
        .sum()
}

pub(super) fn render_help_heading(frame: &mut Frame<'_>, area: Rect, color: bool) {
    frame.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled("?", command_accent_style(color)),
            Span::styled(" — all commands", tone_style(color, Tone::Muted)),
        ])),
        Rect::new(area.x, area.y, area.width, 1),
    );
    let dismissal = "esc to dismiss";
    let dismissal_width = u16::try_from(display_width(dismissal))
        .unwrap_or(u16::MAX)
        .min(area.width);
    frame.render_widget(
        Paragraph::new(Span::styled(dismissal, tone_style(color, Tone::Muted))),
        Rect::new(
            area.right().saturating_sub(dismissal_width),
            area.y,
            dismissal_width,
            1,
        ),
    );
}

pub(super) fn render_help_groups(
    frame: &mut Frame<'_>,
    area: Rect,
    groups: &[HelpGroup],
    column_count: usize,
    color: bool,
) {
    let mut y = area.y;
    for (band_index, band) in groups.chunks(column_count).enumerate() {
        if band_index != 0 {
            y = y.saturating_add(1);
        }
        let row_count = band
            .iter()
            .map(|group| group.commands.len())
            .max()
            .unwrap_or(0);
        let height = u16::try_from(row_count.saturating_add(1)).unwrap_or(u16::MAX);
        let band_area = Rect::new(
            area.x,
            y,
            area.width,
            height.min(area.bottom().saturating_sub(y)),
        );
        for (group, column) in band.iter().zip(help_column_areas(band_area, column_count)) {
            render_help_group(frame, column, group, color);
        }
        y = y.saturating_add(height);
        if y >= area.bottom() {
            break;
        }
    }
}

pub(super) fn help_column_areas(area: Rect, column_count: usize) -> Vec<Rect> {
    let gap_width = 2_u16;
    let gap_count = u16::try_from(column_count.saturating_sub(1)).unwrap_or(u16::MAX);
    let content_width = area
        .width
        .saturating_sub(gap_width.saturating_mul(gap_count));
    let column_count = u16::try_from(column_count).unwrap_or(1).max(1);
    let base_width = content_width / column_count;
    let mut remainder = content_width % column_count;
    let mut x = area.x;
    (0..column_count)
        .map(|_| {
            let width = base_width.saturating_add(u16::from(remainder != 0));
            remainder = remainder.saturating_sub(1);
            let column = Rect::new(x, area.y, width, area.height);
            x = x.saturating_add(width).saturating_add(gap_width);
            column
        })
        .collect()
}

pub(super) fn render_help_group(frame: &mut Frame<'_>, area: Rect, group: &HelpGroup, color: bool) {
    if area.is_empty() {
        return;
    }
    frame.render_widget(
        Paragraph::new(Span::styled(group.title, tone_style(color, Tone::Muted))),
        Rect::new(area.x, area.y, area.width, 1),
    );
    let key_width = usize::from(area.width / 2).min(14);
    let lines = group.commands.iter().map(|command| {
        Line::from(vec![
            Span::styled(padded_text(command.keys, key_width), help_key_style(color)),
            Span::styled("→ ", tone_style(color, Tone::Muted)),
            Span::styled(command.description, tone_style(color, Tone::Neutral)),
        ])
    });
    frame.render_widget(
        Paragraph::new(Text::from_iter(lines)),
        Rect::new(
            area.x,
            area.y.saturating_add(1),
            area.width,
            area.height.saturating_sub(1),
        ),
    );
}
