use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;

use super::*;
use crate::workflow::document::Output;
use crate::workflow::observation::{
    CommandOutputObservation, ExecutionObservation, ExecutionObserver, SourceSequence,
};
use crate::workflow::pi::Thinking;
use crate::workflow::presentation_feed::AcceptedRecordOrder;
use crate::workflow::publication::{
    WorkflowRunCancellation, WorkflowRunResult, WorkflowRunStep, WorkflowRunStepKind,
    WorkflowRunTiming, WorkflowStepTiming,
};
use crate::workflow::resolution::{self, ResolvedWorkflow};
use crate::workflow::run_timing::{ObservationClock, ObservationTime, RunTimingObservation};
use crate::workflow::run_view_model::{WorkflowRunElapsed, WorkflowRunStepLog};
use crate::workflow::runtime::{ActionId, FailurePhase, RunOutcome, StepState, TransitionSequence};
use crate::workflow::step_runtime::{CommandExecutionFailure, StepExecutionFailure};

#[test]
fn selected_terminal_events_map_to_host_controls() {
    for (event, expected) in [
        (
            Event::Key(crossterm::event::KeyEvent::new(
                KeyCode::Char('j'),
                KeyModifiers::NONE,
            )),
            TerminalInputEvent::Down,
        ),
        (
            Event::Key(crossterm::event::KeyEvent::new(
                KeyCode::Char('k'),
                KeyModifiers::NONE,
            )),
            TerminalInputEvent::Up,
        ),
        (
            Event::Key(crossterm::event::KeyEvent::new(
                KeyCode::Char('c'),
                KeyModifiers::CONTROL,
            )),
            TerminalInputEvent::Cancel,
        ),
        (
            Event::Key(crossterm::event::KeyEvent::new(
                KeyCode::Char('3'),
                KeyModifiers::NONE,
            )),
            TerminalInputEvent::ToggleLogChannel('3'),
        ),
        (
            Event::Key(crossterm::event::KeyEvent::new(
                KeyCode::Char('?'),
                KeyModifiers::SHIFT,
            )),
            TerminalInputEvent::Help,
        ),
        (
            Event::Key(crossterm::event::KeyEvent::new(
                KeyCode::Enter,
                KeyModifiers::NONE,
            )),
            TerminalInputEvent::Enter,
        ),
        (
            Event::Key(crossterm::event::KeyEvent::new(
                KeyCode::Esc,
                KeyModifiers::NONE,
            )),
            TerminalInputEvent::Escape,
        ),
        (
            Event::Key(crossterm::event::KeyEvent::new(
                KeyCode::Char('q'),
                KeyModifiers::NONE,
            )),
            TerminalInputEvent::Quit,
        ),
        (
            Event::Key(crossterm::event::KeyEvent::new(
                KeyCode::Up,
                KeyModifiers::NONE,
            )),
            TerminalInputEvent::Up,
        ),
        (Event::Resize(100, 30), TerminalInputEvent::Resize),
    ] {
        assert_eq!(terminal_input_event(event), expected);
    }
}

#[test]
fn enter_opens_the_selected_steps_full_screen_log() {
    let snapshot = direct_snapshot(direct_command_step(
        StepStateKind::Pending,
        None,
        None,
        WorkflowRunOutputDisposition::Pending,
    ));
    let graph = DagLayout::for_steps(&snapshot.steps);
    let cancellation = CancellationSource::new();
    let mut interaction = HostInteraction {
        terminal_area: Rect::new(0, 0, 120, 24),
        ..HostInteraction::default()
    };
    interaction.handle_key(
        terminal_input_event(Event::Key(crossterm::event::KeyEvent::new(
            KeyCode::Enter,
            KeyModifiers::NONE,
        ))),
        &snapshot,
        &cancellation,
    );
    let backend = ratatui::backend::TestBackend::new(120, 24);
    let mut terminal = Terminal::new(backend).unwrap();
    terminal
        .draw(|frame| render(frame, &snapshot, &graph, &mut interaction, false))
        .unwrap();
    let rendered = buffer_text(terminal.backend().buffer());

    assert!(rendered.contains("○  selected-command   cmd"));
    assert!(rendered.contains("pending"));
    assert!(rendered.contains("LOG"));
    assert!(rendered.contains("● following · 0 lines"));
    assert!(!rendered.contains("stdout + stderr"));
    assert!(!rendered.contains("workflow.yaml"));
    let inspector = buffer_position(terminal.backend().buffer(), "○  selected-command   cmd");
    let log = buffer_position(terminal.backend().buffer(), "LOG");
    assert_eq!(inspector.1, 0);
    assert!(log.1 > inspector.1);

    press_key(
        &mut interaction,
        &snapshot,
        KeyCode::Esc,
        KeyModifiers::NONE,
        &cancellation,
    );
    terminal
        .draw(|frame| render(frame, &snapshot, &graph, &mut interaction, false))
        .unwrap();
    let rendered = buffer_text(terminal.backend().buffer());
    assert_eq!(interaction.selected, 0);
    assert!(rendered.contains("workflow"));
    assert!(rendered.contains("▏ ○ selected-command"));
}

#[test]
fn full_screen_log_keeps_each_record_on_one_row() {
    let snapshot = direct_snapshot(long_log_step());
    let graph = DagLayout::for_steps(&snapshot.steps);
    let mut interaction = HostInteraction {
        surface: HostSurface::FullLog,
        ..HostInteraction::default()
    };
    let backend = ratatui::backend::TestBackend::new(64, 20);
    let mut terminal = Terminal::new(backend).unwrap();
    terminal
        .draw(|frame| render(frame, &snapshot, &graph, &mut interaction, false))
        .unwrap();
    let rows = buffer_rows(terminal.backend().buffer());
    let record_rows = rows
        .iter()
        .filter(|row| row.contains("stdout │") || row.contains("stdout ↳"))
        .collect::<Vec<_>>();

    assert_eq!(
        record_rows.len(),
        1,
        "the full-screen log must not soft-wrap retained records: {record_rows:#?}"
    );
}

#[test]
fn full_log_key_bindings_navigate_deterministic_records() {
    let snapshot = direct_snapshot(numbered_log_step(50, 140));

    for code in [KeyCode::Up, KeyCode::Char('k')] {
        let (interaction, _) = run_full_log_keys(&snapshot, 80, 20, &[(code, KeyModifiers::NONE)]);
        assert_eq!(full_log_top_order(&interaction, &snapshot), Some(47));
        assert!(!interaction.full_log.follow);
    }
    for code in [KeyCode::Down, KeyCode::Char('j')] {
        let (interaction, _) = run_full_log_keys(&snapshot, 80, 20, &[(code, KeyModifiers::NONE)]);
        assert_eq!(full_log_top_order(&interaction, &snapshot), Some(48));
        assert!(!interaction.full_log.follow);
    }
    for code in [KeyCode::PageUp, KeyCode::Char('b')] {
        let (interaction, _) = run_full_log_keys(&snapshot, 80, 20, &[(code, KeyModifiers::NONE)]);
        assert_eq!(full_log_top_order(&interaction, &snapshot), Some(45));
    }
    for code in [KeyCode::PageDown, KeyCode::Char('f'), KeyCode::Char(' ')] {
        let (interaction, _) = run_full_log_keys(
            &snapshot,
            80,
            20,
            &[
                (KeyCode::Char('g'), KeyModifiers::NONE),
                (code, KeyModifiers::NONE),
            ],
        );
        assert_eq!(full_log_top_order(&interaction, &snapshot), Some(4));
    }
    for (code, modifiers) in [
        (KeyCode::Char('u'), KeyModifiers::NONE),
        (KeyCode::Char('u'), KeyModifiers::CONTROL),
    ] {
        let (interaction, _) = run_full_log_keys(&snapshot, 80, 20, &[(code, modifiers)]);
        assert_eq!(full_log_top_order(&interaction, &snapshot), Some(47));
    }
    for (code, modifiers) in [
        (KeyCode::Char('d'), KeyModifiers::NONE),
        (KeyCode::Char('d'), KeyModifiers::CONTROL),
    ] {
        let (interaction, _) = run_full_log_keys(
            &snapshot,
            80,
            20,
            &[(KeyCode::Char('g'), KeyModifiers::NONE), (code, modifiers)],
        );
        assert_eq!(full_log_top_order(&interaction, &snapshot), Some(2));
    }
    for (code, modifiers, expected) in [
        (KeyCode::Char('g'), KeyModifiers::NONE, 1),
        (KeyCode::Char('G'), KeyModifiers::SHIFT, 48),
    ] {
        let (interaction, _) = run_full_log_keys(&snapshot, 80, 20, &[(code, modifiers)]);
        assert_eq!(full_log_top_order(&interaction, &snapshot), Some(expected));
    }
    for code in [KeyCode::Right, KeyCode::Char('l')] {
        let (interaction, _) = run_full_log_keys(&snapshot, 80, 20, &[(code, KeyModifiers::NONE)]);
        assert_eq!(interaction.full_log.horizontal_offset, 1);
    }
    for code in [KeyCode::Left, KeyCode::Char('h')] {
        let (interaction, _) = run_full_log_keys(
            &snapshot,
            80,
            20,
            &[
                (KeyCode::Right, KeyModifiers::NONE),
                (code, KeyModifiers::NONE),
            ],
        );
        assert_eq!(interaction.full_log.horizontal_offset, 0);
    }

    let (interaction, _) = run_full_log_keys(
        &snapshot,
        80,
        20,
        &[
            (KeyCode::Up, KeyModifiers::NONE),
            (KeyCode::Char('F'), KeyModifiers::SHIFT),
        ],
    );
    assert!(interaction.full_log.follow);
    assert_eq!(full_log_top_order(&interaction, &snapshot), Some(48));
}

#[test]
fn paused_log_keeps_its_anchor_and_pan_as_output_arrives() {
    let mut snapshot = direct_snapshot(numbered_log_step(30, 120));
    let (mut interaction, cancellation) = run_full_log_keys(
        &snapshot,
        80,
        20,
        &[
            (KeyCode::Up, KeyModifiers::NONE),
            (KeyCode::Right, KeyModifiers::NONE),
            (KeyCode::Right, KeyModifiers::NONE),
            (KeyCode::Right, KeyModifiers::NONE),
        ],
    );
    let anchor = full_log_top_order(&interaction, &snapshot);
    let horizontal_offset = interaction.full_log.horizontal_offset;
    assert_eq!(anchor, Some(27));
    let log = FilteredLog::new(&snapshot.steps[0].log, interaction.log_filters);
    assert_eq!(interaction.full_log.lines_behind(&log), 1);

    append_log_record(&mut snapshot.steps[0].log, 31, "new output one");
    append_log_record(&mut snapshot.steps[0].log, 32, "new output two");
    let (width, rows) = full_log_record_dimensions(interaction.terminal_area, &snapshot.steps[0]);
    let log = FilteredLog::new(&snapshot.steps[0].log, interaction.log_filters);
    interaction.full_log.synchronize(&log, width, rows);

    assert_eq!(full_log_top_order(&interaction, &snapshot), anchor);
    assert_eq!(interaction.full_log.horizontal_offset, horizontal_offset);
    assert_eq!(interaction.full_log.lines_behind(&log), 3);
    let rendered = buffer_text(&render_full_log_snapshot(
        &snapshot,
        &mut interaction,
        120,
        20,
    ));
    assert!(rendered.contains("paused · 3 lines behind"));

    press_key(
        &mut interaction,
        &snapshot,
        KeyCode::Char('F'),
        KeyModifiers::SHIFT,
        &cancellation,
    );
    assert!(interaction.full_log.follow);
    assert_eq!(full_log_top_order(&interaction, &snapshot), Some(30));
    let log = FilteredLog::new(&snapshot.steps[0].log, interaction.log_filters);
    assert_eq!(interaction.full_log.lines_behind(&log), 0);
    assert_eq!(interaction.full_log.horizontal_offset, horizontal_offset);
}

#[test]
fn paused_log_anchor_survives_terminal_resize() {
    let snapshot = direct_snapshot(numbered_log_step(40, 80));
    let (mut interaction, _) =
        run_full_log_keys(&snapshot, 80, 20, &[(KeyCode::Up, KeyModifiers::NONE)]);
    let anchor = full_log_top_order(&interaction, &snapshot);
    assert_eq!(anchor, Some(37));

    let _ = render_full_log_snapshot(&snapshot, &mut interaction, 100, 24);
    assert_eq!(full_log_top_order(&interaction, &snapshot), anchor);
    assert_eq!(interaction.full_log.available_rows, 7);

    let _ = render_full_log_snapshot(&snapshot, &mut interaction, 64, 20);
    assert_eq!(full_log_top_order(&interaction, &snapshot), anchor);
    assert_eq!(interaction.full_log.available_rows, 3);
}

#[test]
fn clamped_log_keeps_retained_and_total_counts_visible_at_minimum_width() {
    let (snapshot, mut interaction) = clamped_log_snapshot(MINIMUM_WIDTH, MINIMUM_HEIGHT);

    let rendered = buffer_text(&render_full_log_snapshot(
        &snapshot,
        &mut interaction,
        MINIMUM_WIDTH,
        MINIMUM_HEIGHT,
    ));
    assert!(
        rendered.contains("30/38 kept"),
        "counts disappeared after clamping: {rendered:?}"
    );
}

#[test]
fn eviction_clamps_a_paused_anchor_and_marks_the_clamp() {
    let (snapshot, mut interaction) = clamped_log_snapshot(140, 20);

    let buffer = render_full_log_snapshot(&snapshot, &mut interaction, 140, 20);
    let rendered = buffer_text(&buffer);
    assert_eq!(full_log_top_order(&interaction, &snapshot), Some(9));
    assert!(interaction.full_log.anchor_clamped);
    assert!(rendered.contains("↑ 8 older lines / 400 bytes discarded | clamped to retained top"));
    assert!(rendered.contains("30 retained / 38 total"));
    assert!(rendered.contains("12:34:56.000 stderr │ record 09"));
}

#[test]
fn full_log_horizontal_pan_reaches_the_end_of_wide_graphemes() {
    let payload = format!("START{}END", "界".repeat(80));
    let snapshot = direct_snapshot(direct_log_step(
        StepStateKind::Running,
        vec![direct_log_record(
            1,
            CommandOutputSource::StandardOutput,
            "2026-08-04T12:34:56Z",
            &payload,
            false,
        )],
        1,
        0,
    ));
    let (mut interaction, cancellation) = entered_full_log(&snapshot, 64, 20);
    for _ in 0..300 {
        press_key(
            &mut interaction,
            &snapshot,
            KeyCode::Right,
            KeyModifiers::NONE,
            &cancellation,
        );
    }

    let rendered = buffer_text(&render_full_log_snapshot(
        &snapshot,
        &mut interaction,
        64,
        20,
    ));
    assert!(
        rendered.contains("END"),
        "far-right payload is not reachable: {rendered:?}"
    );
}

#[test]
fn horizontal_pan_is_bounded_and_clamps_only_when_content_requires_it() {
    let long_payload = format!("START-{}-END", "x".repeat(120));
    let mut snapshot = direct_snapshot(direct_log_step(
        StepStateKind::Running,
        vec![direct_log_record(
            1,
            CommandOutputSource::StandardOutput,
            "2026-08-04T12:34:56Z",
            &long_payload,
            false,
        )],
        1,
        0,
    ));
    let (mut interaction, cancellation) =
        run_full_log_keys(&snapshot, 64, 20, &[(KeyCode::Up, KeyModifiers::NONE)]);
    for _ in 0..200 {
        press_key(
            &mut interaction,
            &snapshot,
            KeyCode::Right,
            KeyModifiers::NONE,
            &cancellation,
        );
    }
    let (available_width, _) =
        full_log_record_dimensions(interaction.terminal_area, &snapshot.steps[0]);
    let log = FilteredLog::new(&snapshot.steps[0].log, interaction.log_filters);
    let maximum = log
        .records
        .iter()
        .map(|record| log_record_horizontal_offset(record, available_width))
        .max()
        .unwrap_or(0);
    assert_eq!(interaction.full_log.horizontal_offset, maximum);
    let rendered = buffer_text(&render_full_log_snapshot(
        &snapshot,
        &mut interaction,
        64,
        20,
    ));
    assert!(rendered.contains("-END"));
    assert!(!rendered.contains("START-"));

    append_log_record(&mut snapshot.steps[0].log, 2, "short new output");
    let _ = render_full_log_snapshot(&snapshot, &mut interaction, 64, 20);
    assert_eq!(interaction.full_log.horizontal_offset, maximum);

    Arc::make_mut(&mut snapshot.steps[0].log.records).remove(0);
    snapshot.steps[0].log.retained_records = 1;
    snapshot.steps[0].log.discarded_records = 1;
    let _ = render_full_log_snapshot(&snapshot, &mut interaction, 64, 20);
    assert_eq!(interaction.full_log.horizontal_offset, 0);
}

#[test]
fn full_ring_eviction_exposes_the_next_horizontal_bound() {
    let records = (1..=4096)
        .map(|order| {
            let payload = match order {
                1 => "x".repeat(180),
                2 => "界".repeat(60),
                _ => "short".to_owned(),
            };
            direct_log_record(
                order,
                CommandOutputSource::StandardOutput,
                "2026-08-04T12:34:56Z",
                &payload,
                false,
            )
        })
        .collect();
    let mut snapshot = direct_snapshot(direct_log_step(StepStateKind::Running, records, 4096, 0));
    let (mut interaction, _) = entered_full_log(&snapshot, 64, 20);
    let (width, _) = full_log_record_dimensions(interaction.terminal_area, &snapshot.steps[0]);
    let first = log_record_horizontal_offset(&snapshot.steps[0].log.records[0], width);
    let second = log_record_horizontal_offset(&snapshot.steps[0].log.records[1], width);
    assert!(first > second && second > 0);
    interaction.full_log.horizontal_offset = first;

    for (appended, expected, bound_order) in [(4097, second, 2), (4098, 0, 4098)] {
        Arc::make_mut(&mut snapshot.steps[0].log.records).pop_front();
        append_log_record(&mut snapshot.steps[0].log, appended, "short");
        snapshot.steps[0].log.discarded_records += 1;
        let _ = render_full_log_snapshot(&snapshot, &mut interaction, 64, 20);
        assert_eq!(interaction.full_log.horizontal_offset, expected);
        assert_eq!(
            interaction
                .full_log
                .maximum_queue
                .front()
                .map(|(order, _)| order.get()),
            Some(bound_order)
        );
        assert_eq!(
            interaction
                .full_log
                .maximum_last
                .map(AcceptedRecordOrder::get),
            Some(appended)
        );
    }
}

#[test]
fn log_preview_preserves_merged_order_and_accents_stderr_without_tinting_content() {
    let step = direct_log_step(
        StepStateKind::Running,
        vec![
            direct_log_record(
                1,
                CommandOutputSource::StandardOutput,
                "2026-08-04T12:34:56.100Z",
                "first from stdout",
                false,
            ),
            direct_log_record(
                2,
                CommandOutputSource::StandardError,
                "2026-08-04T12:34:56.200Z",
                "then from stderr",
                false,
            ),
            direct_log_record(
                3,
                CommandOutputSource::StandardOutput,
                "2026-08-04T12:34:56.300Z",
                "last from stdout",
                false,
            ),
        ],
        3,
        0,
    );
    let buffer = render_direct_log(&step, 80, 7, true);
    let rows = buffer_rows(&buffer);
    let first = row_containing(&rows, "first from stdout");
    let second = row_containing(&rows, "then from stderr");
    let third = row_containing(&rows, "last from stdout");

    assert!(first < second && second < third);
    assert!(rows[first].contains("stdout │ first from stdout"));
    assert!(rows[second].contains("stderr │ then from stderr"));
    assert!(rows[third].contains("stdout │ last from stdout"));

    let payload_column = column_of(&rows[second], "then from stderr");
    let second = u16::try_from(second).unwrap();
    assert_eq!(
        buffer[(payload_column, second)].fg,
        tone_style(true, Tone::Neutral).fg.unwrap()
    );
    let source_column = column_of(&rows[usize::from(second)], "stderr");
    assert_eq!(
        buffer[(source_column, second)].fg,
        tone_style(true, Tone::Blocked).fg.unwrap()
    );
    assert_ne!(
        buffer[(source_column, second)].fg,
        tone_style(true, Tone::Failure).fg.unwrap()
    );
}

#[test]
fn numbered_channels_filter_command_logs_and_report_hidden_records() {
    let step = direct_log_step(
        StepStateKind::Running,
        vec![
            direct_log_record(
                1,
                CommandOutputSource::StandardOutput,
                "2026-08-04T12:34:56Z",
                "visible stdout payload",
                false,
            ),
            direct_log_record(
                2,
                CommandOutputSource::StandardError,
                "2026-08-04T12:34:57Z",
                "hidden stderr payload",
                false,
            ),
        ],
        2,
        0,
    );
    let snapshot = direct_snapshot(step);
    let cancellation = CancellationSource::new();
    let mut interaction = HostInteraction {
        terminal_area: Rect::new(0, 0, 120, 24),
        ..HostInteraction::default()
    };

    press_key(
        &mut interaction,
        &snapshot,
        KeyCode::Char('2'),
        KeyModifiers::NONE,
        &cancellation,
    );
    let filtered = render_snapshot(&snapshot, &mut interaction, 120, 24, true);
    let rendered = buffer_text(&filtered);
    assert!(rendered.contains("visible stdout payload"));
    assert!(!rendered.contains("hidden stderr payload"));
    assert!(rendered.contains("● following · 1 hidden"));
    let (stdout_x, stdout_y) = buffer_position(&filtered, "1 stdout");
    assert!(
        filtered[(stdout_x, stdout_y)]
            .modifier
            .contains(Modifier::UNDERLINED)
    );
    let (stderr_x, stderr_y) = buffer_position(&filtered, "2 stderr");
    assert!(
        filtered[(stderr_x, stderr_y)]
            .modifier
            .contains(Modifier::DIM)
    );

    press_key(
        &mut interaction,
        &snapshot,
        KeyCode::Char('1'),
        KeyModifiers::NONE,
        &cancellation,
    );
    let all_hidden = buffer_text(&render_snapshot(&snapshot, &mut interaction, 120, 24, true));
    assert!(all_hidden.contains("● following · 2 hidden"));
    assert!(all_hidden.contains("All log channels hidden."));
}

#[test]
fn filtered_log_tracks_hidden_appends_and_eviction() {
    let mut snapshot = direct_snapshot(numbered_log_step(4, 20));
    let mut interaction = HostInteraction::default();
    assert!(interaction.log_filters.toggle(&snapshot.steps[0], '2'));
    interaction.prepare_filtered_log(&snapshot.steps[0].log, 0, snapshot.generation);
    assert_eq!(interaction.filtered_log.records.len(), 2);

    append_log_record(&mut snapshot.steps[0].log, 5, "hidden");
    interaction.prepare_filtered_log(&snapshot.steps[0].log, 0, snapshot.generation + 1);
    assert_eq!(interaction.filtered_log.records.len(), 2);
    assert_eq!(interaction.filtered_log.hidden_records, 3);

    Arc::make_mut(&mut snapshot.steps[0].log.records).drain(..3);
    snapshot.steps[0].log.discarded_records = 3;
    append_log_record(&mut snapshot.steps[0].log, 6, "visible");
    interaction.prepare_filtered_log(&snapshot.steps[0].log, 0, snapshot.generation + 2);
    assert_eq!(interaction.filtered_log.records.len(), 2);
    assert_eq!(
        interaction.filtered_log.records[0].accepted_order,
        AcceptedRecordOrder::for_test(4)
    );
    assert_eq!(
        interaction.filtered_log.records[1].payload.as_ref(),
        "visible"
    );
    assert_eq!(interaction.filtered_log.hidden_records, 1);
}

#[test]
fn full_log_navigation_skips_filtered_records() {
    let snapshot = direct_snapshot(numbered_log_step(30, 20));
    let (mut interaction, _) = run_full_log_keys(
        &snapshot,
        120,
        24,
        &[
            (KeyCode::Char('2'), KeyModifiers::NONE),
            (KeyCode::Char('g'), KeyModifiers::NONE),
            (KeyCode::Down, KeyModifiers::NONE),
        ],
    );

    assert_eq!(full_log_top_order(&interaction, &snapshot), Some(4));
    let rendered = buffer_text(&render_full_log_snapshot(
        &snapshot,
        &mut interaction,
        120,
        24,
    ));
    assert!(rendered.contains("15 hidden"));
}

#[test]
fn agent_channels_group_observations_and_dim_secondary_rows() {
    let step = direct_agent_log_step(vec![
        direct_agent_log_record(
            1,
            AgentPresentationObservationKind::Assistant,
            "agent message",
        ),
        direct_agent_log_record(
            2,
            AgentPresentationObservationKind::Reasoning,
            "reasoning message",
        ),
        direct_agent_log_record(3, AgentPresentationObservationKind::ToolCall, "tool call"),
        direct_agent_log_record(
            4,
            AgentPresentationObservationKind::ToolResult,
            "tool result",
        ),
        direct_agent_log_record(
            5,
            AgentPresentationObservationKind::Diagnostic,
            "diagnostic message",
        ),
        direct_agent_log_record(6, AgentPresentationObservationKind::Usage, "usage message"),
    ]);
    let snapshot = direct_snapshot(step.clone());
    let cancellation = CancellationSource::new();
    let mut interaction = HostInteraction {
        terminal_area: Rect::new(0, 0, 180, 24),
        ..HostInteraction::default()
    };
    press_key(
        &mut interaction,
        &snapshot,
        KeyCode::Char('3'),
        KeyModifiers::NONE,
        &cancellation,
    );
    let rendered = render_snapshot(&snapshot, &mut interaction, 180, 24, true);
    let text = buffer_text(&rendered);
    for channel in ["1 agent", "2 reasoning", "3 tools", "4 system"] {
        assert!(text.contains(channel), "missing channel {channel:?}");
    }
    assert!(!text.contains("tool call"));
    assert!(!text.contains("tool result"));
    assert!(text.contains("2 hidden"));
    let (tools_x, tools_y) = buffer_position(&rendered, "3 tools");
    assert!(
        rendered[(tools_x, tools_y)]
            .modifier
            .contains(Modifier::DIM)
    );

    let mut filters = LogFilterState::default();
    assert!(filters.toggle(&step, '3'));
    let without_tools = FilteredLog::new(&step.log, filters);
    assert_eq!(without_tools.hidden_records, 2);
    assert!(
        without_tools
            .records
            .iter()
            .all(|record| !matches!(LogChannel::for_source(record.source), LogChannel::Tools))
    );

    assert!(filters.toggle(&step, '4'));
    let without_tools_or_system = FilteredLog::new(&step.log, filters);
    assert_eq!(without_tools_or_system.hidden_records, 4);
    assert_eq!(
        without_tools_or_system
            .records
            .iter()
            .map(|record| record.payload.as_ref())
            .collect::<Vec<_>>(),
        ["agent message", "reasoning message"]
    );
    assert!(filters.includes(LogChannel::StandardOutput));
    assert!(filters.includes(LogChannel::StandardError));

    assert!(
        log_payload_style(
            WorkflowRunLogSource::Agent(AgentPresentationObservationKind::Reasoning),
            false,
        )
        .add_modifier
        .contains(Modifier::DIM)
    );
    assert!(
        log_payload_style(
            WorkflowRunLogSource::Agent(AgentPresentationObservationKind::ToolResult),
            false,
        )
        .add_modifier
        .contains(Modifier::DIM)
    );
    assert_eq!(
        log_source_style(
            WorkflowRunLogSource::Agent(AgentPresentationObservationKind::Diagnostic),
            true,
        )
        .fg,
        tone_style(true, Tone::Blocked).fg
    );
}

#[test]
fn log_timestamps_are_utc_with_milliseconds_and_elide_before_sources() {
    let step = direct_log_step(
        StepStateKind::Running,
        vec![direct_log_record(
            1,
            CommandOutputSource::StandardError,
            "2026-08-04T12:34:56.789123+02:00",
            "message",
            false,
        )],
        1,
        0,
    );

    let wide = buffer_rows(&render_direct_log(&step, 50, 5, false));
    assert!(
        wide.iter()
            .any(|row| row.contains("10:34:56.789 stderr │ message"))
    );

    let timestamp_boundary = buffer_rows(&render_direct_log(&step, 40, 5, false));
    assert!(
        timestamp_boundary
            .iter()
            .any(|row| row.contains("10:34:56.789 stderr │ message"))
    );

    let narrow = buffer_rows(&render_direct_log(&step, 39, 5, false));
    assert!(narrow.iter().any(|row| row.contains("stderr │ message")));
    assert!(narrow.iter().all(|row| !row.contains("10:34:56.789")));
}

#[test]
fn log_preview_preserves_safety_continuation_record_metadata() {
    let step = direct_log_step(
        StepStateKind::Running,
        vec![
            direct_log_record(
                1,
                CommandOutputSource::StandardError,
                "2026-08-04T12:34:56.100Z",
                "first fragment",
                false,
            ),
            direct_log_record(
                2,
                CommandOutputSource::StandardError,
                "2026-08-04T12:34:56.200Z",
                "continued fragment",
                true,
            ),
        ],
        2,
        0,
    );

    let rows = buffer_rows(&render_direct_log(&step, 80, 5, false));
    assert!(
        rows.iter()
            .any(|row| row.contains("12:34:56.200 stderr ↪ continued fragment")),
        "a safety-continuation record must retain its own timestamp and remain distinct from a visual wrap: {rows:#?}"
    );
}

#[test]
fn log_preview_rewraps_deterministically_and_keeps_the_visual_tail() {
    let step = long_log_step();

    let complete = inner_buffer_rows(&render_direct_log(&step, 34, 8, false));
    assert_eq!(
        complete[2..],
        [
            "  stdout │ abcdefghijklmnopqrs",
            "         ↳ tuvwxyzABCDEFGHIJKL",
            "         ↳ MNOPQRSTUVWXYZ01234",
            "         ↳ 56789",
        ]
    );

    let wide = inner_buffer_rows(&render_direct_log(&step, 34, 7, false));
    assert!(wide[0].trim_start().starts_with("LOG"));
    assert_eq!(
        wide[2..],
        [
            "  stdout ↳ tuvwxyzABCDEFGHIJKL",
            "         ↳ MNOPQRSTUVWXYZ01234",
            "         ↳ 56789",
        ]
    );

    let narrow = inner_buffer_rows(&render_direct_log(&step, 27, 7, false));
    assert!(narrow[0].contains("● 1 line"));
    assert_eq!(
        narrow[2..],
        [
            "  stdout ↳ KLMNOPQRSTUV",
            "         ↳ WXYZ01234567",
            "         ↳ 89",
        ]
    );
    assert_eq!(
        inner_buffer_rows(&render_direct_log(&step, 27, 7, false)),
        narrow
    );
}

#[test]
fn large_history_preview_keeps_only_the_visible_visual_tail() {
    use crate::workflow::presentation_feed::MAX_NORMALIZED_CHILD_RECORD_BYTES;

    let records = (1..=256)
        .map(|order| {
            let payload = if order == 256 {
                format!(
                    "{}NEWEST",
                    "z".repeat(MAX_NORMALIZED_CHILD_RECORD_BYTES - "NEWEST".len())
                )
            } else {
                "x".repeat(MAX_NORMALIZED_CHILD_RECORD_BYTES)
            };
            direct_log_record(
                order,
                CommandOutputSource::StandardOutput,
                "2026-08-04T12:34:56Z",
                &payload,
                false,
            )
        })
        .collect();
    let step = direct_log_step(StepStateKind::Running, records, 256, 0);

    let rows = inner_buffer_rows(&render_direct_log(&step, 34, 7, false));
    assert_eq!(rows.len(), 5);
    assert!(rows[4].ends_with("NEWEST"), "unexpected tail: {rows:#?}");
    assert!(rows[2].contains("stdout"));
    assert!(rows[3..].iter().all(|row| !row.contains("stdout")));
}

#[test]
fn log_preview_reports_counts_following_and_evicted_history() {
    let mut step = direct_log_step(
        StepStateKind::Succeeded,
        vec![
            direct_log_record(
                3,
                CommandOutputSource::StandardOutput,
                "2026-08-04T12:34:56Z",
                "retained one",
                false,
            ),
            direct_log_record(
                4,
                CommandOutputSource::StandardError,
                "2026-08-04T12:34:57Z",
                "retained two",
                false,
            ),
            direct_log_record(
                5,
                CommandOutputSource::StandardOutput,
                "2026-08-04T12:34:58Z",
                "retained three",
                false,
            ),
        ],
        5,
        2,
    );
    step.log.discarded_bytes = 37;
    let buffer = render_direct_log(&step, 100, 8, false);
    let rendered = buffer_text(&buffer);
    let rows = inner_buffer_rows(&buffer);

    assert!(rendered.contains("● following · 3 retained / 5 total"));
    assert!(rows[0].trim_start().starts_with("LOG"));
    assert!(rows[0].contains("● following · 3 retained / 5 total"));
    assert!(rows[1].trim().is_empty());
    assert_eq!(rows[2].trim(), "↑ 2 older lines / 37 bytes discarded");
    assert!(rows[3].ends_with("retained one"));
    assert!(rows[4].ends_with("retained two"));
    assert!(rows[5].ends_with("retained three"));

    let minimum_height = inner_buffer_rows(&render_direct_log(&step, 100, 6, false));
    assert!(minimum_height[0].trim_start().starts_with("LOG"));
    assert!(minimum_height[1].trim().is_empty());
    assert_eq!(
        minimum_height[2].trim(),
        "↑ 2 older lines / 37 bytes discarded"
    );
    assert!(minimum_height[3].ends_with("retained three"));
    assert!(
        !minimum_height
            .iter()
            .any(|row| row.ends_with("retained two"))
    );
}

#[test]
fn empty_log_preview_distinguishes_waiting_and_no_output() {
    for (state, expected) in [
        (StepStateKind::Pending, "Waiting for this step to start."),
        (StepStateKind::Running, "Waiting for output…"),
        (StepStateKind::Succeeded, "No output received."),
    ] {
        let step = direct_log_step(state, Vec::new(), 0, 0);
        let rows = inner_buffer_rows(&render_direct_log(&step, 70, 5, false));
        assert!(rows[0].trim_start().starts_with("LOG"));
        assert!(rows[1].trim().is_empty());
        assert_eq!(rows[2].trim_start(), expected);
    }
}

#[test]
fn navigation_is_bounded_and_ctrl_c_uses_user_request() {
    let cancellation = CancellationSource::new();
    let mut snapshot = direct_snapshot(long_log_step());
    snapshot.steps.push(snapshot.steps[0].clone());
    let mut interaction = HostInteraction {
        terminal_area: Rect::new(0, 0, MINIMUM_WIDTH, MINIMUM_HEIGHT),
        ..HostInteraction::default()
    };
    assert_eq!(
        interaction.handle_key(TerminalInputEvent::Down, &snapshot, &cancellation),
        HostControl::Continue
    );
    assert_eq!(interaction.selected, 1);
    interaction.handle_key(TerminalInputEvent::Down, &snapshot, &cancellation);
    assert_eq!(interaction.selected, 1);
    interaction.handle_key(TerminalInputEvent::Up, &snapshot, &cancellation);
    interaction.handle_key(TerminalInputEvent::Up, &snapshot, &cancellation);
    assert_eq!(interaction.selected, 0);

    press_key(
        &mut interaction,
        &snapshot,
        KeyCode::Char('c'),
        KeyModifiers::CONTROL,
        &cancellation,
    );
    assert_eq!(
        cancellation.cancellation_reason(),
        Some(CancellationReason::UserRequest)
    );
}

#[test]
fn finalization_ctrl_c_escalates_from_graceful_to_force_abort() {
    let cancellation = CancellationSource::new();
    assert!(cancellation.begin_finalization_arm());
    assert!(cancellation.complete_finalization_arm());
    let mut operations = cancellation.subscribe_operations();
    let mut snapshot = direct_snapshot(long_log_step());
    let trigger = crate::workflow::document::FinalizationTrigger::Succeeded;
    snapshot.workflow = WorkflowState::Finalizing {
        trigger,
        gate: crate::workflow::runtime::FinalizationGate::Open,
        primary_issue: None,
    };
    let mut interaction = HostInteraction::default();

    interaction.handle_key(TerminalInputEvent::Cancel, &snapshot, &cancellation);
    assert!(matches!(
        operations.next_operation(),
        Some(CancellationOperation::Graceful {
            reason: CancellationReason::UserRequest,
            ..
        })
    ));

    snapshot.workflow = WorkflowState::Finalizing {
        trigger,
        gate: crate::workflow::runtime::FinalizationGate::Cancelling {
            reason: CancellationReason::UserRequest,
            deadline: Some(time::OffsetDateTime::UNIX_EPOCH),
            force_abort: false,
        },
        primary_issue: None,
    };
    interaction.handle_key(TerminalInputEvent::Cancel, &snapshot, &cancellation);
    assert!(matches!(
        operations.next_operation(),
        Some(CancellationOperation::ForceAbort { .. })
    ));
}

#[test]
fn quit_requires_adapter_completion_on_every_surface() {
    let cancellation = CancellationSource::new();
    let mut snapshot = direct_snapshot(long_log_step());
    snapshot.workflow = WorkflowState::Succeeded;
    snapshot.authoritative_result = true;
    snapshot.quiescent = true;
    snapshot.publication =
        WorkflowRunPublicationState::Completed(WorkflowRunPublicationResult::Succeeded {
            result_directory: "results".to_owned(),
        });
    snapshot.cleanup = WorkflowRunCleanupState::Completed(WorkflowRunCleanupResult::Succeeded);

    let operational = Rect::new(0, 0, MINIMUM_WIDTH, MINIMUM_HEIGHT);
    let mut interactions = [
        HostInteraction {
            terminal_area: operational,
            ..HostInteraction::default()
        },
        HostInteraction {
            surface: HostSurface::FullLog,
            terminal_area: operational,
            ..HostInteraction::default()
        },
        HostInteraction {
            help_visible: true,
            terminal_area: operational,
            ..HostInteraction::default()
        },
        HostInteraction {
            surface: HostSurface::FullLog,
            help_visible: true,
            terminal_area: operational,
            ..HostInteraction::default()
        },
        HostInteraction {
            terminal_area: Rect::new(0, 0, 40, 8),
            ..HostInteraction::default()
        },
    ];

    for interaction in &mut interactions {
        assert_quit_control(interaction, &snapshot, &cancellation, HostControl::Continue);
    }

    press_key(
        &mut interactions[0],
        &snapshot,
        KeyCode::Char('c'),
        KeyModifiers::CONTROL,
        &cancellation,
    );
    assert_eq!(cancellation.cancellation_reason(), None);

    snapshot.quit_eligible = true;
    for interaction in &mut interactions {
        assert_quit_control(interaction, &snapshot, &cancellation, HostControl::Quit);
    }
}

#[test]
fn restoration_attempts_every_operation_after_cursor_failure() {
    let actions = RefCell::new(Vec::new());
    let mut output = io::sink();

    let failure = attempt_terminal_restoration(
        true,
        &mut output,
        |_| {
            actions.borrow_mut().push("leave alternate screen");
            Ok(())
        },
        |_| {
            actions.borrow_mut().push("show cursor");
            Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "injected cursor restoration failure",
            ))
        },
        |_| {
            actions.borrow_mut().push("flush output");
            Ok(())
        },
        || {
            actions.borrow_mut().push("restore input mode");
            Ok(())
        },
    )
    .unwrap_err();

    assert_eq!(failure.kind(), io::ErrorKind::BrokenPipe);
    assert_eq!(
        actions.into_inner(),
        [
            "leave alternate screen",
            "show cursor",
            "flush output",
            "restore input mode"
        ]
    );
}

#[tokio::test]
async fn setup_and_initial_render_failures_attempt_restoration_before_execution() {
    for (failures, expected_operation, expected_actions) in [
        (
            BoundaryFailures {
                setup: true,
                ..BoundaryFailures::default()
            },
            PresentationFailureOperation::TerminalSetup,
            vec![BoundaryAction::Setup, BoundaryAction::Restore],
        ),
        (
            BoundaryFailures {
                draw_at: Some(1),
                ..BoundaryFailures::default()
            },
            PresentationFailureOperation::TerminalDraw,
            vec![
                BoundaryAction::Setup,
                BoundaryAction::Draw(Rect::new(0, 0, 80, 24)),
                BoundaryAction::Restore,
            ],
        ),
    ] {
        let (_temporary, _workflow, view, _) = scripted_host_view();
        let cancellation = CancellationSource::new();
        let (boundary, _input, mut actions) =
            ScriptedTerminalBoundary::new(Rect::new(0, 0, 80, 24), [], failures);

        let mut host =
            WorkflowTerminalHost::start_with_boundary(view, cancellation.clone(), false, boundary)
                .unwrap();
        let failure = host.await_ready().await.unwrap_err();
        assert_eq!(host.wait().await.unwrap_err(), failure);

        assert_eq!(failure.operation, expected_operation);
        assert_eq!(cancellation.cancellation_reason(), None);
        assert_eq!(
            std::iter::from_fn(|| actions.try_recv().ok()).collect::<Vec<_>>(),
            expected_actions
        );
    }
}

#[tokio::test]
async fn terminal_input_is_inert_until_execution_is_activated() {
    let (_temporary, _workflow, view, _) = scripted_host_view();
    let cancellation = CancellationSource::new();
    let (boundary, input, mut actions) =
        ScriptedTerminalBoundary::new(Rect::new(0, 0, 80, 24), [], BoundaryFailures::default());
    let host =
        WorkflowTerminalHost::start_with_boundary(view, cancellation.clone(), false, boundary)
            .unwrap();
    wait_for_action(&mut actions, BoundaryAction::Setup).await;
    wait_for_action(&mut actions, BoundaryAction::Draw(Rect::new(0, 0, 80, 24))).await;

    input.send(ScriptedInput::Failure).unwrap();
    assert_eq!(cancellation.cancellation_reason(), None);
    assert_eq!(host.stop().await.unwrap(), TerminalHostExit::Stopped);
    wait_for_action(&mut actions, BoundaryAction::Restore).await;
    assert_eq!(cancellation.cancellation_reason(), None);
    assert!(actions.try_recv().is_err());
}

#[tokio::test]
async fn scripted_input_keeps_q_inert_during_execution_and_restores_after_cancellation() {
    let (_temporary, workflow, view, now) = scripted_host_view();
    let cancellation = CancellationSource::new();
    let (boundary, input, mut actions) = ScriptedTerminalBoundary::new(
        Rect::new(0, 0, 80, 24),
        [Rect::new(0, 0, 40, 8), Rect::new(0, 0, 100, 30)],
        BoundaryFailures::default(),
    );
    let host = start_active_scripted_host(view.clone(), cancellation.clone(), boundary);
    wait_for_action(&mut actions, BoundaryAction::Setup).await;
    wait_for_action(&mut actions, BoundaryAction::Draw(Rect::new(0, 0, 80, 24))).await;

    input
        .send(ScriptedInput::Event(TerminalInputEvent::Resize))
        .unwrap();
    wait_for_action(&mut actions, BoundaryAction::Resize(Rect::new(0, 0, 40, 8))).await;
    wait_for_action(&mut actions, BoundaryAction::Draw(Rect::new(0, 0, 40, 8))).await;

    input
        .send(ScriptedInput::Event(TerminalInputEvent::Quit))
        .unwrap();
    wait_for_action(
        &mut actions,
        BoundaryAction::Input(TerminalInputEvent::Quit),
    )
    .await;
    wait_for_action(&mut actions, BoundaryAction::Draw(Rect::new(0, 0, 40, 8))).await;
    assert_eq!(cancellation.cancellation_reason(), None);

    input
        .send(ScriptedInput::Event(TerminalInputEvent::Cancel))
        .unwrap();
    assert_eq!(
        cancellation.wait_for_cancellation().await,
        CancellationReason::UserRequest
    );
    wait_for_action(
        &mut actions,
        BoundaryAction::Input(TerminalInputEvent::Cancel),
    )
    .await;

    input
        .send(ScriptedInput::Event(TerminalInputEvent::Resize))
        .unwrap();
    wait_for_action(
        &mut actions,
        BoundaryAction::Resize(Rect::new(0, 0, 100, 30)),
    )
    .await;
    complete_scripted_view(&view, &workflow, now, Some(CancellationReason::UserRequest));
    input
        .send(ScriptedInput::Event(TerminalInputEvent::Quit))
        .unwrap();
    assert_eq!(host.wait().await.unwrap(), TerminalHostExit::Quit);
    wait_for_action(&mut actions, BoundaryAction::Restore).await;
    assert_eq!(
        cancellation.cancellation_reason(),
        Some(CancellationReason::UserRequest)
    );
}

#[tokio::test]
async fn normal_stop_and_injected_runtime_failures_restore_the_terminal() {
    let (_temporary, _workflow, view, _) = scripted_host_view();
    let cancellation = CancellationSource::new();
    let (boundary, _input, mut actions) =
        ScriptedTerminalBoundary::new(Rect::new(0, 0, 80, 24), [], BoundaryFailures::default());
    let host =
        WorkflowTerminalHost::start_with_boundary(view, cancellation.clone(), false, boundary)
            .unwrap();
    assert_eq!(host.stop().await.unwrap(), TerminalHostExit::Stopped);
    wait_for_action(&mut actions, BoundaryAction::Restore).await;
    assert_eq!(cancellation.cancellation_reason(), None);

    assert_scripted_runtime_failure(
        BoundaryFailures {
            draw_at: Some(2),
            ..BoundaryFailures::default()
        },
        ScriptedInput::Event(TerminalInputEvent::Other),
        PresentationFailureOperation::TerminalDraw,
    )
    .await;
    assert_scripted_runtime_failure(
        BoundaryFailures::default(),
        ScriptedInput::Failure,
        PresentationFailureOperation::TerminalInput,
    )
    .await;
}

#[tokio::test]
async fn terminal_task_unwind_requests_cancellation_before_application_join() {
    let (_temporary, _workflow, view, _) = scripted_host_view();
    let cancellation = CancellationSource::new();
    let (boundary, input, mut actions) =
        ScriptedTerminalBoundary::new(Rect::new(0, 0, 80, 24), [], BoundaryFailures::default());
    let host = start_active_scripted_host(view, cancellation.clone(), boundary);
    input.send(ScriptedInput::Panic).unwrap();

    while actions.recv().await.is_some() {}
    let failure = host.wait().await.unwrap_err();
    assert_eq!(
        failure.panic_message.as_deref(),
        Some("injected terminal input panic")
    );

    assert_eq!(
        cancellation.cancellation_reason(),
        Some(CancellationReason::CallerOutputFailure),
        "an active workflow must be cancelled as soon as its terminal task unwinds"
    );
    assert_eq!(
        failure.operation,
        PresentationFailureOperation::TerminalTask
    );
}

#[cfg(unix)]
#[tokio::test]
async fn widget_panic_restores_pty_and_reports_on_stderr() {
    use std::os::unix::ffi::OsStrExt as _;
    use std::process::{Command, Stdio};

    if std::env::var_os("SCHERZO_WIDGET_PANIC_CHILD").is_some() {
        struct PanickingWidget(SystemTerminalBoundary);
        impl TerminalBoundary for PanickingWidget {
            fn setup(&mut self) -> io::Result<Rect> {
                self.0.setup()
            }
            fn next_event(
                &mut self,
            ) -> impl Future<Output = io::Result<TerminalInputEvent>> + Send {
                self.0.next_event()
            }
            fn resize(&mut self) -> io::Result<Rect> {
                self.0.resize()
            }
            fn restore(&mut self) -> io::Result<()> {
                self.0.restore()
            }
        }
        impl WorkflowTerminalBoundary for PanickingWidget {
            fn draw_workflow(
                &mut self,
                _: &WorkflowRunViewSnapshot,
                _: &mut HostInteraction,
                _: bool,
            ) -> io::Result<()> {
                std::panic::panic_any("pty widget diagnostic");
            }
        }
        let (_temporary, _workflow, view, _) = scripted_host_view();
        let original = tcgetattr(io::stdin()).unwrap();
        let mut host = WorkflowTerminalHost::start_with_boundary(
            view,
            CancellationSource::new(),
            false,
            PanickingWidget(SystemTerminalBoundary::new()),
        )
        .unwrap();
        assert_eq!(
            host.await_ready().await.unwrap_err().operation,
            PresentationFailureOperation::TerminalTask
        );
        let failure = host.stop().await.unwrap_err();
        assert_eq!(
            failure.panic_message.as_deref(),
            Some("pty widget diagnostic")
        );
        let restored = tcgetattr(io::stdin()).unwrap();
        assert_eq!(restored.local_modes, original.local_modes);
        assert_eq!(restored.input_modes, original.input_modes);
        writeln!(io::stderr().lock(), "{failure}").unwrap();
        return;
    }

    let master = rustix::pty::openpt(rustix::pty::OpenptFlags::RDWR).unwrap();
    rustix::pty::grantpt(&master).unwrap();
    rustix::pty::unlockpt(&master).unwrap();
    let name = rustix::pty::ptsname(&master, Vec::new()).unwrap();
    let slave = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(std::ffi::OsStr::from_bytes(name.as_bytes()))
        .unwrap();
    rustix::termios::tcsetwinsize(
        &slave,
        rustix::termios::Winsize {
            ws_row: 24,
            ws_col: 80,
            ws_xpixel: 0,
            ws_ypixel: 0,
        },
    )
    .unwrap();
    let before = tcgetattr(&slave).unwrap();
    let child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "workflow::terminal_host::tests::widget_panic_restores_pty_and_reports_on_stderr",
            "--nocapture",
        ])
        .env("SCHERZO_WIDGET_PANIC_CHILD", "1")
        .stdin(Stdio::from(slave.try_clone().unwrap()))
        .stdout(Stdio::from(slave.try_clone().unwrap()))
        .stderr(Stdio::piped())
        .output()
        .unwrap();
    assert!(
        child.status.success(),
        "{}",
        String::from_utf8_lossy(&child.stderr)
    );
    let after = tcgetattr(&slave).unwrap();
    assert_eq!(after.input_modes, before.input_modes);
    assert_eq!(after.output_modes, before.output_modes);
    assert_eq!(after.control_modes, before.control_modes);
    assert_eq!(after.local_modes, before.local_modes);
    let stderr = String::from_utf8_lossy(&child.stderr);
    assert!(
        stderr.contains("workflow run output failure: TerminalTask: pty widget diagnostic"),
        "{stderr}"
    );
    // The child has exited, so every write is complete. Drain while the slave
    // remains open: some PTYs discard unread output when the last slave closes.
    let flags = rustix::fs::fcntl_getfl(&master).unwrap();
    rustix::fs::fcntl_setfl(&master, flags | rustix::fs::OFlags::NONBLOCK).unwrap();
    let mut output = Vec::new();
    let mut chunk = [0_u8; 4096];
    loop {
        match rustix::io::read(&master, &mut chunk) {
            Ok(0) | Err(rustix::io::Errno::AGAIN) | Err(rustix::io::Errno::IO) => break,
            Ok(count) => output.extend_from_slice(&chunk[..count]),
            Err(error) => panic!("read restored pty: {error}"),
        }
    }
    assert!(output.windows(8).any(|window| window == b"\x1b[?1049l"));
}

#[tokio::test]
async fn widget_panic_restores_before_reporting_its_payload() {
    let (_temporary, _workflow, view, _) = scripted_host_view();
    let cancellation = CancellationSource::new();
    let (boundary, input, mut actions) = ScriptedTerminalBoundary::new(
        Rect::new(0, 0, 80, 24),
        [],
        BoundaryFailures {
            panic_at: Some(2),
            ..BoundaryFailures::default()
        },
    );
    let host = start_active_scripted_host(view, cancellation.clone(), boundary);
    input
        .send(ScriptedInput::Event(TerminalInputEvent::Other))
        .unwrap();
    let failure = host.wait().await.unwrap_err();
    let events = std::iter::from_fn(|| actions.try_recv().ok()).collect::<Vec<_>>();
    assert_eq!(events.last(), Some(&BoundaryAction::Restore));
    assert_eq!(
        failure.panic_message.as_deref(),
        Some("injected widget panic")
    );
    assert_eq!(
        cancellation.cancellation_reason(),
        Some(CancellationReason::CallerOutputFailure)
    );
}

#[tokio::test]
async fn teardown_failure_and_task_unwind_attempt_restoration_with_closed_precedence() {
    let (_temporary, workflow, view, now) = scripted_host_view();
    let cancellation = CancellationSource::new();
    let (boundary, input, mut actions) = ScriptedTerminalBoundary::new(
        Rect::new(0, 0, 80, 24),
        [],
        BoundaryFailures {
            restore: true,
            ..BoundaryFailures::default()
        },
    );
    let host = start_active_scripted_host(view.clone(), cancellation.clone(), boundary);
    complete_scripted_view(&view, &workflow, now, None);
    input
        .send(ScriptedInput::Event(TerminalInputEvent::Quit))
        .unwrap();
    let failure = host.wait().await.unwrap_err();
    assert_eq!(
        failure.operation,
        PresentationFailureOperation::TerminalRestore
    );
    assert_eq!(cancellation.cancellation_reason(), None);
    wait_for_action(&mut actions, BoundaryAction::Restore).await;

    assert_scripted_runtime_failure(
        BoundaryFailures::default(),
        ScriptedInput::Panic,
        PresentationFailureOperation::TerminalTask,
    )
    .await;
}

#[test]
fn inspector_renders_authoritative_command_states_and_dispositions() {
    let timing = WorkflowRunElapsed {
        started_at: time::OffsetDateTime::UNIX_EPOCH,
        duration: Duration::from_millis(1_250),
        frozen: true,
    };
    let cases = [
        (
            direct_command_step(
                StepStateKind::Pending,
                None,
                None,
                WorkflowRunOutputDisposition::Pending,
            ),
            ["pending", "file", "—"].as_slice(),
        ),
        (
            direct_command_step(
                StepStateKind::Running,
                None,
                Some(WorkflowRunElapsed {
                    frozen: false,
                    ..timing.clone()
                }),
                WorkflowRunOutputDisposition::Pending,
            ),
            ["running", "1.2s", "1970-01-01 00:00:00Z"].as_slice(),
        ),
        (
            direct_command_step(
                StepStateKind::Succeeded,
                None,
                Some(timing.clone()),
                WorkflowRunOutputDisposition::Committed,
            ),
            ["succeeded", "captured"].as_slice(),
        ),
        (
            direct_command_step(
                StepStateKind::Failed,
                Some(ObservedStepTransition::Failed {
                    detail: super::super::evidence::failure_detail(
                        FailurePhase::Execution,
                        &StepFailureCause::Execution(StepExecutionFailure::Command(
                            CommandExecutionFailure::UnsuccessfulExit { code: Some(17) },
                        )),
                    )
                    .unwrap(),
                }),
                Some(timing.clone()),
                WorkflowRunOutputDisposition::Unavailable(
                    WorkflowRunOutputUnavailableReason::Failed,
                ),
            ),
            [
                "failed",
                "failure       execution · command_exit · exit 17",
                "unavailable (failed)",
            ]
            .as_slice(),
        ),
        (
            direct_command_step(
                StepStateKind::Blocked,
                Some(ObservedStepTransition::Blocked {
                    detail: super::super::evidence::BlockedDetail::new([
                        super::super::evidence::Prerequisite::control("prepare").unwrap(),
                    ])
                    .unwrap(),
                }),
                Some(timing.clone()),
                WorkflowRunOutputDisposition::Unavailable(
                    WorkflowRunOutputUnavailableReason::Blocked,
                ),
            ),
            [
                "blocked",
                "prerequisites_unsatisfied · control prepare",
                "unavailable (blocked)",
            ]
            .as_slice(),
        ),
        (
            direct_command_step(
                StepStateKind::NotRun,
                Some(ObservedStepTransition::NotRun {
                    detail: super::super::evidence::NonExecutionDetail::for_role(
                        super::super::validated::WorkflowNodeRole::Step,
                        super::super::evidence::NonExecutionCode::FailureStop,
                    )
                    .unwrap(),
                }),
                Some(timing.clone()),
                WorkflowRunOutputDisposition::Unavailable(
                    WorkflowRunOutputUnavailableReason::NotRun,
                ),
            ),
            [
                "not-run",
                "not run       failure_stop",
                "unavailable (not-run)",
            ]
            .as_slice(),
        ),
        (
            direct_command_step(
                StepStateKind::Cancelled,
                Some(ObservedStepTransition::Cancelled {
                    detail: super::super::evidence::CancellationDetail::new(
                        CancellationReason::UserRequest,
                    ),
                }),
                Some(timing),
                WorkflowRunOutputDisposition::Unavailable(
                    WorkflowRunOutputUnavailableReason::Cancelled,
                ),
            ),
            [
                "cancelled",
                "cancellation  user_request",
                "unavailable (cancelled)",
            ]
            .as_slice(),
        ),
    ];

    for (step, expected) in cases {
        let rendered = render_direct_inspector(&step, 120, 14);
        assert!(rendered.contains("selected-command   cmd"));
        assert!(rendered.contains("command       build 'héllo world'"));
        assert!(rendered.contains("cwd           work"));
        assert!(rendered.contains("depends on    prepare"));
        assert!(rendered.contains("OUTPUTS"));
        assert!(rendered.contains("report"));
        assert!(!rendered.contains("ID:"));
        assert!(!rendered.contains("Kind:"));
        assert!(!rendered.contains("State:"));
        assert!(!rendered.contains("Duration:"));
        for value in expected {
            assert!(
                rendered.contains(value),
                "missing {value:?} in {rendered:?}"
            );
        }
        if matches!(
            step.state,
            StepStateKind::Pending | StepStateKind::Blocked | StepStateKind::NotRun
        ) {
            assert!(!rendered.contains("started"));
        }
    }
}

#[test]
fn ellipsize_preserves_grapheme_clusters() {
    assert_eq!(ellipsize("e\u{301}clair", 2), "e\u{301}…");
    assert_eq!(ellipsize("a👩‍🚀bc", 4), "a👩‍🚀…");
}

#[test]
fn repeated_values_keep_fitting_values_and_report_omissions() {
    let fitting = vec!["a".to_owned(), "b".to_owned()];
    assert_eq!(summarize_repeated_values(&fitting, 9), "a, b");

    let overflowing = (1..=100)
        .map(|index| format!("d{index}"))
        .collect::<Vec<_>>();
    assert_eq!(summarize_repeated_values(&overflowing, 12), "d1, +99 more");
}

#[test]
fn compact_inspector_ellipsizes_unicode_and_preserves_log_space() {
    let mut step = direct_command_step(
        StepStateKind::Running,
        None,
        Some(WorkflowRunElapsed {
            started_at: time::OffsetDateTime::UNIX_EPOCH,
            duration: Duration::from_secs(3),
            frozen: false,
        }),
        WorkflowRunOutputDisposition::Pending,
    );
    step.id = "構築工程の識別子がとても長くても安全に表示される選択中の工程".repeat(2);
    let WorkflowPresentationStep::Command {
        direct_dependencies,
        outputs,
        ..
    } = &mut step.definition
    else {
        panic!("fixture presentation step was not a command");
    };
    *direct_dependencies = (1..=8).map(|index| format!("dependency-{index}")).collect();
    for index in 2..=8 {
        let name = format!("report-{index}");
        outputs.insert(
            name.clone(),
            Output::FilePath {
                path: format!("{name}.txt"),
                media_type: "text/plain".to_owned(),
            },
        );
        step.outputs
            .insert(name, WorkflowRunOutputDisposition::Pending);
    }
    let snapshot = direct_snapshot(step);
    let graph = DagLayout::for_steps(&snapshot.steps);
    let backend = ratatui::backend::TestBackend::new(MINIMUM_WIDTH, MINIMUM_HEIGHT);
    let mut terminal = Terminal::new(backend).unwrap();
    let mut interaction = HostInteraction::default();
    terminal
        .draw(|frame| {
            render(frame, &snapshot, &graph, &mut interaction, false);
        })
        .unwrap();
    let rendered = buffer_text(terminal.backend().buffer());

    assert!(rendered.contains('構'));
    assert!(rendered.contains('…'));
    assert!(rendered.contains("+"));
    assert!(rendered.contains("more"));
    assert!(rendered.contains("Waiting for output…"));
    assert!(!rendered.contains('\u{fffd}'));
}

#[test]
fn inspector_renders_outputs_in_a_dedicated_structured_panel() {
    let step = direct_command_step(
        StepStateKind::Succeeded,
        None,
        Some(WorkflowRunElapsed {
            started_at: time::OffsetDateTime::UNIX_EPOCH,
            duration: Duration::from_secs(3),
            frozen: true,
        }),
        WorkflowRunOutputDisposition::Committed,
    );
    let buffer = render_direct_inspector_buffer(&step, 120, 14, true);
    let rows = buffer_rows(&buffer);
    let command_y = row_containing(&rows, "command       build");
    let cwd_y = row_containing(&rows, "cwd");
    let started_y = row_containing(&rows, "started");
    let dependencies_y = row_containing(&rows, "depends on");
    let outputs_y = row_containing(&rows, "OUTPUTS");
    let summary_y = row_containing(&rows, "✓  report  file");
    let detail_y = summary_y + 1;

    assert_eq!(cwd_y, command_y + 1);
    assert_eq!(started_y, cwd_y + 1);
    assert_eq!(dependencies_y, started_y + 1);
    assert!(dependencies_y < outputs_y);
    assert!(rows[outputs_y - 1].contains('─'));
    assert_eq!(summary_y, outputs_y + 2);
    assert_eq!(detail_y, summary_y + 1);
    assert!(rows[detail_y].contains('—'));
    assert!(rows[detail_y + 1].replace('│', "").trim().is_empty());
    assert!(rows[summary_y].contains("captured"));
    let (marker_x, marker_y) = buffer_position(&buffer, "✓  report");
    let (status_x, status_y) = buffer_position(&buffer, "captured");
    assert_eq!(
        buffer[(marker_x, marker_y)].fg,
        tone_style(true, Tone::Success).fg.unwrap()
    );
    assert_eq!(
        buffer[(status_x, status_y)].fg,
        tone_style(true, Tone::Success).fg.unwrap()
    );
}

#[test]
fn inspector_reports_when_a_step_declares_no_outputs() {
    let mut step = direct_command_step(
        StepStateKind::Pending,
        None,
        None,
        WorkflowRunOutputDisposition::Pending,
    );
    let WorkflowPresentationStep::Command { outputs, .. } = &mut step.definition else {
        panic!("fixture presentation step was not a command");
    };
    outputs.clear();
    step.outputs.clear();

    let buffer = render_direct_inspector_buffer(&step, 80, 12, false);
    let rows = buffer_rows(&buffer);
    let outputs_y = row_containing(&rows, "OUTPUTS");
    let empty_y = row_containing(&rows, "·  —  none declared");

    assert_eq!(empty_y, outputs_y + 2);
    assert!(rows[empty_y + 1].replace('│', "").trim().is_empty());
    assert!(!buffer_text(&buffer).contains("report"));
}

#[tokio::test]
async fn full_view_renders_header_steps_inspector_and_selected_log_tail() {
    let temporary = tempfile::tempdir().unwrap();
    std::fs::write(
            temporary.path().join("workflow.yaml"),
            "schemaVersion: 1\nsteps:\n  build:\n    kind: cmd\n    command:\n      argv: [\"build\"]\n",
        )
        .unwrap();
    let workflow = resolution::resolve(temporary.path(), Path::new("workflow.yaml")).unwrap();
    let clock = FixedClock {
        now: ObservationTime {
            utc: time::OffsetDateTime::UNIX_EPOCH,
            monotonic: um_support::monotonic_now(),
        },
    };
    let view = WorkflowRunViewModel::new(
        &workflow,
        1,
        RunTimingObservation::new(clock.sample()),
        clock,
    );
    view.observe(ExecutionObservation::<time::OffsetDateTime>::CommandOutput(
        CommandOutputObservation {
            step: "build".to_owned(),
            invocation: ActionId {
                transition_sequence: TransitionSequence::default(),
            },
            source: CommandOutputSource::StandardOutput,
            sequence: SourceSequence::first(),
            bytes: Arc::from(b"compiling workflow host\n".as_slice()),
        },
    ))
    .await;
    let snapshot = view.snapshot();
    let backend = ratatui::backend::TestBackend::new(90, 20);
    let mut terminal = Terminal::new(backend).unwrap();
    let mut interaction = HostInteraction::default();
    terminal
        .draw(|frame| {
            render(
                frame,
                &snapshot,
                &DagLayout::for_steps(&snapshot.steps),
                &mut interaction,
                false,
            );
        })
        .unwrap();
    let rendered = buffer_text(terminal.backend().buffer());

    assert!(rendered.contains("workflow"));
    assert!(!rendered.contains("workflow.yaml"));
    assert!(rendered.contains("▏ ○ build"));
    assert!(rendered.contains("build"));
    assert!(rendered.contains("compiling workflow host"));
    assert!(rendered.contains("^C cancel run"));
}

#[test]
fn responsive_application_composes_wide_stacked_and_too_small_views() {
    let mut snapshot = direct_snapshot(long_log_step());
    snapshot.steps[0].id = "first-step".to_owned();
    let mut second = long_log_step();
    second.id = "selected-second-step".to_owned();
    snapshot.steps.push(second);
    let mut interaction = HostInteraction {
        selected: 1,
        ..HostInteraction::default()
    };

    let wide = render_snapshot(&snapshot, &mut interaction, 120, 24, false);
    let columns = wide_split_columns(Rect::new(0, 0, 120, 22));
    assert_eq!((columns[0].width, columns[1].width), (40, 80));
    let divider_x = columns[1].x.saturating_sub(1);
    let workflow = buffer_position(&wide, "workflow");
    let steps = buffer_position(&wide, "▏ ⠋ selected-second-step");
    let inspector = buffer_position(&wide, "⠋  selected-second-step   cmd");
    let outputs = buffer_position(&wide, "OUTPUTS");
    let log = buffer_position(&wide, "LOG");
    assert_eq!(workflow.1, inspector.1);
    assert!(steps.1 > workflow.1);
    assert!(steps.0 < inspector.0);
    assert_eq!(inspector.0, log.0);
    assert!(inspector.1 < outputs.1 && outputs.1 < log.1);
    assert!((columns[1].x..=columns[1].x + 6).contains(&inspector.0));
    assert_eq!(wide[(divider_x, 0)].symbol(), "│");
    assert_eq!(wide[(divider_x, 1)].symbol(), "│");
    assert_eq!(wide[(divider_x, 2)].symbol(), "┼");
    assert_eq!(wide[(divider_x, outputs.1.saturating_sub(1))].symbol(), "├");
    assert_eq!(wide[(divider_x, log.1.saturating_sub(1))].symbol(), "├");
    assert_eq!(wide[(divider_x, 22)].symbol(), "┴");
    assert_ne!(wide[(columns[1].x, 1)].symbol(), "│");
    assert!(
        wide[(divider_x.saturating_sub(1), steps.1)]
            .modifier
            .contains(Modifier::REVERSED)
    );
    assert!(
        !wide[(divider_x.saturating_sub(1), steps.1 + 1)]
            .modifier
            .contains(Modifier::REVERSED)
    );
    assert!(buffer_text(&wide).contains("selected-second-step"));
    assert!(buffer_text(&wide).contains("2 steps · 2 running"));
    assert!(!buffer_text(&wide).contains("pending 0"));

    let stacked = render_snapshot(&snapshot, &mut interaction, 90, 24, false);
    let steps = buffer_position(&stacked, "▏ ⠋ selected-second-step");
    let inspector_body = buffer_position(&stacked, "command       build");
    let outputs = buffer_position(&stacked, "OUTPUTS");
    let log = buffer_position(&stacked, "LOG");
    assert!(steps.1 < inspector_body.1 && inspector_body.1 < outputs.1 && outputs.1 < log.1);
    assert!(steps.0 < inspector_body.0);
    assert_eq!(inspector_body.0, log.0);

    let too_small = render_snapshot(&snapshot, &mut interaction, 50, 12, false);
    let too_small = buffer_text(&too_small);
    assert!(too_small.contains("Terminal too small"));
    assert!(too_small.contains("64x20"));
    assert!(!too_small.contains("selected-second-step"));
    assert_eq!(interaction.selected, 1);
    assert_eq!(interaction.surface, HostSurface::Split);

    let recovered = render_snapshot(&snapshot, &mut interaction, 120, 24, false);
    let selected_row = buffer_rows(&recovered)
        .into_iter()
        .find(|row| row.contains("▏ ") && row.contains("selected-second-step"))
        .unwrap();
    assert!(selected_row.contains('▏'));
    assert_eq!(interaction.selected, 1);
}

#[test]
fn workflow_header_separates_identity_status_duration_and_counts() {
    let mut snapshot = direct_snapshot(long_log_step());
    snapshot.workflow_path = "plans/plan-implement-test.yaml".to_owned();
    snapshot.timing.duration = Duration::from_secs(20_065);
    let mut interaction = HostInteraction::default();

    let buffer = render_snapshot(&snapshot, &mut interaction, 180, 24, false);
    let rows = buffer_rows(&buffer);
    let name = buffer_position(&buffer, "plan-implement-test");
    let status = buffer_position(&buffer, "running");
    let duration = buffer_position(&buffer, "5h34m25s");
    let divider_x = wide_split_columns(Rect::new(0, 0, 180, 22))[1]
        .x
        .saturating_sub(1);

    assert_eq!(name, (2, 0));
    assert_eq!(status.1, name.1);
    assert_eq!(duration.1, name.1);
    assert!(name.0 < status.0 && status.0 < duration.0);
    assert_eq!(
        duration
            .0
            .saturating_add(u16::try_from(display_width("5h34m25s")).unwrap()),
        divider_x.saturating_sub(2)
    );
    assert!(rows[1].starts_with("  1 step · 1 running"));
    assert!(!rows[0].contains(".yaml"));
    assert!(!rows[0].contains("concurrency"));
    assert!(!rows[0].contains("published"));
}

#[test]
fn workflow_header_status_tracks_failure_and_publication_lifecycle() {
    let mut snapshot = direct_snapshot(long_log_step());
    snapshot.workflow = WorkflowState::Executing {
        gate: SchedulingGate::FailureStopped {
            primary_issue: {
                let cause = StepFailureCause::Execution(StepExecutionFailure::Command(
                    CommandExecutionFailure::UnsuccessfulExit { code: Some(17) },
                ));
                super::super::evidence::PrimaryIssue::failed(
                    super::super::validated::WorkflowNode {
                        id: "selected-command".to_owned(),
                        role: super::super::validated::WorkflowNodeRole::Step,
                    },
                    super::super::evidence::failure_detail(FailurePhase::Execution, &cause)
                        .unwrap(),
                )
            },
        },
    };
    assert_eq!(workflow_header_status(&snapshot).0, "failing");

    snapshot.workflow = WorkflowState::Succeeded;
    snapshot.publication = WorkflowRunPublicationState::Publishing;
    assert_eq!(workflow_header_status(&snapshot).0, "publishing");

    snapshot.publication =
        WorkflowRunPublicationState::Completed(WorkflowRunPublicationResult::Succeeded {
            result_directory: "result".to_owned(),
        });
    snapshot.cleanup = WorkflowRunCleanupState::Cleaning;
    assert_eq!(workflow_header_status(&snapshot).0, "cleaning");

    snapshot.cleanup = WorkflowRunCleanupState::Completed(WorkflowRunCleanupResult::Failed);
    assert_eq!(workflow_header_status(&snapshot).0, "cleanup failed");

    snapshot.cleanup = WorkflowRunCleanupState::Completed(WorkflowRunCleanupResult::Succeeded);
    assert_eq!(workflow_header_status(&snapshot).0, "succeeded");
}

#[test]
fn resize_sequence_preserves_log_surface_viewport_and_help() {
    let mut snapshot = direct_snapshot(numbered_log_step(40, 200));
    snapshot.steps[0].id = "first-step".to_owned();
    let mut selected = numbered_log_step(40, 200);
    selected.id = "selected-second-step".to_owned();
    snapshot.steps.push(selected);
    let cancellation = CancellationSource::new();
    let mut interaction = HostInteraction {
        selected: 1,
        terminal_area: Rect::new(0, 0, 120, 24),
        ..HostInteraction::default()
    };
    press_key(
        &mut interaction,
        &snapshot,
        KeyCode::Enter,
        KeyModifiers::NONE,
        &cancellation,
    );
    let _ = render_snapshot(&snapshot, &mut interaction, 120, 24, false);
    press_key(
        &mut interaction,
        &snapshot,
        KeyCode::Up,
        KeyModifiers::NONE,
        &cancellation,
    );
    for _ in 0..3 {
        press_key(
            &mut interaction,
            &snapshot,
            KeyCode::Right,
            KeyModifiers::NONE,
            &cancellation,
        );
    }
    press_key(
        &mut interaction,
        &snapshot,
        KeyCode::Char('?'),
        KeyModifiers::SHIFT,
        &cancellation,
    );
    let anchor = interaction.full_log.anchor;
    let offset = interaction.full_log.horizontal_offset;

    let stacked = render_snapshot(&snapshot, &mut interaction, 90, 20, false);
    assert!(buffer_text(&stacked).contains("? — all commands"));
    assert_eq!(interaction.full_log.anchor, anchor);
    assert_eq!(interaction.full_log.horizontal_offset, offset);

    let too_small = render_snapshot(&snapshot, &mut interaction, 40, 8, false);
    assert!(buffer_text(&too_small).contains("Terminal too small"));
    assert!(!buffer_text(&too_small).contains("? — all commands"));
    press_key(
        &mut interaction,
        &snapshot,
        KeyCode::Down,
        KeyModifiers::NONE,
        &cancellation,
    );
    assert_eq!(interaction.full_log.anchor, anchor);
    assert_eq!(interaction.full_log.horizontal_offset, offset);

    let recovered = render_snapshot(&snapshot, &mut interaction, 120, 24, false);
    assert!(buffer_text(&recovered).contains("? — all commands"));
    assert_eq!(interaction.selected, 1);
    assert_eq!(interaction.surface, HostSurface::FullLog);
    assert!(interaction.help_visible);
    assert!(!interaction.full_log.follow);
    assert_eq!(interaction.full_log.anchor, anchor);
    assert_eq!(interaction.full_log.horizontal_offset, offset);

    press_key(
        &mut interaction,
        &snapshot,
        KeyCode::Esc,
        KeyModifiers::NONE,
        &cancellation,
    );
    let log = render_snapshot(&snapshot, &mut interaction, 120, 24, false);
    let log = buffer_text(&log);
    assert!(!interaction.help_visible);
    assert_eq!(interaction.surface, HostSurface::FullLog);
    assert!(log.contains("selected-second-step"));
    assert!(log.contains("● paused"));
    assert!(!log.contains("stdout + stderr"));
    assert!(!log.contains("workflow.yaml"));
}

#[test]
fn too_small_view_freezes_hidden_split_and_log_interaction_state() {
    let mut snapshot = direct_snapshot(numbered_log_step(40, 200));
    snapshot.steps[0].id = "first-step".to_owned();
    let mut second = numbered_log_step(40, 200);
    second.id = "selected-second-step".to_owned();
    snapshot.steps.push(second);

    let cancellation = CancellationSource::new();
    let mut split = HostInteraction {
        selected: 1,
        ..HostInteraction::default()
    };
    let _ = render_snapshot(&snapshot, &mut split, 90, 20, false);
    let _ = render_snapshot(&snapshot, &mut split, 40, 8, false);
    for code in [KeyCode::Up, KeyCode::Enter, KeyCode::Char('?')] {
        press_key(
            &mut split,
            &snapshot,
            code,
            KeyModifiers::NONE,
            &cancellation,
        );
    }
    assert_eq!(split.selected, 1);
    assert_eq!(split.surface, HostSurface::Split);
    assert!(!split.help_visible);

    let (mut log, cancellation) = entered_full_log(&snapshot, 120, 24);
    log.selected = 1;
    let _ = render_snapshot(&snapshot, &mut log, 120, 24, false);
    press_key(
        &mut log,
        &snapshot,
        KeyCode::Up,
        KeyModifiers::NONE,
        &cancellation,
    );
    press_key(
        &mut log,
        &snapshot,
        KeyCode::Right,
        KeyModifiers::NONE,
        &cancellation,
    );
    let anchor = log.full_log.anchor;
    let horizontal_offset = log.full_log.horizontal_offset;
    assert!(!log.full_log.follow);

    let _ = render_snapshot(&snapshot, &mut log, 40, 8, false);
    for code in [
        KeyCode::Down,
        KeyCode::PageDown,
        KeyCode::Left,
        KeyCode::Char('F'),
        KeyCode::Esc,
        KeyCode::Char('?'),
    ] {
        press_key(&mut log, &snapshot, code, KeyModifiers::NONE, &cancellation);
    }
    assert_eq!(log.selected, 1);
    assert_eq!(log.surface, HostSurface::FullLog);
    assert!(!log.help_visible);
    assert!(!log.full_log.follow);
    assert_eq!(log.full_log.anchor, anchor);
    assert_eq!(log.full_log.horizontal_offset, horizontal_offset);

    let _ = render_snapshot(&snapshot, &mut log, 120, 24, false);
    assert_eq!(log.surface, HostSurface::FullLog);
    assert!(!log.full_log.follow);
    assert_eq!(log.full_log.anchor, anchor);
    assert_eq!(log.full_log.horizontal_offset, horizontal_offset);
}

#[test]
fn contextual_footers_and_help_remain_discoverable_at_minimum_size() {
    let mut snapshot = direct_snapshot(long_log_step());
    let mut second = long_log_step();
    second.id = "second-step".to_owned();
    snapshot.steps.push(second);
    let cancellation = CancellationSource::new();
    let mut interaction = HostInteraction::default();

    let wide = render_snapshot(&snapshot, &mut interaction, 140, 24, false);
    let wide_footer = buffer_rows(&wide).pop().unwrap();
    assert!(wide_footer.contains("↑/k up"));
    assert!(wide_footer.contains("↓/j down"));
    assert!(wide_footer.contains("↵ open"));
    assert!(wide_footer.contains("^C cancel run"));
    assert!(wide_footer.contains("? help"));

    let minimum = render_snapshot(
        &snapshot,
        &mut interaction,
        MINIMUM_WIDTH,
        MINIMUM_HEIGHT,
        false,
    );
    let minimum_footer = buffer_rows(&minimum).pop().unwrap();
    assert!(minimum_footer.contains("↑/k up"));
    assert!(minimum_footer.contains("↓/j down"));
    assert!(minimum_footer.contains("↵ open"));
    assert!(minimum_footer.contains("^C cancel run"));
    assert!(minimum_footer.contains("? help"));

    press_key(
        &mut interaction,
        &snapshot,
        KeyCode::Char('?'),
        KeyModifiers::SHIFT,
        &cancellation,
    );
    press_key(
        &mut interaction,
        &snapshot,
        KeyCode::Down,
        KeyModifiers::NONE,
        &cancellation,
    );
    let split_help = render_snapshot(
        &snapshot,
        &mut interaction,
        MINIMUM_WIDTH,
        MINIMUM_HEIGHT,
        false,
    );
    let split_help_rows = buffer_rows(&split_help);
    let split_help = buffer_text(&split_help);
    assert_eq!(interaction.selected, 0);
    for expected in [
        "? — all commands",
        "esc to dismiss",
        "MOVE",
        "OPEN",
        "VIEW",
        "FILTER",
        "RUN",
        "↑/k",
        "↓/j",
        "↵",
        "1…n",
        "^C",
    ] {
        assert!(split_help.contains(expected), "missing {expected:?}");
    }
    assert!(split_help.contains("toggle log"));
    assert!(split_help_rows.last().unwrap().contains("DAG"));
    assert!(split_help_rows.last().unwrap().contains("? help"));

    press_key(
        &mut interaction,
        &snapshot,
        KeyCode::Esc,
        KeyModifiers::NONE,
        &cancellation,
    );
    press_key(
        &mut interaction,
        &snapshot,
        KeyCode::Enter,
        KeyModifiers::NONE,
        &cancellation,
    );
    press_key(
        &mut interaction,
        &snapshot,
        KeyCode::Char('?'),
        KeyModifiers::SHIFT,
        &cancellation,
    );
    let log_help = render_snapshot(
        &snapshot,
        &mut interaction,
        MINIMUM_WIDTH,
        MINIMUM_HEIGHT,
        false,
    );
    let log_help_rows = buffer_rows(&log_help);
    let log_help = buffer_text(&log_help);
    for expected in [
        "? — all commands",
        "MOVE",
        "JUMP",
        "VIEW",
        "FILTER",
        "RUN",
        "↑/k",
        "↓/j",
        "PgUp/b",
        "PgDn/f/Space",
        "u/^U",
        "d/^D",
        "←/h",
        "→/l",
        "F",
        "1…n",
        "^C",
    ] {
        assert!(log_help.contains(expected), "missing {expected:?}");
    }
    assert!(log_help_rows.last().unwrap().contains("LOG"));
    assert!(log_help_rows.last().unwrap().contains("? help"));

    press_key(
        &mut interaction,
        &snapshot,
        KeyCode::Esc,
        KeyModifiers::NONE,
        &cancellation,
    );
    let log = render_snapshot(
        &snapshot,
        &mut interaction,
        MINIMUM_WIDTH,
        MINIMUM_HEIGHT,
        false,
    );
    let log_footer = buffer_rows(&log).pop().unwrap();
    for expected in ["Esc", "↑/k", "↓/j", "F", "^C", "? help"] {
        assert!(log_footer.contains(expected), "missing {expected:?}");
    }

    press_key(
        &mut interaction,
        &snapshot,
        KeyCode::Esc,
        KeyModifiers::NONE,
        &cancellation,
    );
    snapshot.workflow = WorkflowState::Succeeded;
    snapshot.quit_eligible = true;
    let completed = render_snapshot(
        &snapshot,
        &mut interaction,
        MINIMUM_WIDTH,
        MINIMUM_HEIGHT,
        false,
    );
    let completed_footer = buffer_rows(&completed).pop().unwrap();
    assert!(completed_footer.contains("q quit"));
    assert!(completed_footer.contains("? help"));
    assert!(!completed_footer.contains("^C"));

    press_key(
        &mut interaction,
        &snapshot,
        KeyCode::Char('?'),
        KeyModifiers::SHIFT,
        &cancellation,
    );
    let completed_help = render_snapshot(
        &snapshot,
        &mut interaction,
        MINIMUM_WIDTH,
        MINIMUM_HEIGHT,
        false,
    );
    let completed_help = buffer_text(&completed_help);
    assert!(completed_help.contains("q"));
    assert!(completed_help.contains("quit"));
    assert!(!completed_help.contains("^C"));
}

#[test]
fn contextual_footer_and_help_use_the_command_palette() {
    let snapshot = direct_snapshot(long_log_step());
    let cancellation = CancellationSource::new();
    let mut interaction = HostInteraction::default();
    let _ = render_snapshot(&snapshot, &mut interaction, 140, 24, true);
    press_key(
        &mut interaction,
        &snapshot,
        KeyCode::Char('?'),
        KeyModifiers::SHIFT,
        &cancellation,
    );

    let buffer = render_snapshot(&snapshot, &mut interaction, 140, 24, true);
    let rows = buffer_rows(&buffer);
    for (needle, expected) in [
        ("? — all commands", Color::Rgb(203, 166, 247)),
        ("MOVE", Color::Rgb(127, 132, 156)),
        ("↑/k", Color::Rgb(249, 226, 175)),
        ("previous step", Color::Rgb(186, 194, 222)),
    ] {
        let y = rows
            .iter()
            .position(|row| row.contains(needle))
            .unwrap_or_else(|| panic!("missing {needle:?} in {}", rows.join("\n")));
        let x = column_of(&rows[y], needle);
        assert_eq!(
            buffer[(x, u16::try_from(y).unwrap())].fg,
            expected,
            "wrong color for {needle:?}"
        );
    }

    let footer_y = buffer.area.height.saturating_sub(1);
    let footer = &rows[usize::from(footer_y)];
    for (needle, expected) in [
        ("DAG", Color::Rgb(203, 166, 247)),
        ("↑/k", Color::Rgb(180, 190, 254)),
        ("? help", Color::Rgb(203, 166, 247)),
    ] {
        let x = column_of(footer, needle);
        assert_eq!(
            buffer[(x, footer_y)].fg,
            expected,
            "wrong footer color for {needle:?}"
        );
    }
    let separator_y = footer_y.saturating_sub(1);
    assert_eq!(buffer[(0, separator_y)].fg, Color::Rgb(49, 50, 68));
    assert!(
        !rows[usize::from(separator_y)].contains('┴'),
        "the covered split junction must not protrude through the help menu"
    );
}

#[test]
fn terminal_lifecycle_hides_quit_until_adapter_completion() {
    let mut snapshot = direct_snapshot(long_log_step());
    snapshot.workflow = WorkflowState::Succeeded;
    snapshot.authoritative_result = true;
    snapshot.quiescent = true;
    snapshot.publication = WorkflowRunPublicationState::Publishing;
    let cancellation = CancellationSource::new();
    let mut interaction = HostInteraction::default();

    let publishing = render_minimum_snapshot_text(&snapshot, &mut interaction);
    assert!(publishing.contains("publishing"));
    assert!(!minimum_footer(&snapshot, &mut interaction).contains("q quit"));

    assert_help_omits_quit(&mut interaction, &snapshot, &cancellation);

    press_unmodified_key(&mut interaction, &snapshot, KeyCode::Esc, &cancellation);
    press_unmodified_key(&mut interaction, &snapshot, KeyCode::Enter, &cancellation);
    assert!(!minimum_footer(&snapshot, &mut interaction).contains("q quit"));

    assert_help_omits_quit(&mut interaction, &snapshot, &cancellation);
    assert!(!render_too_small_text(&snapshot).contains("q to quit"));

    snapshot.publication =
        WorkflowRunPublicationState::Completed(WorkflowRunPublicationResult::Succeeded {
            result_directory: "results".to_owned(),
        });
    snapshot.cleanup = WorkflowRunCleanupState::Cleaning;
    interaction.surface = HostSurface::Split;
    interaction.help_visible = false;
    assert!(render_minimum_snapshot_text(&snapshot, &mut interaction).contains("cleaning"));
    assert!(!minimum_footer(&snapshot, &mut interaction).contains("q quit"));

    snapshot.cleanup = WorkflowRunCleanupState::Completed(WorkflowRunCleanupResult::Succeeded);
    snapshot.quit_eligible = true;
    assert!(minimum_footer(&snapshot, &mut interaction).contains("q quit"));

    press_unmodified_key(&mut interaction, &snapshot, KeyCode::Enter, &cancellation);
    assert!(minimum_footer(&snapshot, &mut interaction).contains("q quit"));

    open_help(&mut interaction, &snapshot, &cancellation);
    let completed_help = render_minimum_snapshot_text(&snapshot, &mut interaction);
    assert!(completed_help.contains("RUN"));
    assert!(completed_help.contains("→ quit"));
    assert!(render_too_small_text(&snapshot).contains("q to quit"));
}

#[test]
fn cancelling_workflow_does_not_advertise_an_inactive_cancel_command() {
    let mut snapshot = direct_snapshot(long_log_step());
    snapshot.workflow = WorkflowState::Executing {
        gate: SchedulingGate::Cancelling {
            reason: CancellationReason::UserRequest,
            prior_issue: None,
        },
    };
    snapshot.cancellation = Some(
        crate::workflow::run_view_model::WorkflowRunCancellationView {
            reason: CancellationReason::UserRequest,
            force_stop_deadline: time::OffsetDateTime::UNIX_EPOCH,
        },
    );
    let mut interaction = HostInteraction::default();

    let buffer = render_snapshot(
        &snapshot,
        &mut interaction,
        MINIMUM_WIDTH,
        MINIMUM_HEIGHT,
        false,
    );

    assert!(buffer_text(&buffer).contains("cancelling"));
    let footer = buffer_rows(&buffer).pop().unwrap();
    assert!(
        !footer.contains("^C"),
        "an already-cancelling workflow must not advertise a no-op cancel command: {footer:?}"
    );
}

#[test]
fn color_disabled_rendering_retains_symbols_labels_and_focus() {
    let snapshot = direct_snapshot(long_log_step());
    let mut interaction = HostInteraction::default();
    let buffer = render_snapshot(&snapshot, &mut interaction, 90, 20, false);
    let rendered = buffer_text(&buffer);

    for expected in [
        "▏",
        "command",
        "following",
        "stdout",
        "^C cancel run",
        "? help",
    ] {
        assert!(rendered.contains(expected), "missing {expected:?}");
    }
    assert!(
        buffer
            .content()
            .iter()
            .all(|cell| cell.fg == Color::Reset && cell.bg == Color::Reset)
    );
    let (x, y) = buffer_position(&buffer, "selected-command");
    assert!(buffer[(x, y)].modifier.contains(Modifier::REVERSED));
    assert!(!buffer[(x, y + 1)].modifier.contains(Modifier::REVERSED));
}

#[test]
fn color_enabled_rendering_separates_structure_content_and_focus() {
    let mut step = long_log_step();
    step.timing = Some(WorkflowRunElapsed {
        started_at: time::OffsetDateTime::UNIX_EPOCH,
        duration: Duration::from_secs(3),
        frozen: false,
    });
    let snapshot = direct_snapshot(step);
    let mut interaction = HostInteraction::default();
    let buffer = render_snapshot(&snapshot, &mut interaction, 120, 24, true);
    let divider_x = wide_split_columns(Rect::new(0, 0, 120, 22))[1]
        .x
        .saturating_sub(1);

    assert_eq!(buffer[(divider_x, 1)].fg, separator_style(true).fg.unwrap());
    let (title_x, title_y) = buffer_position(&buffer, "selected-command");
    assert_eq!(
        buffer[(title_x, title_y)].fg,
        tone_style(true, Tone::Primary).fg.unwrap()
    );
    assert!(buffer[(title_x, title_y)].modifier.contains(Modifier::BOLD));
    let (badge_x, badge_y) = buffer_position(&buffer, " cmd ");
    assert_eq!(buffer[(badge_x, badge_y)].bg, Color::Rgb(49, 50, 68));
    assert_eq!(
        buffer[(badge_x, badge_y)].fg,
        tone_style(true, Tone::Muted).fg.unwrap()
    );
    let rows = buffer_rows(&buffer);
    let header_row = &rows[usize::from(title_y)];
    let duration_byte = header_row.rfind("3.0s").unwrap();
    let duration_x = u16::try_from(display_width(&header_row[..duration_byte])).unwrap();
    assert_eq!(
        buffer[(duration_x, title_y)].fg,
        tone_style(true, Tone::Active).fg.unwrap()
    );
    let (payload_x, payload_y) = buffer_position(&buffer, "abcdefghijklmnopqrstuvwxyz");
    assert_eq!(
        buffer[(payload_x, payload_y)].fg,
        tone_style(true, Tone::Neutral).fg.unwrap()
    );
    let (footer_x, footer_y) = buffer_position(&buffer, "DAG");
    assert_eq!(
        buffer[(footer_x, footer_y)].fg,
        command_accent_style(true).fg.unwrap()
    );
    let selected_y = buffer_position(&buffer, "▏ ⠋ selected-command").1;
    assert_eq!(
        buffer[(divider_x.saturating_sub(1), selected_y)].bg,
        step_selection_style(true).bg.unwrap()
    );
    let step_row = &rows[usize::from(selected_y)];
    let duration_byte = step_row.find("3.0s").unwrap();
    let duration_x = u16::try_from(display_width(&step_row[..duration_byte])).unwrap();
    assert_eq!(
        buffer[(duration_x, selected_y)].fg,
        tone_style(true, Tone::Active).fg.unwrap()
    );
}

#[test]
fn running_step_indicator_advances_with_elapsed_time() {
    let mut step = long_log_step();
    step.timing = Some(WorkflowRunElapsed {
        started_at: time::OffsetDateTime::UNIX_EPOCH,
        duration: Duration::ZERO,
        frozen: false,
    });
    assert_eq!(step_state_glyph(&step), "⠋");

    step.timing.as_mut().unwrap().duration = REDRAW_INTERVAL;
    assert_eq!(step_state_glyph(&step), "⠙");

    step.timing.as_mut().unwrap().duration = REDRAW_INTERVAL * 10;
    assert_eq!(step_state_glyph(&step), "⠋");
}

#[test]
fn live_dag_separates_ordinary_and_finalization_phases_after_trigger_commit() {
    let mut snapshot = snapshot_from_yaml(
        "schemaVersion: 1
steps:
  complete:
    kind: cmd
    command:
      argv: [\"true\"]
finalizers:
  cleanup:
    kind: cmd
    command:
      argv: [\"true\"]
",
    );

    let before_trigger =
        render_steps_lines(&snapshot, &HostInteraction::default(), 80, 10).join("\n");
    assert!(before_trigger.contains("ordinary phase"));
    assert!(before_trigger.contains("finalization phase"));
    assert!(!before_trigger.contains("finalization phase · trigger"));

    snapshot.workflow = WorkflowState::Finalizing {
        trigger: crate::workflow::document::FinalizationTrigger::Succeeded,
        gate: crate::workflow::runtime::FinalizationGate::Open,
        primary_issue: None,
    };
    let after_trigger =
        render_steps_lines(&snapshot, &HostInteraction::default(), 80, 10).join("\n");
    let ordinary = after_trigger.find("ordinary phase").unwrap();
    let ordinary_step = after_trigger.find("complete").unwrap();
    let finalization = after_trigger
        .find("finalization phase · trigger succeeded")
        .unwrap();
    let finalizer = after_trigger.find("cleanup").unwrap();
    assert!(ordinary < ordinary_step);
    assert!(ordinary_step < finalization);
    assert!(finalization < finalizer);
}

#[test]
fn graph_layout_is_stable_when_step_state_and_timing_change() {
    let mut snapshot = snapshot_from_yaml(
        "schemaVersion: 1\nsteps:\n  root:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n  left:\n    kind: cmd\n    dependsOn: [root]\n    command:\n      argv: [\"true\"]\n  right:\n    kind: cmd\n    dependsOn: [root]\n    command:\n      argv: [\"true\"]\n  join:\n    kind: cmd\n    dependsOn: [left, right]\n    command:\n      argv: [\"true\"]\n",
    );
    let pending = DagLayout::for_steps(&snapshot.steps);

    snapshot.steps[0].state = StepStateKind::Succeeded;
    snapshot.steps[0].timing = Some(super::super::run_view_model::WorkflowRunElapsed {
        started_at: time::OffsetDateTime::UNIX_EPOCH,
        duration: Duration::from_secs(4),
        frozen: true,
    });
    snapshot.steps[1].state = StepStateKind::Running;
    snapshot.steps[1].timing = Some(super::super::run_view_model::WorkflowRunElapsed {
        started_at: time::OffsetDateTime::UNIX_EPOCH,
        duration: Duration::from_millis(1250),
        frozen: false,
    });

    assert_eq!(DagLayout::for_steps(&snapshot.steps), pending);
    let rendered = render_steps_lines(&snapshot, &HostInteraction::default(), 80, 8);
    assert!(rendered.iter().any(|line| line.contains("4.0s")));
    assert!(rendered.iter().any(|line| line.contains("1.2s")));
}

#[test]
fn responsive_rows_drop_detail_before_kind_and_then_ellipsize_identity() {
    let mut snapshot = snapshot_from_yaml(
        "schemaVersion: 1\nsteps:\n  buildartifact:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n",
    );
    snapshot.steps[0].state = StepStateKind::Succeeded;
    let graph = DagLayout::for_steps(&snapshot.steps);
    let id_width = display_width("buildartifact");
    let exact_with_kind = 2 + graph.gutter_width() + 1 + id_width + 2 + 5 + 2 + 1;

    let wide = StepColumns::for_steps(
        exact_with_kind + 2 + MINIMUM_DETAIL_WIDTH,
        graph.gutter_width(),
        &snapshot.steps,
    );
    let medium = StepColumns::for_steps(exact_with_kind, graph.gutter_width(), &snapshot.steps);
    let narrow = StepColumns::for_steps(exact_with_kind - 1, graph.gutter_width(), &snapshot.steps);
    let compact_width = 2 + graph.gutter_width() + 1 + id_width + 2 + 1 - 1;
    let compact = StepColumns::for_steps(compact_width, graph.gutter_width(), &snapshot.steps);

    assert!(wide.detail && wide.kind);
    assert!(!medium.detail && medium.kind);
    assert!(!narrow.detail && !narrow.kind);
    assert!(!compact.detail && !compact.kind);
    assert!(compact.id_width < id_width);
    assert_eq!(
        live_step_detail(&snapshot.steps[0]).as_deref(),
        Some("exit 0")
    );
}

#[test]
fn scrolling_and_resize_keep_selection_visible_with_boundary_connectors() {
    let snapshot = snapshot_from_yaml(
        "schemaVersion: 1\nsteps:\n  root:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n  middleone:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n  middletwo:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n  middlethree:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n  middlefour:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n  middlefive:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n  middlesix:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n  middleseven:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n  middleeight:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n  branch:\n    kind: cmd\n    dependsOn: [root]\n    command:\n      argv: [\"true\"]\n",
    );
    let interaction = HostInteraction {
        selected: 8,
        ..HostInteraction::default()
    };

    let compact = render_steps_lines(&snapshot, &interaction, 70, 6);
    let selected = compact
        .iter()
        .find(|line| line.contains("middleeight"))
        .unwrap();
    let top = compact
        .iter()
        .find(|line| line.contains("middleseven"))
        .unwrap();
    assert!(selected.contains("▏ │"));
    assert!(top.contains("│"));
    assert_eq!(
        display_width(&selected[..selected.find("middleeight").unwrap()]),
        display_width(&top[..top.find("middleseven").unwrap()]),
    );
    assert!(!compact.iter().any(|line| line.contains("branch")));

    let resized = render_steps_lines(&snapshot, &interaction, 64, 8);
    assert!(
        resized
            .iter()
            .any(|line| line.contains("▏ │") && line.contains("middleeight"))
    );
    assert!(!resized.iter().any(|line| line.contains("branch")));
}

#[test]
fn too_small_view_only_advertises_the_available_lifecycle_action() {
    let mut snapshot = direct_snapshot(long_log_step());
    let backend = ratatui::backend::TestBackend::new(40, 8);
    let mut terminal = Terminal::new(backend).unwrap();
    terminal
        .draw(|frame| render_too_small(frame, frame.area(), &snapshot, false))
        .unwrap();
    let running = buffer_text(terminal.backend().buffer());

    assert!(running.contains("Terminal too small"));
    assert!(running.contains("Resize to at least 64x20"));
    assert!(running.contains("Ctrl-C cancels"));
    assert!(!running.contains("q to quit"));

    snapshot.workflow = WorkflowState::Succeeded;
    snapshot.quit_eligible = true;
    terminal
        .draw(|frame| render_too_small(frame, frame.area(), &snapshot, false))
        .unwrap();
    let terminal = buffer_text(terminal.backend().buffer());
    assert!(terminal.contains("Press q to quit"));
    assert!(!terminal.contains("Ctrl-C cancels"));
}

fn start_active_scripted_host(
    view: WorkflowRunViewModel<FixedClock>,
    cancellation: CancellationSource,
    boundary: ScriptedTerminalBoundary,
) -> WorkflowTerminalHost {
    let mut host =
        WorkflowTerminalHost::start_with_boundary(view, cancellation, false, boundary).unwrap();
    host.activate_execution().unwrap();
    host
}

async fn assert_scripted_runtime_failure(
    failures: BoundaryFailures,
    scripted_input: ScriptedInput,
    expected_operation: PresentationFailureOperation,
) {
    let (_temporary, _workflow, view, _) = scripted_host_view();
    let cancellation = CancellationSource::new();
    let (boundary, input, mut actions) =
        ScriptedTerminalBoundary::new(Rect::new(0, 0, 80, 24), [], failures);
    let host = start_active_scripted_host(view, cancellation.clone(), boundary);
    input.send(scripted_input).unwrap();

    let failure = host.wait().await.unwrap_err();

    assert_eq!(failure.operation, expected_operation);
    assert_eq!(
        cancellation.cancellation_reason(),
        Some(CancellationReason::CallerOutputFailure)
    );
    wait_for_action(&mut actions, BoundaryAction::Restore).await;
}

fn scripted_host_view() -> (
    tempfile::TempDir,
    ResolvedWorkflow,
    WorkflowRunViewModel<FixedClock>,
    ObservationTime,
) {
    let temporary = tempfile::tempdir().unwrap();
    std::fs::write(
            temporary.path().join("workflow.yaml"),
            "schemaVersion: 1\nsteps:\n  complete:\n    kind: cmd\n    command:\n      argv: [\"true\"]\n",
        )
        .unwrap();
    let workflow = resolution::resolve(temporary.path(), Path::new("workflow.yaml")).unwrap();
    let now = ObservationTime {
        utc: time::OffsetDateTime::UNIX_EPOCH,
        monotonic: um_support::monotonic_now(),
    };
    let clock = FixedClock { now };
    let view = WorkflowRunViewModel::new(&workflow, 1, RunTimingObservation::new(now), clock);
    (temporary, workflow, view, now)
}

fn complete_scripted_view(
    view: &WorkflowRunViewModel<FixedClock>,
    workflow: &ResolvedWorkflow,
    started: ObservationTime,
    cancellation: Option<CancellationReason>,
) {
    let duration = Duration::from_millis(20);
    let (outcome, cancellation_fact, step_state, step_timing) = match cancellation {
        Some(reason) => (
            RunOutcome::Cancelled { reason },
            Some(WorkflowRunCancellation {
                reason,
                force_stop_deadline: started.utc + Duration::from_secs(10),
            }),
            StepState::Cancelled {
                detail: super::super::evidence::CancellationDetail::new(reason),
            },
            None,
        ),
        None => (
            RunOutcome::Succeeded,
            None,
            StepState::Succeeded {
                outputs: BTreeMap::new(),
            },
            Some(WorkflowStepTiming {
                started_at: started.utc,
                duration,
            }),
        ),
    };
    let run = WorkflowRunResult {
        run_directory: workflow.source.source_root.clone(),
        attempt_number: 1,
        continuation: None,
        output_producers: BTreeMap::new(),
        workflow_path: workflow.source.workflow_path.clone(),
        source_root: workflow.source.source_root.clone(),
        content_digest: workflow.content_digest.clone(),
        execution_root: workflow.source.source_root.clone(),
        maximum_parallel_steps: NonZeroUsize::new(1).unwrap(),
        maximum_retained_bytes_per_stream: super::super::MAXIMUM_RETAINED_BYTES_PER_STREAM,
        cloud_capacity: None,
        maximum_result_bytes: 202_027_692,
        timing: WorkflowRunTiming {
            started_at: started.utc,
            finished_at: started.utc + duration,
            duration,
        },
        outcome,
        cancellation: cancellation_fact,
        force_abort: None,
        steps: vec![WorkflowRunStep {
            id: "complete".to_owned(),
            role: crate::workflow::validated::WorkflowNodeRole::Step,
            kind: WorkflowRunStepKind::Command,
            failure_policy: FailurePolicy::Required,
            state: step_state,
            timing: step_timing,
            command_output: None,
            recovery: None,
            invocations: Vec::new(),
        }],
        finalization: None,
        exports: BTreeMap::new(),
        export_sources: BTreeMap::new(),
        export_presentation: BTreeMap::new(),
    };
    view.reconcile_terminal_result(&run).unwrap();
    view.mark_quiescent();
    view.begin_publication();
    view.complete_publication(WorkflowRunPublicationResult::Succeeded {
        result_directory: "result".to_owned(),
    });
    view.begin_cleanup();
    view.complete_cleanup(WorkflowRunCleanupResult::Succeeded);
    view.mark_adapter_lifecycle_completed();
    assert!(view.snapshot().quit_eligible);
}

#[derive(Clone, Copy)]
struct FixedClock {
    now: ObservationTime,
}

impl ObservationClock for FixedClock {
    fn sample(&self) -> ObservationTime {
        self.now
    }
}

fn direct_log_record(
    order: u64,
    source: CommandOutputSource,
    observed_at: &str,
    payload: &str,
    continuation: bool,
) -> WorkflowRunLogRecord {
    direct_source_log_record(
        order,
        WorkflowRunLogSource::Command(source),
        observed_at,
        payload,
        continuation,
    )
}

fn direct_agent_log_record(
    order: u64,
    source: AgentPresentationObservationKind,
    payload: &str,
) -> WorkflowRunLogRecord {
    direct_source_log_record(
        order,
        WorkflowRunLogSource::Agent(source),
        "2026-08-04T12:34:56Z",
        payload,
        false,
    )
}

fn direct_source_log_record(
    order: u64,
    source: WorkflowRunLogSource,
    observed_at: &str,
    payload: &str,
    continuation: bool,
) -> WorkflowRunLogRecord {
    WorkflowRunLogRecord {
        accepted_order: AcceptedRecordOrder::for_test(order),
        observed_at: time::OffsetDateTime::parse(
            observed_at,
            &time::format_description::well_known::Rfc3339,
        )
        .unwrap(),
        invocation: ActionId {
            transition_sequence: TransitionSequence::default(),
        },
        source,
        source_sequence: SourceSequence::first().get(),
        payload: Arc::from(payload),
        display_width: display_width(payload),
        continuation,
    }
}

fn clamped_log_snapshot(width: u16, height: u16) -> (WorkflowRunViewSnapshot, HostInteraction) {
    let mut snapshot = direct_snapshot(numbered_log_step(30, 40));
    let (interaction, _) = run_full_log_keys(
        &snapshot,
        width,
        height,
        &[(KeyCode::Char('g'), KeyModifiers::NONE)],
    );
    let discarded_bytes = Arc::make_mut(&mut snapshot.steps[0].log.records)
        .drain(0..8)
        .map(|record| u64::try_from(record.payload.len()).unwrap())
        .sum();
    for order in 31..=38 {
        append_log_record(
            &mut snapshot.steps[0].log,
            order,
            &format!("record {order}"),
        );
    }
    snapshot.steps[0].log.discarded_records = 8;
    snapshot.steps[0].log.discarded_bytes = discarded_bytes;
    snapshot.steps[0].log.retained_records = 30;
    snapshot.steps[0].log.observed_records = 38;
    (snapshot, interaction)
}

fn numbered_log_step(record_count: u64, payload_width: usize) -> WorkflowRunStepView {
    let records = (1..=record_count)
        .map(|order| {
            let payload = format!("record {order:02} {}", "x".repeat(payload_width));
            direct_log_record(
                order,
                if order.is_multiple_of(2) {
                    CommandOutputSource::StandardOutput
                } else {
                    CommandOutputSource::StandardError
                },
                "2026-08-04T12:34:56Z",
                &payload,
                false,
            )
        })
        .collect();
    direct_log_step(StepStateKind::Running, records, record_count, 0)
}

fn append_log_record(log: &mut WorkflowRunStepLog, order: u64, payload: &str) {
    Arc::make_mut(&mut log.records).push_back(direct_log_record(
        order,
        if order.is_multiple_of(2) {
            CommandOutputSource::StandardOutput
        } else {
            CommandOutputSource::StandardError
        },
        "2026-08-04T12:34:56Z",
        payload,
        false,
    ));
    log.observed_records = log.observed_records.max(order);
    log.retained_records = u64::try_from(log.records.len()).unwrap();
    log.retained_bytes = log.records.iter().fold(0_u64, |total, record| {
        total.saturating_add(u64::try_from(record.payload.len()).unwrap())
    });
}

fn entered_full_log(
    snapshot: &WorkflowRunViewSnapshot,
    width: u16,
    height: u16,
) -> (HostInteraction, CancellationSource) {
    let cancellation = CancellationSource::new();
    let mut interaction = HostInteraction {
        terminal_area: Rect::new(0, 0, width, height),
        ..HostInteraction::default()
    };
    press_key(
        &mut interaction,
        snapshot,
        KeyCode::Enter,
        KeyModifiers::NONE,
        &cancellation,
    );
    (interaction, cancellation)
}

fn run_full_log_keys(
    snapshot: &WorkflowRunViewSnapshot,
    width: u16,
    height: u16,
    keys: &[(KeyCode, KeyModifiers)],
) -> (HostInteraction, CancellationSource) {
    let (mut interaction, cancellation) = entered_full_log(snapshot, width, height);
    for &(code, modifiers) in keys {
        press_key(&mut interaction, snapshot, code, modifiers, &cancellation);
    }
    (interaction, cancellation)
}

fn assert_quit_control(
    interaction: &mut HostInteraction,
    snapshot: &WorkflowRunViewSnapshot,
    cancellation: &CancellationSource,
    expected: HostControl,
) {
    assert_eq!(
        press_unmodified_key(interaction, snapshot, KeyCode::Char('q'), cancellation),
        expected
    );
}

fn assert_help_omits_quit(
    interaction: &mut HostInteraction,
    snapshot: &WorkflowRunViewSnapshot,
    cancellation: &CancellationSource,
) {
    open_help(interaction, snapshot, cancellation);
    assert!(
        !render_minimum_snapshot_text(snapshot, interaction)
            .contains("Quit the completed workflow")
    );
}

fn open_help(
    interaction: &mut HostInteraction,
    snapshot: &WorkflowRunViewSnapshot,
    cancellation: &CancellationSource,
) {
    press_key(
        interaction,
        snapshot,
        KeyCode::Char('?'),
        KeyModifiers::SHIFT,
        cancellation,
    );
}

fn press_unmodified_key(
    interaction: &mut HostInteraction,
    snapshot: &WorkflowRunViewSnapshot,
    code: KeyCode,
    cancellation: &CancellationSource,
) -> HostControl {
    press_key(
        interaction,
        snapshot,
        code,
        KeyModifiers::NONE,
        cancellation,
    )
}

fn press_key(
    interaction: &mut HostInteraction,
    snapshot: &WorkflowRunViewSnapshot,
    code: KeyCode,
    modifiers: KeyModifiers,
    cancellation: &CancellationSource,
) -> HostControl {
    interaction.handle_key(
        terminal_input_event(Event::Key(crossterm::event::KeyEvent::new(code, modifiers))),
        snapshot,
        cancellation,
    )
}

fn full_log_top_order(
    interaction: &HostInteraction,
    snapshot: &WorkflowRunViewSnapshot,
) -> Option<u64> {
    let step = &snapshot.steps[interaction.selected];
    let log = FilteredLog::new(&step.log, interaction.log_filters);
    log.records
        .get(interaction.full_log.top_index(&log))
        .map(|record| record.accepted_order.get())
}

fn render_full_log_snapshot(
    snapshot: &WorkflowRunViewSnapshot,
    interaction: &mut HostInteraction,
    width: u16,
    height: u16,
) -> ratatui::buffer::Buffer {
    render_snapshot(snapshot, interaction, width, height, false)
}

fn render_minimum_snapshot_text(
    snapshot: &WorkflowRunViewSnapshot,
    interaction: &mut HostInteraction,
) -> String {
    buffer_text(&render_snapshot(
        snapshot,
        interaction,
        MINIMUM_WIDTH,
        MINIMUM_HEIGHT,
        false,
    ))
}

fn minimum_footer(snapshot: &WorkflowRunViewSnapshot, interaction: &mut HostInteraction) -> String {
    buffer_rows(&render_snapshot(
        snapshot,
        interaction,
        MINIMUM_WIDTH,
        MINIMUM_HEIGHT,
        false,
    ))
    .pop()
    .unwrap()
}

fn render_too_small_text(snapshot: &WorkflowRunViewSnapshot) -> String {
    let backend = ratatui::backend::TestBackend::new(40, 8);
    let mut terminal = Terminal::new(backend).unwrap();
    terminal
        .draw(|frame| render_too_small(frame, frame.area(), snapshot, false))
        .unwrap();
    buffer_text(terminal.backend().buffer())
}

fn render_snapshot(
    snapshot: &WorkflowRunViewSnapshot,
    interaction: &mut HostInteraction,
    width: u16,
    height: u16,
    color: bool,
) -> ratatui::buffer::Buffer {
    let backend = ratatui::backend::TestBackend::new(width, height);
    let mut terminal = Terminal::new(backend).unwrap();
    let graph = DagLayout::for_steps(&snapshot.steps);
    terminal
        .draw(|frame| render(frame, snapshot, &graph, interaction, color))
        .unwrap();
    terminal.backend().buffer().clone()
}

fn long_log_step() -> WorkflowRunStepView {
    direct_log_step(
        StepStateKind::Running,
        vec![direct_log_record(
            1,
            CommandOutputSource::StandardOutput,
            "2026-08-04T12:34:56Z",
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789",
            false,
        )],
        1,
        0,
    )
}

fn direct_agent_log_step(records: Vec<WorkflowRunLogRecord>) -> WorkflowRunStepView {
    let observed_records = u64::try_from(records.len()).unwrap();
    let mut step = direct_log_step(StepStateKind::Running, records, observed_records, 0);
    step.definition = WorkflowPresentationStep::Agent {
        profile: "test".to_owned(),
        harness: AgentPresentationHarness::Pi {
            model: "test-model".to_owned(),
            thinking: Thinking::Medium,
        },
        failure_policy: FailurePolicy::Required,
        direct_dependencies: Vec::new(),
        outputs: BTreeMap::new(),
    };
    step.outputs.clear();
    step
}

fn direct_log_step(
    state: StepStateKind,
    records: Vec<WorkflowRunLogRecord>,
    observed_records: u64,
    discarded_records: u64,
) -> WorkflowRunStepView {
    let retained_records = u64::try_from(records.len()).unwrap();
    let retained_bytes = records.iter().fold(0_u64, |total, record| {
        total.saturating_add(u64::try_from(record.payload.len()).unwrap())
    });
    let mut step = direct_command_step(state, None, None, WorkflowRunOutputDisposition::Pending);
    step.log = WorkflowRunStepLog {
        records: Arc::new(records.into()),
        observed_records,
        retained_records,
        retained_bytes,
        discarded_records,
        discarded_bytes: 0,
    };
    step
}

fn direct_command_step(
    state: StepStateKind,
    fact: Option<ObservedStepTransition>,
    timing: Option<WorkflowRunElapsed>,
    output_disposition: WorkflowRunOutputDisposition,
) -> WorkflowRunStepView {
    let output = Output::FilePath {
        path: "report.txt".to_owned(),
        media_type: "text/plain".to_owned(),
    };
    WorkflowRunStepView {
        id: "selected-command".to_owned(),
        role: crate::workflow::validated::WorkflowNodeRole::Step,
        definition: WorkflowPresentationStep::Command {
            argv: vec!["build".to_owned(), "héllo world".to_owned()],
            cwd: Some("work".to_owned()),
            failure_policy: FailurePolicy::Required,
            direct_dependencies: vec!["prepare".to_owned()],
            outputs: BTreeMap::from([("report".to_owned(), output)]),
        },
        state,
        fact,
        inherited: None,
        timing,
        outputs: BTreeMap::from([("report".to_owned(), output_disposition)]),
        log: WorkflowRunStepLog {
            records: Arc::new(VecDeque::new()),
            observed_records: 0,
            retained_records: 0,
            retained_bytes: 0,
            discarded_records: 0,
            discarded_bytes: 0,
        },
    }
}

fn direct_snapshot(step: WorkflowRunStepView) -> WorkflowRunViewSnapshot {
    WorkflowRunViewSnapshot {
        generation: 0,
        workflow_path: "workflow.yaml".to_owned(),
        maximum_parallel_steps: 1,
        workflow: WorkflowState::Executing {
            gate: SchedulingGate::Open,
        },
        timing: WorkflowRunElapsed {
            started_at: time::OffsetDateTime::UNIX_EPOCH,
            duration: Duration::from_secs(3),
            frozen: false,
        },
        steps: vec![step],
        finalization_start: None,
        cancellation: None,
        force_abort: None,
        finalization: None,
        authoritative_result: false,
        quiescent: false,
        publication: WorkflowRunPublicationState::NotStarted,
        cleanup: WorkflowRunCleanupState::NotStarted,
        quit_eligible: false,
    }
}

fn render_direct_inspector(step: &WorkflowRunStepView, width: u16, height: u16) -> String {
    buffer_text(&render_direct_inspector_buffer(step, width, height, false))
}

fn render_direct_inspector_buffer(
    step: &WorkflowRunStepView,
    width: u16,
    height: u16,
    color: bool,
) -> ratatui::buffer::Buffer {
    let backend = ratatui::backend::TestBackend::new(width, height);
    let mut terminal = Terminal::new(backend).unwrap();
    terminal
        .draw(|frame| {
            render_inspector(frame, frame.area(), Some(step), color, Borders::ALL);
        })
        .unwrap();
    terminal.backend().buffer().clone()
}

fn render_direct_log(
    step: &WorkflowRunStepView,
    width: u16,
    height: u16,
    color: bool,
) -> ratatui::buffer::Buffer {
    let snapshot = direct_snapshot(step.clone());
    let backend = ratatui::backend::TestBackend::new(width, height);
    let mut terminal = Terminal::new(backend).unwrap();
    terminal
        .draw(|frame| {
            render_log(
                frame,
                frame.area(),
                &snapshot,
                &mut HostInteraction::default(),
                color,
                Borders::ALL,
            );
        })
        .unwrap();
    terminal.backend().buffer().clone()
}

fn buffer_rows(buffer: &ratatui::buffer::Buffer) -> Vec<String> {
    let area = buffer.area;
    (0..area.height)
        .map(|y| {
            (0..area.width).fold(String::new(), |mut line, x| {
                line.push_str(buffer[(x, y)].symbol());
                line
            })
        })
        .collect()
}

fn inner_buffer_rows(buffer: &ratatui::buffer::Buffer) -> Vec<String> {
    let area = buffer.area;
    (1..area.height.saturating_sub(1))
        .map(|y| {
            (1..area.width.saturating_sub(1)).fold(String::new(), |mut line, x| {
                line.push_str(buffer[(x, y)].symbol());
                line
            })
        })
        .map(|line| line.trim_end().to_owned())
        .collect()
}

fn row_containing(rows: &[String], needle: &str) -> usize {
    rows.iter().position(|row| row.contains(needle)).unwrap()
}

fn column_of(row: &str, needle: &str) -> u16 {
    let byte_index = row.find(needle).unwrap();
    u16::try_from(display_width(&row[..byte_index])).unwrap()
}

fn snapshot_from_yaml(source: &str) -> WorkflowRunViewSnapshot {
    let temporary = tempfile::tempdir().unwrap();
    std::fs::write(temporary.path().join("workflow.yaml"), source).unwrap();
    let workflow = resolution::resolve(temporary.path(), Path::new("workflow.yaml")).unwrap();
    let clock = super::super::presentation::SystemObservationClock;
    WorkflowRunViewModel::new(
        &workflow,
        1,
        RunTimingObservation::new(clock.sample()),
        clock,
    )
    .snapshot()
}

fn render_steps_lines(
    snapshot: &WorkflowRunViewSnapshot,
    interaction: &HostInteraction,
    width: u16,
    height: u16,
) -> Vec<String> {
    let backend = ratatui::backend::TestBackend::new(width, height);
    let mut terminal = Terminal::new(backend).unwrap();
    let graph = DagLayout::for_steps(&snapshot.steps);
    terminal
        .draw(|frame| {
            render_projected_steps(
                frame,
                frame.area(),
                &snapshot.steps,
                &graph,
                interaction.selected,
                false,
                StepPanel {
                    borders: Borders::ALL,
                    show_title: true,
                    phase_boundary: live_step_phase_boundary(snapshot),
                },
            );
        })
        .unwrap();
    buffer_rows(terminal.backend().buffer())
}

fn buffer_position(buffer: &ratatui::buffer::Buffer, needle: &str) -> (u16, u16) {
    let rows = buffer_rows(buffer);
    let y = rows.iter().position(|row| row.contains(needle)).unwrap();
    let x = column_of(&rows[y], needle);
    (x, u16::try_from(y).unwrap())
}
