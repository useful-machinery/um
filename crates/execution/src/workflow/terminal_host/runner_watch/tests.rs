use std::collections::BTreeSet;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64};

use ratatui::backend::TestBackend;
use ratatui::buffer::Buffer;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use super::assignment::OpenAssignment;
use super::run_steps::{phase_boundary, step_state_kind};
use super::view::RunnerWatchDisk;
use super::widgets::{
    IN_FLIGHT_LABEL, LAST_KNOWN_LABEL, RESTARTED_LABEL, mark_glyph, render_runner_watch, short_id,
};
use super::*;
use crate::workflow::test_support::host_scaled_watchdog_with_diagnosis;

const BOOT_A: &str = "rbt_01jx8m2k4p6q8r0s2t4v6w9xya";
const BOOT_B: &str = "rbt_01jx9n3m5q7s9t1v3w5x7y2bcd";
const MINIMUM: Rect = Rect::new(0, 0, MINIMUM_WIDTH, MINIMUM_HEIGHT);
const RELOADED: &str = "credential rrc_01jx8m2k4p6q8r0s2t4v6w7def is current";
const RELOAD_KEY: TerminalInputEvent = TerminalInputEvent::Character('C');
const SETUP_KEY: TerminalInputEvent = TerminalInputEvent::Character('L');
const RUN_URL: &str = "https://cloud.example.test/runs/run_01k0z6r1w8f4jy2m7q9v3x5abc";
const RUN_LOG_TEXT: &str = "resumed the run from its checkpoint";
const PLAN_OUTPUT_TEXT: &str = "wrote the plan to plan.md";

// The one scripted terminal boundary renders every presentation; this one draws the frame
// into a test backend and sends its text to the test.
impl RunnerWatchBoundary for ScriptedTerminalBoundary {
    fn draw_runner_watch(
        &mut self,
        screen: &RunnerWatchScreen,
        interaction: &mut RunnerWatchInteraction,
        color: bool,
    ) -> io::Result<()> {
        let area = interaction.terminal_area;
        self.scripted_draw(area, "injected runner watch panic")?;
        let buffer = render_screen(screen, interaction, area, color);
        if let Some(frames) = &self.frames {
            let _ = frames.send(buffer_rows(&buffer).join("\n"));
        }
        Ok(())
    }
}

// REQ-RUNNERENROLL-140, REQ-LOCALRUN-6
#[test]
fn overview_draws_each_fact_whole_and_without_color_at_the_minimum_size() {
    let mut screen = RunnerWatchScreen::new(overview(BOOT_A), observed_at(0));
    let mut interaction = RunnerWatchInteraction::new(MINIMUM);
    let buffer = render_screen(&screen, &mut interaction, MINIMUM, false);
    let rendered = buffer_text(&buffer);
    for expected in [
        "AUTO-751",
        "AUTO-644",
        "run_5abq01",
        "build › implement",
        "external",
        "approval",
        "lease ok 42s",
        "lease renewing 51s",
        "1 working",
        "2 waiting",
        "succeeded",
        "connection failed (network), next attempt in 4s",
    ] {
        assert!(
            rendered.contains(expected),
            "missing {expected:?}: {rendered:?}"
        );
    }
    let rows = buffer_rows(&buffer);
    let row_of = |title: &str| {
        rows.iter()
            .find(|row| row.contains(title))
            .cloned()
            .unwrap_or_default()
    };
    let working = mark_glyph(RunnerWatchMark::Active);
    let waiting = mark_glyph(RunnerWatchMark::Pending);
    assert_ne!(working, waiting);
    assert!(row_of("AUTO-751").contains(working), "{rows:#?}");
    assert!(row_of("AUTO-644").contains(waiting), "{rows:#?}");
    assert!(!row_of("AUTO-644").contains(working), "{rows:#?}");
    assert!(
        buffer
            .content()
            .iter()
            .all(|cell| cell.fg == ratatui::style::Color::Reset
                && cell.bg == ratatui::style::Color::Reset),
        "a cell carries a color"
    );

    let narrow = Rect::new(0, 0, MINIMUM_WIDTH - 1, MINIMUM_HEIGHT);
    let too_small = buffer_text(&render_screen(&screen, &mut interaction, narrow, false));
    assert!(
        too_small.contains(&format!("{MINIMUM_WIDTH}x{MINIMUM_HEIGHT}")),
        "{too_small:?}"
    );
    for title in ["AUTO-751", "AUTO-644", "run_5abq01"] {
        assert!(!too_small.contains(title), "{too_small:?}");
    }

    screen.overview.assignments = (101..=112)
        .map(|number| assignment(&format!("AUTO-{number}")))
        .collect();
    screen.overview.counts.running = 12;
    screen.overview.counts.waiting = 0;
    let mut interaction = RunnerWatchInteraction::new(MINIMUM);
    let crowded = render_screen(&screen, &mut interaction, MINIMUM, false);
    let rows = buffer_rows(&crowded);
    for (heading, first_entry) in [("RECENT", "AUTO-740"), ("EVENTS", "14:02:10")] {
        let heading_row = rows
            .iter()
            .position(|row| row.trim_start().starts_with(heading))
            .unwrap_or_else(|| panic!("missing {heading}: {rows:#?}"));
        assert!(
            rows[heading_row + 1].contains(first_entry),
            "missing the first {heading} entry: {rows:#?}"
        );
    }
    assert!(!buffer_text(&crowded).contains("AUTO-112"));
    assert_eq!(
        interaction.handle_key(TerminalInputEvent::Bottom, &mut screen),
        None
    );
    let scrolled = buffer_text(&render_screen(&screen, &mut interaction, MINIMUM, false));
    assert!(scrolled.contains("AUTO-112"), "{scrolled:?}");
}

// REQ-RUNNERENROLL-140
#[test]
fn a_recent_entry_gives_up_no_more_than_the_row_needs() {
    let mut screen = RunnerWatchScreen::new(overview(BOOT_A), observed_at(0));
    screen.overview.recent[1].result = "interrupted".to_owned();
    screen.overview.recent[1].detail = Some("execution_lease_expired".to_owned());
    let recent_row = |area: Rect| {
        let mut interaction = RunnerWatchInteraction::new(area);
        buffer_rows(&render_screen(&screen, &mut interaction, area, false))
            .into_iter()
            .find(|row| row.contains("AUTO-733"))
            .unwrap_or_default()
    };

    // At the minimum size the cause fits once the workflow gives way.
    let narrow = recent_row(MINIMUM);
    for kept in ["interrupted", "execution_lease_expired", "41s"] {
        assert!(narrow.contains(kept), "missing {kept:?}: {narrow:?}");
    }
    // Side by side at 100 columns it does not fit, and it alone is left out.
    let beside = recent_row(Rect::new(0, 0, 100, 30));
    for kept in ["engineering", "interrupted", "41s"] {
        assert!(beside.contains(kept), "missing {kept:?}: {beside:?}");
    }
}

// REQ-RUNNERENROLL-141
#[tokio::test]
async fn host_keeps_the_last_overview_while_the_runner_is_away() {
    let area = Rect::new(0, 0, 100, 30);
    let (mut boundary, input, mut actions) =
        ScriptedTerminalBoundary::new(area, [], BoundaryFailures::default());
    let (frame_sender, mut frames) = tokio::sync::mpsc::unbounded_channel();
    boundary.frames = Some(frame_sender);
    let source = ScriptedSource::new([
        Ok(overview(BOOT_A)),
        Err(RunnerWatchSourceError::NotReachable),
        Ok(overview(BOOT_B)),
    ]);
    let host = RunnerWatchHost::start_with_boundary(
        source,
        overview(BOOT_A),
        SteppingClock::new(),
        false,
        boundary,
    )
    .unwrap();

    let away = next_frame(&mut frames, |frame| frame.contains(LAST_KNOWN_LABEL)).await;
    assert!(away.contains(BOOT_A), "{away:?}");
    assert!(!away.contains(BOOT_B), "{away:?}");
    let back = next_frame(&mut frames, |frame| frame.contains(BOOT_B)).await;
    assert!(!back.contains(LAST_KNOWN_LABEL), "{back:?}");
    assert!(back.contains(RESTARTED_LABEL), "{back:?}");
    while let Ok(action) = actions.try_recv() {
        assert_ne!(action, BoundaryAction::Restore, "the host ended while away");
    }

    input
        .send(ScriptedInput::Event(TerminalInputEvent::Quit))
        .unwrap();
    assert_eq!(host.wait().await.unwrap(), ArchivedTerminalHostExit::Quit);
}

// REQ-RUNNERENROLL-143, REQ-LOCALRUN-70
#[tokio::test]
async fn host_endings_restore_and_the_source_is_only_read() {
    for (input, expected) in [
        (
            Some(TerminalInputEvent::Quit),
            ArchivedTerminalHostExit::Quit,
        ),
        (
            Some(TerminalInputEvent::Cancel),
            ArchivedTerminalHostExit::Interrupted,
        ),
        (None, ArchivedTerminalHostExit::Terminated),
    ] {
        let area = Rect::new(0, 0, 100, 30);
        let (boundary, sender, mut actions) =
            ScriptedTerminalBoundary::new(area, [], BoundaryFailures::default());
        let source = ScriptedSource::new([Ok(overview(BOOT_A))]);
        let calls = Arc::clone(&source.calls);
        let host = RunnerWatchHost::start_with_boundary(
            source,
            overview(BOOT_A),
            SteppingClock::new(),
            false,
            boundary,
        )
        .unwrap();
        wait_for_action(&mut actions, BoundaryAction::Draw(area)).await;
        // Let at least one overview read reach the source before the ending.
        wait_for_action(&mut actions, BoundaryAction::Draw(area)).await;
        if let Some(input) = input {
            sender.send(ScriptedInput::Event(input)).unwrap();
        } else {
            host.exit_request()
                .request(ArchivedTerminalHostExit::Terminated);
        }
        assert_eq!(host.wait().await.unwrap(), expected);
        wait_for_action(&mut actions, BoundaryAction::Restore).await;
        let calls = calls.lock().unwrap();
        assert!(!calls.is_empty());
        assert!(calls.iter().all(|call| *call == SourceCall::Overview));
    }
}

// REQ-LOCALRUN-72
#[tokio::test]
async fn host_draw_failure_restores_and_answers_a_presentation_failure() {
    let (boundary, _input, mut actions) = ScriptedTerminalBoundary::new(
        Rect::new(0, 0, 100, 30),
        [],
        BoundaryFailures {
            draw_at: Some(2),
            ..BoundaryFailures::default()
        },
    );
    let host = RunnerWatchHost::start_with_boundary(
        ScriptedSource::new([Ok(overview(BOOT_A))]),
        overview(BOOT_A),
        SteppingClock::new(),
        false,
        boundary,
    )
    .unwrap();
    let failure = host.wait().await.unwrap_err();
    assert_eq!(
        failure.operation,
        PresentationFailureOperation::TerminalDraw
    );
    wait_for_action(&mut actions, BoundaryAction::Restore).await;
}

// REQ-RUNNERENROLL-142
#[tokio::test]
async fn an_operation_runs_off_the_draw_loop_and_its_outcome_stays_shown() {
    let area = Rect::new(0, 0, 100, 30);
    let (mut boundary, input, _actions) =
        ScriptedTerminalBoundary::new(area, [], BoundaryFailures::default());
    let (frame_sender, mut frames) = tokio::sync::mpsc::unbounded_channel();
    boundary.frames = Some(frame_sender);
    let (source, gate) = ScriptedSource::new([Ok(overview(BOOT_A))]).gated();
    let calls = Arc::clone(&source.calls);
    let host = RunnerWatchHost::start_with_boundary(
        source,
        overview(BOOT_A),
        SteppingClock::new(),
        false,
        boundary,
    )
    .unwrap();

    for key in [SETUP_KEY, SETUP_KEY, RELOAD_KEY] {
        input.send(ScriptedInput::Event(key)).unwrap();
    }
    next_frame(&mut frames, |frame| frame.contains(IN_FLIGHT_LABEL)).await;
    let reads = count_calls(&calls, SourceCall::Overview);
    let still_waiting = next_frame(&mut frames, |_| {
        count_calls(&calls, SourceCall::Overview) >= reads + 2
    })
    .await;
    assert!(still_waiting.contains(IN_FLIGHT_LABEL), "{still_waiting:?}");
    assert_eq!(count_calls(&calls, SourceCall::SetUpAgentLogins), 1);
    assert_eq!(count_calls(&calls, SourceCall::ReloadCredential), 0);

    gate.send(RunnerWatchOutcome::Refused(vec![
        "the runner is busy with another setup (runner_busy)".to_owned(),
    ]))
    .unwrap();
    let refused = next_frame(&mut frames, |frame| frame.contains("runner_busy")).await;
    assert!(!refused.contains(IN_FLIGHT_LABEL), "{refused:?}");

    input.send(ScriptedInput::Event(RELOAD_KEY)).unwrap();
    let reloaded = next_frame(&mut frames, |frame| frame.contains(RELOADED)).await;
    assert!(!reloaded.contains("runner_busy"), "{reloaded:?}");
    assert_eq!(count_calls(&calls, SourceCall::ReloadCredential), 1);
    assert_eq!(count_calls(&calls, SourceCall::SetUpAgentLogins), 1);

    input
        .send(ScriptedInput::Event(TerminalInputEvent::Quit))
        .unwrap();
    assert_eq!(host.wait().await.unwrap(), ArchivedTerminalHostExit::Quit);
}

// REQ-RUNNERENROLL-140, REQ-LOCALRUN-6: the disk reading sits on the identity line, and
// while the disk is low a warning with the minimum takes the line under it instead, told
// by its mark and words rather than by color.
#[test]
fn the_disk_reading_and_its_low_warning_are_drawn_without_color() {
    const GIB: u64 = 1 << 30;
    let wide = Rect::new(0, 0, 120, 30);
    let draw = |disk: Option<RunnerWatchDisk>, area: Rect| {
        let mut screen = RunnerWatchScreen::new(overview(BOOT_A), observed_at(0));
        screen.overview.disk = disk;
        let mut interaction = RunnerWatchInteraction::new(area);
        let buffer = render_screen(&screen, &mut interaction, area, false);
        assert!(
            buffer
                .content()
                .iter()
                .all(|cell| cell.fg == ratatui::style::Color::Reset
                    && cell.bg == ratatui::style::Color::Reset),
            "a cell carries a color"
        );
        let rows = buffer_rows(&buffer);
        let identity = rows
            .iter()
            .position(|row| row.contains("v0.83.0"))
            .unwrap_or_else(|| panic!("missing the identity line: {rows:#?}"));
        (rows, identity)
    };
    let reading = |available_bytes: u64, low: bool| RunnerWatchDisk {
        available_bytes,
        total_bytes: 423_054_278_656,
        minimum_free_bytes: 5 * GIB,
        low,
    };

    // The narrowest row keeps the reading's shorter form.
    for (area, shown) in [
        (MINIMUM, "disk 120 GiB free"),
        (wide, "disk 120 GiB free of 394 GiB"),
    ] {
        let (rows, identity) = draw(Some(reading(120 * GIB, false)), area);
        assert!(rows[identity].contains(shown), "{rows:#?}");
        assert!(
            !rows.iter().any(|row| row.contains("disk low")),
            "{rows:#?}"
        );
    }
    for area in [MINIMUM, wide] {
        let (rows, identity) = draw(Some(reading(6_186_599, true)), area);
        assert!(!rows[identity].contains("disk"), "{rows:#?}");
        let warning = &rows[identity + 1];
        for fact in [
            mark_glyph(RunnerWatchMark::Failure),
            "disk low",
            "5.9 MiB free of 394 GiB",
            "minimum 5.0 GiB",
        ] {
            assert!(warning.contains(fact), "missing {fact:?}: {rows:#?}");
        }

        let (rows, identity) = draw(None, area);
        assert!(!rows[identity].contains("disk"), "{rows:#?}");
        assert!(
            !rows.iter().any(|row| row.contains("disk low")),
            "{rows:#?}"
        );
    }
}

// REQ-RUNNERENROLL-142
#[test]
fn an_outcome_shows_each_fact_whole_or_counts_it_at_the_minimum_size() {
    let facts = [
        "claude-code set up",
        "codex set up",
        "linear not set up: the token was refused by the provider (not_authorized)",
        "github set up",
        "the next scheduled setup is at 15:20:07Z",
        "two providers were not asked because no workflow on this runner names them",
        "gemini set up after 3 attempts",
    ]
    .map(str::to_owned);
    let mut screen = RunnerWatchScreen::new(overview(BOOT_A), observed_at(0));
    screen.notice = Some(RunnerWatchNotice::Answered {
        operation: RunnerWatchOperation::SetUpAgentLogins,
        outcome: RunnerWatchOutcome::Done(facts.to_vec()),
    });
    let mut interaction = RunnerWatchInteraction::new(MINIMUM);
    let rows = buffer_rows(&render_screen(&screen, &mut interaction, MINIMUM, false));
    let left_out = facts
        .iter()
        .filter(|fact| !rows.iter().any(|row| row.contains(fact.as_str())))
        .count();
    assert!(left_out < facts.len(), "{rows:#?}");
    if left_out != 0 {
        assert!(
            rows.iter()
                .any(|row| row.contains(&format!("+{left_out} more"))),
            "{left_out} facts left out uncounted: {rows:#?}"
        );
    }
    for (heading, first_entry) in [("RECENT", "AUTO-740"), ("EVENTS", "14:02:10")] {
        let heading_row = rows
            .iter()
            .position(|row| row.trim_start().starts_with(heading))
            .unwrap_or_else(|| panic!("missing {heading}: {rows:#?}"));
        assert!(
            rows[heading_row + 1].contains(first_entry),
            "missing the first {heading} entry: {rows:#?}"
        );
    }
}

// REQ-RUNNERENROLL-143
#[tokio::test]
async fn quitting_never_waits_for_an_operation_in_flight() {
    let area = Rect::new(0, 0, 100, 30);
    let (mut boundary, input, mut actions) =
        ScriptedTerminalBoundary::new(area, [], BoundaryFailures::default());
    let (frame_sender, mut frames) = tokio::sync::mpsc::unbounded_channel();
    boundary.frames = Some(frame_sender);
    let (source, gate) = ScriptedSource::new([Ok(overview(BOOT_A))]).gated();
    let calls = Arc::clone(&source.calls);
    let returned = Arc::clone(&source.setup_returned);
    let host = RunnerWatchHost::start_with_boundary(
        source,
        overview(BOOT_A),
        SteppingClock::new(),
        false,
        boundary,
    )
    .unwrap();

    input.send(ScriptedInput::Event(SETUP_KEY)).unwrap();
    next_frame(&mut frames, |frame| frame.contains(IN_FLIGHT_LABEL)).await;
    next_frame(&mut frames, |_| {
        count_calls(&calls, SourceCall::SetUpAgentLogins) == 1
    })
    .await;
    input
        .send(ScriptedInput::Event(TerminalInputEvent::Quit))
        .unwrap();
    let exit = host.wait().await;
    let still_waiting = !returned.load(Ordering::SeqCst);
    // Open the gate before any assertion, so no blocked worker holds the runtime open.
    drop(gate);
    assert_eq!(exit.unwrap(), ArchivedTerminalHostExit::Quit);
    assert!(still_waiting, "the host waited for the operation");
    wait_for_action(&mut actions, BoundaryAction::Restore).await;
}

// REQ-RUNNERENROLL-146, REQ-LOCALRUN-6
#[test]
fn a_run_given_as_words_draws_its_graph_and_inspector_without_color() {
    let run = five_step_run();
    let steps = &run.steps;
    let graph = DagLayout::for_steps(steps);
    let phase_boundary = phase_boundary(steps);
    let ids = ["plan", "implement", "review", "odd", "cleanup"];
    let inspected: [(usize, &[&str]); 4] = [
        (0, &["scripts/plan", "tools"]),
        (1, &["implement.yaml"]),
        (2, &["reviewer", "opus-4.1", "high"]),
        (3, &["paused_by_moon"]),
    ];
    let wide = Rect::new(0, 0, 120, 30);
    for area in [wide, MINIMUM] {
        let mut kinds_seen = BTreeSet::new();
        let mut ids_seen = BTreeSet::new();
        for (selected, _) in steps.iter().enumerate() {
            let (buffer, layout) = render_run(steps, &graph, phase_boundary, selected, area);
            assert!(
                buffer
                    .content()
                    .iter()
                    .all(|cell| cell.fg == ratatui::style::Color::Reset
                        && cell.bg == ratatui::style::Color::Reset),
                "a cell carries a color"
            );
            let graph_rows = region_rows(&buffer, layout.dag);
            let row_of = |id: &str| {
                graph_rows
                    .iter()
                    .position(|row| row.split_whitespace().any(|word| word == id))
            };
            // Every step drawn has a row of its own, in the run's order, and the
            // selected step is always drawn.
            let drawn = ids
                .iter()
                .filter_map(|id| row_of(id).map(|row| (*id, row)))
                .collect::<Vec<_>>();
            assert!(
                drawn.windows(2).all(|pair| pair[0].1 < pair[1].1),
                "{graph_rows:#?}"
            );
            assert!(row_of(ids[selected]).is_some(), "{graph_rows:#?}");
            ids_seen.extend(drawn.iter().map(|(id, _)| *id));
            for row in &graph_rows {
                for kind in ["cmd", "agent", "workflow", "script"] {
                    if row.split_whitespace().any(|word| word == kind) {
                        kinds_seen.insert(kind);
                    }
                }
            }
            if let Some(plan) = row_of("plan") {
                assert!(graph_rows[plan].contains('✓'), "{graph_rows:#?}");
            }
            if let Some(odd) = row_of("odd") {
                let odd = &graph_rows[odd];
                assert!(odd.contains(UNKNOWN_STATE_MARK), "{odd:?}");
                assert!(!odd.contains('○'), "{odd:?}");
            }
            if let (Some(odd), Some(cleanup)) = (row_of("odd"), row_of("cleanup")) {
                assert!(
                    graph_rows[odd + 1..cleanup].iter().any(|row| {
                        row.chars().any(char::is_alphabetic)
                            && !ids.iter().any(|id| row.contains(id))
                    }),
                    "no phase label between odd and cleanup: {graph_rows:#?}"
                );
            }
            if area == wide {
                assert_eq!(drawn.len(), ids.len(), "{graph_rows:#?}");
            }

            let inspector = region_rows(&buffer, layout.inspector);
            let holds = |text: &str| inspector.iter().any(|row| row.contains(text));
            if let Some((_, expected)) = inspected.iter().find(|(index, _)| *index == selected) {
                for text in *expected {
                    assert!(holds(text), "missing {text:?}: {inspector:#?}");
                }
            }
            match ids[selected] {
                "plan" => assert!(
                    inspector.iter().any(|row| {
                        let words = row.split_whitespace().collect::<Vec<_>>();
                        words.contains(&"plan") && words.contains(&"text")
                    }),
                    "{inspector:#?}"
                ),
                "implement" => {
                    let dependency = inspector
                        .iter()
                        .any(|row| row.split_whitespace().last() == Some("plan"));
                    let running = inspector
                        .iter()
                        .filter(|row| row.split_whitespace().any(|word| word == "running"))
                        .count();
                    // The title's state and the child run's state.
                    assert!(dependency && running >= 2, "{inspector:#?}");
                }
                _ => {}
            }
        }
        assert_eq!(
            kinds_seen,
            BTreeSet::from(["cmd", "agent", "workflow", "script"])
        );
        assert_eq!(ids_seen, ids.into_iter().collect::<BTreeSet<_>>());
    }
}

// REQ-RUNNERENROLL-135
#[test]
fn each_state_word_reads_as_its_kind_and_another_word_as_none() {
    for word in [
        "pending",
        "starting",
        "running",
        "capturing_outputs",
        "cancelling",
        "succeeded",
        "inherited",
        "failed",
        "blocked",
        "skipped",
        "not_run",
        "cancelled",
    ] {
        assert_eq!(step_state_kind(word).map(StepStateKind::as_str), Some(word));
    }
    assert_eq!(step_state_kind("paused_by_moon"), None);
}

// REQ-RUNNERENROLL-146: an opened assignment draws its facts, its URL whole or the width
// that shows it, and its run's graph and inspector at both layouts, and below the minimum
// size the notice alone.
#[test]
fn an_opened_assignment_draws_its_facts_url_and_run() {
    let ids = ["plan", "implement", "review", "odd", "cleanup"];
    let wide = Rect::new(0, 0, 120, 30);
    let long_url = format!("{RUN_URL}{}", "/a".repeat(9));
    assert_eq!((RUN_URL.len(), long_url.len()), (62, 80));
    for (url, area) in [
        (RUN_URL.to_owned(), MINIMUM),
        (RUN_URL.to_owned(), wide),
        (long_url.clone(), MINIMUM),
        (long_url.clone(), wide),
    ] {
        let mut entry = assignment("AUTO-751");
        entry.assignment_id = "asn_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned();
        entry.run_id = "run_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned();
        entry.run_url = Some(url.clone());
        let (mut screen, mut interaction) = opened(entry.clone(), area);
        take_answer(
            &mut screen,
            &mut interaction,
            root_query(&entry.assignment_id),
            held(entry.clone(), Some(five_step_run()), Some(run_log())),
        );
        // At the minimum size the graph has room for the selected step's rows, so each
        // step is selected in turn.
        for id in ids {
            select(&mut interaction, id);
            let rows = buffer_rows(&render_screen(&screen, &mut interaction, area, false));
            let drawn = |text: &str| rows.iter().any(|row| row.contains(text));
            let words = |words: &[&str]| rows_with(&rows, words);
            let short = short_id(&entry.assignment_id).unwrap();
            assert!(drawn(&entry.assignment_id) || drawn(&short), "{rows:#?}");
            assert!(drawn("attempt 2"), "{rows:#?}");
            assert!(words(&["running"]) && words(&["ok", "42s"]), "{rows:#?}");
            assert!(words(&["engineering"]), "{rows:#?}");
            assert!(words(&[id]), "missing {id}: {rows:#?}");
            if area == wide {
                assert!(ids.iter().all(|id| words(&[id])), "{rows:#?}");
            }
            if url.len() <= usize::from(area.width) - 2 {
                assert!(drawn(&url), "{rows:#?}");
            } else {
                assert!(!drawn(&url[..20]), "{rows:#?}");
            }
        }
        let small = Rect::new(0, 0, MINIMUM_WIDTH - 1, MINIMUM_HEIGHT);
        let rows = buffer_rows(&render_screen(&screen, &mut interaction, small, false));
        assert!(
            rows.iter()
                .any(|row| row.contains(&format!("{MINIMUM_WIDTH}x{MINIMUM_HEIGHT}"))),
            "{rows:#?}"
        );
        assert!(ids.iter().all(|id| !rows_with(&rows, &[id])), "{rows:#?}");
    }
}

// REQ-RUNNERENROLL-146: Enter asks for the selected entry's root, a `workflow` step in
// flight is never named, and the step selected is asked for without another's output
// shown under it until its answer comes.
#[tokio::test]
async fn enter_asks_for_the_assignment_and_the_selected_step_for_its_output() {
    let (gate, gated) = std::sync::mpsc::channel::<()>();
    let source = ScriptedSource::new([Ok(overview(BOOT_A))]).with_assignments(move |query| {
        if query.step.as_deref() == Some("plan") {
            let _ = gated.recv();
            return held(
                assignment("AUTO-751"),
                Some(five_step_run()),
                Some(plan_output()),
            );
        }
        held(
            assignment("AUTO-751"),
            Some(five_step_run()),
            Some(run_log()),
        )
    });
    let calls = Arc::clone(&source.calls);
    let mut watch = Watch::start(source, SteppingClock::new());

    watch.press(TerminalInputEvent::Enter);
    watch.frame(|frame| frame.contains(RUN_LOG_TEXT)).await;
    assert_eq!(
        assignment_reads(&calls).first(),
        Some(&root_query("asg_AUTO-751"))
    );
    watch.frame(|_| assignment_reads(&calls).len() >= 3).await;
    assert!(
        assignment_reads(&calls)
            .iter()
            .all(|read| *read == root_query("asg_AUTO-751")),
        "{:?}",
        assignment_reads(&calls)
    );

    watch.press(TerminalInputEvent::Up);
    let asked = watch
        .frame(|frame| rows_with(&frame_rows(frame), &["plan", "succeeded"]))
        .await;
    assert!(!asked.contains(RUN_LOG_TEXT), "{asked}");
    assert!(!asked.contains(PLAN_OUTPUT_TEXT), "{asked}");
    watch
        .frame(|_| {
            assignment_reads(&calls)
                .last()
                .is_some_and(|read| read.step.as_deref() == Some("plan"))
        })
        .await;
    gate.send(()).unwrap();
    let answered = watch.frame(|frame| frame.contains(PLAN_OUTPUT_TEXT)).await;
    // The invocation's number and the earlier invocations not shown.
    assert!(
        rows_with(&frame_rows(&answered), &["plan", "3", "1"]),
        "{answered}"
    );
    assert!(!answered.contains(RUN_LOG_TEXT), "{answered}");

    drop(gate);
    watch.quit().await;
}

// REQ-RUNNERENROLL-146: the step wanted is asked for at once, with no second passing.
#[tokio::test]
async fn a_newly_selected_step_is_asked_for_at_once() {
    let source = ScriptedSource::new([Ok(overview(BOOT_A))]).with_assignments(|query| {
        let log = if query.step.is_some() {
            plan_output()
        } else {
            run_log()
        };
        held(assignment("AUTO-751"), Some(five_step_run()), Some(log))
    });
    let calls = Arc::clone(&source.calls);
    let mut watch = Watch::start(source, StillClock(observed_at(0)));

    watch.press(TerminalInputEvent::Enter);
    watch.frame(|frame| frame.contains(RUN_LOG_TEXT)).await;
    watch.press(TerminalInputEvent::Up);
    watch.frame(|frame| frame.contains(PLAN_OUTPUT_TEXT)).await;
    let reads = assignment_reads(&calls);
    assert_eq!(reads.len(), 2, "{reads:?}");
    assert_eq!(reads[1].step.as_deref(), Some("plan"));

    watch.quit().await;
}

// REQ-RUNNERENROLL-146: Enter on a `workflow` step opens its child run, Esc returns to the
// parent with the step selected and then to the overview, which alone is asked for then.
#[tokio::test]
async fn enter_walks_into_a_child_run_and_esc_walks_back() {
    let source = ScriptedSource::new([Ok(overview(BOOT_A))]).with_assignments(|query| {
        let run = if query.path.is_empty() {
            five_step_run()
        } else {
            child_run()
        };
        held(assignment("AUTO-751"), Some(run), Some(run_log()))
    });
    let calls = Arc::clone(&source.calls);
    let mut watch = Watch::start(source, SteppingClock::new());
    let child = RunnerWatchRunElement {
        step: "implement".to_owned(),
        item: None,
        ordinal: 1,
    };

    watch.press(TerminalInputEvent::Enter);
    watch
        .frame(|frame| rows_with(&frame_rows(frame), &["odd"]))
        .await;
    watch.press(TerminalInputEvent::Enter);
    let inside = watch
        .frame(|frame| rows_with(&frame_rows(frame), &["draft"]))
        .await;
    assert!(
        rows_with(&frame_rows(&inside), &["engineering", "implement"]),
        "{inside}"
    );
    assert!(
        assignment_reads(&calls)
            .iter()
            .any(|read| read.path == [child.clone()] && read.step.is_none())
    );

    watch.press(TerminalInputEvent::Escape);
    let parent = watch
        .frame(|frame| rows_with(&frame_rows(frame), &["odd"]))
        .await;
    // The inspector's title names the `workflow` step with its failure policy.
    assert!(
        rows_with(&frame_rows(&parent), &["implement", "required"]),
        "{parent}"
    );
    watch
        .frame(|_| {
            assignment_reads(&calls)
                .last()
                .is_some_and(|read| *read == root_query("asg_AUTO-751"))
        })
        .await;

    watch.press(TerminalInputEvent::Escape);
    watch.frame(|frame| frame.contains("ASSIGNMENTS")).await;
    let overviews = count_calls(&calls, SourceCall::Overview);
    watch
        .frame(|_| count_calls(&calls, SourceCall::Overview) >= overviews + 2)
        .await;
    let settled = assignment_reads(&calls).len();
    watch
        .frame(|_| count_calls(&calls, SourceCall::Overview) >= overviews + 5)
        .await;
    assert_eq!(assignment_reads(&calls).len(), settled);

    watch.quit().await;
}

// REQ-RUNNERENROLL-146: the selection is held by step ID when a step changes state and
// another appears before it.
#[tokio::test]
async fn the_selection_follows_its_step_through_an_answer() {
    let source = ScriptedSource::new([Ok(overview(BOOT_A))]).with_assignments(|query| {
        if query.step.is_none() {
            return held(
                assignment("AUTO-751"),
                Some(five_step_run()),
                Some(run_log()),
            );
        }
        let mut run = five_step_run();
        run.steps[0].state = "failed".to_owned();
        let mut prep = run.steps[0].clone();
        prep.id = "prep".to_owned();
        prep.state = "succeeded".to_owned();
        run.steps.insert(0, prep);
        held(assignment("AUTO-751"), Some(run), Some(plan_output()))
    });
    let calls = Arc::clone(&source.calls);
    let mut watch = Watch::start(source, SteppingClock::new());

    watch.press(TerminalInputEvent::Enter);
    watch.frame(|frame| frame.contains(RUN_LOG_TEXT)).await;
    watch.press(TerminalInputEvent::Up);
    let changed = watch
        .frame(|frame| rows_with(&frame_rows(frame), &["prep"]))
        .await;
    assert!(
        rows_with(&frame_rows(&changed), &["plan", "failed"]),
        "{changed}"
    );
    let reads = assignment_reads(&calls).len();
    watch
        .frame(|_| assignment_reads(&calls).len() >= reads + 2)
        .await;
    assert!(
        assignment_reads(&calls)[reads..]
            .iter()
            .all(|read| read.step.as_deref() == Some("plan"))
    );

    watch.quit().await;
}

// REQ-RUNNERENROLL-146: a run absent from an answer keeps the one drawn, and an assignment
// never given a run draws its facts and no graph.
#[tokio::test]
async fn a_run_absent_from_an_answer_keeps_the_last_one_drawn() {
    let ids = ["plan", "implement", "review", "odd", "cleanup"];
    for ever_given in [true, false] {
        let mut answered = 0;
        let source = ScriptedSource::new([Ok(overview(BOOT_A))]).with_assignments(move |_| {
            answered += 1;
            if ever_given && answered == 1 {
                return held(assignment("AUTO-751"), Some(five_step_run()), None);
            }
            // The facts row follows each answer.
            let mut entry = assignment("AUTO-751");
            if let Some(lease) = &mut entry.lease {
                lease.time_left = Duration::from_secs(39);
            }
            held(entry, None, None)
        });
        let mut watch = Watch::start(source, SteppingClock::new());

        watch.press(TerminalInputEvent::Enter);
        let frame = watch
            .frame(|frame| opened_frame(frame) && rows_with(&frame_rows(frame), &["39s"]))
            .await;
        let rows = frame_rows(&frame);
        assert!(rows_with(&rows, &["attempt", "2"]), "{frame}");
        for id in ids {
            assert_eq!(rows_with(&rows, &[id]), ever_given, "{id}: {frame}");
        }

        watch.quit().await;
    }
}

// REQ-RUNNERENROLL-146: an assignment the runner holds no longer shows its result from the
// overview's recent list beside the last run drawn, and is asked for no more.
#[tokio::test]
async fn an_assignment_no_longer_held_shows_its_result_and_is_asked_for_no_more() {
    let mut ended = overview(BOOT_A);
    ended.recent[1].assignment_id = "asg_AUTO-751".to_owned();
    let mut answered = 0;
    let source = ScriptedSource::new([Ok(ended)]).with_assignments(move |_| {
        answered += 1;
        if answered == 1 {
            held(
                assignment("AUTO-751"),
                Some(five_step_run()),
                Some(run_log()),
            )
        } else {
            Ok(RunnerWatchAssignmentAnswer::NotHeld)
        }
    });
    let calls = Arc::clone(&source.calls);
    let mut watch = Watch::start(source, SteppingClock::new());

    watch.press(TerminalInputEvent::Enter);
    let frame = watch
        .frame(|frame| opened_frame(frame) && rows_with(&frame_rows(frame), &["step_failed"]))
        .await;
    let rows = frame_rows(&frame);
    assert!(rows_with(&rows, &["failed", "step_failed"]), "{frame}");
    assert!(
        rows_with(&rows, &["odd"]) && rows_with(&rows, &["plan"]),
        "{frame}"
    );
    let reads = assignment_reads(&calls).len();
    let overviews = count_calls(&calls, SourceCall::Overview);
    watch
        .frame(|_| count_calls(&calls, SourceCall::Overview) >= overviews + 3)
        .await;
    assert_eq!(assignment_reads(&calls).len(), reads);

    watch.press(TerminalInputEvent::Escape);
    watch.frame(|frame| frame.contains("ASSIGNMENTS")).await;

    watch.quit().await;
}

// REQ-RUNNERENROLL-141: an assignment read the runner does not answer keeps the run drawn
// under the last-known banner, and the reads go on.
#[tokio::test]
async fn an_unanswered_assignment_read_keeps_the_run_drawn() {
    let mut answered = 0;
    let source = ScriptedSource::new([Ok(overview(BOOT_A))]).with_assignments(move |_| {
        answered += 1;
        if answered == 1 {
            held(
                assignment("AUTO-751"),
                Some(five_step_run()),
                Some(run_log()),
            )
        } else {
            Err(RunnerWatchSourceError::NotReachable)
        }
    });
    let calls = Arc::clone(&source.calls);
    let mut watch = Watch::start(source, SteppingClock::new());

    watch.press(TerminalInputEvent::Enter);
    let away = watch
        .frame(|frame| frame.contains(LAST_KNOWN_LABEL) && frame.contains(RUN_LOG_TEXT))
        .await;
    assert!(rows_with(&frame_rows(&away), &["odd"]), "{away}");
    let reads = assignment_reads(&calls).len();
    watch
        .frame(|_| assignment_reads(&calls).len() >= reads + 2)
        .await;

    watch.quit().await;
}

// REQ-RUNNERENROLL-142, REQ-RUNNERENROLL-143: with an assignment open the operation keys
// reach the runner with nothing, and `q` never waits for an assignment read.
#[tokio::test]
async fn an_open_assignment_sends_no_operation_and_quits_without_waiting() {
    let (gate, gated) = std::sync::mpsc::channel::<()>();
    let returned = Arc::new(AtomicBool::new(false));
    let source = ScriptedSource::new([Ok(overview(BOOT_A))]).with_assignments({
        let returned = Arc::clone(&returned);
        move |_| {
            let _ = gated.recv();
            returned.store(true, Ordering::SeqCst);
            Ok(RunnerWatchAssignmentAnswer::NotHeld)
        }
    });
    let calls = Arc::clone(&source.calls);
    let mut watch = Watch::start(source, SteppingClock::new());

    watch.press(TerminalInputEvent::Enter);
    watch.frame(|_| !assignment_reads(&calls).is_empty()).await;
    for key in [RELOAD_KEY, SETUP_KEY, TerminalInputEvent::Quit] {
        watch.press(key);
    }
    let exit = watch.host.wait().await;
    let still_waiting = !returned.load(Ordering::SeqCst);
    // Open the gate before any assertion, so no blocked worker holds the runtime open.
    drop(gate);
    assert_eq!(exit.unwrap(), ArchivedTerminalHostExit::Quit);
    assert!(still_waiting, "the host waited for the assignment read");
    wait_for_action(&mut watch.actions, BoundaryAction::Restore).await;
    assert_eq!(count_calls(&calls, SourceCall::ReloadCredential), 0);
    assert_eq!(count_calls(&calls, SourceCall::SetUpAgentLogins), 0);
}

// REQ-RUNNERENROLL-146: Enter on a settled step opens its output full-screen under the
// inspector at the newest line, following it; a movement key pauses it on a line that later
// answers keep at the top, `F` follows again, Left and Right pan, and leaving it forgets the
// position.
#[tokio::test]
async fn a_full_screen_output_follows_pauses_on_a_line_and_forgets_it_on_leaving() {
    let log = Arc::new(Mutex::new(numbered_output(1..=40, 3)));
    // `plan` declares no output here, so that its inspector leaves the log rows at 64 by 20.
    let mut run = five_step_run();
    run.steps[0].outputs.clear();
    let source = ScriptedSource::new([Ok(overview(BOOT_A))]).with_assignments({
        let log = Arc::clone(&log);
        move |query| {
            let log = if query.step.as_deref() == Some("plan") {
                log.lock().unwrap().clone()
            } else {
                run_log()
            };
            held(assignment("AUTO-751"), Some(run.clone()), Some(log))
        }
    });
    let mut watch = Watch::start_at(source, SteppingClock::new(), MINIMUM);

    watch.press(TerminalInputEvent::Enter);
    watch
        .frame(|frame| rows_with(&frame_rows(frame), &["implement", "workflow"]))
        .await;
    watch.press(TerminalInputEvent::Up);
    watch.frame(|frame| frame.contains("err line 3")).await;

    watch.press(TerminalInputEvent::Enter);
    let following = watch
        .frame(|frame| !has_facts_row(frame) && frame.contains("err line 3"))
        .await;
    let rows = lowercase_rows(&following);
    // Each stream's name, retained and discarded bytes and the bytes left out before it.
    assert!(
        rows_with(&rows, &["stdout", "480", "9", "120"]),
        "{following}"
    );
    assert!(
        rows_with(&rows, &["stderr", "33", "2", "30"]),
        "{following}"
    );
    assert!(!following.contains("paused"), "{following}");

    watch.press(TerminalInputEvent::Top);
    let paused = watch
        .frame(|frame| frame.contains("out line 01") && frame.contains("paused"))
        .await;
    assert!(!has_facts_row(&paused), "{paused}");
    for _ in 0..12 {
        watch.press(TerminalInputEvent::Down);
    }
    watch.frame(|frame| first_out_line(frame) == Some(11)).await;
    // Standard output loses its first 10 lines to the bytes left out and gains 5.
    *log.lock().unwrap() = numbered_output(11..=45, 3);
    let moved_on = watch
        .frame(|frame| rows_with(&lowercase_rows(frame), &["stdout", "240"]))
        .await;
    assert_eq!(first_out_line(&moved_on), Some(11), "{moved_on}");
    assert!(moved_on.contains("paused"), "{moved_on}");

    watch.press(TerminalInputEvent::Bottom);
    watch.press(TerminalInputEvent::Follow);
    watch
        .frame(|frame| frame.contains("err line 3") && !frame.contains("paused"))
        .await;
    *log.lock().unwrap() = numbered_output(11..=45, 4);
    watch.frame(|frame| frame.contains("err line 4")).await;

    let wide = format!("err wide {} TAIL", "x".repeat(80));
    {
        let mut log = log.lock().unwrap();
        *log = numbered_output(11..=45, 4);
        log.streams[1].text.push_str(&format!("{wide}\n"));
    }
    watch
        .frame(|frame| frame.contains("err wide") && !frame.contains("TAIL"))
        .await;
    for _ in 0..wide.len() {
        watch.press(TerminalInputEvent::PanRight);
    }
    watch
        .frame(|frame| frame.contains("TAIL") && !frame.contains("err wide"))
        .await;
    for _ in 0..wide.len() {
        watch.press(TerminalInputEvent::PanLeft);
    }
    watch
        .frame(|frame| frame.contains("err wide") && !frame.contains("TAIL"))
        .await;

    watch.press(TerminalInputEvent::Top);
    watch
        .frame(|frame| frame.contains("out line 11") && frame.contains("paused"))
        .await;
    watch.press(TerminalInputEvent::Escape);
    let split = watch.frame(has_facts_row).await;
    assert!(
        rows_with(&frame_rows(&split), &["plan", "succeeded"]),
        "{split}"
    );
    watch.press(TerminalInputEvent::Enter);
    let reopened = watch.frame(|frame| !has_facts_row(frame)).await;
    assert!(reopened.contains("err wide"), "{reopened}");
    assert!(!reopened.contains("out line 11"), "{reopened}");
    assert!(!reopened.contains("paused"), "{reopened}");

    watch.quit().await;
}

// REQ-RUNNERENROLL-146: `o` on a `workflow` step opens the run log full-screen and Esc
// returns to the run's split view; an answer with no log draws none and every movement key
// leaves the viewer drawing.
#[tokio::test]
async fn o_opens_the_run_log_full_screen_with_or_without_a_log() {
    let without_log = Arc::new(AtomicBool::new(false));
    let source = ScriptedSource::new([Ok(overview(BOOT_A))]).with_assignments({
        let without_log = Arc::clone(&without_log);
        move |_| {
            let log = (!without_log.load(Ordering::SeqCst)).then(run_log);
            held(assignment("AUTO-751"), Some(five_step_run()), log)
        }
    });
    let calls = Arc::clone(&source.calls);
    let mut watch = Watch::start_at(source, SteppingClock::new(), MINIMUM);

    watch.press(TerminalInputEvent::Enter);
    watch
        .frame(|frame| rows_with(&frame_rows(frame), &["implement", "workflow"]))
        .await;
    watch.press(TerminalInputEvent::Character('o'));
    let full = watch
        .frame(|frame| !has_facts_row(frame) && frame.contains(RUN_LOG_TEXT))
        .await;
    // The bytes of the run log left out before its text.
    assert!(rows_with(&frame_rows(&full), &["4096"]), "{full}");

    watch.press(TerminalInputEvent::Escape);
    let split = watch.frame(has_facts_row).await;
    assert!(opened_frame(&split), "{split}");
    assert!(
        rows_with(&frame_rows(&split), &["implement", "running"]),
        "{split}"
    );
    let asked = assignment_reads(&calls).len();
    watch
        .frame(|_| assignment_reads(&calls).len() >= asked + 2)
        .await;
    assert!(
        assignment_reads(&calls)
            .iter()
            .all(|read| *read == root_query("asg_AUTO-751"))
    );

    without_log.store(true, Ordering::SeqCst);
    watch.frame(|frame| !frame.contains(RUN_LOG_TEXT)).await;
    watch.press(TerminalInputEvent::Character('o'));
    watch
        .frame(|frame| !has_facts_row(frame) && !frame.contains(RUN_LOG_TEXT))
        .await;
    for key in [
        TerminalInputEvent::Up,
        TerminalInputEvent::Down,
        TerminalInputEvent::PageUp,
        TerminalInputEvent::PageDown,
        TerminalInputEvent::HalfPageUp,
        TerminalInputEvent::HalfPageDown,
        TerminalInputEvent::Top,
        TerminalInputEvent::Bottom,
        TerminalInputEvent::PanLeft,
        TerminalInputEvent::PanRight,
        TerminalInputEvent::Follow,
    ] {
        watch.press(key);
    }
    let reads = assignment_reads(&calls).len();
    let after = watch
        .frame(|_| assignment_reads(&calls).len() >= reads + 2)
        .await;
    assert!(!has_facts_row(&after), "{after}");

    watch.quit().await;
}

// REQ-RUNNERENROLL-146: at the minimum size the full-screen output still draws its newest
// line under an inspector that takes every row it may, with and without the status row: the
// stream headings give way to the line.
#[test]
fn a_full_screen_output_keeps_its_newest_line_under_the_tallest_inspector() {
    for (declared_outputs, ended) in [(2, false), (3, true)] {
        let entry = assignment("AUTO-751");
        let (mut screen, mut interaction) = opened(entry.clone(), MINIMUM);
        let mut run = five_step_run();
        let declared = run.steps[0].outputs[0].clone();
        run.steps[0].outputs = (0..declared_outputs)
            .map(|index| RunnerWatchRunOutput {
                name: format!("plan-{index}"),
                ..declared.clone()
            })
            .collect();
        take_answer(
            &mut screen,
            &mut interaction,
            root_query(&entry.assignment_id),
            held(entry.clone(), Some(run.clone()), Some(run_log())),
        );
        select(&mut interaction, "plan");
        let names_plan = RunnerWatchAssignmentQuery {
            step: Some("plan".to_owned()),
            ..root_query(&entry.assignment_id)
        };
        take_answer(
            &mut screen,
            &mut interaction,
            names_plan.clone(),
            held(entry, Some(run), Some(numbered_output(1..=40, 3))),
        );
        if ended {
            take_answer(
                &mut screen,
                &mut interaction,
                names_plan,
                Ok(RunnerWatchAssignmentAnswer::NotHeld),
            );
        }
        assert_eq!(
            interaction.handle_key(TerminalInputEvent::Enter, &mut screen),
            None
        );

        let rows = buffer_rows(&render_screen(&screen, &mut interaction, MINIMUM, false));
        assert!(!rows_with(&rows, &["attempt", "2"]), "{rows:#?}");
        assert!(
            rows.iter().any(|row| row.contains("err line 3")),
            "{declared_outputs} outputs, ended {ended}: {rows:#?}"
        );
    }
}

// REQ-RUNNERENROLL-146: once the runner holds the assignment no longer it is asked for no
// more, so the log pane of a step whose output was never read stops saying it is asked for.
#[test]
fn a_step_never_read_is_not_drawn_as_asked_for_once_the_assignment_ended() {
    let entry = assignment("AUTO-751");
    let (mut screen, mut interaction) = opened(entry.clone(), MINIMUM);
    let root = root_query(&entry.assignment_id);
    take_answer(
        &mut screen,
        &mut interaction,
        root.clone(),
        held(entry, Some(five_step_run()), Some(run_log())),
    );
    select(&mut interaction, "plan");
    // The log pane's first row is the last row that names the selected step.
    let pane_heading = |screen: &RunnerWatchScreen, interaction: &mut RunnerWatchInteraction| {
        buffer_rows(&render_screen(screen, interaction, MINIMUM, false))
            .into_iter()
            .rev()
            .find(|row| row.split_whitespace().any(|word| word == "plan"))
            .expect("the log pane names the selected step")
    };

    let asked_for = pane_heading(&screen, &mut interaction);
    take_answer(
        &mut screen,
        &mut interaction,
        root,
        Ok(RunnerWatchAssignmentAnswer::NotHeld),
    );
    let ended = pane_heading(&screen, &mut interaction);

    assert_ne!(asked_for.trim(), ended.trim());
}

// REQ-RUNNERENROLL-146, REQ-RUNNERENROLL-135: a step's records draw in the log pane under a
// first row naming the step and the older records and bytes not shown, one row per record
// in the order given, each beginning with the run interface's label for its source, a
// continuation under the split record's marker and a word this release does not know under
// the word as received.
#[test]
fn a_step_s_records_draw_in_order_under_the_run_interface_s_labels() {
    let area = Rect::new(0, 0, 120, 40);
    let label = |source| log_source_presentation(source).label;
    let records = [
        (
            "stdout",
            label(WorkflowRunLogSource::Command(
                CommandOutputSource::StandardOutput,
            )),
            "compiling core",
            false,
        ),
        (
            "stderr",
            label(WorkflowRunLogSource::Command(
                CommandOutputSource::StandardError,
            )),
            "warning: unused import",
            false,
        ),
        (
            "assistant",
            label(WorkflowRunLogSource::Agent(
                AgentPresentationObservationKind::Assistant,
            )),
            "the build is green",
            false,
        ),
        (
            "stdout",
            label(WorkflowRunLogSource::Command(
                CommandOutputSource::StandardOutput,
            )),
            "the rest of a long line",
            true,
        ),
        (
            "telepathy",
            "telepathy",
            "a word from a later release",
            false,
        ),
    ];
    let log = records_log(
        Some("plan"),
        12,
        3_400,
        records
            .iter()
            .map(|(word, _, text, continuation)| record(word, text, *continuation))
            .collect(),
    );
    let (mut screen, mut interaction) = plan_selected(area);
    take_answer(
        &mut screen,
        &mut interaction,
        step_query("plan"),
        held(assignment("AUTO-751"), Some(five_step_run()), Some(log)),
    );

    let rows = buffer_rows(&render_screen(&screen, &mut interaction, area, false));
    let mut previous = rows
        .iter()
        .position(|row| rows_with(std::slice::from_ref(row), &["plan", "12", "3400"]))
        .unwrap_or_else(|| panic!("no row names the step and what was left out: {rows:#?}"));
    for (_, label, text, continuation) in records {
        let index = rows
            .iter()
            .position(|row| row.contains(text))
            .unwrap_or_else(|| panic!("{text} is not drawn: {rows:#?}"));
        assert!(index > previous, "{text} is out of order: {rows:#?}");
        previous = index;
        let (before, _) = rows[index].split_once(text).unwrap();
        let before = before.split_whitespace().collect::<Vec<_>>();
        assert!(
            before.contains(&label),
            "{text} is not under {label}: {rows:#?}"
        );
        assert_eq!(before.contains(&"↪"), continuation, "{rows:#?}");
    }
}

// REQ-RUNNERENROLL-146: a step with no records says no output has been received, as one row
// of its own and not as an empty pane.
#[test]
fn a_step_with_no_records_says_so_in_one_row() {
    let area = Rect::new(0, 0, 120, 40);
    let (mut screen, mut interaction) = plan_selected(area);
    take_answer(
        &mut screen,
        &mut interaction,
        step_query("plan"),
        held(
            assignment("AUTO-751"),
            Some(five_step_run()),
            Some(records_log(Some("plan"), 0, 0, Vec::new())),
        ),
    );
    let document_rows = shown_rows(&screen, &mut interaction).expect("the step's document");
    let [notice] = document_rows.as_slice() else {
        panic!("not one row: {document_rows:#?}");
    };
    assert!(!notice.trim().is_empty());
    let rows = buffer_rows(&render_screen(&screen, &mut interaction, area, false));
    assert!(
        rows.iter().any(|row| row.contains(notice.as_str())),
        "{rows:#?}"
    );
}

// REQ-RUNNERENROLL-146: a records log naming no step draws its own heading and no row, and
// the pane is drawn neither as a log asked for nor as one yet to come.
#[test]
fn a_records_log_naming_no_step_is_drawn_as_its_own_and_not_as_one_to_come() {
    let area = Rect::new(0, 0, 120, 40);
    // Each opens on `implement`, a `workflow` step in flight, beside which the pane wants the
    // run's log.
    let drawn = |query_step: Option<&str>, log: Option<RunnerWatchLog>| {
        let entry = assignment("AUTO-751");
        let (mut screen, mut interaction) = opened(entry.clone(), area);
        let query = RunnerWatchAssignmentQuery {
            step: query_step.map(str::to_owned),
            ..root_query(&entry.assignment_id)
        };
        take_answer(
            &mut screen,
            &mut interaction,
            query,
            held(entry, Some(five_step_run()), log),
        );
        let document = shown_document_facts(&screen, &mut interaction);
        let rows = buffer_rows(&render_screen(&screen, &mut interaction, area, false));
        (rows, document)
    };
    let (asked_for, _) = drawn(Some("plan"), Some(run_log()));
    let (no_log, _) = drawn(None, None);
    let (no_step, document) = drawn(None, Some(records_log(None, 0, 0, Vec::new())));

    let (heading, document_rows) = document.expect("the records log's document");
    assert!(document_rows.is_empty(), "{document_rows:#?}");
    assert!(!heading.trim().is_empty());
    assert!(
        no_step.iter().any(|row| row.contains(heading.as_str())),
        "{no_step:#?}"
    );
    assert_ne!(no_step, asked_for);
    assert_ne!(no_step, no_log);
}

// REQ-RUNNERENROLL-146: an answer whose records name another step than the one asked about
// is not drawn under the step selected, which keeps the records it was given.
#[test]
fn records_of_another_step_are_not_drawn_under_the_step_selected() {
    let area = Rect::new(0, 0, 120, 40);
    let (mut screen, mut interaction) = plan_selected(area);
    for (step, text) in [("plan", "from plan"), ("review", "from review")] {
        take_answer(
            &mut screen,
            &mut interaction,
            step_query("plan"),
            held(
                assignment("AUTO-751"),
                Some(five_step_run()),
                Some(records_log(
                    Some(step),
                    0,
                    0,
                    vec![record("stdout", text, false)],
                )),
            ),
        );
    }
    let rows = buffer_rows(&render_screen(&screen, &mut interaction, area, false));
    assert!(
        rows.iter().any(|row| row.contains("from plan")),
        "{rows:#?}"
    );
    assert!(
        !rows.iter().any(|row| row.contains("from review")),
        "{rows:#?}"
    );
}

// REQ-RUNNERENROLL-146, REQ-LOCALRUN-47: in the full-screen log, paused on a record, an
// answer that left three more older records out and added two newer ones keeps the same
// record at the top.
#[test]
fn a_paused_full_screen_log_keeps_its_top_record_through_older_records_left_out() {
    let area = Rect::new(0, 0, 120, 30);
    let numbered = |first: u64, last: u64| {
        records_log(
            Some("plan"),
            first - 1,
            (first - 1) * 11,
            (first..=last)
                .map(|number| record("stdout", &format!("record {number:03}"), false))
                .collect(),
        )
    };
    let (mut screen, mut interaction) = plan_selected(area);
    let answer = |screen: &mut RunnerWatchScreen,
                  interaction: &mut RunnerWatchInteraction,
                  log: RunnerWatchLog| {
        take_answer(
            screen,
            interaction,
            step_query("plan"),
            held(assignment("AUTO-751"), Some(five_step_run()), Some(log)),
        );
    };
    answer(&mut screen, &mut interaction, numbered(13, 72));
    let draw = |screen: &RunnerWatchScreen, interaction: &mut RunnerWatchInteraction| {
        buffer_rows(&render_screen(screen, interaction, area, false))
    };
    let press = |screen: &mut RunnerWatchScreen,
                 interaction: &mut RunnerWatchInteraction,
                 key: TerminalInputEvent| {
        assert_eq!(interaction.handle_key(key, screen), None);
    };
    press(&mut screen, &mut interaction, TerminalInputEvent::Enter);
    draw(&screen, &mut interaction);
    press(&mut screen, &mut interaction, TerminalInputEvent::Top);
    draw(&screen, &mut interaction);
    for _ in 0..5 {
        press(&mut screen, &mut interaction, TerminalInputEvent::Down);
    }
    let paused = draw(&screen, &mut interaction);
    assert_eq!(first_record(&paused), Some(18), "{paused:#?}");

    answer(&mut screen, &mut interaction, numbered(16, 74));
    let moved_on = draw(&screen, &mut interaction);
    assert_eq!(first_record(&moved_on), Some(18), "{moved_on:#?}");
    assert!(
        moved_on.iter().any(|row| row.contains("paused")),
        "{moved_on:#?}"
    );
}

/// Draws a run's step graph and the selected step's inspector, as the assignment's split
/// view does, without color.
fn render_run(
    steps: &[RunnerWatchRunStep],
    graph: &DagLayout,
    phase_boundary: Option<StepPhaseBoundary>,
    selected: usize,
    area: Rect,
) -> (Buffer, SplitBodyLayout) {
    let selected_step = steps.get(selected);
    let layout = split_body_layout(
        area,
        0,
        inspector_desired_height(selected_step),
        wide_split_columns(area),
    );
    let mut terminal = Terminal::new(TestBackend::new(area.width, area.height)).unwrap();
    terminal
        .draw(|frame| {
            render_split_steps(frame, layout, steps, graph, phase_boundary, selected, false);
            render_inspector(
                frame,
                layout.inspector,
                selected_step,
                false,
                layout.inspector_borders(),
            );
        })
        .unwrap();
    (terminal.backend().buffer().clone(), layout)
}

/// The rows of a buffer's region, each as its cells' text.
fn region_rows(buffer: &Buffer, region: Rect) -> Vec<String> {
    (region.y..region.bottom())
        .map(|y| {
            (region.x..region.right())
                .map(|x| buffer[(x, y)].symbol())
                .collect()
        })
        .collect()
}

/// A run of five steps given as words: a settled command, a stage in flight with one child
/// run, a pending agent, a step whose kind and state this release does not know, and a
/// pending finalizer.
fn five_step_run() -> RunnerWatchRun {
    let step = |id: &str, kind: &str, state: &str, work: RunnerWatchRunWork| RunnerWatchRunStep {
        id: id.to_owned(),
        role: "step".to_owned(),
        kind: kind.to_owned(),
        state: state.to_owned(),
        failure_policy: "required".to_owned(),
        direct_dependencies: Vec::new(),
        started_at: None,
        duration: None,
        work,
        outputs: Vec::new(),
        child_runs: Vec::new(),
        child_runs_omitted: 0,
    };
    let mut plan = step(
        "plan",
        "cmd",
        "succeeded",
        RunnerWatchRunWork::Command {
            argv: vec!["scripts/plan".to_owned()],
            cwd: Some("tools".to_owned()),
        },
    );
    plan.started_at = Some(timestamp("2026-11-19T14:10:00Z"));
    plan.duration = Some(Duration::from_millis(4_100));
    plan.outputs = vec![RunnerWatchRunOutput {
        name: "plan".to_owned(),
        kind: "text".to_owned(),
        committed: true,
    }];
    let mut implement = step(
        "implement",
        "workflow",
        "running",
        RunnerWatchRunWork::Workflow {
            file: "implement.yaml".to_owned(),
        },
    );
    implement.direct_dependencies = vec!["plan".to_owned()];
    implement.started_at = Some(timestamp("2026-11-19T14:10:05Z"));
    implement.duration = Some(Duration::from_secs(61));
    implement.child_runs = vec![RunnerWatchChildRun {
        ordinal: 1,
        item: None,
        state: "running".to_owned(),
        outcome: None,
    }];
    let mut review = step(
        "review",
        "agent",
        "pending",
        RunnerWatchRunWork::Agent {
            profile: "reviewer".to_owned(),
            harness: "claude_code".to_owned(),
            model: Some("opus-4.1".to_owned()),
            effort: Some("high".to_owned()),
        },
    );
    review.direct_dependencies = vec!["implement".to_owned()];
    review.outputs = vec![RunnerWatchRunOutput {
        name: "verdict".to_owned(),
        kind: "json".to_owned(),
        committed: false,
    }];
    let odd = step(
        "odd",
        "script",
        "paused_by_moon",
        RunnerWatchRunWork::Unknown,
    );
    let mut cleanup = step(
        "cleanup",
        "cmd",
        "pending",
        RunnerWatchRunWork::Command {
            argv: vec!["scripts/cleanup".to_owned()],
            cwd: None,
        },
    );
    cleanup.role = "finalizer".to_owned();
    RunnerWatchRun {
        path: Vec::new(),
        workflow_path: "workflows/engineering.yaml".to_owned(),
        state: "running".to_owned(),
        outcome: None,
        attempt_number: 1,
        attempts: 1,
        attempt_state: "running".to_owned(),
        wait: None,
        steps: vec![plan, implement, review, odd, cleanup],
        steps_omitted: 0,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum SourceCall {
    Overview,
    Assignment(RunnerWatchAssignmentQuery),
    ReloadCredential,
    SetUpAgentLogins,
}

type AssignmentResult = Result<RunnerWatchAssignmentAnswer, RunnerWatchSourceError>;
/// Answers each assignment read from its query; a script may block a read on a gate.
type AssignmentScript = Box<dyn FnMut(&RunnerWatchAssignmentQuery) -> AssignmentResult + Send>;

/// Answers its script in order and then its last answer again, logging every call. An agent
/// login setup waits for the outcome its gate is given, or answers at once without a gate;
/// a gate whose sender is dropped opens with a done outcome. An assignment read answers its
/// assignment script, the runner holding none by default.
struct ScriptedSource {
    answers: Mutex<VecDeque<Result<RunnerWatchOverview, RunnerWatchSourceError>>>,
    assignments: Mutex<AssignmentScript>,
    calls: Arc<Mutex<Vec<SourceCall>>>,
    login_gate: Mutex<Option<std::sync::mpsc::Receiver<RunnerWatchOutcome>>>,
    /// Set once an agent login setup has returned.
    setup_returned: Arc<AtomicBool>,
}

impl ScriptedSource {
    fn new(
        answers: impl IntoIterator<Item = Result<RunnerWatchOverview, RunnerWatchSourceError>>,
    ) -> Self {
        Self {
            answers: Mutex::new(answers.into_iter().collect()),
            assignments: Mutex::new(Box::new(|_| Ok(RunnerWatchAssignmentAnswer::NotHeld))),
            calls: Arc::default(),
            login_gate: Mutex::new(None),
            setup_returned: Arc::default(),
        }
    }

    /// Makes agent login setup wait until the returned sender gives it its outcome.
    fn gated(mut self) -> (Self, std::sync::mpsc::Sender<RunnerWatchOutcome>) {
        let (sender, receiver) = std::sync::mpsc::channel();
        self.login_gate = Mutex::new(Some(receiver));
        (self, sender)
    }

    fn with_assignments(
        mut self,
        script: impl FnMut(&RunnerWatchAssignmentQuery) -> AssignmentResult + Send + 'static,
    ) -> Self {
        self.assignments = Mutex::new(Box::new(script));
        self
    }
}

/// The assignment reads the source was asked for, in order.
fn assignment_reads(calls: &Mutex<Vec<SourceCall>>) -> Vec<RunnerWatchAssignmentQuery> {
    calls
        .lock()
        .unwrap()
        .iter()
        .filter_map(|call| match call {
            SourceCall::Assignment(query) => Some(query.clone()),
            _ => None,
        })
        .collect()
}

fn count_calls(calls: &Mutex<Vec<SourceCall>>, call: SourceCall) -> usize {
    calls
        .lock()
        .unwrap()
        .iter()
        .filter(|logged| **logged == call)
        .count()
}

impl RunnerWatchSource for ScriptedSource {
    fn overview(&self) -> Result<RunnerWatchOverview, RunnerWatchSourceError> {
        self.calls.lock().unwrap().push(SourceCall::Overview);
        let mut answers = self.answers.lock().unwrap();
        if answers.len() > 1 {
            answers.pop_front().unwrap()
        } else {
            answers.front().cloned().unwrap()
        }
    }

    fn assignment(&self, query: &RunnerWatchAssignmentQuery) -> AssignmentResult {
        self.calls
            .lock()
            .unwrap()
            .push(SourceCall::Assignment(query.clone()));
        (self.assignments.lock().unwrap())(query)
    }

    fn reload_credential(&self) -> RunnerWatchOutcome {
        self.calls
            .lock()
            .unwrap()
            .push(SourceCall::ReloadCredential);
        RunnerWatchOutcome::Done(vec![RELOADED.to_owned()])
    }

    fn set_up_agent_logins(&self) -> RunnerWatchOutcome {
        self.calls
            .lock()
            .unwrap()
            .push(SourceCall::SetUpAgentLogins);
        let outcome = match self.login_gate.lock().unwrap().as_ref() {
            Some(gate) => gate
                .recv()
                .unwrap_or_else(|_| RunnerWatchOutcome::Done(Vec::new())),
            None => RunnerWatchOutcome::Done(Vec::new()),
        };
        self.setup_returned.store(true, Ordering::SeqCst);
        outcome
    }
}

/// A clock one second later at every sample, so each redraw tick may start a request.
#[derive(Clone)]
struct SteppingClock {
    base: ObservationTime,
    samples: Arc<AtomicU64>,
}

impl SteppingClock {
    fn new() -> Self {
        Self {
            base: observed_at(0),
            samples: Arc::default(),
        }
    }
}

impl ObservationClock for SteppingClock {
    fn sample(&self) -> ObservationTime {
        let step = Duration::from_secs(self.samples.fetch_add(1, Ordering::SeqCst));
        ObservationTime {
            utc: self.base.utc + step,
            monotonic: self.base.monotonic + step,
        }
    }
}

async fn next_frame(
    frames: &mut tokio::sync::mpsc::UnboundedReceiver<String>,
    wanted: impl Fn(&str) -> bool,
) -> String {
    loop {
        let frame = frames.recv().await.expect("frame channel closed");
        if wanted(&frame) {
            return frame;
        }
    }
}

fn render_screen(
    screen: &RunnerWatchScreen,
    interaction: &mut RunnerWatchInteraction,
    area: Rect,
    color: bool,
) -> Buffer {
    let mut terminal = Terminal::new(TestBackend::new(area.width, area.height)).unwrap();
    terminal
        .draw(|frame| render_runner_watch(frame, screen, interaction, color))
        .unwrap();
    terminal.backend().buffer().clone()
}

fn buffer_rows(buffer: &Buffer) -> Vec<String> {
    buffer
        .content()
        .chunks(usize::from(buffer.area.width))
        .map(|row| row.iter().map(ratatui::buffer::Cell::symbol).collect())
        .collect()
}

fn observed_at(seconds: u64) -> ObservationTime {
    ObservationTime {
        utc: timestamp("2026-11-19T14:12:00Z") + Duration::from_secs(seconds),
        monotonic: useful_support::monotonic_now() + Duration::from_secs(seconds),
    }
}

fn timestamp(value: &str) -> OffsetDateTime {
    OffsetDateTime::parse(value, &Rfc3339).unwrap()
}

fn overview(boot_id: &str) -> RunnerWatchOverview {
    RunnerWatchOverview {
        runner_id: "rnr_01jx8m2k4p6q8r0s2t4v6w5abc".to_owned(),
        boot_id: boot_id.to_owned(),
        version: "0.83.0".to_owned(),
        uptime: Duration::from_secs(3 * 3_600 + 12 * 60),
        disk: None,
        stop_time_left: None,
        connection: RunnerWatchConnection {
            mark: RunnerWatchMark::Active,
            state: "connected".to_owned(),
            connected: true,
            last_connected_at: Some(timestamp("2026-11-19T14:02:10Z")),
            last_failure: None,
            attempts: None,
            next_attempt_in: None,
        },
        reports_waiting: 2,
        current_credential_id: Some("rrc_01jx8m2k4p6q8r0s2t4v6w5abc".to_owned()),
        pending_credential_id: Some("rrc_01jx8m2k4p6q8r0s2t4v6w7def".to_owned()),
        logins: vec![
            RunnerWatchLogin {
                provider: "claude-code".to_owned(),
                ready: true,
                reason: None,
            },
            RunnerWatchLogin {
                provider: "linear".to_owned(),
                ready: false,
                reason: Some("not_authorized".to_owned()),
            },
            RunnerWatchLogin {
                provider: "codex".to_owned(),
                ready: true,
                reason: None,
            },
        ],
        counts: RunnerWatchCounts {
            running: 1,
            waiting: 2,
            ..RunnerWatchCounts::default()
        },
        assignments: vec![
            assignment("AUTO-751"),
            RunnerWatchAssignment {
                assignment_id: "asg_644".to_owned(),
                run_id: "run_644".to_owned(),
                attempt_number: 1,
                continues_earlier_attempt: false,
                run_url: None,
                title: RunnerWatchTitle::Subject("AUTO-644".to_owned()),
                workflow_path: "workflows/merge.yaml".to_owned(),
                phase: "waiting".to_owned(),
                mark: RunnerWatchMark::Pending,
                held: Duration::from_secs(3 * 60),
                lease: Some(RunnerWatchLease {
                    health: "renewing".to_owned(),
                    time_left: Duration::from_secs(51),
                }),
                detail: RunnerWatchAssignmentDetail::Wait(RunnerWatchWait {
                    kind: "external".to_owned(),
                    step: vec!["queue".to_owned()],
                    since: Some(timestamp("2026-11-19T14:10:00Z")),
                }),
            },
            RunnerWatchAssignment {
                assignment_id: "asg_5abq".to_owned(),
                run_id: "run_5abq01".to_owned(),
                attempt_number: 1,
                continues_earlier_attempt: false,
                run_url: None,
                title: RunnerWatchTitle::RunId("run_5abq01".to_owned()),
                workflow_path: "workflows/engineering.yaml".to_owned(),
                phase: "waiting".to_owned(),
                mark: RunnerWatchMark::Pending,
                held: Duration::from_secs(3_600 + 2 * 60),
                lease: None,
                detail: RunnerWatchAssignmentDetail::Wait(RunnerWatchWait {
                    kind: "approval".to_owned(),
                    step: vec!["review".to_owned()],
                    since: Some(timestamp("2026-11-19T13:05:41Z")),
                }),
            },
        ],
        assignments_omitted: 0,
        recent: vec![
            RunnerWatchRecent {
                assignment_id: "asg_AUTO-740".to_owned(),
                title: RunnerWatchTitle::Subject("AUTO-740".to_owned()),
                workflow_path: "workflows/review.yaml".to_owned(),
                result: "succeeded".to_owned(),
                succeeded: true,
                detail: Some("merged".to_owned()),
                duration: Duration::from_secs(12 * 60),
                ended_at: timestamp("2026-11-19T14:00:00Z"),
            },
            RunnerWatchRecent {
                assignment_id: "asg_AUTO-733".to_owned(),
                title: RunnerWatchTitle::Subject("AUTO-733".to_owned()),
                workflow_path: "workflows/engineering.yaml".to_owned(),
                result: "failed".to_owned(),
                succeeded: false,
                detail: Some("step_failed".to_owned()),
                duration: Duration::from_secs(41),
                ended_at: timestamp("2026-11-19T13:40:00Z"),
            },
        ],
        recent_omitted: 10,
        events: vec![
            event(
                "2026-11-19T14:02:10Z",
                "connected",
                1,
                RunnerWatchMark::Success,
            ),
            event(
                "2026-11-19T14:01:58Z",
                "connection failed (network), next attempt in 4s",
                3,
                RunnerWatchMark::Failure,
            ),
            event(
                "2026-11-19T13:20:07Z",
                "agent logins set up (schedule)",
                12,
                RunnerWatchMark::Success,
            ),
        ],
        events_omitted: 54,
    }
}

/// A working assignment with one running step under a child run and a healthy lease.
fn assignment(subject: &str) -> RunnerWatchAssignment {
    RunnerWatchAssignment {
        assignment_id: format!("asg_{subject}"),
        run_id: format!("run_{subject}"),
        attempt_number: 2,
        continues_earlier_attempt: false,
        run_url: Some(RUN_URL.to_owned()),
        title: RunnerWatchTitle::Subject(subject.to_owned()),
        workflow_path: "workflows/engineering.yaml".to_owned(),
        phase: "running".to_owned(),
        mark: RunnerWatchMark::Active,
        held: Duration::from_secs(18 * 60),
        lease: Some(RunnerWatchLease {
            health: "ok".to_owned(),
            time_left: Duration::from_secs(42),
        }),
        detail: RunnerWatchAssignmentDetail::Steps {
            steps: vec![RunnerWatchStep {
                path: vec!["build".to_owned(), "implement".to_owned()],
            }],
            omitted: 0,
        },
    }
}

fn event(at: &str, text: &str, count: u64, mark: RunnerWatchMark) -> RunnerWatchEvent {
    RunnerWatchEvent {
        at: timestamp(at),
        text: text.to_owned(),
        count,
        mark,
    }
}

/// A viewer over the scripted boundary, at 120 by 30 unless given a size, with its frames'
/// rows.
struct Watch {
    host: RunnerWatchHost,
    input: tokio::sync::mpsc::UnboundedSender<ScriptedInput>,
    frames: tokio::sync::mpsc::UnboundedReceiver<String>,
    actions: tokio::sync::mpsc::UnboundedReceiver<BoundaryAction>,
}

impl Watch {
    fn start(source: ScriptedSource, clock: impl ObservationClock) -> Self {
        Self::start_at(source, clock, Rect::new(0, 0, 120, 30))
    }

    fn start_at(source: ScriptedSource, clock: impl ObservationClock, area: Rect) -> Self {
        let (mut boundary, input, actions) =
            ScriptedTerminalBoundary::new(area, [], BoundaryFailures::default());
        let (frame_sender, frames) = tokio::sync::mpsc::unbounded_channel();
        boundary.frames = Some(frame_sender);
        let host =
            RunnerWatchHost::start_with_boundary(source, overview(BOOT_A), clock, false, boundary)
                .unwrap();
        Self {
            host,
            input,
            frames,
            actions,
        }
    }

    fn press(&self, key: TerminalInputEvent) {
        self.input.send(ScriptedInput::Event(key)).unwrap();
    }

    /// The next frame `wanted` holds; a test fails with the last frame drawn when none comes
    /// within the host-scaled fixture wait rather than waiting for ever.
    async fn frame(&mut self, wanted: impl Fn(&str) -> bool) -> String {
        let last = std::cell::RefCell::new(String::new());
        let frames = &mut self.frames;
        host_scaled_watchdog_with_diagnosis(
            "a wanted runner watch frame",
            useful_test_support::watchdog::FIXTURE_WAIT_BASE,
            || Some(format!("the last frame drawn:\n{}", last.borrow())),
            async {
                loop {
                    let frame = frames.recv().await.expect("frame channel closed");
                    if wanted(&frame) {
                        return frame;
                    }
                    *last.borrow_mut() = frame;
                }
            },
        )
        .await
    }

    async fn quit(self) {
        self.press(TerminalInputEvent::Quit);
        assert_eq!(
            self.host.wait().await.unwrap(),
            ArchivedTerminalHostExit::Quit
        );
    }
}

/// A clock at which no time ever passes.
#[derive(Clone)]
struct StillClock(ObservationTime);

impl ObservationClock for StillClock {
    fn sample(&self) -> ObservationTime {
        self.0
    }
}

/// A screen and an interaction with `entry` opened from the overview, at `area`.
fn opened(entry: RunnerWatchAssignment, area: Rect) -> (RunnerWatchScreen, RunnerWatchInteraction) {
    let mut overview = overview(BOOT_A);
    overview.assignments[0] = entry;
    let mut screen = RunnerWatchScreen::new(overview, observed_at(0));
    let mut interaction = RunnerWatchInteraction::new(area);
    assert_eq!(
        interaction.handle_key(TerminalInputEvent::Enter, &mut screen),
        None
    );
    (screen, interaction)
}

fn open_assignment(interaction: &mut RunnerWatchInteraction) -> &mut OpenAssignment {
    match &mut interaction.surface {
        RunnerWatchSurface::Assignment(open) => open,
        RunnerWatchSurface::Overview => panic!("no assignment is open"),
    }
}

fn take_answer(
    screen: &mut RunnerWatchScreen,
    interaction: &mut RunnerWatchInteraction,
    query: RunnerWatchAssignmentQuery,
    answer: AssignmentResult,
) {
    let shown = screen.assignment.as_mut().expect("no assignment is shown");
    shown.take(open_assignment(interaction), query, answer.unwrap());
}

/// Selects the open run's step `id`.
fn select(interaction: &mut RunnerWatchInteraction, id: &str) {
    if let Some(level) = open_assignment(interaction).levels.last_mut() {
        level.selected = Some(id.to_owned());
    }
}

fn root_query(assignment_id: &str) -> RunnerWatchAssignmentQuery {
    RunnerWatchAssignmentQuery {
        assignment_id: assignment_id.to_owned(),
        path: Vec::new(),
        step: None,
    }
}

fn held(
    entry: RunnerWatchAssignment,
    run: Option<RunnerWatchRun>,
    log: Option<RunnerWatchLog>,
) -> AssignmentResult {
    Ok(RunnerWatchAssignmentAnswer::Held(Box::new(
        RunnerWatchHeldAssignment { entry, run, log },
    )))
}

fn run_log() -> RunnerWatchLog {
    RunnerWatchLog {
        source: RunnerWatchLogSource::RunLog,
        streams: vec![RunnerWatchLogStream {
            name: None,
            retained_bytes: None,
            discarded_bytes: None,
            omitted_bytes: 4_096,
            text: format!("{RUN_LOG_TEXT}\n"),
        }],
        records: Vec::new(),
    }
}

/// `plan`'s retained output: invocation 3, one earlier invocation not shown.
fn plan_output() -> RunnerWatchLog {
    let stream = |name: &str, text: &str| RunnerWatchLogStream {
        name: Some(name.to_owned()),
        retained_bytes: Some(u64::try_from(text.len()).unwrap()),
        discarded_bytes: Some(0),
        omitted_bytes: 0,
        text: text.to_owned(),
    };
    RunnerWatchLog {
        source: RunnerWatchLogSource::Step {
            step: "plan".to_owned(),
            invocation: Some(3),
            earlier_invocations_omitted: 1,
        },
        streams: vec![
            stream("stdout", &format!("{PLAN_OUTPUT_TEXT}\n")),
            stream("stderr", ""),
        ],
        records: Vec::new(),
    }
}

/// A step's records log naming `step`, with `omitted_records` older records of
/// `omitted_bytes` left out.
fn records_log(
    step: Option<&str>,
    omitted_records: u64,
    omitted_bytes: u64,
    records: Vec<RunnerWatchLogRecord>,
) -> RunnerWatchLog {
    RunnerWatchLog {
        source: RunnerWatchLogSource::StepRecords {
            step: step.map(str::to_owned),
            omitted_records,
            omitted_bytes,
        },
        streams: Vec::new(),
        records,
    }
}

fn record(source: &str, text: &str, continuation: bool) -> RunnerWatchLogRecord {
    RunnerWatchLogRecord {
        source: source.to_owned(),
        text: text.to_owned(),
        continuation,
    }
}

/// The read of the root run that names `step` of `asg_AUTO-751`.
fn step_query(step: &str) -> RunnerWatchAssignmentQuery {
    RunnerWatchAssignmentQuery {
        step: Some(step.to_owned()),
        ..root_query("asg_AUTO-751")
    }
}

/// `AUTO-751` opened at `area`, its five-step run given with no log and `plan` selected.
fn plan_selected(area: Rect) -> (RunnerWatchScreen, RunnerWatchInteraction) {
    let entry = assignment("AUTO-751");
    let (mut screen, mut interaction) = opened(entry.clone(), area);
    take_answer(
        &mut screen,
        &mut interaction,
        root_query(&entry.assignment_id),
        held(entry, Some(five_step_run()), None),
    );
    select(&mut interaction, "plan");
    (screen, interaction)
}

/// The heading and the rows' text of the document the log pane shows now.
fn shown_document_facts(
    screen: &RunnerWatchScreen,
    interaction: &mut RunnerWatchInteraction,
) -> Option<(String, Vec<String>)> {
    let shown = screen.assignment.as_ref()?;
    let document = open_assignment(interaction).shown_document(shown)?;
    Some((
        document.heading.clone(),
        document.rows.iter().map(|row| row.text.clone()).collect(),
    ))
}

fn shown_rows(
    screen: &RunnerWatchScreen,
    interaction: &mut RunnerWatchInteraction,
) -> Option<Vec<String>> {
    shown_document_facts(screen, interaction).map(|(_, rows)| rows)
}

/// The number of the first numbered record the rows draw.
fn first_record(rows: &[String]) -> Option<u64> {
    rows.iter()
        .flat_map(|row| row.split("record ").skip(1))
        .filter_map(|rest| rest.get(..3)?.parse().ok())
        .min()
}

/// The child run of `implement`: two command steps, one in flight.
fn child_run() -> RunnerWatchRun {
    let mut run = five_step_run();
    run.path = vec![RunnerWatchRunElement {
        step: "implement".to_owned(),
        item: None,
        ordinal: 1,
    }];
    run.workflow_path = "implement.yaml".to_owned();
    let mut draft = run.steps[0].clone();
    draft.id = "draft".to_owned();
    let mut polish = run.steps[0].clone();
    polish.id = "polish".to_owned();
    polish.state = "running".to_owned();
    run.steps = vec![draft, polish];
    run
}

/// Whether a frame draws an opened assignment, not the overview and its headings.
fn opened_frame(frame: &str) -> bool {
    !frame.contains("ASSIGNMENTS")
}

fn frame_rows(frame: &str) -> Vec<String> {
    frame.lines().map(str::to_owned).collect()
}

/// Whether a row holds every one of `words` as a word of its own.
fn rows_with(rows: &[String], words: &[&str]) -> bool {
    rows.iter().any(|row| {
        let row_words = row.split_whitespace().collect::<Vec<_>>();
        words.iter().all(|word| row_words.contains(word))
    })
}

/// `plan`'s retained output as numbered lines: standard output's lines `out`, the first 120
/// bytes and every line before the first left out, and `errors` lines of standard error
/// after 30 bytes left out.
fn numbered_output(out: std::ops::RangeInclusive<u64>, errors: u64) -> RunnerWatchLog {
    let stream = |name: &str, text: String, discarded: u64, omitted: u64| RunnerWatchLogStream {
        name: Some(name.to_owned()),
        retained_bytes: Some(u64::try_from(text.len()).unwrap()),
        discarded_bytes: Some(discarded),
        omitted_bytes: omitted,
        text,
    };
    let out_text = out
        .clone()
        .map(|line| format!("out line {line:02}\n"))
        .collect::<String>();
    let err_text = (1..=errors)
        .map(|line| format!("err line {line}\n"))
        .collect();
    RunnerWatchLog {
        source: RunnerWatchLogSource::Step {
            step: "plan".to_owned(),
            invocation: Some(3),
            earlier_invocations_omitted: 1,
        },
        streams: vec![
            stream("stdout", out_text, 9, 120 + (out.start() - 1) * 12),
            stream("stderr", err_text, 2, 30),
        ],
        records: Vec::new(),
    }
}

/// The number of the first numbered line of standard output a frame draws.
fn first_out_line(frame: &str) -> Option<u64> {
    frame
        .split("out line ")
        .skip(1)
        .filter_map(|rest| rest.get(..2)?.parse().ok())
        .min()
}

/// Whether a frame draws the facts row above an opened assignment's panes.
fn has_facts_row(frame: &str) -> bool {
    rows_with(&frame_rows(frame), &["attempt", "2"])
}

fn lowercase_rows(frame: &str) -> Vec<String> {
    frame.lines().map(str::to_lowercase).collect()
}
