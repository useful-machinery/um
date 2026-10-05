use std::collections::BTreeMap;
use std::os::unix::ffi::OsStrExt as _;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use super::*;
use crate::workflow::archived_attempt::{
    ArchivedAttemptState, ArchivedAttemptTrigger, ArchivedCommandOutput, ArchivedExecution,
    ArchivedFailure,
};
use crate::workflow::document::Output;
use crate::workflow::evidence::{
    FailureCode, FailureDetail, FailurePhase, NodeDetail, PrimaryIssue,
};
use crate::workflow::presentation_feed::WorkflowPresentationDefinition;
use crate::workflow::resolution::{ContentDigestAlgorithm, WorkflowContentDigest};
use crate::workflow::validated::WorkflowNodeRole;

// The one scripted terminal boundary renders both live and archived presentations.
impl ArchivedTerminalBoundary for ScriptedTerminalBoundary {
    fn draw_archived(
        &mut self,
        _view: &ArchivedTerminalView,
        interaction: &mut ArchivedHostInteraction,
        _color: bool,
    ) -> io::Result<()> {
        self.draw_count = self.draw_count.saturating_add(1);
        self.record(BoundaryAction::Draw(interaction.terminal_area));
        if self.failures.panic_at == Some(self.draw_count) {
            std::panic::panic_any("injected archived widget panic");
        }
        if self.failures.draw_at == Some(self.draw_count) {
            Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "injected draw failure",
            ))
        } else {
            Ok(())
        }
    }
}

#[test]
fn frozen_archive_renders_context_dag_inspector_and_declarations() {
    let view = ArchivedTerminalView::new(archived_attempt(Some(hostile_output())));
    let graph = DagLayout::for_steps(&view.steps);
    let mut interaction = ArchivedHostInteraction::default();
    let buffer = render_view(&view, &graph, &mut interaction, 300, 40);
    let rendered = buffer_text(&buffer);

    for expected in [
        "run /tmp/archive-run",
        "attempt 2 of 3 · historical · explicit retry",
        "workflow workflows/archive.yaml · 2 steps · concurrency 2",
        "attempt state workflow_failed · outcome failed",
        "result /tmp/archive-run/attempts/0002/r",
        "created 2026-08-06 12:00:00Z",
        "execution 2026-08-06 12:00:01Z → 2026-08-06 12:00:04Z · 3.0s",
        "primary issue step verify · Failed · execution · command_exit · exit 17",
        "▏ ✓ prepare",
        "× verify",
        "prepare   cmd",
        "succeeded · required · 1.0s",
        "command",
        "printf payload",
        "report",
        "file",
        "1 output committed",
    ] {
        assert!(
            rendered.contains(expected),
            "missing {expected:?}: {rendered:?}"
        );
    }
    assert!(!rendered.contains("captured"));
    assert!(!rendered.contains("pending"));
    assert!(!rendered.contains("unavailable"));
    assert!(!rendered.contains('\u{1b}'));

    assert_eq!(
        interaction.handle_key(TerminalInputEvent::Down, &view),
        None
    );
    let selected = buffer_text(&render_view(&view, &graph, &mut interaction, 300, 40));
    assert!(selected.contains("▏ × verify"));
    assert!(
        selected.contains("failure       execution · command_exit · exit 17"),
        "missing selected failure: {selected:?}"
    );
}

#[test]
fn hostile_node_evidence_is_safe_in_live_and_archived_presentations() {
    let detail = crate::workflow::evidence::BlockedDetail::new([
        crate::workflow::evidence::Prerequisite::control("before\u{1b}]0;hostile\u{7}after")
            .unwrap(),
    ])
    .unwrap();
    // The live plain renderer uses this canonical detail in its blocked transition.
    let live_plain_detail = crate::workflow::presentation::canonical_blocked_detail(&detail);
    let mut attempt = archived_attempt(None);
    attempt.steps[1].state = ArchivedStepState::Blocked;
    attempt.steps[1].detail = ArchivedStepDetail::Evidence(NodeDetail::Blocked(detail));
    let plain = crate::workflow::archived_presentation::render_plain(&attempt, false).unwrap();
    let view = ArchivedTerminalView::new(attempt);
    let graph = DagLayout::for_steps(&view.steps);
    let mut interaction = ArchivedHostInteraction {
        selected: 1,
        terminal_area: Rect::new(0, 0, 140, 40),
        ..ArchivedHostInteraction::default()
    };
    let tui = buffer_text(&render_view(&view, &graph, &mut interaction, 140, 40));
    assert!(live_plain_detail.contains("before\\x1b]0;hostile\\x07after"));
    for rendered in [&plain, &tui] {
        assert!(rendered.contains("beforeafter"), "{rendered:?}");
        assert!(!rendered.contains("hostile"), "{rendered:?}");
    }
    for rendered in [&live_plain_detail, &plain, &tui] {
        assert!(!rendered.contains('\u{1b}'), "{rendered:?}");
    }
}

#[test]
fn retained_prefixes_are_independent_safe_documents_with_exact_facts() {
    let view = ArchivedTerminalView::new(archived_attempt(Some(hostile_output())));
    let step = &view.steps[0];
    let document = &step.document;
    let text = document
        .iter()
        .map(|row| row.text.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    let stdout = text.find("RETAINED STDOUT PREFIX").unwrap();
    let stderr = text.find("RETAINED STDERR PREFIX").unwrap();

    assert!(stdout < stderr);
    assert!(text.contains("cross-stream order is unavailable"));
    assert!(text.contains("retained 24 B · discarded 0 B · truncated no · fully drained yes"));
    assert!(text.contains(&format!(
        "retained {} B · discarded 9 B · truncated yes · fully drained no (incomplete drain)",
        super::super::super::MAXIMUM_RETAINED_BYTES_PER_STREAM
    )));
    assert!(text.contains("stdout red\\xff  end"));
    assert!(text.contains("stderr\\x00warning"));
    assert!(text.contains("retained-prefix boundary"));
    assert!(!text.contains('\u{1b}'));
    assert_eq!(
        safe_text("left\u{1b}]0;hostile title\u{7}right\n"),
        "leftright\\x0a"
    );
    assert_eq!(
        safe_path(std::path::Path::new(std::ffi::OsStr::from_bytes(
            b"/tmp/\xff\x1b]0;title\x07safe",
        ))),
        "/tmp/\\xffsafe"
    );
    for prohibited in [
        "following",
        "paused",
        "observed",
        "discarded records",
        "12:00:",
    ] {
        assert!(
            !text.contains(prohibited),
            "invented archive fact {prohibited:?}"
        );
    }

    let graph = DagLayout::for_steps(&view.steps);
    let mut interaction = ArchivedHostInteraction {
        terminal_area: Rect::new(0, 0, 120, 30),
        ..ArchivedHostInteraction::default()
    };
    interaction.handle_key(TerminalInputEvent::Enter, &view);
    let full = buffer_text(&render_view(&view, &graph, &mut interaction, 120, 30));
    assert!(full.contains("cross-stream order is unavailable"));
    assert!(full.contains("RETAINED STDOUT PREFIX"));
    assert!(!full.contains("F follow"));
}

#[test]
fn empty_and_missing_command_output_remain_distinct() {
    let empty = ArchivedCommandOutput {
        stdout: stream(Vec::new(), 0, true),
        stderr: stream(Vec::new(), 0, true),
    };
    let empty_view = ArchivedTerminalView::new(archived_attempt(Some(empty)));
    let empty_document = &empty_view.steps[0].document;
    let empty_text = empty_document
        .iter()
        .map(|row| row.text.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(empty_text.contains("RETAINED STDOUT PREFIX"));
    assert!(empty_text.contains("retained 0 B · discarded 0 B · truncated no · fully drained yes"));
    assert_eq!(empty_text.matches("empty retained prefix").count(), 2);

    let missing_view = ArchivedTerminalView::new(archived_attempt(None));
    let missing = &missing_view.steps[0].document;
    assert_eq!(missing.len(), 1);
    assert!(
        missing[0]
            .text
            .contains("No durable command-stream prefixes exist")
    );
    assert!(!missing[0].text.contains("STDOUT"));
    assert!(!missing[0].text.contains("0 B"));
}

#[test]
fn archived_command_arguments_keep_scalar_normalization_unambiguous() {
    let mut attempt = archived_attempt(None);
    let original = vec!["line\nbreak".to_owned(), r"line\x0abreak".to_owned()];
    let WorkflowPresentationStep::Command { argv, .. } =
        attempt.workflow.steps.get_mut("prepare").unwrap()
    else {
        panic!("prepare must remain a command step");
    };
    *argv = original.clone();
    let view = ArchivedTerminalView::new(attempt);
    let fields = inspector_fields(&view.steps[0], 200, 5);
    let command = fields
        .iter()
        .find(|field| field.label == "command")
        .unwrap();
    let expected = original
        .iter()
        .map(|argument| shell_quote(argument))
        .collect::<Vec<_>>()
        .join(" ");

    assert_eq!(command.value, expected);
}

#[test]
fn archived_navigation_help_and_resize_preserve_static_viewport() {
    let view = ArchivedTerminalView::new(archived_attempt(Some(multiline_output(80))));
    let graph = DagLayout::for_steps(&view.steps);
    let mut interaction = ArchivedHostInteraction {
        terminal_area: Rect::new(0, 0, 120, 30),
        ..ArchivedHostInteraction::default()
    };
    interaction.handle_key(TerminalInputEvent::Enter, &view);
    interaction.handle_key(TerminalInputEvent::PageDown, &view);
    interaction.handle_key(TerminalInputEvent::PanRight, &view);
    let top = interaction.output.top;
    let horizontal_offset = interaction.output.horizontal_offset;
    assert!(top > 0);
    assert!(horizontal_offset > 0);

    interaction.handle_key(TerminalInputEvent::Help, &view);
    let help = buffer_text(&render_view(&view, &graph, &mut interaction, 120, 30));
    for expected in [
        "? — all commands",
        "MOVE",
        "JUMP",
        "PgDn/f/Space",
        "top",
        "bottom",
        "VIEWER",
        "interrupt",
    ] {
        assert!(help.contains(expected), "missing {expected:?}");
    }
    assert!(!help.contains("follow latest"));
    assert!(!help.contains("FILTER"));

    let too_small = buffer_text(&render_view(&view, &graph, &mut interaction, 40, 8));
    assert!(too_small.contains("Terminal too small"));
    assert_eq!(interaction.output.top, top);
    assert_eq!(interaction.output.horizontal_offset, horizontal_offset);
    assert!(interaction.help_visible);

    let recovered = buffer_text(&render_view(&view, &graph, &mut interaction, 120, 30));
    assert!(recovered.contains("? — all commands"));
    assert_eq!(interaction.output.top, top);
    assert_eq!(interaction.output.horizontal_offset, horizontal_offset);
    assert_eq!(interaction.selected, 0);
    assert_eq!(interaction.surface, HostSurface::FullLog);
}

#[test]
fn archived_help_requested_while_too_small_opens_after_resize() {
    let view = ArchivedTerminalView::new(archived_attempt(None));
    let graph = DagLayout::for_steps(&view.steps);
    let mut interaction = ArchivedHostInteraction {
        terminal_area: Rect::new(0, 0, 40, 8),
        ..ArchivedHostInteraction::default()
    };

    assert_eq!(
        interaction.handle_key(TerminalInputEvent::Help, &view),
        None
    );
    assert!(
        interaction.help_visible,
        "a help request made in the too-small view must be retained"
    );
    let resized = buffer_text(&render_view(&view, &graph, &mut interaction, 120, 30));
    assert!(resized.contains("? — all commands"));
}

#[tokio::test]
async fn archived_host_quit_interrupt_and_termination_restore_without_a_summary() {
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
        let (host, sender, mut actions) = start_scripted_archive_host(BoundaryFailures::default());
        wait_for_action(&mut actions, BoundaryAction::Draw(Rect::new(0, 0, 100, 30))).await;
        let result = if let Some(input) = input {
            sender.send(ScriptedInput::Event(input)).unwrap();
            host.wait().await.unwrap()
        } else {
            let request = host.exit_request();
            request.request(ArchivedTerminalHostExit::Terminated);
            host.wait().await.unwrap()
        };
        assert_eq!(result, expected);
        wait_for_action(&mut actions, BoundaryAction::Restore).await;
        assert!(actions.try_recv().is_err());
    }
}

#[expect(
    clippy::disallowed_methods,
    reason = "real time only bounds a regression that leaves the terminal task detached"
)]
#[tokio::test]
async fn dropping_archived_host_restores_the_terminal() {
    let (host, _sender, mut actions) = start_scripted_archive_host(BoundaryFailures::default());
    wait_for_action(&mut actions, BoundaryAction::Draw(Rect::new(0, 0, 100, 30))).await;
    let cleanup = host.exit_request();

    drop(host);
    let restored = tokio::time::timeout(
        Duration::from_secs(1),
        wait_for_action(&mut actions, BoundaryAction::Restore),
    )
    .await;
    if restored.is_err() {
        cleanup.request(ArchivedTerminalHostExit::Terminated);
        wait_for_action(&mut actions, BoundaryAction::Restore).await;
    }
    assert!(
        restored.is_ok(),
        "dropping an active archived host must restore its terminal"
    );
}

#[tokio::test]
async fn archived_host_resize_uses_boundary_area_and_redraws() {
    let initial = Rect::new(0, 0, 100, 30);
    let resized = Rect::new(0, 0, 80, 24);
    let (mut boundary, sender, mut actions) =
        ScriptedTerminalBoundary::new(initial, [], BoundaryFailures::default());
    boundary.resize_areas.push_back(resized);
    let host =
        ArchivedWorkflowTerminalHost::start_with_boundary(archived_attempt(None), false, boundary)
            .unwrap();
    wait_for_action(&mut actions, BoundaryAction::Draw(initial)).await;

    sender
        .send(ScriptedInput::Event(TerminalInputEvent::Resize))
        .unwrap();
    wait_for_action(&mut actions, BoundaryAction::Draw(resized)).await;
    sender
        .send(ScriptedInput::Event(TerminalInputEvent::Quit))
        .unwrap();

    assert_eq!(host.wait().await.unwrap(), ArchivedTerminalHostExit::Quit);
    wait_for_action(&mut actions, BoundaryAction::Restore).await;
}

#[tokio::test]
async fn archived_widget_panics_restore_and_carry_diagnostics() {
    let (boundary, _sender, mut actions) = ScriptedTerminalBoundary::new(
        Rect::new(0, 0, 100, 30),
        [],
        BoundaryFailures {
            panic_at: Some(1),
            ..BoundaryFailures::default()
        },
    );
    let failure =
        ArchivedWorkflowTerminalHost::start_with_boundary(archived_attempt(None), false, boundary)
            .err()
            .unwrap();
    assert_eq!(
        failure.panic_message.as_deref(),
        Some("injected archived widget panic")
    );
    wait_for_action(&mut actions, BoundaryAction::Restore).await;

    let (host, sender, mut actions) = start_scripted_archive_host(BoundaryFailures {
        panic_at: Some(2),
        ..BoundaryFailures::default()
    });
    sender
        .send(ScriptedInput::Event(TerminalInputEvent::Other))
        .unwrap();
    let failure = host.wait().await.unwrap_err();
    assert_eq!(
        failure.panic_message.as_deref(),
        Some("injected archived widget panic")
    );
    wait_for_action(&mut actions, BoundaryAction::Restore).await;
}

#[tokio::test]
async fn archived_host_failures_restore_and_preserve_failure_precedence() {
    for (failures, input, expected_operation) in [
        (
            BoundaryFailures {
                draw_at: Some(2),
                ..BoundaryFailures::default()
            },
            ScriptedInput::Event(TerminalInputEvent::Other),
            PresentationFailureOperation::TerminalDraw,
        ),
        (
            BoundaryFailures::default(),
            ScriptedInput::Failure,
            PresentationFailureOperation::TerminalInput,
        ),
    ] {
        let (host, sender, mut actions) = start_scripted_archive_host(failures);
        sender.send(input).unwrap();
        let failure = host.wait().await.unwrap_err();
        assert_eq!(failure.operation, expected_operation);
        wait_for_action(&mut actions, BoundaryAction::Restore).await;
    }

    for (failures, expected_operation) in [
        (
            BoundaryFailures {
                setup: true,
                ..BoundaryFailures::default()
            },
            PresentationFailureOperation::TerminalSetup,
        ),
        (
            BoundaryFailures {
                draw_at: Some(1),
                ..BoundaryFailures::default()
            },
            PresentationFailureOperation::TerminalDraw,
        ),
    ] {
        let (boundary, _sender, mut actions) =
            ScriptedTerminalBoundary::new(Rect::new(0, 0, 100, 30), [], failures);
        let failure = ArchivedWorkflowTerminalHost::start_with_boundary(
            archived_attempt(None),
            false,
            boundary,
        )
        .err()
        .unwrap();
        assert_eq!(failure.operation, expected_operation);
        wait_for_action(&mut actions, BoundaryAction::Restore).await;
    }

    let (host, sender, mut actions) = start_scripted_archive_host(BoundaryFailures::default());
    sender.send(ScriptedInput::Panic).unwrap();
    let failure = host.wait().await.unwrap_err();
    assert_eq!(
        failure.operation,
        PresentationFailureOperation::TerminalTask
    );
    wait_for_action(&mut actions, BoundaryAction::Restore).await;

    let (host, sender, mut actions) = start_scripted_archive_host(BoundaryFailures {
        restore: true,
        ..BoundaryFailures::default()
    });
    sender
        .send(ScriptedInput::Event(TerminalInputEvent::Quit))
        .unwrap();
    let failure = host.wait().await.unwrap_err();
    assert_eq!(
        failure.operation,
        PresentationFailureOperation::TerminalRestore
    );
    wait_for_action(&mut actions, BoundaryAction::Restore).await;
}

fn archived_attempt(command_output: Option<ArchivedCommandOutput>) -> LocalArchivedAttempt {
    let started = timestamp("2026-08-06T12:00:01Z");
    let failure: ArchivedFailure = FailureDetail::new(
        FailurePhase::Execution,
        FailureCode::CommandExit,
        None,
        None,
        None,
        Some(17),
    )
    .unwrap();
    let prepare_definition = WorkflowPresentationStep::Command {
        argv: vec![
            "printf".to_owned(),
            "\u{1b}]0;hostile title\u{7}payload".to_owned(),
        ],
        cwd: Some("work".to_owned()),
        failure_policy: FailurePolicy::Required,
        direct_dependencies: Vec::new(),
        outputs: BTreeMap::from([(
            "report".to_owned(),
            Output::FilePath {
                path: "report.txt".to_owned(),
                media_type: "text/plain".to_owned(),
            },
        )]),
    };
    let verify_definition = WorkflowPresentationStep::Command {
        argv: vec!["verify".to_owned()],
        cwd: None,
        failure_policy: FailurePolicy::Required,
        direct_dependencies: vec!["prepare".to_owned()],
        outputs: BTreeMap::new(),
    };
    LocalArchivedAttempt {
        run_directory: PathBuf::from("/tmp/archive-run"),
        current_attempt_number: 3,
        attempt_number: 2,
        prior_attempt_number: Some(1),
        continuation: None,
        workspace_modified: crate::workflow::publication::WorkspaceModifiedV1::Unknown(
            crate::workflow::publication::WorkspaceModifiedUnknownV1::Unknown,
        ),
        result_directory: PathBuf::from("/tmp/archive-run/attempts/0002/result"),
        trigger: ArchivedAttemptTrigger::ExplicitRetry,
        state: ArchivedAttemptState::WorkflowFailed,
        created_at: timestamp("2026-08-06T12:00:00Z"),
        started_at: Some(started),
        settled_at: timestamp("2026-08-06T12:00:05Z"),
        workflow_path: "workflows/archive.yaml".to_owned(),
        source_root: PathBuf::from("/tmp/source"),
        workflow_digest: WorkflowContentDigest {
            algorithm: ContentDigestAlgorithm::Sha256,
            value: "a".repeat(64),
        },
        workflow: WorkflowPresentationDefinition {
            workflow_path: "workflows/archive.yaml".to_owned(),
            presentation_order: vec!["prepare".to_owned(), "verify".to_owned()],
            finalization_start: None,
            steps: BTreeMap::from([
                ("prepare".to_owned(), prepare_definition),
                ("verify".to_owned(), verify_definition),
            ]),
            node_roles: BTreeMap::from([
                ("prepare".to_owned(), WorkflowNodeRole::Step),
                ("verify".to_owned(), WorkflowNodeRole::Step),
            ]),
        },
        execution: ArchivedExecution {
            execution_root: PathBuf::from("/tmp/execution"),
            maximum_parallel_steps: 2,
            started_at: started,
            finished_at: timestamp("2026-08-06T12:00:04Z"),
            duration: Duration::from_secs(3),
        },
        outcome: ArchivedWorkflowOutcome::Failed,
        primary_issue: Some(PrimaryIssue::failed(
            crate::workflow::validated::WorkflowNode {
                id: "verify".to_owned(),
                role: WorkflowNodeRole::Step,
            },
            failure.clone(),
        )),
        cancellation: None,
        force_abort: None,
        finalization: None,
        steps: vec![
            ArchivedStep {
                id: "prepare".to_owned(),
                role: WorkflowNodeRole::Step,
                failure_policy: FailurePolicy::Required,
                state: ArchivedStepState::Succeeded,
                inherited_data_available: false,
                started_at: Some(started),
                duration: Some(Duration::from_secs(1)),
                detail: ArchivedStepDetail::Succeeded,
                command_output,
                recovery: None,
                invocations: Vec::new(),
            },
            ArchivedStep {
                id: "verify".to_owned(),
                role: WorkflowNodeRole::Step,
                failure_policy: FailurePolicy::Required,
                state: ArchivedStepState::Failed,
                inherited_data_available: false,
                started_at: Some(started + Duration::from_secs(1)),
                duration: Some(Duration::from_secs(2)),
                detail: ArchivedStepDetail::Evidence(NodeDetail::Failed(failure)),
                command_output: None,
                recovery: None,
                invocations: Vec::new(),
            },
        ],
    }
}

fn hostile_output() -> ArchivedCommandOutput {
    let stdout = b"stdout \x1b[31mred\x1b[0m\xff\tend".to_vec();
    assert_eq!(stdout.len(), 24);
    let mut stderr = b"stderr\0\x1b]0;title\x07warning\n".to_vec();
    stderr.resize(
        usize::try_from(super::super::super::MAXIMUM_RETAINED_BYTES_PER_STREAM).unwrap(),
        b'x',
    );
    ArchivedCommandOutput {
        stdout: stream(stdout, 0, true),
        stderr: stream(stderr, 9, false),
    }
}

fn multiline_output(lines: usize) -> ArchivedCommandOutput {
    let stdout = (0..lines)
        .map(|index| format!("line-{index:03}-{}\n", "x".repeat(160)))
        .collect::<String>()
        .into_bytes();
    ArchivedCommandOutput {
        stdout: stream(stdout, 0, true),
        stderr: stream(b"stderr\n".to_vec(), 0, true),
    }
}

fn stream(bytes: Vec<u8>, discarded_bytes: u64, fully_drained: bool) -> ArchivedDiagnosticStream {
    ArchivedDiagnosticStream {
        retained_bytes: u64::try_from(bytes.len()).unwrap(),
        bytes: Arc::from(bytes),
        discarded_bytes,
        truncated: discarded_bytes != 0,
        fully_drained,
    }
}

fn timestamp(value: &str) -> time::OffsetDateTime {
    time::OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339).unwrap()
}

fn render_view(
    view: &ArchivedTerminalView,
    graph: &DagLayout,
    interaction: &mut ArchivedHostInteraction,
    width: u16,
    height: u16,
) -> ratatui::buffer::Buffer {
    let backend = ratatui::backend::TestBackend::new(width, height);
    let mut terminal = Terminal::new(backend).unwrap();
    terminal
        .draw(|frame| render_archived(frame, view, graph, interaction, false))
        .unwrap();
    terminal.backend().buffer().clone()
}

fn start_scripted_archive_host(
    failures: BoundaryFailures,
) -> (
    ArchivedWorkflowTerminalHost,
    tokio::sync::mpsc::UnboundedSender<ScriptedInput>,
    tokio::sync::mpsc::UnboundedReceiver<BoundaryAction>,
) {
    let (boundary, sender, actions) =
        ScriptedTerminalBoundary::new(Rect::new(0, 0, 100, 30), [], failures);
    let host =
        ArchivedWorkflowTerminalHost::start_with_boundary(archived_attempt(None), false, boundary)
            .unwrap();
    (host, sender, actions)
}
