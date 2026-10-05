use super::*;

pub(super) fn render_workflow_summary(
    frame: &mut Frame<'_>,
    area: Rect,
    snapshot: &WorkflowRunViewSnapshot,
    color: bool,
    borders: Borders,
) {
    let block = summary_block(borders, color);
    let content = block.inner(area);
    frame.render_widget(block, area);

    let duration = human_duration(snapshot.timing.duration);
    let (status, status_tone) = workflow_header_status(snapshot);
    let status_width = display_width(status)
        .saturating_add(2)
        .saturating_add(display_width(&duration));
    let status_width = status_width.min(usize::from(content.width));
    let title_width = usize::from(content.width)
        .saturating_sub(status_width)
        .saturating_sub(2);
    let title = ellipsize(&workflow_display_name(&snapshot.workflow_path), title_width);

    if !title.is_empty() {
        frame.render_widget(
            Paragraph::new(Span::styled(
                title,
                tone_style(color, Tone::Primary).add_modifier(Modifier::BOLD),
            )),
            Rect::new(
                content.x,
                content.y,
                u16::try_from(title_width).unwrap_or(u16::MAX),
                1,
            ),
        );
    }

    let status_width = u16::try_from(status_width).unwrap_or(content.width);
    frame.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled(status, tone_style(color, status_tone)),
            Span::raw("  "),
            Span::styled(duration, tone_style(color, Tone::Muted)),
        ])),
        Rect::new(
            content.right().saturating_sub(status_width),
            content.y,
            status_width,
            1,
        ),
    );

    let counts = step_count_summary(&step_counts(snapshot), snapshot.steps.len());
    frame.render_widget(
        Paragraph::new(Span::styled(
            ellipsize(&counts, usize::from(content.width)),
            tone_style(color, Tone::Muted),
        )),
        Rect::new(content.x, content.y.saturating_add(1), content.width, 1),
    );
}

pub(super) fn summary_block(borders: Borders, color: bool) -> Block<'static> {
    Block::default()
        .borders(borders)
        .border_style(separator_style(color))
        .padding(Padding::horizontal(2))
}

pub(super) fn workflow_display_name(workflow_path: &str) -> String {
    std::path::Path::new(workflow_path)
        .file_stem()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .map(visible_text)
        .unwrap_or_else(|| visible_text(workflow_path))
}

// Both live and archived steps expose the same read-only identity and timing to widgets.
pub(super) struct StepHeader<'a> {
    pub(super) id: &'a str,
    pub(super) definition: &'a WorkflowPresentationStep,
    pub(super) state: StepStateKind,
    pub(super) timing: Option<&'a crate::workflow::run_view_model::WorkflowRunElapsed>,
}

impl<'a> StepHeader<'a> {
    pub(super) fn new(
        id: &'a str,
        definition: &'a WorkflowPresentationStep,
        state: StepStateKind,
        timing: Option<&'a crate::workflow::run_view_model::WorkflowRunElapsed>,
    ) -> Self {
        Self {
            id,
            definition,
            state,
            timing,
        }
    }
}

pub(super) trait StepProjection {
    fn header(&self) -> StepHeader<'_>;

    fn id(&self) -> &str {
        self.header().id
    }

    fn definition(&self) -> &WorkflowPresentationStep {
        self.header().definition
    }

    fn state(&self) -> StepStateKind {
        self.header().state
    }

    fn timing(&self) -> Option<&crate::workflow::run_view_model::WorkflowRunElapsed> {
        self.header().timing
    }

    fn dag_detail(&self) -> Option<String>;

    fn inspector_command(&self) -> Option<String>;

    fn inspector_fact(&self) -> Option<InspectorField>;

    fn inspector_outputs(&self) -> Vec<InspectorOutput>;

    fn show_empty_outputs(&self) -> bool;
}

#[derive(Clone, Copy)]
pub(super) struct StepPanel {
    pub(super) borders: Borders,
    pub(super) show_title: bool,
    pub(super) phase_boundary: Option<StepPhaseBoundary>,
}

#[derive(Clone, Copy)]
pub(super) struct StepPhaseBoundary {
    pub(super) finalization_start: usize,
    pub(super) trigger: Option<&'static str>,
}

pub(super) fn live_step_phase_boundary(
    snapshot: &WorkflowRunViewSnapshot,
) -> Option<StepPhaseBoundary> {
    let finalization_start = snapshot.finalization_start?;
    let trigger = snapshot
        .finalization
        .as_ref()
        .map(|finalization| finalization.trigger)
        .or(match &snapshot.workflow {
            WorkflowState::Finalizing { trigger, .. } => Some(*trigger),
            WorkflowState::Executing { .. }
            | WorkflowState::Succeeded
            | WorkflowState::Failed { .. }
            | WorkflowState::Cancelled { .. } => None,
        })
        .map(finalization_trigger);
    Some(StepPhaseBoundary {
        finalization_start,
        trigger,
    })
}
