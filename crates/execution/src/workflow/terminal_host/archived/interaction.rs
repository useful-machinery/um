use super::*;

#[derive(Default)]
pub(super) struct ArchivedHostInteraction {
    pub(super) selected: usize,
    pub(super) surface: HostSurface,
    pub(super) help_visible: bool,
    pub(super) terminal_area: Rect,
    /// The retained output's viewport, its rows keyed by their index and never followed.
    pub(super) output: OutputViewport<usize>,
}

impl ArchivedHostInteraction {
    pub(super) fn handle_key(
        &mut self,
        event: TerminalInputEvent,
        view: &ArchivedTerminalView,
    ) -> Option<ArchivedTerminalHostExit> {
        clamp_step_selection(&mut self.selected, view.steps.len());
        if event == TerminalInputEvent::Quit {
            return Some(ArchivedTerminalHostExit::Quit);
        }
        if event == TerminalInputEvent::Cancel {
            return Some(ArchivedTerminalHostExit::Interrupted);
        }
        if self.help_visible {
            if event == TerminalInputEvent::Escape {
                self.help_visible = false;
            }
            return None;
        }
        if event == TerminalInputEvent::Help {
            self.help_visible = true;
            return None;
        }
        if !operational_area(self.terminal_area) {
            return None;
        }

        if self.surface == HostSurface::FullLog {
            self.synchronize_output(view);
            if let Some(navigation) = vertical_navigation(event) {
                self.output
                    .navigate(self.selected_document(view), navigation);
                return None;
            }
        }

        match event {
            TerminalInputEvent::Enter
                if self.surface == HostSurface::Split && !view.steps.is_empty() =>
            {
                self.surface = HostSurface::FullLog;
                self.output = OutputViewport::default();
                self.synchronize_output(view);
            }
            TerminalInputEvent::Escape => self.surface = HostSurface::Split,
            TerminalInputEvent::Up if self.surface == HostSurface::Split => {
                self.selected = self.selected.saturating_sub(1);
            }
            TerminalInputEvent::Down if self.surface == HostSurface::Split => {
                if self.selected.saturating_add(1) < view.steps.len() {
                    self.selected += 1;
                }
            }
            TerminalInputEvent::PanLeft if self.surface == HostSurface::FullLog => {
                self.output.pan_left();
            }
            TerminalInputEvent::PanRight if self.surface == HostSurface::FullLog => {
                let width = view
                    .steps
                    .get(self.selected)
                    .map_or(0, |step| step.maximum_document_width);
                self.output.pan_right(width);
            }
            _ => {}
        }
        None
    }

    pub(super) fn synchronize_output(&mut self, view: &ArchivedTerminalView) {
        let Some(step) = view.steps.get(self.selected) else {
            return;
        };
        let (width, rows) = archived_output_dimensions(self.terminal_area, step);
        self.output.synchronize(
            step.document.as_slice(),
            step.maximum_document_width,
            width,
            rows,
        );
    }

    pub(super) fn selected_document<'a>(&self, view: &'a ArchivedTerminalView) -> &'a [OutputRow] {
        view.steps
            .get(self.selected)
            .map_or(&[], |step| step.document.as_slice())
    }
}
