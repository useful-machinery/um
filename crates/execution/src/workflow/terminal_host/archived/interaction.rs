use super::*;

#[derive(Default)]
pub(super) struct ArchivedHostInteraction {
    pub(super) selected: usize,
    pub(super) surface: HostSurface,
    pub(super) help_visible: bool,
    pub(super) terminal_area: Rect,
    pub(super) output: ArchivedOutputInteraction,
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
                let row_count = self.selected_document(view).len();
                self.output.navigate(row_count, navigation);
                return None;
            }
        }

        match event {
            TerminalInputEvent::Enter
                if self.surface == HostSurface::Split && !view.steps.is_empty() =>
            {
                self.surface = HostSurface::FullLog;
                self.output = ArchivedOutputInteraction::default();
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
                self.output.horizontal_offset = self.output.horizontal_offset.saturating_sub(1);
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
            step.document.len(),
            step.maximum_document_width,
            width,
            rows,
        );
    }

    pub(super) fn selected_document<'a>(
        &self,
        view: &'a ArchivedTerminalView,
    ) -> &'a [ArchivedOutputRow] {
        view.steps
            .get(self.selected)
            .map_or(&[], |step| step.document.as_slice())
    }
}

#[derive(Default)]
pub(super) struct ArchivedOutputInteraction {
    pub(super) top: usize,
    pub(super) horizontal_offset: usize,
    pub(super) available_width: usize,
    pub(super) available_rows: usize,
}

impl ArchivedOutputInteraction {
    pub(super) fn synchronize(
        &mut self,
        row_count: usize,
        document_width: usize,
        width: usize,
        rows: usize,
    ) {
        self.available_width = width;
        self.available_rows = rows;
        self.top = self.top.min(maximum_document_top(row_count, rows));
        self.horizontal_offset = self
            .horizontal_offset
            .min(document_width.saturating_sub(width));
    }

    pub(super) fn navigate(&mut self, row_count: usize, navigation: VerticalNavigation) {
        let bottom = maximum_document_top(row_count, self.available_rows);
        let page = self.available_rows.max(1);
        let half_page = (page / 2).max(1);
        self.top = match navigation {
            VerticalNavigation::Up => self.top.saturating_sub(1),
            VerticalNavigation::Down => self.top.saturating_add(1).min(bottom),
            VerticalNavigation::PageUp => self.top.saturating_sub(page),
            VerticalNavigation::PageDown => self.top.saturating_add(page).min(bottom),
            VerticalNavigation::HalfPageUp => self.top.saturating_sub(half_page),
            VerticalNavigation::HalfPageDown => self.top.saturating_add(half_page).min(bottom),
            VerticalNavigation::Top => 0,
            VerticalNavigation::Bottom => bottom,
        };
    }

    pub(super) fn pan_right(&mut self, document_width: usize) {
        self.horizontal_offset = self
            .horizontal_offset
            .saturating_add(1)
            .min(document_width.saturating_sub(self.available_width));
    }
}

pub(super) fn maximum_document_top(row_count: usize, available_rows: usize) -> usize {
    row_count.saturating_sub(available_rows)
}
