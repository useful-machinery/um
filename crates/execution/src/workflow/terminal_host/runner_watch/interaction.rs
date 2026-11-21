use super::assignment::{AssignmentScreen, OpenAssignment};
use super::*;

/// What the viewer shows: the overview, or an opened assignment (REQ-RUNNERENROLL-146).
pub(super) enum RunnerWatchSurface {
    Overview,
    Assignment(OpenAssignment),
}

/// What the viewer's keys have chosen: the surface, the selected assignment, the first one
/// in view, whether the help panel is open and the operation asked for that the loop has
/// not taken.
pub(super) struct RunnerWatchInteraction {
    pub(super) surface: RunnerWatchSurface,
    pub(super) selected: usize,
    pub(super) top: usize,
    pub(super) help_visible: bool,
    pub(super) terminal_area: Rect,
    /// The assignments the last frame had room for.
    pub(super) visible: usize,
    /// An operation a key asked for, which the loop starts unless one is in flight.
    pub(super) requested: Option<RunnerWatchOperation>,
}

impl RunnerWatchInteraction {
    pub(super) fn new(terminal_area: Rect) -> Self {
        Self {
            surface: RunnerWatchSurface::Overview,
            selected: 0,
            top: 0,
            help_visible: false,
            terminal_area,
            visible: 1,
            requested: None,
        }
    }

    pub(super) fn handle_key(
        &mut self,
        event: TerminalInputEvent,
        screen: &mut RunnerWatchScreen,
    ) -> Option<ArchivedTerminalHostExit> {
        match event {
            TerminalInputEvent::Quit => return Some(ArchivedTerminalHostExit::Quit),
            TerminalInputEvent::Cancel => return Some(ArchivedTerminalHostExit::Interrupted),
            _ => {}
        }
        // Below the minimum size only quitting and interrupting act (REQ-LOCALRUN-10).
        if !operational_area(self.terminal_area) {
            return None;
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
        if let RunnerWatchSurface::Assignment(open) = &mut self.surface {
            if let Some(shown) = &mut screen.assignment
                && open.handle_key(event, shown)
            {
                return None;
            }
            // Esc at the root returns to the overview with the assignment's entry selected.
            if event == TerminalInputEvent::Escape {
                if let Some(position) = screen
                    .overview
                    .assignments
                    .iter()
                    .position(|entry| entry.assignment_id == open.assignment_id)
                {
                    self.selected = position;
                }
                self.surface = RunnerWatchSurface::Overview;
                screen.assignment = None;
            }
            return None;
        }
        match event {
            TerminalInputEvent::Enter => {
                if let Some(entry) = screen.overview.assignments.get(self.selected) {
                    self.surface = RunnerWatchSurface::Assignment(OpenAssignment::new(
                        entry.assignment_id.clone(),
                    ));
                    screen.assignment = Some(AssignmentScreen::new(entry.clone()));
                }
            }
            TerminalInputEvent::Escape => {
                screen.restarted_at = None;
                if !screen.operation_in_flight() {
                    screen.notice = None;
                }
            }
            TerminalInputEvent::Character('C') => {
                self.requested = Some(RunnerWatchOperation::ReloadCredential);
            }
            TerminalInputEvent::Character('L') => {
                self.requested = Some(RunnerWatchOperation::SetUpAgentLogins);
            }
            _ => {
                if let Some(navigation) = vertical_navigation(event) {
                    self.navigate(navigation, screen.overview.assignments.len());
                }
            }
        }
        None
    }

    fn navigate(&mut self, navigation: VerticalNavigation, count: usize) {
        let last = count.saturating_sub(1);
        let page = self.visible.max(1);
        self.selected = match navigation {
            VerticalNavigation::Up => self.selected.saturating_sub(1),
            VerticalNavigation::Down => self.selected.saturating_add(1),
            VerticalNavigation::PageUp => self.selected.saturating_sub(page),
            VerticalNavigation::PageDown => self.selected.saturating_add(page),
            VerticalNavigation::HalfPageUp => self.selected.saturating_sub(page.div_ceil(2)),
            VerticalNavigation::HalfPageDown => self.selected.saturating_add(page.div_ceil(2)),
            VerticalNavigation::Top => 0,
            VerticalNavigation::Bottom => last,
        }
        .min(last);
    }

    /// Keeps the selection within `count` assignments and in a window of `visible` of them.
    pub(super) fn scroll(&mut self, count: usize, visible: usize) {
        self.visible = visible;
        self.selected = self.selected.min(count.saturating_sub(1));
        if self.selected < self.top {
            self.top = self.selected;
        } else if visible != 0 && self.selected >= self.top.saturating_add(visible) {
            self.top = self.selected.saturating_add(1).saturating_sub(visible);
        }
        self.top = self.top.min(count.saturating_sub(visible));
    }
}
