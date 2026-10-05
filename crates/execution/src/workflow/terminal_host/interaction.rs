use super::*;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) enum HostSurface {
    #[default]
    Split,
    FullLog,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum LogChannel {
    StandardOutput,
    StandardError,
    Agent,
    Reasoning,
    Tools,
    System,
}

impl LogChannel {
    pub(super) const fn bit(self) -> u8 {
        match self {
            Self::StandardOutput => 1 << 0,
            Self::StandardError => 1 << 1,
            Self::Agent => 1 << 2,
            Self::Reasoning => 1 << 3,
            Self::Tools => 1 << 4,
            Self::System => 1 << 5,
        }
    }

    pub(super) const fn for_source(source: WorkflowRunLogSource) -> Self {
        match source {
            WorkflowRunLogSource::Command(CommandOutputSource::StandardOutput) => {
                Self::StandardOutput
            }
            WorkflowRunLogSource::Command(CommandOutputSource::StandardError) => {
                Self::StandardError
            }
            WorkflowRunLogSource::Agent(AgentPresentationObservationKind::Assistant) => Self::Agent,
            WorkflowRunLogSource::Agent(AgentPresentationObservationKind::Reasoning) => {
                Self::Reasoning
            }
            WorkflowRunLogSource::Agent(
                AgentPresentationObservationKind::ToolCall
                | AgentPresentationObservationKind::ToolResult,
            ) => Self::Tools,
            WorkflowRunLogSource::Agent(
                AgentPresentationObservationKind::Diagnostic
                | AgentPresentationObservationKind::Usage
                | AgentPresentationObservationKind::Model
                | AgentPresentationObservationKind::Lifecycle
                | AgentPresentationObservationKind::ValueRejected
                | AgentPresentationObservationKind::HarnessEvent,
            ) => Self::System,
        }
    }
}

#[derive(Clone, Copy)]
pub(super) struct LogChannelOption {
    pub(super) key: char,
    pub(super) channel: LogChannel,
    pub(super) label: &'static str,
    pub(super) compact_label: &'static str,
}

pub(super) const COMMAND_LOG_CHANNELS: [LogChannelOption; 2] = [
    LogChannelOption {
        key: '1',
        channel: LogChannel::StandardOutput,
        label: "stdout",
        compact_label: "out",
    },
    LogChannelOption {
        key: '2',
        channel: LogChannel::StandardError,
        label: "stderr",
        compact_label: "err",
    },
];

pub(super) const AGENT_LOG_CHANNELS: [LogChannelOption; 4] = [
    LogChannelOption {
        key: '1',
        channel: LogChannel::Agent,
        label: "agent",
        compact_label: "agt",
    },
    LogChannelOption {
        key: '2',
        channel: LogChannel::Reasoning,
        label: "reasoning",
        compact_label: "rsn",
    },
    LogChannelOption {
        key: '3',
        channel: LogChannel::Tools,
        label: "tools",
        compact_label: "tool",
    },
    LogChannelOption {
        key: '4',
        channel: LogChannel::System,
        label: "system",
        compact_label: "sys",
    },
];

pub(super) fn log_channel_options(step: &WorkflowRunStepView) -> &'static [LogChannelOption] {
    match &step.definition {
        WorkflowPresentationStep::Command { .. } => &COMMAND_LOG_CHANNELS,
        WorkflowPresentationStep::Agent { .. } => &AGENT_LOG_CHANNELS,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct LogFilterState {
    pub(super) enabled: u8,
}

impl Default for LogFilterState {
    fn default() -> Self {
        Self {
            enabled: LogChannel::StandardOutput.bit()
                | LogChannel::StandardError.bit()
                | LogChannel::Agent.bit()
                | LogChannel::Reasoning.bit()
                | LogChannel::Tools.bit()
                | LogChannel::System.bit(),
        }
    }
}

impl LogFilterState {
    pub(super) const fn includes(self, channel: LogChannel) -> bool {
        self.enabled & channel.bit() != 0
    }

    pub(super) fn toggle(&mut self, step: &WorkflowRunStepView, key: char) -> bool {
        let Some(option) = log_channel_options(step)
            .iter()
            .find(|option| option.key == key)
        else {
            return false;
        };
        self.enabled ^= option.channel.bit();
        true
    }
}

#[derive(Default)]
pub(super) struct FilteredLog {
    pub(super) records: VecDeque<WorkflowRunLogRecord>,
    pub(super) hidden_records: usize,
    pub(super) last_seen: Option<AcceptedRecordOrder>,
}

impl FilteredLog {
    pub(super) fn new(log: &WorkflowRunStepLog, filters: LogFilterState) -> Self {
        let records = log
            .records
            .iter()
            .filter(|record| filters.includes(LogChannel::for_source(record.source)))
            .cloned()
            .collect::<VecDeque<_>>();
        let hidden_records = log.records.len().saturating_sub(records.len());
        Self {
            records,
            hidden_records,
            last_seen: log.records.back().map(|record| record.accepted_order),
        }
    }

    pub(super) fn extend(&mut self, log: &WorkflowRunStepLog, filters: LogFilterState) {
        let first = log.records.front().map(|record| record.accepted_order);
        while self
            .records
            .front()
            .is_some_and(|record| first.is_none_or(|first| record.accepted_order < first))
        {
            self.records.pop_front();
        }
        let last = self.last_seen;
        let new_start = log
            .records
            .partition_point(|record| last.is_some_and(|last| record.accepted_order <= last));
        self.records.extend(
            log.records
                .iter()
                .skip(new_start)
                .filter(|record| filters.includes(LogChannel::for_source(record.source)))
                .cloned(),
        );
        self.hidden_records = log.records.len().saturating_sub(self.records.len());
        self.last_seen = log.records.back().map(|record| record.accepted_order);
    }
}

#[derive(Default)]
pub struct HostInteraction {
    pub(super) selected: usize,
    pub(super) surface: HostSurface,
    pub(super) help_visible: bool,
    pub(super) terminal_area: Rect,
    pub(super) full_log: FullLogInteraction,
    pub(super) log_filters: LogFilterState,
    pub(super) filtered_log: FilteredLog,
    pub(super) filtered_key: Option<(usize, LogFilterState, u64, Option<AcceptedRecordOrder>, u64)>,
}

impl HostInteraction {
    pub(super) fn prepare_filtered_log(
        &mut self,
        log: &WorkflowRunStepLog,
        selected: usize,
        generation: u64,
    ) {
        let first = log.records.front().map(|record| record.accepted_order);
        let key = (
            selected,
            self.log_filters,
            generation,
            first,
            log.observed_records,
        );
        match self.filtered_key {
            Some((step, filters, ..)) if step == selected && filters == self.log_filters => {
                if self
                    .filtered_key
                    .is_some_and(|(_, _, _, old_first, old_observed)| {
                        old_first != first || old_observed != log.observed_records
                    })
                {
                    self.filtered_log.extend(log, self.log_filters);
                }
            }
            _ => {
                self.filtered_log = FilteredLog::new(log, self.log_filters);
                self.full_log.maximum_width = None;
            }
        }
        self.filtered_key = Some(key);
    }
}

pub(super) struct FullLogInteraction {
    pub(super) follow: bool,
    pub(super) anchor: Option<AcceptedRecordOrder>,
    pub(super) anchor_clamped: bool,
    pub(super) horizontal_offset: usize,
    pub(super) available_width: usize,
    pub(super) available_rows: usize,
    pub(super) maximum_width: Option<usize>,
    pub(super) maximum_last: Option<AcceptedRecordOrder>,
    // Decreasing offsets: an evicted maximum exposes the next surviving bound.
    pub(super) maximum_queue: VecDeque<(AcceptedRecordOrder, usize)>,
}

impl Default for FullLogInteraction {
    fn default() -> Self {
        Self {
            follow: true,
            anchor: None,
            anchor_clamped: false,
            horizontal_offset: 0,
            available_width: 0,
            available_rows: 0,
            maximum_width: None,
            maximum_last: None,
            maximum_queue: VecDeque::new(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum VerticalNavigation {
    Up,
    Down,
    PageUp,
    PageDown,
    HalfPageUp,
    HalfPageDown,
    Top,
    Bottom,
}

impl FullLogInteraction {
    pub(super) fn synchronize(&mut self, log: &FilteredLog, available_width: usize, rows: usize) {
        self.available_width = available_width;
        self.available_rows = rows;
        if self.follow {
            self.anchor = None;
            self.anchor_clamped = false;
        } else if log.records.is_empty() {
            self.anchor = None;
        } else if let Some(anchor) = self.anchor {
            if let Err(insertion) = log
                .records
                .binary_search_by_key(&anchor, |record| record.accepted_order)
            {
                self.anchor = log
                    .records
                    .get(insertion)
                    .or_else(|| log.records.back())
                    .map(|record| record.accepted_order);
                self.anchor_clamped = true;
            }
        } else {
            self.anchor = log.records.front().map(|record| record.accepted_order);
        }
        let maximum = self.maximum_offset(log, available_width);
        self.horizontal_offset = self.horizontal_offset.min(maximum);
    }

    pub(super) fn navigate(&mut self, log: &FilteredLog, navigation: VerticalNavigation) {
        self.synchronize(log, self.available_width, self.available_rows);
        let current = self.top_index(log);
        let viewport_rows = self.available_rows.max(1);
        let bottom = log.records.len().saturating_sub(viewport_rows);
        let page = viewport_rows;
        let half_page = (viewport_rows / 2).max(1);
        let target = match navigation {
            VerticalNavigation::Up => current.saturating_sub(1),
            VerticalNavigation::Down => {
                if self.lines_behind_from(log, current) == 0 {
                    current
                } else {
                    current.saturating_add(1).min(bottom)
                }
            }
            VerticalNavigation::PageUp => current.saturating_sub(page),
            VerticalNavigation::PageDown => {
                if self.lines_behind_from(log, current) == 0 {
                    current
                } else {
                    current.saturating_add(page).min(bottom)
                }
            }
            VerticalNavigation::HalfPageUp => current.saturating_sub(half_page),
            VerticalNavigation::HalfPageDown => {
                if self.lines_behind_from(log, current) == 0 {
                    current
                } else {
                    current.saturating_add(half_page).min(bottom)
                }
            }
            VerticalNavigation::Top => 0,
            VerticalNavigation::Bottom => bottom,
        };
        self.follow = false;
        self.anchor = log.records.get(target).map(|record| record.accepted_order);
        self.anchor_clamped = false;
    }

    pub(super) fn pan(&mut self, log: &FilteredLog, right: bool) {
        self.synchronize(log, self.available_width, self.available_rows);
        if right {
            self.horizontal_offset = self
                .horizontal_offset
                .saturating_add(1)
                .min(self.maximum_offset(log, self.available_width));
        } else {
            self.horizontal_offset = self.horizontal_offset.saturating_sub(1);
        }
    }

    pub(super) fn maximum_offset(&mut self, log: &FilteredLog, width: usize) -> usize {
        if self.maximum_width != Some(width) {
            self.maximum_queue.clear();
            self.maximum_last = None;
            self.maximum_width = Some(width);
        }

        let first = log.records.front().map(|record| record.accepted_order);
        while self
            .maximum_queue
            .front()
            .is_some_and(|(order, _)| first.is_none_or(|first| *order < first))
        {
            self.maximum_queue.pop_front();
        }
        let start = log.records.partition_point(|record| {
            self.maximum_last
                .is_some_and(|last| record.accepted_order <= last)
        });
        for record in log.records.iter().skip(start) {
            let offset = log_record_horizontal_offset(record, width);
            while self
                .maximum_queue
                .back()
                .is_some_and(|(_, previous)| *previous <= offset)
            {
                self.maximum_queue.pop_back();
            }
            self.maximum_queue
                .push_back((record.accepted_order, offset));
        }
        self.maximum_last = log.records.back().map(|record| record.accepted_order);
        self.maximum_queue.front().map_or(0, |(_, offset)| *offset)
    }

    pub(super) fn resume_follow(&mut self) {
        self.follow = true;
        self.anchor = None;
        self.anchor_clamped = false;
    }

    pub(super) fn top_index(&self, log: &FilteredLog) -> usize {
        if self.follow {
            return log.records.len().saturating_sub(self.available_rows);
        }
        self.anchor
            .and_then(|anchor| {
                log.records
                    .binary_search_by_key(&anchor, |record| record.accepted_order)
                    .ok()
            })
            .unwrap_or(0)
    }

    pub(super) fn lines_behind(&self, log: &FilteredLog) -> usize {
        self.lines_behind_from(log, self.top_index(log))
    }

    pub(super) fn lines_behind_from(&self, log: &FilteredLog, top: usize) -> usize {
        log.records
            .len()
            .saturating_sub(top.saturating_add(self.available_rows))
    }
}

pub(super) fn log_record_horizontal_offset(
    record: &WorkflowRunLogRecord,
    available_width: usize,
) -> usize {
    let gutter_width = LogGutter::for_width(available_width).width();
    let line_width = gutter_width.saturating_add(record.display_width);
    let nominal_offset = line_width.saturating_sub(available_width);
    crate::workflow::text_fit::next_grapheme_boundary(&record.payload, gutter_width, nominal_offset)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum HostControl {
    Continue,
    Quit,
}

pub(super) fn clamp_step_selection(selected: &mut usize, step_count: usize) {
    if step_count == 0 {
        *selected = 0;
    } else if *selected >= step_count {
        *selected = step_count - 1;
    }
}

impl HostInteraction {
    pub(super) fn handle_key(
        &mut self,
        event: TerminalInputEvent,
        snapshot: &WorkflowRunViewSnapshot,
        cancellation: &CancellationSource,
    ) -> HostControl {
        clamp_step_selection(&mut self.selected, snapshot.steps.len());

        if event == TerminalInputEvent::Cancel {
            match finalization_signal_action(snapshot) {
                FinalizationSignalAction::Graceful => {
                    cancellation.request_cancellation(CancellationReason::UserRequest);
                }
                FinalizationSignalAction::ForceAbort => {
                    cancellation.request_force_abort();
                }
                FinalizationSignalAction::Inert => {}
            }
            return HostControl::Continue;
        }
        if event == TerminalInputEvent::Quit && snapshot.quit_eligible {
            return HostControl::Quit;
        }
        if !operational_area(self.terminal_area) {
            return HostControl::Continue;
        }
        if self.help_visible {
            if event == TerminalInputEvent::Escape {
                self.help_visible = false;
            }
            return HostControl::Continue;
        }
        if event == TerminalInputEvent::Help {
            self.help_visible = true;
            return HostControl::Continue;
        }

        if let TerminalInputEvent::ToggleLogChannel(key) = event {
            if let Some(step) = snapshot.steps.get(self.selected)
                && self.log_filters.toggle(step, key)
                && self.surface == HostSurface::FullLog
            {
                let (width, rows) = full_log_record_dimensions(self.terminal_area, step);
                self.prepare_filtered_log(&step.log, self.selected, snapshot.generation);
                self.full_log.synchronize(&self.filtered_log, width, rows);
            }
            return HostControl::Continue;
        }

        if self.surface == HostSurface::FullLog
            && let Some(step) = snapshot.steps.get(self.selected)
        {
            let (width, rows) = full_log_record_dimensions(self.terminal_area, step);
            self.prepare_filtered_log(&step.log, self.selected, snapshot.generation);
            self.full_log.synchronize(&self.filtered_log, width, rows);
        }

        if self.surface == HostSurface::FullLog
            && snapshot.steps.get(self.selected).is_some()
            && let Some(navigation) = vertical_navigation(event)
        {
            self.full_log.navigate(&self.filtered_log, navigation);
            return HostControl::Continue;
        }

        match event {
            TerminalInputEvent::Enter
                if self.surface == HostSurface::Split && !snapshot.steps.is_empty() =>
            {
                self.surface = HostSurface::FullLog;
                self.full_log = FullLogInteraction::default();
                if let Some(step) = snapshot.steps.get(self.selected) {
                    let (width, rows) = full_log_record_dimensions(self.terminal_area, step);
                    self.prepare_filtered_log(&step.log, self.selected, snapshot.generation);
                    self.full_log.synchronize(&self.filtered_log, width, rows);
                }
            }
            TerminalInputEvent::Escape => {
                self.surface = HostSurface::Split;
            }
            TerminalInputEvent::Up if self.surface == HostSurface::Split => {
                self.selected = self.selected.saturating_sub(1);
            }
            TerminalInputEvent::Down if self.surface == HostSurface::Split => {
                if self.selected.saturating_add(1) < snapshot.steps.len() {
                    self.selected += 1;
                }
            }
            TerminalInputEvent::PanLeft | TerminalInputEvent::PanRight
                if self.surface == HostSurface::FullLog =>
            {
                self.full_log
                    .pan(&self.filtered_log, event == TerminalInputEvent::PanRight);
            }
            TerminalInputEvent::Follow if self.surface == HostSurface::FullLog => {
                self.full_log.resume_follow();
            }
            _ => {}
        }
        HostControl::Continue
    }
}

pub(super) fn vertical_navigation(event: TerminalInputEvent) -> Option<VerticalNavigation> {
    match event {
        TerminalInputEvent::Up => Some(VerticalNavigation::Up),
        TerminalInputEvent::Down => Some(VerticalNavigation::Down),
        TerminalInputEvent::PageUp => Some(VerticalNavigation::PageUp),
        TerminalInputEvent::PageDown => Some(VerticalNavigation::PageDown),
        TerminalInputEvent::HalfPageUp => Some(VerticalNavigation::HalfPageUp),
        TerminalInputEvent::HalfPageDown => Some(VerticalNavigation::HalfPageDown),
        TerminalInputEvent::Top => Some(VerticalNavigation::Top),
        TerminalInputEvent::Bottom => Some(VerticalNavigation::Bottom),
        TerminalInputEvent::PanLeft
        | TerminalInputEvent::PanRight
        | TerminalInputEvent::Follow
        | TerminalInputEvent::ToggleLogChannel(_)
        | TerminalInputEvent::Help
        | TerminalInputEvent::Enter
        | TerminalInputEvent::Escape
        | TerminalInputEvent::Quit
        | TerminalInputEvent::Cancel
        | TerminalInputEvent::Resize
        | TerminalInputEvent::Other => None,
    }
}

pub(super) fn operational_area(area: Rect) -> bool {
    area.width >= MINIMUM_WIDTH && area.height >= MINIMUM_HEIGHT
}

pub(super) fn selected_lower_panel_area<Step: StepProjection>(area: Rect, step: &Step) -> Rect {
    let content_area = Rect::new(
        area.x,
        area.y,
        area.width,
        area.height.saturating_sub(FOOTER_HEIGHT),
    );
    inspector_and_log_areas(content_area, inspector_desired_height(Some(step)))[1]
}

pub(super) fn full_log_record_dimensions(area: Rect, step: &WorkflowRunStepView) -> (usize, usize) {
    let log_content = log_block(Borders::TOP, false).inner(selected_lower_panel_area(area, step));
    let records_area = log_content_areas(log_content)[1];
    let marker_rows = usize::from(step.log.discarded_records != 0);
    (
        usize::from(records_area.width),
        usize::from(records_area.height).saturating_sub(marker_rows),
    )
}
