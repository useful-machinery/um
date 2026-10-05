pub(crate) mod archived;
mod dag_layout;
mod host;
mod input;
mod interaction;
mod layout;
mod status;
mod widget_footer;
mod widget_inspector;
mod widget_log;
mod widget_steps;
mod widget_summary;

#[cfg(test)]
mod test_boundary;
#[cfg(test)]
use self::test_boundary::*;
#[cfg(test)]
mod tests;

use std::collections::VecDeque;
use std::future::Future;
use std::io::{self, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use super::text_fit::{display_width, ellipsize, fit_text};
use crossterm::cursor::{Hide, Show};
use crossterm::event::{Event, EventStream, KeyCode, KeyEventKind, KeyModifiers};
use crossterm::terminal::{EnterAlternateScreen, LeaveAlternateScreen};
use crossterm::{execute, queue};
use futures_util::{FutureExt as _, StreamExt as _};
use ratatui::backend::CrosstermBackend;
pub use ratatui::layout::Rect as TerminalRect;
use ratatui::layout::{Alignment, Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Block, Borders, Clear, List, ListItem, ListState, Padding, Paragraph};
use ratatui::{Frame, Terminal, TerminalOptions, Viewport};
use rustix::termios::{OptionalActions, Termios, tcgetattr, tcgetwinsize, tcsetattr};
use std::panic::AssertUnwindSafe;
use time::UtcOffset;
use tokio::sync::oneshot;
use unicode_segmentation::UnicodeSegmentation as _;

use self::dag_layout::DagLayout;
#[cfg(test)]
use super::admission::CancellationOperation;
use super::admission::{CancellationReason, CancellationSource};
use super::document::{FailurePolicy, Output as WorkflowOutput};
use super::observation::{CommandOutputSource, ObservedStepTransition};
use super::presentation::{
    PresentationFailure, PresentationFailureOperation, cancellation_reason,
    canonical_blocked_detail, canonical_failure_detail, finalization_trigger, header_timestamp,
    human_duration, recovery_progress_detail, shell_quote, shell_quote_visible_argument, step_kind,
    visible_text,
};
use super::presentation_feed::{
    AcceptedRecordOrder, AgentPresentationHarness, AgentPresentationObservationKind,
    WorkflowPresentationStep,
};
use super::run_view_model::{
    WorkflowRunCleanupResult, WorkflowRunCleanupState, WorkflowRunLogRecord, WorkflowRunLogSource,
    WorkflowRunOutputDisposition, WorkflowRunOutputUnavailableReason, WorkflowRunPublicationResult,
    WorkflowRunPublicationState, WorkflowRunStepLog, WorkflowRunStepView, WorkflowRunViewModel,
    WorkflowRunViewSnapshot,
};
use super::runtime::{SchedulingGate, StepStateKind, WorkflowState};
#[cfg(test)]
use super::step_runtime::StepFailureCause;

use self::host::*;
pub use self::host::{
    TerminalBoundary, TerminalHostExit, TerminalLifecycleEvent, WorkflowTerminalBoundary,
    WorkflowTerminalHost,
};
pub use self::input::TerminalInputEvent;
use self::input::*;
pub use self::interaction::HostInteraction;
use self::interaction::*;
use self::layout::*;
use self::status::*;
use self::widget_footer::*;
use self::widget_inspector::*;
use self::widget_log::*;
pub(super) use self::widget_steps::live_step_detail;
use self::widget_steps::*;
use self::widget_summary::*;

const MINIMUM_WIDTH: u16 = 64;
const MINIMUM_HEIGHT: u16 = 20;
const WIDE_LAYOUT_WIDTH: u16 = 100;
const WORKFLOW_SUMMARY_HEIGHT: u16 = 3;
const FOOTER_HEIGHT: u16 = 2;
const MINIMUM_INSPECTOR_HEIGHT: u16 = 8;
const INSPECTOR_HEADER_HEIGHT: u16 = 3;
const INSPECTOR_PANEL_PADDING: u16 = 2;
const MINIMUM_OUTPUT_PANEL_HEIGHT: u16 = 2;
const MINIMUM_LOG_HEIGHT: u16 = 4;
const REDRAW_INTERVAL: Duration = Duration::from_millis(100);
const RUNNING_INDICATOR_FRAMES: [&str; 10] = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];
const KIND_COLUMN_WIDTH: usize = 5;
const MINIMUM_DETAIL_WIDTH: usize = 12;
const INSPECTOR_LABEL_WIDTH: usize = 14;
const LOG_HEADER_HEIGHT: u16 = 2;
const LOG_TIMESTAMP_WIDTH: usize = 12;
const LOG_SOURCE_WIDTH: usize = 6;
const LOG_SEPARATOR_WIDTH: usize = 3;
const LOG_SOURCE_GUTTER_WIDTH: usize = LOG_SOURCE_WIDTH + LOG_SEPARATOR_WIDTH;
const LOG_TIMESTAMPED_GUTTER_WIDTH: usize = LOG_TIMESTAMP_WIDTH + 1 + LOG_SOURCE_GUTTER_WIDTH;
const MINIMUM_TIMESTAMPED_LOG_CONTENT_WIDTH: usize = 12;
