//! The Claude Code lifecycle matrix (INV-HARNESS-13): cells generated from the lifecycle
//! model's dimensions in `docs/architecture/14-harness-adapters.md` (mode, main-thread work,
//! subagent work, followup, round, ending), each driven natively against the pinned binary
//! through the production adapter and the loopback provider.
//!
//! `pinned_real_claude_code_11_lifecycle_matrix_drives_every_cell` prints one line per cell,
//! `cell <name>: ok`, `gap <ticket>`, `skipped` or `FAILED <reason>`, and fails at the end
//! naming every failed cell, so one run reports the whole matrix. Two environment variables
//! narrow a developer's run; the qualification gate sets neither:
//!
//! - `UM_CLAUDE_CODE_LIFECYCLE_CELLS`: comma-separated name fragments; only cells whose name
//!   contains one of them run.
//! - `UM_CLAUDE_CODE_CONFORMANCE_SKIP_SLOW_CELLS=1`: the slow tier (any `ScheduleWakeup`,
//!   a subagent's two-minute promotion) prints `skipped` instead of running.

use std::collections::VecDeque;
use std::io::Write as _;
use std::os::unix::fs::OpenOptionsExt as _;
use std::panic::AssertUnwindSafe;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use futures_util::FutureExt as _;
use nix::sys::stat::Mode as FifoMode;
use nix::unistd::mkfifo;
use rustix::process::{Pid, getpgid};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::process::Command;

use super::test_support::{
    BACKGROUND_SUBAGENT_DESCRIPTION, BACKGROUND_SUBAGENT_PROMPT, BackgroundSubagent,
    ControlledProviderRequest, FixtureSignal, LoopbackBlock, LoopbackProvider, MODEL,
    RecordingObservationSink, RunningProductionClaudeCode, SyntheticClaudeCodeRoot, WATCHDOG,
    backgrounded_subagent_statuses, completed_turns, exclusive_conformance_executable,
    launched_agent_id, wait_for_completed_turns, wait_for_subagent_status,
};
use super::{
    STRUCTURED_OUTPUT_TOOL_NAME, initial_user_text_frame, normal_mode_arguments,
    result_mode_arguments,
};
use crate::workflow::admission::{CancellationReason, CancellationSource};
use crate::workflow::agent::{
    AgentDiagnosticLevel, AgentFailureCause, AgentLifecycleMilestone, AgentObservation,
    AgentOutcome, AgentProcessControl, AgentValueMode, CompletedAgentInvocation,
    RetainedJsonSchema,
};
#[cfg(target_os = "linux")]
use crate::workflow::process_group::process_group_is_quiescent;

const CELL_FILTER: &str = "UM_CLAUDE_CODE_LIFECYCLE_CELLS";
const SKIP_SLOW_CELLS: &str = "UM_CLAUDE_CODE_CONFORMANCE_SKIP_SLOW_CELLS";
/// Claude Code's foreground-to-background promotion timeout. 2.1.310 reads it for the main
/// agent only and floors it at 2000 ms, so only a main-thread promotion is fast.
const PROMOTION_TIMEOUT: (&str, &str) = ("CLAUDE_CODE_AUTO_BACKGROUND_TIMEOUT_MS", "2000");
const RESPONSE: &str = "lifecycle cell complete";
const RESULT: i64 = 7;
const WAITING: &str = "waiting for the work to report";
/// The opening of the enforcement nag, the synthetic user text 2.1.310 sends in Result mode
/// after a turn that ended without `StructuredOutput`.
const ENFORCEMENT_NAG: &str = "[structured-output-enforce]";
const FOREGROUND_SUBAGENT_PROMPT: &str = "Run the controlled foreground subagent command.";
const FOREGROUND_SUBAGENT_DESCRIPTION: &str = "Run a foreground subagent";
const SUBAGENT_RESPONSE: &str = "lifecycle subagent finished";
/// `ScheduleWakeup`'s delay: 2.1.310's schema clamps `delaySeconds` to `[60, 3600]` with no
/// override, so a wakeup cell waits at least a minute.
const WAKEUP_DELAY_SECONDS: u64 = 60;
/// The anti-hang watchdog of a slow cell, which waits out the 60 s wakeup floor or a
/// subagent command's two-minute promotion besides its fast work.
const SLOW_WATCHDOG: Duration = Duration::from_secs(180);
const INIT_PROBE_SESSION_ID: &str = "00000000-0000-4000-8000-000000000013";
/// What 2.1.310's `init` lists among `tools`. It names the subagent tool by its legacy
/// name, `Task`; the provider request's `tools[]` names it `Agent`.
const INIT_LIFECYCLE_TOOLS: [&str; 5] =
    ["Task", "Bash", "SendMessage", "TaskStop", "ScheduleWakeup"];

// ---------------------------------------------------------------------------------------
// Dimensions

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Mode {
    Response,
    Result,
    None,
}

impl Mode {
    pub(super) const ALL: [Self; 3] = [Self::Response, Self::Result, Self::None];

    const fn slug(self) -> &'static str {
        match self {
            Self::Response => "response",
            Self::Result => "result",
            Self::None => "none",
        }
    }
}

/// When a subagent's own backgrounded command is released: while the main thread's
/// exchange is still active, or after its turn ended, in the drain.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Release {
    InExchange,
    InDrain,
}

/// What the subagent's own turn does.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SubagentWork {
    ForegroundBash,
    BackgroundBash {
        released: Release,
    },
    /// A foreground command promoted at Claude Code's two-minute limit, which
    /// `CLAUDE_CODE_AUTO_BACKGROUND_TIMEOUT_MS` does not shorten for a subagent.
    PromotedBash,
    /// A command run under 2.1.310's deferred `Monitor` tool, still running when the
    /// subagent closes its turn; it prints nothing before its release, so the monitor's
    /// only notification is its end.
    Monitor,
}

impl SubagentWork {
    pub(super) const ALL: [Self; 5] = [
        Self::ForegroundBash,
        Self::BackgroundBash {
            released: Release::InExchange,
        },
        Self::BackgroundBash {
            released: Release::InDrain,
        },
        Self::PromotedBash,
        Self::Monitor,
    ];

    const fn slug(self) -> &'static str {
        match self {
            Self::ForegroundBash => "foreground-bash",
            Self::BackgroundBash {
                released: Release::InExchange,
            } => "background-bash-in-exchange",
            Self::BackgroundBash {
                released: Release::InDrain,
            } => "background-bash-in-drain",
            Self::PromotedBash => "promoted-bash",
            Self::Monitor => "monitor",
        }
    }
}

/// What the main thread's first turn does.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Work {
    Nothing,
    ForegroundBash,
    /// A foreground command that blocks past `CLAUDE_CODE_AUTO_BACKGROUND_TIMEOUT_MS=2000`.
    PromotedBash,
    BackgroundBash,
    /// A command run under 2.1.310's deferred `Monitor` tool, which registers it as a
    /// backgrounded `local_bash` task under the `Monitor` call, background work the parser
    /// tracks as a monitor. The command prints one line before it blocks, the event that opens
    /// a turn of its own after the first turn ends paused on the monitor.
    Monitor,
    ForegroundAgent {
        subagent: SubagentWork,
    },
    BackgroundAgent {
        subagent: SubagentWork,
    },
}

impl Work {
    pub(super) fn all() -> Vec<Self> {
        let mut work = vec![
            Self::Nothing,
            Self::ForegroundBash,
            Self::PromotedBash,
            Self::BackgroundBash,
            Self::Monitor,
        ];
        work.extend(
            SubagentWork::ALL
                .into_iter()
                .map(|subagent| Self::ForegroundAgent { subagent }),
        );
        work.extend(
            SubagentWork::ALL
                .into_iter()
                .map(|subagent| Self::BackgroundAgent { subagent }),
        );
        work
    }

    const fn subagent(self) -> Option<SubagentWork> {
        match self {
            Self::ForegroundAgent { subagent } | Self::BackgroundAgent { subagent } => {
                Some(subagent)
            }
            _ => None,
        }
    }

    /// Whether the main thread's turn ends with this work outstanding, so the drain pauses.
    const fn pauses_the_drain(self) -> bool {
        matches!(
            self,
            Self::PromotedBash
                | Self::BackgroundBash
                | Self::Monitor
                | Self::BackgroundAgent { .. }
        )
    }

    fn name(self) -> String {
        match self {
            Self::Nothing => "nothing".to_owned(),
            Self::ForegroundBash => "foreground-bash".to_owned(),
            Self::PromotedBash => "promoted-bash".to_owned(),
            Self::BackgroundBash => "background-bash".to_owned(),
            Self::Monitor => "monitor".to_owned(),
            Self::ForegroundAgent { subagent } => format!("foreground-agent[{}]", subagent.slug()),
            Self::BackgroundAgent { subagent } => format!("background-agent[{}]", subagent.slug()),
        }
    }
}

/// What the main thread does once the work has reported.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Followup {
    None,
    /// A `SendMessage` to the finished backgrounded subagent.
    Resume,
    ResumeTwice,
    /// `TaskStop` on the backgrounded command or subagent, the turn settling with the
    /// stopped subagent's notification.
    TaskStop,
    /// `TaskStop` on the backgrounded subagent, answered on the round that returns the
    /// `TaskStop` result, before the stopped subagent's queued `<task-notification>` reaches
    /// the main thread, so that it opens an epilogue after the `result` (REQ-AGENT-76).
    TaskStopUnawaited,
    /// `ScheduleWakeup` while the work runs.
    ScheduleWakeup,
    ResumeThenWakeup,
    /// A `SendMessage` to the finished backgrounded subagent whose own command outlived it,
    /// answered on the round after the message's result while the messaged run's queued
    /// notification stands and Claude Code's own resume of the subagent for the command's
    /// report is outstanding: the notification's turn follows the answer's, with a model call
    /// when it is dequeued before the report run has queued its own notification (every
    /// drive on 2.1.310) and empty otherwise. In Result mode the candidate is premature and
    /// the adapter's feedback joins Claude Code's queue behind the notification, so its turn
    /// precedes the correction turn (AUTO-64).
    ResumeUnawaited,
}

impl Followup {
    pub(super) const ALL: [Self; 8] = [
        Self::None,
        Self::Resume,
        Self::ResumeTwice,
        Self::TaskStop,
        Self::TaskStopUnawaited,
        Self::ScheduleWakeup,
        Self::ResumeThenWakeup,
        Self::ResumeUnawaited,
    ];

    const fn slug(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Resume => "resume",
            Self::ResumeTwice => "resume-twice",
            Self::TaskStop => "task-stop",
            Self::TaskStopUnawaited => "task-stop-unawaited",
            Self::ScheduleWakeup => "schedule-wakeup",
            Self::ResumeThenWakeup => "resume-then-wakeup",
            Self::ResumeUnawaited => "resume-unawaited",
        }
    }

    /// Whether the followup has something to act on after `work`.
    const fn applies_to(self, work: Work) -> bool {
        match self {
            Self::None => true,
            // A stopped command's own notification settles it before the `TaskStop` result
            // and queues nothing for the model, so its `TaskStop` cell already answers at once.
            Self::Resume | Self::ResumeTwice | Self::ResumeThenWakeup | Self::TaskStopUnawaited => {
                matches!(work, Work::BackgroundAgent { .. })
            }
            Self::TaskStop | Self::ScheduleWakeup => matches!(
                work,
                Work::BackgroundBash | Work::Monitor | Work::BackgroundAgent { .. }
            ),
            // Only a command that outlives its subagent has Claude Code resume the subagent
            // on its own, superseding the messaged run's notification.
            Self::ResumeUnawaited => matches!(
                work,
                Work::BackgroundAgent {
                    subagent: SubagentWork::BackgroundBash {
                        released: Release::InDrain,
                    },
                }
            ),
        }
    }

    /// How many times the followup messages the finished subagent.
    const fn resumes(self) -> usize {
        match self {
            Self::Resume | Self::ResumeThenWakeup | Self::ResumeUnawaited => 1,
            Self::ResumeTwice => 2,
            Self::None | Self::TaskStop | Self::TaskStopUnawaited | Self::ScheduleWakeup => 0,
        }
    }

    /// Whether the followup stops the work with `TaskStop`, so the subagent ends `stopped`
    /// and no resume delivers its leftover command's report.
    const fn stops_the_work(self) -> bool {
        matches!(self, Self::TaskStop | Self::TaskStopUnawaited)
    }

    const fn waits_for_a_wakeup(self) -> bool {
        matches!(self, Self::ScheduleWakeup | Self::ResumeThenWakeup)
    }
}

/// Where the turn's answer goes relative to the followup's call.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Round {
    /// The followup's round returns, and the turn's answer comes on a later round, after the
    /// work reported.
    AnotherRound,
    /// The followup's call and the turn's answer are blocks of one model message, with the
    /// work it acts on outstanding.
    SameRound,
}

impl Round {
    pub(super) const ALL: [Self; 2] = [Self::AnotherRound, Self::SameRound];

    /// The name's segment after the followup's; `AnotherRound` cells keep their four
    /// segments.
    const fn slug(self) -> Option<&'static str> {
        match self {
            Self::AnotherRound => None,
            Self::SameRound => Some("same-round"),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Ending {
    Settle,
    /// Cancellation while a main-thread provider request is pending.
    CancelInExchange,
    /// Cancellation after a turn ended with work outstanding.
    CancelInDrain,
}

impl Ending {
    pub(super) const ALL: [Self; 3] = [Self::Settle, Self::CancelInExchange, Self::CancelInDrain];

    const fn slug(self) -> &'static str {
        match self {
            Self::Settle => "settle",
            Self::CancelInExchange => "cancel-in-exchange",
            Self::CancelInDrain => "cancel-in-drain",
        }
    }

    const fn cancels(self) -> bool {
        !matches!(self, Self::Settle)
    }
}

// ---------------------------------------------------------------------------------------
// Cells and the generator

/// What a cell must observe.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Expectation {
    /// The mode's typed completion (`Cancelled` for a cancellation ending), no protocol
    /// failure, no unrecognized event but `background_tasks_changed`, and the subagent
    /// diagnostics the work implies.
    Settles,
    /// The pinned binary cannot settle the cell today: the run must fail as a protocol
    /// failure with this rejection reason, and the gap is named with its ticket in the
    /// chapter's Gaps. No cell is one today; the variant is the mechanism INV-HARNESS-13
    /// keeps for the next gap a drive finds.
    #[allow(
        dead_code,
        reason = "no cell is a known gap on 2.1.310; INV-HARNESS-13 keeps the form"
    )]
    KnownGap {
        reason: &'static str,
        ticket: &'static str,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct LifecycleCell {
    pub(super) mode: Mode,
    pub(super) work: Work,
    pub(super) followup: Followup,
    pub(super) round: Round,
    pub(super) ending: Ending,
}

impl LifecycleCell {
    /// The cell's stable name, as `result/background-agent[foreground-bash]/task-stop/settle`
    /// or `result/background-bash/task-stop/same-round/settle`.
    pub(super) fn name(&self) -> String {
        let round = self
            .round
            .slug()
            .map(|slug| format!("{slug}/"))
            .unwrap_or_default();
        format!(
            "{}/{}/{}/{round}{}",
            self.mode.slug(),
            self.work.name(),
            self.followup.slug(),
            self.ending.slug()
        )
    }

    /// A slow cell waits on a floor the binary hard-codes: `ScheduleWakeup`'s 60 s minimum
    /// delay or a subagent command's two-minute promotion.
    pub(super) fn is_slow(&self) -> bool {
        self.followup.waits_for_a_wakeup()
            || self.work.subagent() == Some(SubagentWork::PromotedBash)
    }

    /// Variables the cell adds to the child environment.
    pub(super) fn environment_overrides(&self) -> Vec<(&'static str, &'static str)> {
        match self.work {
            Work::PromotedBash => vec![PROMOTION_TIMEOUT],
            _ => Vec::new(),
        }
    }

    /// What 2.1.310 does with the cell: every cell settles. A Result-mode same-round resume
    /// submits the value while the resumed run is outstanding; the adapter rejects the
    /// premature candidate and begins the correction exchange, and the resumed run's first
    /// frame, which arrives before that exchange's `init`, is read as the turn's (UM-3111).
    pub(super) const fn expectation(&self) -> Expectation {
        Expectation::Settles
    }

    /// A Result-mode same-round `TaskStop` on a backgrounded subagent, whose message the adapter
    /// rejects as an ambiguous candidate, beginning the correction exchange: the stopped
    /// subagent's thread reports its interruption just before the turn's `result` or, when it
    /// loses that race, before the correction turn's `init`, in the window the parser reads as
    /// the turn's (UM-3111). The driver notes the side the binary took.
    const fn stop_races_the_correction(&self) -> bool {
        matches!(
            (self.mode, self.work, self.followup, self.round),
            (
                Mode::Result,
                Work::BackgroundAgent { .. },
                Followup::TaskStop,
                Round::SameRound
            )
        )
    }

    /// A messaged subagent whose own backgrounded command outlives it, released in the drain
    /// and not cancelled: the messaged run settles in the drain and Claude Code's own resume
    /// that delivers the command's report follows at once, so two notifications are queued for
    /// the main thread, the messaged run's, which the resume supersedes, and the report run's
    /// (chapter 14's "command outliving it" row). The binary opens the superseded
    /// notification's empty turn and the notified turn in either order; the driver notes which
    /// came first.
    const fn queues_two_notifications(&self) -> bool {
        self.outlives_its_subagent_in_the_drain()
            && self.followup.resumes() > 0
            && !self.ending.cancels()
    }

    /// The turns a settling cell completes on 2.1.310, where the matrix pins the count: a
    /// wakeup cell's and a same-round cell's. In Result mode a same-round answer submits the
    /// value beside the followup's call, which the adapter rejects (an ambiguous candidate
    /// beside `TaskStop`, a premature one while the work runs), and the correction turn
    /// submits it again.
    const fn expected_turns(&self) -> Option<usize> {
        let correction =
            matches!((self.mode, self.round), (Mode::Result, Round::SameRound)) as usize;
        // The first turn ends paused on the work, the wakeup turn ends paused again, and the
        // work's notification wakes the turn that settles; a resume adds its woken turn.
        if self.followup.waits_for_a_wakeup() {
            return Some(3 + self.followup.resumes() + correction);
        }
        // The first turn ends paused on the monitor and its event opens the second, which
        // ends paused again until the command's end wakes the third; a `TaskStop` from the
        // event turn settles that turn instead.
        if matches!(self.work, Work::Monitor) && !self.ending.cancels() {
            return Some(match self.followup {
                Followup::TaskStop => 2 + correction,
                _ => 3,
            });
        }
        // The answer's turn ends with the report's resumed run outstanding and the messaged
        // run's notification queued: its turn (with a model call, or empty), in Result mode
        // the correction turn for the rejected premature candidate, and the report run's
        // notified turn follow.
        if matches!(self.followup, Followup::ResumeUnawaited) {
            return Some(4 + matches!(self.mode, Mode::Result) as usize);
        }
        if !matches!(self.round, Round::SameRound) {
            return None;
        }
        match (self.followup, self.work) {
            // The stopped command settles before the `TaskStop` result and queues nothing.
            (Followup::TaskStop, Work::BackgroundBash) => Some(1 + correction),
            // The stopped subagent's queued notification opens an epilogue after the `result`.
            (Followup::TaskStop, Work::BackgroundAgent { .. }) => Some(2 + correction),
            // The answer's turn ends with the resumed run outstanding, whose notification wakes
            // one more turn. The resume goes out from the first turn when the subagent's
            // command was released in the exchange, from the second otherwise; for a command
            // outliving the subagent, Claude Code's own resume that delivers its report adds
            // an empty turn for the messaged run's notification and the notified turn. In
            // Result mode the premature candidate's correction turn adds one.
            (
                Followup::Resume,
                Work::BackgroundAgent {
                    subagent:
                        SubagentWork::BackgroundBash {
                            released: Release::InExchange,
                        },
                },
            ) => Some(2 + correction),
            (
                Followup::Resume,
                Work::BackgroundAgent {
                    subagent:
                        SubagentWork::BackgroundBash {
                            released: Release::InDrain,
                        },
                },
            ) => Some(4 + correction),
            (Followup::Resume, Work::BackgroundAgent { .. }) => Some(3 + correction),
            _ => None,
        }
    }

    /// Whether a Result-mode turn of the cell ends with text while work is outstanding:
    /// 2.1.310 answers a turn that ends without `StructuredOutput` with the
    /// `[structured-output-enforce]` nag and a further model round, which the main thread
    /// refuses with text so that the drain pauses on the work (REQ-AGENT-35). A stop's turn
    /// leaves nothing outstanding when it ends: it polls until the stopped work has settled,
    /// or submits the value on the stop's round. A subagent whose command is released in the
    /// exchange completes while the main thread holds its first turn, so only a resume's turn
    /// ends on it.
    const fn refuses_the_nag(&self) -> bool {
        if !matches!(
            (self.mode, self.round, self.ending),
            (Mode::Result, Round::AnotherRound, Ending::Settle)
        ) {
            return false;
        }
        match self.followup {
            Followup::TaskStop | Followup::TaskStopUnawaited | Followup::ResumeUnawaited => false,
            Followup::Resume
            | Followup::ResumeTwice
            | Followup::ScheduleWakeup
            | Followup::ResumeThenWakeup => true,
            Followup::None => {
                self.work.pauses_the_drain()
                    && !matches!(
                        self.work,
                        Work::BackgroundAgent {
                            subagent: SubagentWork::BackgroundBash {
                                released: Release::InExchange,
                            },
                        }
                    )
            }
        }
    }

    /// A backgrounded subagent whose own backgrounded command or monitor outlives it,
    /// released in the drain after the subagent's completion. Claude Code resumes the finished
    /// subagent to deliver the command's report, or the monitor's event and end, unless
    /// `TaskStop` stopped the command with it.
    const fn outlives_its_subagent_in_the_drain(&self) -> bool {
        matches!(
            self.work,
            Work::BackgroundAgent {
                subagent: SubagentWork::BackgroundBash {
                    released: Release::InDrain,
                } | SubagentWork::Monitor,
            }
        )
    }
}

/// A combination the generator does not produce, with the reason it does not exist.
pub(super) struct PruneRule {
    pub(super) name: &'static str,
    pub(super) reason: &'static str,
    pub(super) forbids: fn(&LifecycleCell) -> bool,
}

pub(super) const PRUNE_RULES: [PruneRule; 9] = [
    PruneRule {
        name: "followup-without-leftover-work",
        reason: "a followup acts on a backgrounded command or subagent the first turn left",
        forbids: |cell| !cell.followup.applies_to(cell.work),
    },
    PruneRule {
        name: "drain-cancellation-without-a-paused-drain",
        reason: "only work that outlives the first turn pauses the drain to cancel in",
        forbids: |cell| cell.ending == Ending::CancelInDrain && !cell.work.pauses_the_drain(),
    },
    PruneRule {
        name: "cancellation-with-a-followup",
        reason: "a cancellation ending is driven over the work alone",
        forbids: |cell| cell.ending.cancels() && cell.followup != Followup::None,
    },
    PruneRule {
        name: "none-mode-cancellation",
        reason: "None completes on Response's parser path, so it runs Settle cells alone",
        forbids: |cell| cell.mode == Mode::None && cell.ending.cancels(),
    },
    PruneRule {
        name: "slow-cell-outside-result-settle",
        reason: "a slow cell runs once, in Result mode, where the enforcement nag can push a \
                 premature candidate",
        forbids: |cell| cell.is_slow() && (cell.mode != Mode::Result || cell.ending.cancels()),
    },
    PruneRule {
        name: "slow-cell-with-varied-companions",
        reason: "a slow value is paired with the plainest companions: a wakeup with a \
                 backgrounded command or a subagent running a foreground command, a \
                 subagent's promotion with no followup",
        forbids: |cell| match cell.work {
            Work::BackgroundAgent {
                subagent: SubagentWork::PromotedBash,
            } => cell.followup != Followup::None,
            Work::BackgroundAgent { subagent } => {
                cell.followup.waits_for_a_wakeup() && subagent != SubagentWork::ForegroundBash
            }
            _ => false,
        },
    },
    PruneRule {
        name: "monitor-companions",
        reason: "a wakeup over a monitor is `background-bash/schedule-wakeup`'s mechanism, \
                 both `local_bash` tasks, and a slow cell, so a monitor is stopped or \
                 cancelled but never paired with a wakeup",
        forbids: |cell| cell.work == Work::Monitor && cell.followup == Followup::ScheduleWakeup,
    },
    PruneRule {
        name: "monitor-subagent-companions",
        reason: "a monitoring subagent is paired with the plainest companions: no followup, \
                 or `TaskStop` on the subagent that runs the monitor",
        forbids: |cell| {
            cell.work.subagent() == Some(SubagentWork::Monitor)
                && !matches!(cell.followup, Followup::None | Followup::TaskStop)
        },
    },
    PruneRule {
        name: "same-round-over-a-single-followup",
        reason: "the followup's call shares the answer's round only when it is one call: a \
                 resume, a stop or a wakeup; the compound followups, the unawaited stop and no \
                 followup keep one shape",
        forbids: |cell| {
            cell.round == Round::SameRound
                && !matches!(
                    cell.followup,
                    Followup::Resume | Followup::TaskStop | Followup::ScheduleWakeup
                )
        },
    },
];

/// The Cartesian product of the dimensions less the combinations a prune rule forbids.
pub(super) fn lifecycle_cells() -> Vec<LifecycleCell> {
    let mut cells = Vec::new();
    for mode in Mode::ALL {
        for work in Work::all() {
            for followup in Followup::ALL {
                for round in Round::ALL {
                    for ending in Ending::ALL {
                        let cell = LifecycleCell {
                            mode,
                            work,
                            followup,
                            round,
                            ending,
                        };
                        if !PRUNE_RULES.iter().any(|rule| (rule.forbids)(&cell)) {
                            cells.push(cell);
                        }
                    }
                }
            }
        }
    }
    cells
}

// ---------------------------------------------------------------------------------------
// The driver

/// What the driver observed of one cell besides its outcome.
#[derive(Default)]
struct DrivenFacts {
    /// `init["tools"]` of a launch with the cell's arguments, read for `Work::Nothing`.
    init_tools: Option<Vec<String>>,
    /// The names in the first main-thread provider request's `tools[]`.
    request_tools: Vec<String>,
    /// What the driver saw the binary do that a failure's reason should name.
    notes: Vec<String>,
    /// For `Followup::TaskStopUnawaited`: whether the main thread's answer went out before
    /// the stopped subagent's notification reached it, so that the notification opens an
    /// epilogue after the `result`; the binary decides the race.
    answered_before_the_notification: Option<bool>,
    /// The main-thread requests that carried Result mode's `[structured-output-enforce]` nag
    /// and that the driver answered with text while the work was outstanding.
    nags_refused: usize,
}

enum Verdict {
    Passed,
    /// A known gap met its rejection; no cell is one today (see `Expectation::KnownGap`).
    #[allow(
        dead_code,
        reason = "no cell is a known gap on 2.1.310; INV-HARNESS-13 keeps the form"
    )]
    Gap(&'static str),
}

/// The invocation's cancellation controls. A cancellation cell cancels through them; dropping
/// them requests cancellation of an invocation the driver abandons, by a panic or the
/// watchdog, so the native process does not outlive its cell, and is a no-op after a
/// cancellation already made.
struct CancelOnDrop {
    cancellation: CancellationSource,
    process_control: AgentProcessControl,
}

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if self
            .cancellation
            .request_cancellation(CancellationReason::UserRequest)
        {
            self.process_control.interrupt();
        }
    }
}

/// A command blocked on a fifo, which reports `ready` and its shell's process id when it
/// starts and `finished` once released.
struct BlockedCommand {
    signal: FixtureSignal,
    blocker: PathBuf,
}

impl BlockedCommand {
    fn new(root: &SyntheticClaudeCodeRoot, name: &str) -> Self {
        let signal = FixtureSignal::create(root.private().join(format!("{name}.signal")));
        let blocker = root.private().join(format!("{name}.blocker"));
        mkfifo(&blocker, FifoMode::S_IRUSR | FifoMode::S_IWUSR).unwrap();
        Self { signal, blocker }
    }

    /// The shell command; `printf` and `read` are builtins, so it needs no coreutils.
    fn script(&self) -> String {
        format!(
            "printf 'ready %s\\n' \"$$\" > '{signal}'; IFS= read -r done < '{blocker}'; printf 'finished\\n' > '{signal}'; printf 'command finished\\n'",
            signal = self.signal.path().display(),
            blocker = self.blocker.display(),
        )
    }

    fn bash(&self, description: &str, run_in_background: bool) -> Value {
        let mut input = json!({"command": self.script(), "description": description});
        if run_in_background {
            input["run_in_background"] = json!(true);
        }
        input
    }

    /// The input of a `Monitor` call that watches the command; `timeout_ms` and `persistent`
    /// keep their defaults. With `an_event` the command prints one line before it blocks,
    /// which the monitor reports as an event while the command runs.
    fn monitor(&self, description: &str, an_event: bool) -> Value {
        let script = self.script();
        let command = if an_event {
            format!("printf 'first-event\\n'; {script}")
        } else {
            script
        };
        json!({"command": command, "description": description})
    }

    /// Waits for the command to start and returns its process group.
    async fn started(&self) -> Pid {
        let ready = self.signal.receive().await;
        let shell = std::str::from_utf8(&ready)
            .ok()
            .and_then(|line| line.strip_prefix("ready "))
            .and_then(|pid| pid.trim().parse().ok())
            .and_then(Pid::from_raw)
            .unwrap_or_else(|| panic!("the command reported {ready:?} as its start"));
        getpgid(Some(shell)).unwrap_or_else(|error| {
            panic!("the command's shell exited before its process group was read: {error}")
        })
    }

    /// Releases the command, and reports whether it was still waiting: the blocker is
    /// opened without blocking, and only a command the harness stopped has no reader on it.
    async fn release(&self) -> bool {
        let blocker = self.blocker.clone();
        tokio::task::spawn_blocking(move || {
            match std::fs::OpenOptions::new()
                .write(true)
                .custom_flags(libc::O_NONBLOCK)
                .open(blocker)
            {
                Ok(mut writer) => {
                    writer.write_all(b"done\n").unwrap();
                    true
                }
                Err(error) if error.raw_os_error() == Some(libc::ENXIO) => false,
                Err(error) => panic!("failed to release the blocked command: {error}"),
            }
        })
        .await
        .unwrap()
    }

    async fn finished(&self) {
        assert_eq!(self.signal.receive().await, b"finished\n");
    }
}

/// A subagent thread's script: its tool calls in order, then its closing text.
struct SubagentThread {
    prompt: &'static str,
    calls: VecDeque<(&'static str, Value)>,
    /// Whether the subagent keeps its turn open until its own backgrounded command's
    /// notification reaches it, so the command settles while the subagent still owns it.
    awaits_notification: Option<AwaitedNotification>,
}

/// How a subagent waits for its own command: the request after its calls is held until the
/// driver has released the command, then answered with quick foreground rounds until a
/// request carries the command's `<task-notification>`, which Claude Code delivers at a
/// round boundary.
#[derive(Default)]
struct AwaitedNotification {
    held: Option<ControlledProviderRequest>,
    polling: bool,
    polls: usize,
}

const MAXIMUM_NOTIFICATION_POLLS: usize = 40;

/// A quick foreground command whose round lets Claude Code deliver a queued
/// `<task-notification>` to the thread that polls.
fn poll_call() -> Value {
    json!({"command": "IFS= read -t 0.1 -r _ < /dev/zero; printf 'polled\\n'",
        "description": "Check on the background work"})
}

/// 2.1.310 defers `Monitor`: a thread loads its schema with this `ToolSearch` input before
/// it can call it.
fn load_monitor() -> Value {
    json!({"query": "select:Monitor", "max_results": 1})
}

/// A subagent's monitor: the `ToolSearch` round that loads `Monitor`, then the call that
/// watches `command`.
fn monitor_calls(command: &BlockedCommand) -> [(&'static str, Value); 2] {
    [
        ("ToolSearch", load_monitor()),
        (
            "Monitor",
            command.monitor("Watch the subagent's command", false),
        ),
    ]
}

/// Asserts that the request offers the tool `name` and that its input schema has every
/// field `input` sets.
fn assert_the_schema_takes(body: &Value, name: &str, input: &Value) {
    let schema = tool_schema(body, name);
    let unknown = input
        .as_object()
        .into_iter()
        .flat_map(|input| input.keys())
        .filter(|field| schema["properties"].get(field.as_str()).is_none())
        .collect::<Vec<_>>();
    assert!(
        unknown.is_empty(),
        "{name}'s input schema has no {unknown:?}: {schema}"
    );
}

/// Releases the `name` call as the message's second block, after `preface`. The loopback
/// numbers tool uses by their block, so a main-thread call after the work's own (block 0)
/// gets a different tool-use id, as it does in production, and its result cannot retire the
/// work's task.
fn release_second(request: ControlledProviderRequest, preface: &str, name: &str, input: Value) {
    request.release_blocks(vec![
        LoopbackBlock::text(preface),
        LoopbackBlock::tool_use(name, input),
    ]);
}

/// The input of a `SendMessage` that resumes the finished subagent `agent_id`.
fn send_message(agent_id: &str) -> Value {
    json!({"to": agent_id, "summary": "Report once more",
        "message": "Answer once more as the resumed subagent."})
}

/// How many of Result mode's enforcement nags the conversation a request continues carries:
/// the synthetic user text 2.1.310 sends after a turn that ended without `StructuredOutput`,
/// which `documented_unrecognized` lets through on the event side. Claude Code merges the nag
/// into the user message that returns the turn's tool results and may append a queued
/// notification after it, so the newest nag is found by count rather than by position.
pub(super) fn enforcement_nags(body: &Value) -> usize {
    body["messages"]
        .to_string()
        .matches(ENFORCEMENT_NAG)
        .count()
}

/// Whether a `<task-notification>` has reached the thread the request continues. Only the
/// conversation counts: 2.1.310's tool descriptions name the tag too.
fn carries_a_notification(request: &ControlledProviderRequest) -> bool {
    request.body()["messages"]
        .to_string()
        .contains("<task-notification>")
}

impl SubagentThread {
    fn new(prompt: &'static str, calls: impl IntoIterator<Item = (&'static str, Value)>) -> Self {
        Self {
            prompt,
            calls: calls.into_iter().collect(),
            awaits_notification: None,
        }
    }

    fn awaiting_its_notification(mut self) -> Self {
        self.hold_its_turn();
        self
    }

    /// Holds the subagent's next request once its calls are done, until `close_its_turn`
    /// answers it.
    fn hold_its_turn(&mut self) {
        self.awaits_notification = Some(AwaitedNotification::default());
    }

    const fn holds_its_turn(&self) -> bool {
        self.awaits_notification.is_some()
    }

    /// Whether the subagent holds its turn for the driver to close rather than polling for
    /// its command's notification.
    fn awaits_its_closing(&self) -> bool {
        self.awaits_notification
            .as_ref()
            .is_some_and(|awaited| !awaited.polling)
    }

    /// Whether the subagent's request after its calls is being held.
    fn holds_a_request(&self) -> bool {
        self.awaits_notification
            .as_ref()
            .is_some_and(|awaited| awaited.held.is_some())
    }

    fn answer(&mut self, request: ControlledProviderRequest) {
        if let Some((name, input)) = self.calls.pop_front() {
            assert_the_schema_takes(request.body(), name, &input);
            return request.release_tool_use(name, input);
        }
        match &mut self.awaits_notification {
            None => request.release_text(SUBAGENT_RESPONSE),
            Some(awaited) if !awaited.polling => {
                assert!(
                    awaited.held.replace(request).is_none(),
                    "the subagent asked again while its request was held"
                );
            }
            Some(_) if carries_a_notification(&request) => {
                self.awaits_notification = None;
                request.release_text(SUBAGENT_RESPONSE);
            }
            Some(awaited) => {
                awaited.polls += 1;
                assert!(
                    awaited.polls <= MAXIMUM_NOTIFICATION_POLLS,
                    "the subagent's command notification never reached the subagent"
                );
                request.release_tool_use("Bash", poll_call());
            }
        }
    }

    /// Closes the subagent's held turn with its text instead, leaving its own command
    /// blocked.
    fn close_its_turn(&mut self) {
        let awaited = self
            .awaits_notification
            .take()
            .expect("the subagent does not hold its turn");
        if let Some(held) = awaited.held {
            held.release_text(SUBAGENT_RESPONSE);
        }
    }

    /// Starts the subagent's wait for its command's notification, once the command has
    /// been released and has finished.
    fn poll_for_its_notification(&mut self) {
        let awaited = self
            .awaits_notification
            .as_mut()
            .expect("the subagent does not await a notification");
        awaited.polling = true;
        if let Some(held) = awaited.held.take() {
            self.answer(held);
        }
    }
}

/// One cell's invocation: the loopback serves every thread, so a request is the
/// subagent's when its conversation opens with the subagent's prompt, in either order
/// with the main thread's.
struct Session {
    mode: Mode,
    provider: LoopbackProvider,
    terminal: tokio::sync::oneshot::Receiver<AgentOutcome>,
    observations: RecordingObservationSink,
    subagent: Option<SubagentThread>,
    /// The backgrounded subagent's id from its `Agent` call's result, which `SendMessage`
    /// and `TaskStop` address.
    agent_id: Option<String>,
    first_request_tools: Option<Vec<String>>,
    /// The process groups of the work's commands, which a cancellation must leave quiescent.
    process_groups: Vec<Pid>,
    /// The outcome of an invocation that ended while the driver awaited a turn: a known
    /// gap's rejection.
    ended: Option<AgentOutcome>,
    notes: Vec<String>,
    answered_before_the_notification: Option<bool>,
    /// The enforcement nags the main thread's conversation carried at its latest request, and
    /// how many of them that request added.
    nags_in_conversation: usize,
    nags_in_the_last_request: usize,
    /// The nags `wait_out_turn` answered with text.
    nags_refused: usize,
}

impl Session {
    /// Serves the subagent's requests until the main thread asks, and returns that request.
    async fn next_main_request(&mut self) -> ControlledProviderRequest {
        match self.next_turn_or_end().await {
            Some(request) => request,
            None => panic!(
                "invocation ended while the driver awaited a request: {}",
                describe(self.ended.as_ref().expect("the invocation ended"))
            ),
        }
    }

    /// As `next_main_request`, or `None` once the invocation has ended, keeping its outcome
    /// in `ended`.
    async fn next_turn_or_end(&mut self) -> Option<ControlledProviderRequest> {
        loop {
            let request = tokio::select! {
                request = self.provider.next_request() => request,
                outcome = &mut self.terminal => {
                    self.ended = Some(outcome.unwrap());
                    return None;
                }
            };
            match &mut self.subagent {
                Some(subagent) if request.opens_with(subagent.prompt) => subagent.answer(request),
                _ => {
                    if self.first_request_tools.is_none() {
                        self.first_request_tools = Some(tool_names(request.body()));
                    }
                    let nags = enforcement_nags(request.body());
                    self.nags_in_the_last_request = nags.saturating_sub(self.nags_in_conversation);
                    self.nags_in_conversation = nags;
                    return Some(request);
                }
            }
        }
    }

    /// Serves the subagent while `condition` completes; the main thread must not ask.
    async fn serve_subagent_until<T>(&mut self, condition: impl Future<Output = T>) -> T {
        tokio::select! {
            request = self.next_main_request() => panic!(
                "the main thread asked while it should be blocked: {}",
                last_message(request.body())
            ),
            value = condition => value,
        }
    }

    /// Ends the main thread's turn with the work outstanding: every main-thread request,
    /// Result mode's `[structured-output-enforce]` retries included, is answered with text
    /// until the invocation has completed `turns` turns.
    async fn wait_out_turn(&mut self, turns: usize) {
        let observations = self.observations.clone();
        let completed = wait_for_completed_turns(&observations, turns);
        tokio::pin!(completed);
        loop {
            tokio::select! {
                request = self.next_main_request() => {
                    self.nags_refused += self.nags_in_the_last_request;
                    request.release_text(WAITING);
                }
                () = &mut completed => return,
            }
        }
    }

    /// Serves the subagent until `command` has started, and keeps its process group. The
    /// main thread must not ask meanwhile.
    async fn await_started(&mut self, command: &BlockedCommand) {
        let group = self.serve_subagent_until(command.started()).await;
        self.process_groups.push(group);
    }

    fn subagent(&mut self) -> &mut SubagentThread {
        self.subagent.as_mut().expect("the cell runs a subagent")
    }

    fn close_the_subagents_turn(&mut self) {
        self.subagent().close_its_turn();
    }

    fn completed_turns(&self) -> usize {
        completed_turns(&self.observations)
    }

    /// `09d`'s and `09e`'s followup: messages the finished backgrounded subagent from
    /// `request`'s turn, ends that turn while the resumed run's first request is held (with
    /// `wakeup`, after scheduling a wakeup, whose turn then runs during the resumed run), and
    /// returns the request of the turn the resumed run's notification wakes, or `None` once
    /// the invocation has ended instead (a known gap's rejection). The resumed run's
    /// requests open with the subagent's prompt, and its thread, its calls done, answers
    /// them with its closing text.
    async fn resume(
        &mut self,
        request: ControlledProviderRequest,
        wakeup: bool,
    ) -> Option<ControlledProviderRequest> {
        let agent_id = self
            .agent_id
            .clone()
            .expect("the cell launched a backgrounded subagent");
        let turns = self.completed_turns();
        release_second(
            request,
            "messaging the finished subagent",
            "SendMessage",
            send_message(&agent_id),
        );
        self.subagent().hold_its_turn();
        if wakeup {
            let request = self.next_main_request().await;
            self.schedule_wakeup(request, Round::AnotherRound).await;
        } else {
            self.wait_out_turn(turns + 1).await;
        }
        self.close_the_subagents_turn();
        self.next_turn_or_end().await
    }

    /// The same-round resume: messages the finished backgrounded subagent from `request`'s
    /// turn in the message that answers the turn, the resumed run holding its turn as
    /// `resume`'s does, and returns the main thread's next request, which `drive` answers
    /// before the resumed run is closed (`close_its_held_turn_after`). When the answer ended
    /// the turn instead, the resumed run is closed once held and the request of the turn its
    /// notification wakes is returned, or `None` once the invocation has ended.
    async fn resume_in_the_same_round(
        &mut self,
        request: ControlledProviderRequest,
    ) -> Option<ControlledProviderRequest> {
        let agent_id = self
            .agent_id
            .clone()
            .expect("the cell launched a backgrounded subagent");
        let turns = self.completed_turns();
        self.answer_with(
            request,
            "messaging the finished subagent",
            "SendMessage",
            send_message(&agent_id),
        );
        self.subagent().hold_its_turn();
        let observations = self.observations.clone();
        let ended = wait_for_completed_turns(&observations, turns + 1);
        tokio::pin!(ended);
        tokio::select! {
            request = self.next_turn_or_end() => return request,
            () = &mut ended => {}
        }
        self.notes
            .push("the same-round answer ended the turn without another round".into());
        self.close_its_held_turn_after(turns).await;
        self.next_turn_or_end().await
    }

    /// The in-drain resume answered before the report (AUTO-64): messages the finished
    /// backgrounded subagent from `request`'s turn, and while the main thread's next round
    /// is held, closes the messaged run, whose notification is queued for the main thread
    /// and superseded when Claude Code resumes the subagent once more for its command's
    /// report; holds that resumed run and answers the held round with the mode's value while
    /// it is outstanding. In Result mode the candidate is premature and the adapter's
    /// correction prompt joins the queue behind the superseded notification. The resumed run
    /// is closed once the turn has ended (after the rejection in Result mode), and the main
    /// thread's next request is returned once the parser has seen the run complete, or
    /// `None` once the invocation has ended.
    async fn resume_unawaited(
        &mut self,
        request: ControlledProviderRequest,
    ) -> Option<ControlledProviderRequest> {
        let agent_id = self
            .agent_id
            .clone()
            .expect("the cell launched a backgrounded subagent");
        let turns = self.completed_turns();
        release_second(
            request,
            "messaging the finished subagent",
            "SendMessage",
            send_message(&agent_id),
        );
        self.subagent().hold_its_turn();
        let held = self.next_main_request().await;
        self.serve_the_subagent_until_it_holds_a_request().await;
        self.close_the_subagents_turn();
        self.subagent().hold_its_turn();
        self.serve_the_subagent_until_it_holds_a_request().await;
        let observations = self.observations.clone();
        self.serve_subagent_until(wait_for_subagent_status(&observations, "resumed", 2))
            .await;
        self.settle(held);
        let ended = wait_for_completed_turns(&observations, turns + 1);
        tokio::pin!(ended);
        tokio::select! {
            request = self.next_turn_or_end() => {
                let request = request?;
                panic!("the main thread asked again before its turn ended: {}", last_message(request.body()));
            }
            () = &mut ended => {}
        }
        if self.mode == Mode::Result {
            let rejected = observations.wait_for(|observation| {
                matches!(observation, AgentObservation::ValueRejected { .. })
            });
            tokio::pin!(rejected);
            tokio::select! {
                request = self.next_turn_or_end() => {
                    let request = request?;
                    panic!("the main thread asked before the candidate was rejected: {}", last_message(request.body()));
                }
                () = &mut rejected => {}
            }
        }
        self.close_the_subagents_turn();
        let mut pending = None;
        let completed = wait_for_subagent_status(&observations, "completed", 3);
        tokio::pin!(completed);
        loop {
            tokio::select! {
                request = self.next_turn_or_end() => match request {
                    Some(request) => {
                        assert!(pending.replace(request).is_none(), "the main thread asked twice");
                    }
                    None => return None,
                },
                () = &mut completed => break,
            }
        }
        let request = match pending {
            Some(request) => request,
            None => self.next_turn_or_end().await?,
        };
        if self.mode == Mode::Result {
            self.notes.push(if carries_a_notification(&request) {
                "the queued notification's turn came before the correction turn".into()
            } else {
                "the correction turn came before the queued notification's".into()
            });
        }
        Some(request)
    }

    /// Once the main thread's turn after `turns` has completed, closes the subagent's turn the
    /// driver holds, as soon as its request is held; a no-op when the subagent holds none.
    /// The main thread must not ask meanwhile.
    async fn close_its_held_turn_after(&mut self, turns: usize) {
        if !self
            .subagent
            .as_ref()
            .is_some_and(SubagentThread::awaits_its_closing)
        {
            return;
        }
        let observations = self.observations.clone();
        self.serve_subagent_until(wait_for_completed_turns(&observations, turns + 1))
            .await;
        self.serve_the_subagent_until_it_holds_a_request().await;
        self.close_the_subagents_turn();
    }

    /// Serves the subagent until its thread holds a request; the main thread must not ask
    /// meanwhile.
    async fn serve_the_subagent_until_it_holds_a_request(&mut self) {
        while !self.subagent().holds_a_request() {
            let request = tokio::select! {
                request = self.provider.next_request() => request,
                outcome = &mut self.terminal => panic!(
                    "the invocation ended before the subagent's request was held: {}",
                    describe(&outcome.unwrap())
                ),
            };
            let subagent = self.subagent();
            assert!(
                request.opens_with(subagent.prompt),
                "the main thread asked while the subagent's request was awaited: {}",
                last_message(request.body())
            );
            subagent.answer(request);
        }
    }

    /// A same-round followup: releases the `name` call and the turn's answer as blocks of one
    /// message, the answer after the call in Result mode (`StructuredOutput`) and before it
    /// otherwise (`RESPONSE`). The loopback numbers tool uses by block, as `release_second`
    /// relies on.
    fn answer_with(
        &self,
        request: ControlledProviderRequest,
        preface: &str,
        name: &str,
        input: Value,
    ) {
        request.release_blocks(match self.mode {
            Mode::Result => vec![
                LoopbackBlock::text(preface),
                LoopbackBlock::tool_use(name, input),
                LoopbackBlock::tool_use(STRUCTURED_OUTPUT_TOOL_NAME, json!({"result": RESULT})),
            ],
            Mode::Response | Mode::None => vec![
                LoopbackBlock::text(RESPONSE),
                LoopbackBlock::tool_use(name, input),
            ],
        });
    }

    /// Starts a monitor of `command`, which prints its event before it blocks, from the main
    /// thread's `request`: the `ToolSearch` round that loads the deferred `Monitor`, then the
    /// call, whose input the loaded schema must take. The call is its message's only block,
    /// so its id is not the one a later `ToolSearch` round's call reuses: the loopback numbers
    /// a message's blocks, and a later round's result under the monitor's id would retire it
    /// as a foreground completion.
    async fn start_a_monitor(
        &mut self,
        request: ControlledProviderRequest,
        command: &BlockedCommand,
    ) {
        release_second(request, "loading Monitor", "ToolSearch", load_monitor());
        let request = self.next_main_request().await;
        let input = command.monitor("Watch a controlled long command", true);
        assert_the_schema_takes(request.body(), "Monitor", &input);
        request.release_tool_use("Monitor", input);
    }

    /// Schedules a wakeup from the main thread's `request` while the work still runs, ends
    /// that turn, and waits out the wakeup turn, which the main thread ends with text so the
    /// drain pauses again on the work. In the `SameRound` the call's message also submits
    /// the value, a premature candidate the adapter rejects, so its correction turn, ended
    /// with text too, comes before the wakeup turn. 2.1.310 lists `ScheduleWakeup` in `tools[]`; its
    /// `command_lifecycle` `started` in the drain is what authorizes the wakeup turn's `init`.
    async fn schedule_wakeup(&mut self, request: ControlledProviderRequest, round: Round) {
        // 2.1.310's schema lists no `required`: every field but `stop` is "required unless
        // `stop` is true", which the tool enforces, so the input sets exactly those.
        let schema = tool_schema(request.body(), "ScheduleWakeup");
        let input = json!({"delaySeconds": WAKEUP_DELAY_SECONDS,
            "reason": "Check on the background work",
            "prompt": "Check whether the background work has reported.",
            "noop": true});
        let fields = schema["properties"]
            .as_object()
            .map(|properties| {
                properties
                    .keys()
                    .filter(|field| *field != "stop")
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        assert!(
            input
                .as_object()
                .is_some_and(|input| input.len() == fields.len()
                    && fields.iter().all(|field| input.contains_key(field))),
            "ScheduleWakeup's input schema asks for {fields:?}, not {input}: {schema}"
        );
        let turns = self.completed_turns();
        match round {
            Round::AnotherRound => {
                release_second(request, "scheduling a wakeup", "ScheduleWakeup", input);
            }
            Round::SameRound => {
                self.answer_with(request, "scheduling a wakeup", "ScheduleWakeup", input);
            }
        }
        let rounds = match round {
            Round::AnotherRound => 2,
            Round::SameRound => 3,
        };
        for turn in 1..=rounds {
            self.wait_out_turn(turns + turn).await;
        }
    }

    /// Holds the subagent's foreground `command`, already running, past Claude Code's
    /// two-minute promotion, which `CLAUDE_CODE_AUTO_BACKGROUND_TIMEOUT_MS` does not shorten
    /// for a subagent: the promoted command's `tool_result` reaches the subagent, whose
    /// thread holds that request; the command is then released (see
    /// `release_the_subagents_command`). The main thread must not ask meanwhile; the cell's
    /// watchdog bounds the wait.
    async fn await_the_subagents_promotion(&mut self, command: &BlockedCommand) {
        self.serve_the_subagent_until_it_holds_a_request().await;
        self.release_the_subagents_command(command).await;
    }

    /// Releases the subagent's foreground `command` while its thread holds its turn. A
    /// command Claude Code promoted first has already answered the subagent, whose thread
    /// holds that request, so the subagent polls until the promoted command's notification
    /// reaches it; otherwise the command's foreground result follows the release and the
    /// subagent closes its turn on it.
    async fn release_the_subagents_command(&mut self, command: &BlockedCommand) {
        if self.subagent().holds_a_request() {
            self.notes
                .push("the subagent's foreground command was promoted before its release".into());
            release_while_the_subagent_waits(self, command).await;
        } else {
            assert!(
                command.release().await,
                "the harness stopped the subagent's foreground command before its release"
            );
            self.serve_subagent_until(command.finished()).await;
            self.close_the_subagents_turn();
        }
    }

    /// Stops the work the main thread's `request` sees running with a `TaskStop` call naming
    /// `task_id`, and hands back the turn's next request with `command`, which the harness
    /// must have stopped by the end. 2.1.310 defers `TaskStop`, so the call's input is read
    /// from the schema `ToolSearch` loads. A stopped subagent's `<task-notification>` is
    /// queued for the main thread, so when it is `awaited` the main thread polls until it
    /// arrives and the turn settles with it rather than waking another; otherwise the round
    /// that returns the `TaskStop` result is handed back as it is. In the `SameRound` the call
    /// and the turn's answer are one message, nothing is polled, and the main thread's next
    /// request is handed back, or none once the invocation has ended.
    async fn stop_the_work(
        &mut self,
        request: ControlledProviderRequest,
        task_id: &str,
        awaited: bool,
        round: Round,
        command: BlockedCommand,
    ) -> Handoff {
        release_second(
            request,
            "loading TaskStop",
            "ToolSearch",
            json!({"query": "select:TaskStop", "max_results": 1}),
        );
        let request = self.next_main_request().await;
        let schema = tool_schema(request.body(), "TaskStop");
        assert!(
            schema["properties"]["task_id"].is_object(),
            "TaskStop's input schema names no task_id: {schema}"
        );
        let input = json!({"task_id": task_id});
        if round == Round::SameRound {
            self.answer_with(request, "stopping the work", "TaskStop", input);
            let request = self.next_turn_or_end().await;
            self.close_a_stopped_subagents_turn();
            return Handoff {
                request,
                blocked: vec![command],
            };
        }
        release_second(request, "stopping the work", "TaskStop", input);
        let mut request = self.next_main_request().await;
        self.close_a_stopped_subagents_turn();
        let mut polls = 0;
        while awaited && !carries_a_notification(&request) {
            polls += 1;
            assert!(
                polls <= MAXIMUM_NOTIFICATION_POLLS,
                "the stopped work's notification never reached the main thread"
            );
            release_second(request, "checking on the stopped work", "Bash", poll_call());
            request = self.next_main_request().await;
        }
        Handoff::leaving(Some(request), command)
    }

    /// Closes the turn a stopped subagent held, if it held one.
    fn close_a_stopped_subagents_turn(&mut self) {
        if self
            .subagent
            .as_ref()
            .is_some_and(SubagentThread::holds_its_turn)
        {
            self.close_the_subagents_turn();
        }
    }

    /// Ends the main thread's first turn with `command` still blocked, so the drain pauses on
    /// the work (a turn already ended, as a wakeup cell's are, is not waited for again). A
    /// `CancelInDrain` cell is handed back there, before the release; any other releases the
    /// command and continues with the turn its notification begins.
    async fn end_the_turn_on(&mut self, command: BlockedCommand, ending: Ending) -> Handoff {
        self.wait_out_turn(1).await;
        if ending == Ending::CancelInDrain {
            return Handoff::leaving(None, command);
        }
        assert!(
            command.release().await,
            "the harness stopped the work's command before its release"
        );
        Handoff::turn(self.next_main_request().await)
    }

    /// Answers with the mode's final value, which a `Settle` cell submits only on the turn
    /// after the work reported.
    fn settle(&self, request: ControlledProviderRequest) {
        match self.mode {
            Mode::Result => request.release_structured_output(json!({"result": RESULT})),
            Mode::Response | Mode::None => request.release_text(RESPONSE),
        }
    }

    /// Serves the subagent until the invocation ends. A main-thread request after the final
    /// answer is answered the same way and noted in the cell's line: the outcome decides.
    async fn outcome(&mut self) -> AgentOutcome {
        loop {
            tokio::select! {
                request = self.provider.next_request() => match &mut self.subagent {
                    Some(subagent) if request.opens_with(subagent.prompt) => subagent.answer(request),
                    _ => {
                        self.notes.push(format!(
                            "the main thread asked again after the final answer: {}",
                            last_message(request.body())
                        ));
                        self.settle(request);
                    }
                },
                outcome = &mut self.terminal => return outcome.unwrap(),
            }
        }
    }
}

fn information_diagnostics(observations: &RecordingObservationSink) -> Vec<String> {
    observations
        .snapshot()
        .iter()
        .filter_map(|envelope| match envelope.observation() {
            AgentObservation::Diagnostic {
                level: AgentDiagnosticLevel::Information,
                message,
            } => Some(message.to_string()),
            _ => None,
        })
        .collect()
}

/// The newest message of a request, bounded, for a failure's reason.
fn last_message(body: &Value) -> String {
    let message = body["messages"]
        .as_array()
        .and_then(|messages| messages.last())
        .map(Value::to_string)
        .unwrap_or_default();
    message.chars().take(600).collect()
}

/// The input schema of the tool `name` in the request's `tools[]`.
fn tool_schema(body: &Value, name: &str) -> Value {
    body["tools"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|tool| tool["name"] == name)
        .map(|tool| tool["input_schema"].clone())
        .unwrap_or_else(|| {
            panic!(
                "the request offers no {name}: tools {:?}, {}",
                tool_names(body),
                last_message(body)
            )
        })
}

pub(super) fn tool_names(body: &Value) -> Vec<String> {
    body["tools"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|tool| tool["name"].as_str().map(str::to_owned))
        .collect()
}

pub(super) fn value_mode(mode: Mode) -> AgentValueMode {
    match mode {
        Mode::Response => AgentValueMode::Response {
            output: Arc::from("response"),
        },
        // The `09c` schema: any candidate is checked, and `RESULT` satisfies it.
        Mode::Result => {
            let document = json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "integer",
                "minimum": 1,
            });
            let bytes = Arc::<[u8]>::from(serde_json::to_vec(&document).unwrap());
            AgentValueMode::Result {
                output: Arc::from("result"),
                schema: RetainedJsonSchema::compile(bytes, Arc::new(document)).unwrap(),
            }
        }
        Mode::None => AgentValueMode::None,
    }
}

/// `init["tools"]` of a launch with the mode's production arguments from its own root. The
/// production adapter does not surface `init`, which the release writes before its first
/// provider request, so the probe reads that line and stops the process.
async fn init_tools(executable: &Path, mode: Mode) -> Vec<String> {
    let provider = LoopbackProvider::start().await;
    let root = SyntheticClaudeCodeRoot::new();
    let arguments = match mode {
        Mode::Result => {
            result_mode_arguments(MODEL, "xhigh", INIT_PROBE_SESSION_ID, root.system_prompt())
        }
        Mode::Response | Mode::None => {
            normal_mode_arguments(MODEL, "xhigh", INIT_PROBE_SESSION_ID, root.system_prompt())
        }
    };
    let mut command = Command::new(executable);
    command
        .args(arguments)
        .kill_on_drop(true)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    root.configure_command(&mut command, &provider);
    let mut child = command.spawn().unwrap();
    let mut stdin = child.stdin.take().unwrap();
    stdin
        .write_all(&initial_user_text_frame("Report the lifecycle tools.").unwrap())
        .await
        .unwrap();
    let mut init = String::new();
    BufReader::new(child.stdout.take().unwrap())
        .read_line(&mut init)
        .await
        .unwrap();
    child.kill().await.unwrap();
    drop(stdin);
    provider.shutdown().await;
    let init: Value = serde_json::from_str(&init).unwrap();
    assert_eq!(init["subtype"], "init");
    tool_names_of_init(&init)
}

fn tool_names_of_init(init: &Value) -> Vec<String> {
    init["tools"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|tool| tool.as_str().map(str::to_owned))
        .collect()
}

/// Drives one cell to its outcome. A panic here is the cell's failure.
async fn drive(
    executable: PathBuf,
    cell: LifecycleCell,
    root: Arc<SyntheticClaudeCodeRoot>,
    observations: RecordingObservationSink,
) -> (AgentOutcome, DrivenFacts) {
    let mut facts = DrivenFacts::default();
    if cell.work == Work::Nothing {
        facts.init_tools = Some(init_tools(&executable, cell.mode).await);
    }
    let provider = LoopbackProvider::start().await;
    let running = RunningProductionClaudeCode::launch(
        executable,
        &root,
        &provider,
        value_mode(cell.mode),
        observations.clone(),
    )
    .await_started()
    .await;
    let abandoned = CancelOnDrop {
        cancellation: running.cancellation.clone(),
        process_control: running.process_control.clone(),
    };
    let mut session = Session {
        mode: cell.mode,
        provider,
        terminal: running.terminal,
        observations,
        subagent: None,
        agent_id: None,
        first_request_tools: None,
        process_groups: Vec::new(),
        ended: None,
        notes: Vec::new(),
        answered_before_the_notification: None,
        nags_in_conversation: 0,
        nags_in_the_last_request: 0,
        nags_refused: 0,
    };
    let Handoff { request, blocked } = drive_work(&mut session, cell, &root).await;
    assert_eq!(
        request.is_some(),
        cell.ending != Ending::CancelInDrain && session.ended.is_none(),
        "the work's script stopped at the wrong point for {:?}",
        cell.ending
    );
    let outcome = if let Some(outcome) = session.ended.take() {
        outcome
    } else if cell.ending.cancels() {
        // `05`'s shape in the exchange, `06e`'s in the drain: the pending request, if any,
        // stays unanswered until the invocation has ended, and the drop guard's own request
        // then finds the cancellation already made.
        assert!(
            abandoned
                .cancellation
                .request_cancellation(CancellationReason::UserRequest),
            "the invocation was already cancelled"
        );
        abandoned.process_control.interrupt();
        let outcome = session.outcome().await;
        drop(request);
        outcome
    } else {
        let mut request = Some(request.expect("a settling cell's script ends on the final turn"));
        for _ in 0..cell.followup.resumes() {
            if let Some(turn) = request.take() {
                request = match (cell.followup, cell.round) {
                    (Followup::ResumeUnawaited, _) => session.resume_unawaited(turn).await,
                    (_, Round::AnotherRound) => {
                        session
                            .resume(turn, cell.followup == Followup::ResumeThenWakeup)
                            .await
                    }
                    (_, Round::SameRound) => session.resume_in_the_same_round(turn).await,
                };
            }
        }
        match request {
            Some(request) => {
                let turns = session.completed_turns();
                session.settle(request);
                session.close_its_held_turn_after(turns).await;
                session.outcome().await
            }
            None => session.ended.take().expect("the invocation ended"),
        }
    };
    running.task.await.unwrap();
    assert!(
        !session.provider.has_pending_request(),
        "a provider request was left unanswered"
    );
    // A failed invocation's teardown stops Claude Code, not a command 2.1.310 runs as its own
    // process group, so the command is released and the outcome reported.
    let failed = matches!(outcome, AgentOutcome::Failed(_));
    for command in blocked {
        assert!(
            !command.release().await || failed,
            "a command the harness should have stopped was still waiting at the end"
        );
    }
    // INV-HARNESS-8: the test process is the subreaper only on Linux, where the work's
    // stopped descendants are reaped into the test, so only there is the group's absence
    // observable here.
    #[cfg(target_os = "linux")]
    if cell.ending.cancels() {
        for group in &session.process_groups {
            assert!(
                process_group_is_quiescent(*group),
                "the work's process group {group:?} outlived the cancellation"
            );
        }
    }
    facts.request_tools = session.first_request_tools.take().unwrap_or_default();
    facts.notes = std::mem::take(&mut session.notes);
    facts.answered_before_the_notification = session.answered_before_the_notification;
    facts.nags_refused = session.nags_refused;
    if cell.stop_races_the_correction() {
        let reported = stopped_thread_reported_in_the_turn(&session.observations);
        facts.notes.push(if reported {
            "the stopped subagent's thread reported before the turn's result".into()
        } else {
            "the stopped subagent's thread did not report before the turn's result".into()
        });
    }
    if cell.queues_two_notifications()
        && let Some(followed) = empty_turn_followed_the_notified_turn(&session.observations)
    {
        facts.notes.push(if followed {
            "the superseded notification's empty turn followed the notified turn".into()
        } else {
            "the superseded notification's empty turn preceded the notified turn".into()
        });
    }
    session.provider.shutdown().await;
    (outcome, facts)
}

/// Where a work's script hands the invocation back to `drive`.
struct Handoff {
    /// The pending main-thread request of the final turn, which a `Settle` cell answers and a
    /// `CancelInExchange` cell cancels under; `None` for `CancelInDrain`, whose script stops
    /// once the first turn has ended with the work outstanding.
    request: Option<ControlledProviderRequest>,
    /// The commands still blocked at the handoff, which the harness must have stopped by the
    /// end: by the cancellation, by the `TaskStop` call, or, for a foreground subagent's own
    /// backgrounded command, at the subagent's completion.
    blocked: Vec<BlockedCommand>,
}

impl Handoff {
    fn turn(request: ControlledProviderRequest) -> Self {
        Self {
            request: Some(request),
            blocked: Vec::new(),
        }
    }

    fn leaving(request: Option<ControlledProviderRequest>, blocked: BlockedCommand) -> Self {
        Self {
            request,
            blocked: vec![blocked],
        }
    }
}

/// Serves the cell's work and hands back the final turn's request (see `Handoff`). The
/// dispatch point for every work's script.
async fn drive_work(
    session: &mut Session,
    cell: LifecycleCell,
    root: &SyntheticClaudeCodeRoot,
) -> Handoff {
    match cell.work {
        Work::Nothing => Handoff::turn(session.next_main_request().await),
        Work::ForegroundBash => {
            let command = BlockedCommand::new(root, "main-command");
            session.next_main_request().await.release_tool_use(
                "Bash",
                command.bash("Run a controlled foreground command", false),
            );
            session.await_started(&command).await;
            assert!(
                command.release().await,
                "the harness stopped the main thread's foreground command"
            );
            Handoff::turn(session.next_main_request().await)
        }
        Work::PromotedBash | Work::BackgroundBash => {
            // A promoted command's result carries `backgroundTaskId` after the 2 s
            // timeout, a backgrounded one's at once; either way the turn ends with the
            // command outstanding and its notification begins the next. The main thread
            // may ask before the command reports, so its start is awaited outside the
            // session.
            let command = BlockedCommand::new(root, "main-command");
            session.next_main_request().await.release_tool_use(
                "Bash",
                command.bash(
                    "Run a controlled long command",
                    cell.work == Work::BackgroundBash,
                ),
            );
            let group = command.started().await;
            session.process_groups.push(group);
            let main_thread = session.next_main_request().await;
            continue_over_the_command(session, cell, main_thread, command).await
        }
        Work::Monitor => {
            let command = BlockedCommand::new(root, "main-monitor");
            let request = session.next_main_request().await;
            session.start_a_monitor(request, &command).await;
            let group = command.started().await;
            session.process_groups.push(group);
            let main_thread = session.next_main_request().await;
            continue_over_the_monitor(session, cell, main_thread, command).await
        }
        Work::ForegroundAgent { subagent } => {
            drive_foreground_agent(session, subagent, cell.ending, root).await
        }
        Work::BackgroundAgent { subagent } => {
            drive_background_agent(session, subagent, cell, root).await
        }
    }
}

/// Continues from the main thread's `main_thread` request, the round after the call that
/// left the main thread's `command` running in the background: the followup acts on it
/// (`TaskStop` names the task the call's acknowledgement reports), then the turn ends on it.
async fn continue_over_the_command(
    session: &mut Session,
    cell: LifecycleCell,
    main_thread: ControlledProviderRequest,
    command: BlockedCommand,
) -> Handoff {
    match cell.followup {
        Followup::TaskStop => {
            let task_id = background_task_id(main_thread.body());
            return session
                .stop_the_work(main_thread, &task_id, false, cell.round, command)
                .await;
        }
        Followup::ScheduleWakeup => session.schedule_wakeup(main_thread, cell.round).await,
        _ => main_thread.release_text(WAITING),
    }
    session.end_the_turn_on(command, cell.ending).await
}

/// Continues from the main thread's `main_thread` request, the round after the `Monitor`
/// call whose command has printed its event and blocked. A `CancelInExchange` cell is handed
/// back there. Otherwise the first turn ends with text, paused on the monitor, and the event
/// opens the next turn with nothing on the stream announcing it but the outstanding monitor
/// (REQ-AGENT-69). `TaskStop` stops the monitor from that event turn, naming the task the
/// call's acknowledgement reports; otherwise the event turn ends with text too, the drain
/// pauses on the monitor again, and `end_the_turn_on` continues as for a backgrounded
/// command.
async fn continue_over_the_monitor(
    session: &mut Session,
    cell: LifecycleCell,
    main_thread: ControlledProviderRequest,
    command: BlockedCommand,
) -> Handoff {
    if cell.ending == Ending::CancelInExchange {
        return Handoff::leaving(Some(main_thread), command);
    }
    let task_id = background_task_id(main_thread.body());
    main_thread.release_text(WAITING);
    session.wait_out_turn(1).await;
    let event_turn = session.next_main_request().await;
    if cell.followup == Followup::TaskStop {
        return session
            .stop_the_work(event_turn, &task_id, false, cell.round, command)
            .await;
    }
    event_turn.release_text(WAITING);
    session.wait_out_turn(2).await;
    session.end_the_turn_on(command, cell.ending).await
}

async fn drive_foreground_agent(
    session: &mut Session,
    subagent: SubagentWork,
    ending: Ending,
    root: &SyntheticClaudeCodeRoot,
) -> Handoff {
    let launch = json!({
        "description": FOREGROUND_SUBAGENT_DESCRIPTION,
        "prompt": FOREGROUND_SUBAGENT_PROMPT,
        "subagent_type": "general-purpose",
        "run_in_background": false,
    });
    let command = BlockedCommand::new(root, "subagent-command");
    match subagent {
        SubagentWork::ForegroundBash => {
            session.subagent = Some(SubagentThread::new(
                FOREGROUND_SUBAGENT_PROMPT,
                [(
                    "Bash",
                    command.bash("Run the subagent's foreground command", false),
                )],
            ));
            session
                .next_main_request()
                .await
                .release_tool_use("Agent", launch);
            // The main thread is blocked in the Agent call until the subagent closes.
            session.await_started(&command).await;
            assert!(
                command.release().await,
                "the harness stopped the subagent's foreground command"
            );
            Handoff::turn(session.next_main_request().await)
        }
        SubagentWork::BackgroundBash {
            released: Release::InExchange,
        } => {
            // The main thread is blocked in the Agent call, so its exchange stays active
            // while the subagent waits for its own command's notification.
            session.subagent = Some(
                SubagentThread::new(
                    FOREGROUND_SUBAGENT_PROMPT,
                    [(
                        "Bash",
                        command.bash("Run the subagent's background command", true),
                    )],
                )
                .awaiting_its_notification(),
            );
            session
                .next_main_request()
                .await
                .release_tool_use("Agent", launch);
            session.await_started(&command).await;
            release_while_the_subagent_waits(session, &command).await;
            Handoff::turn(session.next_main_request().await)
        }
        SubagentWork::BackgroundBash {
            released: Release::InDrain,
        }
        | SubagentWork::Monitor => {
            // The subagent holds its turn until its own command or monitor is running, then
            // closes it with the command still blocked, and the main thread continues from the
            // Agent call's result. 2.1.310 stops the command when the subagent completes.
            let calls = if subagent == SubagentWork::Monitor {
                monitor_calls(&command).to_vec()
            } else {
                vec![(
                    "Bash",
                    command.bash("Run the subagent's background command", true),
                )]
            };
            session.subagent = Some(
                SubagentThread::new(FOREGROUND_SUBAGENT_PROMPT, calls).awaiting_its_notification(),
            );
            session
                .next_main_request()
                .await
                .release_tool_use("Agent", launch);
            session.await_started(&command).await;
            session.close_the_subagents_turn();
            let main_thread = session.next_main_request().await;
            match ending {
                Ending::Settle | Ending::CancelInExchange => {
                    Handoff::leaving(Some(main_thread), command)
                }
                Ending::CancelInDrain => {
                    panic!("the generator keeps {subagent:?} from {ending:?}")
                }
            }
        }
        SubagentWork::PromotedBash => {
            // The main thread is blocked in the Agent call, so its exchange stays active
            // through the subagent's promotion and its command's notification.
            session.subagent = Some(
                SubagentThread::new(
                    FOREGROUND_SUBAGENT_PROMPT,
                    [(
                        "Bash",
                        command.bash("Block the subagent past its promotion", false),
                    )],
                )
                .awaiting_its_notification(),
            );
            session
                .next_main_request()
                .await
                .release_tool_use("Agent", launch);
            session.await_started(&command).await;
            session.await_the_subagents_promotion(&command).await;
            Handoff::turn(session.next_main_request().await)
        }
    }
}

async fn drive_background_agent(
    session: &mut Session,
    subagent: SubagentWork,
    cell: LifecycleCell,
    root: &SyntheticClaudeCodeRoot,
) -> Handoff {
    let LifecycleCell {
        followup, ending, ..
    } = cell;
    let command = BlockedCommand::new(root, "subagent-command");
    let thread = match subagent {
        // `09`'s shape: the subagent's foreground command blocks while the main thread ends
        // its turn, and its notification begins the next.
        SubagentWork::ForegroundBash => SubagentThread::new(
            BACKGROUND_SUBAGENT_PROMPT,
            [(
                "Bash",
                command.bash("Block the subagent until released", false),
            )],
        ),
        // The subagent holds its turn once its own command runs: until the command's
        // notification reaches it when the command is released in the exchange, through the
        // main thread's turn when the cell cancels in the drain, so that it is still
        // outstanding then; otherwise it closes its turn with the command blocked.
        SubagentWork::BackgroundBash { .. } => SubagentThread::new(
            BACKGROUND_SUBAGENT_PROMPT,
            [(
                "Bash",
                command.bash("Run the subagent's background command", true),
            )],
        )
        .awaiting_its_notification(),
        // As a command released in the drain: the subagent closes its turn with its monitor
        // still running once the main thread's first turn has ended, or holds it through that
        // turn when the cell cancels in the drain.
        SubagentWork::Monitor => {
            SubagentThread::new(BACKGROUND_SUBAGENT_PROMPT, monitor_calls(&command))
                .awaiting_its_notification()
        }
        // The subagent's foreground command blocks past its two-minute promotion while the
        // main thread's turn has ended, and the subagent holds its turn until the promoted
        // command's notification reaches it.
        SubagentWork::PromotedBash => SubagentThread::new(
            BACKGROUND_SUBAGENT_PROMPT,
            [(
                "Bash",
                command.bash("Block the subagent past its promotion", false),
            )],
        )
        .awaiting_its_notification(),
    };
    let main_thread = launch_background_agent(session, thread, &command).await;
    match followup {
        Followup::TaskStop | Followup::TaskStopUnawaited => {
            let awaited = followup == Followup::TaskStop;
            return stop_the_subagent(session, main_thread, command, awaited, cell.round).await;
        }
        Followup::ScheduleWakeup => {
            // The subagent's foreground command stays blocked through the wakeup, which
            // 2.1.310 fires on a minute boundary 60 s to 120 s after the call and so can
            // outlast the command's two-minute promotion; the subagent holds its turn so that
            // a promoted command still settles under it.
            session.subagent().hold_its_turn();
            session.schedule_wakeup(main_thread, cell.round).await;
            session.release_the_subagents_command(&command).await;
            return Handoff::turn(session.next_main_request().await);
        }
        _ => {}
    }
    match (subagent, ending) {
        (SubagentWork::ForegroundBash, _)
        | (SubagentWork::BackgroundBash { .. } | SubagentWork::Monitor, Ending::CancelInDrain) => {
            main_thread.release_text(WAITING);
            session.end_the_turn_on(command, ending).await
        }
        (
            SubagentWork::BackgroundBash {
                released: Release::InExchange,
            },
            Ending::Settle | Ending::CancelInExchange,
        ) => Handoff::turn(
            hold_the_exchange_while_released(session, root, main_thread, &command).await,
        ),
        (
            SubagentWork::BackgroundBash {
                released: Release::InDrain,
            }
            | SubagentWork::Monitor,
            Ending::CancelInExchange,
        ) => {
            session.close_the_subagents_turn();
            Handoff::leaving(Some(main_thread), command)
        }
        (
            SubagentWork::BackgroundBash {
                released: Release::InDrain,
            }
            | SubagentWork::Monitor,
            Ending::Settle,
        ) => {
            main_thread.release_text(WAITING);
            release_after_the_subagent(session, command).await
        }
        (SubagentWork::PromotedBash, _) => {
            main_thread.release_text(WAITING);
            session.wait_out_turn(1).await;
            session.await_the_subagents_promotion(&command).await;
            Handoff::turn(session.next_main_request().await)
        }
    }
}

/// Launches the backgrounded subagent with `thread` as its script and returns the main
/// thread's next request, unanswered, once the subagent's `command` is running.
async fn launch_background_agent(
    session: &mut Session,
    thread: SubagentThread,
    command: &BlockedCommand,
) -> ControlledProviderRequest {
    session.subagent = Some(thread);
    session
        .next_main_request()
        .await
        .release_tool_use("Agent", BackgroundSubagent::launch_input());
    let main_thread = session.next_main_request().await;
    session.agent_id = Some(launched_agent_id(main_thread.body()));
    session.await_started(command).await;
    main_thread
}

/// Stops the backgrounded subagent, still running its `command`, from the main thread's
/// `request` after its launch, polling for its notification when `awaited`. An unawaited
/// stop hands back the `TaskStop` round for the answer and records whether that round
/// already carried the notification, which the binary decides: when it did not, the queued
/// notification opens an epilogue after the `result`.
async fn stop_the_subagent(
    session: &mut Session,
    request: ControlledProviderRequest,
    command: BlockedCommand,
    awaited: bool,
    round: Round,
) -> Handoff {
    let agent_id = session
        .agent_id
        .clone()
        .expect("the cell launched a backgrounded subagent");
    let handoff = session
        .stop_the_work(request, &agent_id, awaited, round, command)
        .await;
    if !awaited {
        let request = handoff
            .request
            .as_ref()
            .expect("the TaskStop round is handed back for the answer");
        let answered_first = !carries_a_notification(request);
        session.answered_before_the_notification = Some(answered_first);
        session.notes.push(if answered_first {
            "the main thread answered before the stopped subagent's notification reached it".into()
        } else {
            "the stopped subagent's notification reached the main thread's TaskStop round".into()
        });
    }
    handoff
}

/// The backgrounded subagent closes its turn once the main thread's first turn has ended,
/// leaving its own backgrounded `command` blocked, so the subagent's completion alone
/// pauses the drain. The command is released after that completion, once the notified
/// turn has begun, so it outlives the subagent; the notified turn's request is handed back
/// once the command has finished.
async fn release_after_the_subagent(session: &mut Session, command: BlockedCommand) -> Handoff {
    session.wait_out_turn(1).await;
    session.close_the_subagents_turn();
    let Some(notified) = session.next_turn_or_end().await else {
        // The parser refused the drain. 2.1.310 runs every Bash command as its own process
        // group, so the abandoned command would outlive the invocation's teardown.
        command.release().await;
        panic!(
            "the invocation ended before the subagent's completion was delivered: {}",
            describe(session.ended.as_ref().expect("the invocation ended"))
        );
    };
    assert!(
        command.release().await,
        "the harness stopped the subagent's command at the subagent's completion"
    );
    command.finished().await;
    Handoff::turn(notified)
}

/// The id of the background command whose launch the request's newest `tool_result`
/// acknowledges.
fn background_task_id(body: &Value) -> String {
    let message = last_message(body);
    acknowledged_task_id(&message)
        .unwrap_or_else(|| panic!("no background command acknowledged in {message}"))
}

/// The task id in 2.1.310's acknowledgement of a backgrounded command, the text of the
/// `tool_result` that carries its `backgroundTaskId`: a command started in the background,
/// one promoted at its two-minute foreground limit, or a monitor (`Monitor started (task
/// <id>, …)`).
fn acknowledged_task_id(text: &str) -> Option<String> {
    text.split_once("Command running in background with ID: ")
        .or_else(|| text.split_once("was moved to the background (ID: "))
        .or_else(|| text.split_once("Monitor started (task "))
        .map(|(_, rest)| {
            rest.chars()
                .take_while(char::is_ascii_alphanumeric)
                .collect::<String>()
        })
        .filter(|task_id| !task_id.is_empty())
}

/// Releases the subagent's own backgrounded command, already running, while the subagent
/// holds its turn open, and lets the subagent poll until the command's notification
/// reaches it.
async fn release_while_the_subagent_waits(session: &mut Session, command: &BlockedCommand) {
    assert!(
        command.release().await,
        "the harness stopped the subagent's backgrounded command before its release"
    );
    session.serve_subagent_until(command.finished()).await;
    session
        .subagent
        .as_mut()
        .expect("the cell runs a subagent")
        .poll_for_its_notification();
}

/// `09d`'s lead blocker: the main thread holds its turn in a foreground command while the
/// backgrounded subagent's own command is released, so the command's notification lands in
/// the active exchange; once the subagent has completed, the main thread continues.
async fn hold_the_exchange_while_released(
    session: &mut Session,
    root: &SyntheticClaudeCodeRoot,
    main_thread: ControlledProviderRequest,
    command: &BlockedCommand,
) -> ControlledProviderRequest {
    let lead = BlockedCommand::new(root, "lead-command");
    main_thread.release_tool_use(
        "Bash",
        lead.bash("Wait while the subagent's command finishes", false),
    );
    session.await_started(&lead).await;
    release_while_the_subagent_waits(session, command).await;
    let observations = session.observations.clone();
    session
        .serve_subagent_until(observations.wait_for(|observation| {
            matches!(observation, AgentObservation::Diagnostic { message, .. }
                if message.contains(BACKGROUND_SUBAGENT_DESCRIPTION) && message.contains("completed"))
        }))
        .await;
    assert!(
        lead.release().await,
        "the harness stopped the lead's command"
    );
    session.next_main_request().await
}

// ---------------------------------------------------------------------------------------
// Verdicts

fn describe(outcome: &AgentOutcome) -> String {
    match outcome {
        AgentOutcome::Failed(failure) => match failure.protocol_rejection() {
            Some(rejection) => format!(
                "{:?} rejected: {}",
                failure.cause(),
                serde_json::to_value(rejection).unwrap()["detail"]
            ),
            None => format!("{failure:?}"),
        },
        other => format!("{other:?}"),
    }
}

/// Whether a subagent-thread frame (its interruption, recorded as unrecognized) reached the
/// parser after the backgrounded subagent's `stopped` diagnostic and before the turn's
/// `result` completed the turn.
fn stopped_thread_reported_in_the_turn(observations: &RecordingObservationSink) -> bool {
    observations
        .snapshot()
        .iter()
        .map(|envelope| envelope.observation())
        .skip_while(|observation| {
            !matches!(observation, AgentObservation::Diagnostic { message, .. }
                if message.contains(BACKGROUND_SUBAGENT_DESCRIPTION) && message.contains("stopped"))
        })
        .take_while(|observation| {
            !matches!(
                observation,
                AgentObservation::Lifecycle {
                    milestone: AgentLifecycleMilestone::TurnCompleted,
                }
            )
        })
        .any(|observation| {
            matches!(observation, AgentObservation::UnrecognizedHarnessEvent { event }
                if !event["parent_tool_use_id"].is_null())
        })
}

/// Whether the last turn the parser opened, from its `TurnStarted` to its `TurnCompleted`,
/// carried no main-thread message: the superseded notification's empty turn, which makes no
/// model call, came after the notified turn rather than before it. `None` when no turn was
/// opened, so a cell that failed before its first `init` keeps its own failure as its report
/// rather than a panic here.
fn empty_turn_followed_the_notified_turn(observations: &RecordingObservationSink) -> Option<bool> {
    let snapshot = observations.snapshot();
    let observations: Vec<_> = snapshot
        .iter()
        .map(|envelope| envelope.observation())
        .collect();
    let last_turn = observations.iter().rposition(|observation| {
        matches!(
            observation,
            AgentObservation::Lifecycle {
                milestone: AgentLifecycleMilestone::TurnStarted,
            }
        )
    })?;
    let followed = !observations[last_turn..]
        .iter()
        .take_while(|observation| {
            !matches!(
                observation,
                AgentObservation::Lifecycle {
                    milestone: AgentLifecycleMilestone::TurnCompleted,
                }
            )
        })
        .any(|observation| {
            matches!(
                observation,
                AgentObservation::AssistantText { .. } | AgentObservation::ToolCall { .. }
            )
        });
    Some(followed)
}

/// Whether the invocation failed as a protocol failure with the rejection `reason`.
fn rejected_as(outcome: &AgentOutcome, reason: &str) -> bool {
    matches!(outcome, AgentOutcome::Failed(failure)
    if failure.cause() == &AgentFailureCause::HarnessProtocolFailed
        && failure.protocol_rejection().is_some_and(|rejection| {
            serde_json::to_value(rejection).unwrap()["detail"]["reason"] == reason
        }))
}

/// The unrecognized records the lifecycle model's frame-kind table documents for a settling
/// cell on 2.1.310: `background_tasks_changed` for a background launch, or for a resume
/// naming a subagent a wakeup forgot (its `SendMessage` row; one record per invocation stands
/// for both, REQ-AGENT-38), and a `user` frame whose block is not a `tool_result` on a
/// subagent thread (a foreground subagent's prompt, a stopped subagent's interruption) or,
/// in Result mode, the synthetic `[structured-output-enforce]` nag.
fn documented_unrecognized(mode: Mode, event: &Value) -> bool {
    match event["type"].as_str() {
        Some("system") => event["subtype"] == "background_tasks_changed",
        Some("user") => {
            let blocks = event["message"]["content"].as_array();
            let no_tool_result = blocks
                .is_some_and(|blocks| blocks.iter().all(|block| block["type"] != "tool_result"));
            no_tool_result && !event["parent_tool_use_id"].is_null()
                || (mode == Mode::Result
                    && event["isSynthetic"] == true
                    && event["message"]["content"][0]["text"]
                        .as_str()
                        .is_some_and(|text| text.starts_with(ENFORCEMENT_NAG)))
        }
        _ => false,
    }
}

fn verify(
    cell: LifecycleCell,
    outcome: &AgentOutcome,
    observations: &RecordingObservationSink,
    facts: &DrivenFacts,
) -> Result<Verdict, String> {
    if let Expectation::KnownGap { reason, ticket } = cell.expectation() {
        return if rejected_as(outcome, reason) {
            Ok(Verdict::Gap(ticket))
        } else {
            Err(format!(
                "known gap {ticket} expected {reason}, got {}",
                describe(outcome)
            ))
        };
    }
    let completed = match (cell.ending, cell.mode, outcome) {
        (
            Ending::CancelInExchange | Ending::CancelInDrain,
            _,
            AgentOutcome::Cancelled { reason },
        ) => *reason == CancellationReason::UserRequest,
        (
            Ending::Settle,
            Mode::Response,
            AgentOutcome::Completed(CompletedAgentInvocation::Response(response)),
        ) => response.as_str() == RESPONSE,
        (
            Ending::Settle,
            Mode::Result,
            AgentOutcome::Completed(CompletedAgentInvocation::Result(result)),
        ) => result.value() == &json!(RESULT),
        (
            Ending::Settle,
            Mode::None,
            AgentOutcome::Completed(CompletedAgentInvocation::NoValue),
        ) => true,
        _ => false,
    };
    if !completed {
        let expected = if cell.ending.cancels() {
            "the user-requested cancellation".to_owned()
        } else {
            format!("the {:?} completion", cell.mode)
        };
        return Err(format!("expected {expected}, got {}", describe(outcome)));
    }
    let snapshot = observations.snapshot();
    let events = snapshot
        .iter()
        .filter_map(|envelope| match envelope.observation() {
            AgentObservation::UnrecognizedHarnessEvent { event } => Some(&**event),
            _ => None,
        })
        .collect::<Vec<_>>();
    let unrecognized = events
        .into_iter()
        .filter(|event| !documented_unrecognized(cell.mode, event))
        .map(|event| format!("{}/{}", event["type"], event["subtype"]))
        .collect::<Vec<_>>();
    if !unrecognized.is_empty() {
        return Err(format!("unrecognized harness events: {unrecognized:?}"));
    }
    // A cancelled subagent reports `started` alone, a stopped one `stopped`; each resume a
    // further `resumed`, `completed`, and so does Claude Code's own resume that delivers the
    // report of a command the subagent left running (REQ-AGENT-64), unless `TaskStop`
    // stopped the command with the subagent.
    if matches!(cell.work, Work::BackgroundAgent { .. }) && !cell.ending.cancels() {
        let end = if cell.followup.stops_the_work() {
            "stopped"
        } else {
            "completed"
        };
        let mut expected = vec!["started", end];
        for _ in 0..cell.followup.resumes() {
            expected.extend(["resumed", "completed"]);
        }
        if cell.outlives_its_subagent_in_the_drain() && !cell.followup.stops_the_work() {
            expected.extend(["resumed", "completed"]);
        }
        let statuses = backgrounded_subagent_statuses(observations);
        if statuses != expected {
            return Err(format!(
                "backgrounded subagent statuses {statuses:?}, expected {expected:?}"
            ));
        }
    }
    // An answer that went out before the stopped subagent's notification reached the main
    // thread: the queued notification opens one more turn after the `result`, an epilogue
    // (REQ-AGENT-76) that leaves the value settled; a notification that reached the
    // `TaskStop` round settles the turn with it and wakes none.
    if cell.followup == Followup::TaskStopUnawaited {
        let turns = match facts.answered_before_the_notification {
            Some(true) => 2,
            Some(false) => 1,
            None => return Err("the unawaited stop recorded no race outcome".to_owned()),
        };
        let completed = completed_turns(observations);
        if completed != turns {
            return Err(format!("{completed} turns completed, expected {turns}"));
        }
    }
    // A wakeup scheduled while the work runs: Claude Code's `command_lifecycle` `started` in
    // the drain is the parser's `scheduled wakeup`, and its turn ends paused, so the work's
    // notification wakes one turn more.
    if cell.followup.waits_for_a_wakeup() {
        let wakeups = information_diagnostics(observations)
            .iter()
            .filter(|message| message.as_str() == "scheduled wakeup")
            .count();
        if wakeups != 1 {
            return Err(format!(
                "{wakeups} scheduled wakeup diagnostics, expected 1"
            ));
        }
    }
    if let Some(turns) = cell.expected_turns() {
        let completed = completed_turns(observations);
        if completed != turns {
            return Err(format!("{completed} turns completed, expected {turns}"));
        }
    }
    // A Result-mode turn that ends with text while the work runs draws the enforcement nag,
    // which the main thread refuses with text; the drain then pauses on the work.
    if cell.refuses_the_nag() && facts.nags_refused == 0 {
        return Err("0 nags refused, expected at least 1".to_owned());
    }
    if let Some(init_tools) = &facts.init_tools {
        let missing = INIT_LIFECYCLE_TOOLS
            .iter()
            .filter(|tool| !init_tools.iter().any(|name| name == *tool))
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            return Err(format!("init tools {init_tools:?} lack {missing:?}"));
        }
        if !facts.request_tools.iter().any(|name| name == "Agent") {
            return Err(format!(
                "the first request's tools {:?} lack Agent",
                facts.request_tools
            ));
        }
    }
    Ok(Verdict::Passed)
}

/// Keeps a failed cell's native transcript and rejection for its filing; the root's
/// temporary directory is removed with the cell.
fn retain_failure(
    cell: LifecycleCell,
    root: &SyntheticClaudeCodeRoot,
    observations: &RecordingObservationSink,
    outcome: Option<&AgentOutcome>,
    reason: &str,
) -> PathBuf {
    let directory = std::env::temp_dir()
        .join("um-claude-code-lifecycle-matrix")
        .join(cell.name().replace(['/', '[', ']'], "_"));
    std::fs::create_dir_all(&directory).unwrap();
    std::fs::write(directory.join("reason.txt"), reason).unwrap();
    let transcript = root.retained_transcript();
    if transcript.exists() {
        std::fs::copy(transcript, directory.join("transcript.jsonl")).unwrap();
    }
    let observations = observations
        .snapshot()
        .iter()
        .map(|envelope| format!("{:?}\n", envelope.observation()))
        .collect::<String>();
    std::fs::write(directory.join("observations.txt"), observations).unwrap();
    if let Some(AgentOutcome::Failed(failure)) = outcome {
        let rejection = json!({
            "rejection": failure.protocol_rejection(),
            "excerpt": failure.stream_excerpt(),
        });
        std::fs::write(
            directory.join("rejection.json"),
            serde_json::to_vec_pretty(&rejection).unwrap(),
        )
        .unwrap();
    }
    directory
}

#[expect(
    clippy::disallowed_methods,
    reason = "real time is only an anti-hang boundary for each cell's native process"
)]
/// Drives one cell and returns its report (`ok`, `gap <ticket>` or `FAILED <reason>`, then
/// any notes of the driver) and whether it failed.
async fn run_cell(executable: PathBuf, cell: LifecycleCell) -> (String, bool) {
    let root = Arc::new(SyntheticClaudeCodeRoot::with_environment_overrides(
        &cell.environment_overrides(),
    ));
    let observations = RecordingObservationSink::default();
    let watchdog = if cell.is_slow() {
        SLOW_WATCHDOG
    } else {
        WATCHDOG
    };
    let driven = AssertUnwindSafe(tokio::time::timeout(
        watchdog,
        drive(executable, cell, Arc::clone(&root), observations.clone()),
    ))
    .catch_unwind()
    .await;
    let mut notes = Vec::new();
    let mut outcome = None;
    let verdict = match driven {
        Ok(Ok((driven_outcome, facts))) => {
            notes.clone_from(&facts.notes);
            let verdict = verify(cell, &driven_outcome, &observations, &facts);
            outcome = Some(driven_outcome);
            verdict
        }
        Ok(Err(_)) => Err("watchdog expired".to_owned()),
        Err(panic) => Err(panic
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| panic.downcast_ref::<&str>().map(|text| (*text).to_owned()))
            .unwrap_or_else(|| "the driver panicked".to_owned())),
    };
    let (report, failed) = match verdict {
        Ok(Verdict::Passed) => ("ok".to_owned(), false),
        Ok(Verdict::Gap(ticket)) => (format!("gap {ticket}"), false),
        Err(reason) => {
            let retained = retain_failure(cell, &root, &observations, outcome.as_ref(), &reason);
            (
                format!("FAILED {reason} (retained in {})", retained.display()),
                true,
            )
        }
    };
    let notes = notes
        .iter()
        .map(|note| format!("; note: {note}"))
        .collect::<String>();
    (format!("{report}{notes}"), failed)
}

fn selected(cell: &LifecycleCell, filter: Option<&str>) -> bool {
    filter.is_none_or(|filter| {
        let name = cell.name();
        filter
            .split(',')
            .map(str::trim)
            .filter(|fragment| !fragment.is_empty())
            .any(|fragment| name.contains(fragment))
    })
}

// INV-HARNESS-8
// INV-HARNESS-13
// REQ-AGENT-35
// REQ-AGENT-64
// REQ-AGENT-73
// REQ-AGENT-76
// SC-AGENT-7
#[tokio::test]
#[ignore = "requires pinned harness"]
async fn pinned_real_claude_code_11_lifecycle_matrix_drives_every_cell() {
    let (executable, _exclusive) = exclusive_conformance_executable().await;
    // A cancellation cell's quiescence check observes the work's stopped descendants,
    // which are reparented to the test, as `06e` does.
    #[cfg(target_os = "linux")]
    nix::sys::prctl::set_child_subreaper(true).unwrap();
    let filter = std::env::var(CELL_FILTER).ok();
    let skip_slow = std::env::var_os(SKIP_SLOW_CELLS).is_some_and(|value| value == "1");
    let mut failed = Vec::new();
    let mut ran = 0;
    for cell in lifecycle_cells() {
        if !selected(&cell, filter.as_deref()) {
            continue;
        }
        let name = cell.name();
        if skip_slow && cell.is_slow() {
            println!("cell {name}: skipped");
            continue;
        }
        ran += 1;
        let (report, cell_failed) = run_cell(executable.clone(), cell).await;
        println!("cell {name}: {report}");
        if cell_failed {
            failed.push(name);
        }
    }
    println!(
        "lifecycle matrix: {ran} cells driven, {} failed",
        failed.len()
    );
    assert!(ran > 0 || filter.is_some(), "the matrix drove no cell");
    assert!(failed.is_empty(), "lifecycle cells failed: {failed:?}");
}

// INV-HARNESS-13
// REQ-AGENT-35
// REQ-AGENT-69
// REQ-AGENT-73
#[test]
fn lifecycle_matrix_enumerates_every_dimension_value_once() {
    let cells = lifecycle_cells();
    let names = cells
        .iter()
        .map(LifecycleCell::name)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(names.len(), cells.len(), "cell names must be unique");

    for mode in Mode::ALL {
        assert!(cells.iter().any(|cell| cell.mode == mode), "{mode:?}");
    }
    for work in Work::all() {
        assert!(cells.iter().any(|cell| cell.work == work), "{work:?}");
    }
    for subagent in SubagentWork::ALL {
        assert!(
            cells
                .iter()
                .any(|cell| cell.work.subagent() == Some(subagent)),
            "{subagent:?}"
        );
    }
    for followup in Followup::ALL {
        assert!(
            cells.iter().any(|cell| cell.followup == followup),
            "{followup:?}"
        );
    }
    for ending in Ending::ALL {
        assert!(cells.iter().any(|cell| cell.ending == ending), "{ending:?}");
    }
    for round in Round::ALL {
        assert!(cells.iter().any(|cell| cell.round == round), "{round:?}");
    }

    // Combinations the model rules out, stated independently of the rules that prune them.
    assert!(
        !cells
            .iter()
            .any(|cell| cell.mode == Mode::None && cell.ending != Ending::Settle)
    );
    assert!(
        !cells
            .iter()
            .any(|cell| cell.work == Work::Nothing && cell.followup != Followup::None)
    );
    assert!(
        !cells
            .iter()
            .any(|cell| cell.is_slow() && cell.mode != Mode::Result)
    );
    // No followup and no cancellation is driven over a known gap: a work whose plain cell
    // the binary cannot settle is driven alone.
    let is_gap = |cell: &LifecycleCell| matches!(cell.expectation(), Expectation::KnownGap { .. });
    assert!(!cells.iter().any(|cell| {
        (cell.followup != Followup::None || cell.ending.cancels())
            && is_gap(&LifecycleCell {
                followup: Followup::None,
                round: Round::AnotherRound,
                ending: Ending::Settle,
                ..*cell
            })
    }));
    // A monitor is driven on the main thread and inside a subagent (REQ-AGENT-73); a
    // monitoring subagent is paired with no followup or with `TaskStop` alone.
    assert!(cells.iter().any(|cell| cell.work == Work::Monitor));
    assert!(
        cells
            .iter()
            .any(|cell| cell.work.subagent() == Some(SubagentWork::Monitor))
    );
    assert!(!cells.iter().any(|cell| {
        cell.work.subagent() == Some(SubagentWork::Monitor)
            && !matches!(cell.followup, Followup::None | Followup::TaskStop)
    }));
    // A main-thread monitor is background work (REQ-AGENT-35) whose outstanding task
    // announces its event turns (REQ-AGENT-69): it is stopped from its event turn in both
    // rounds and cancelled in the exchange and in the drain it pauses, while a wakeup over it
    // shares `background-bash/schedule-wakeup`'s mechanism and adds no slow cell.
    for round in Round::ALL {
        assert!(
            cells.iter().any(|cell| cell.work == Work::Monitor
                && cell.followup == Followup::TaskStop
                && cell.round == round),
            "{round:?}"
        );
    }
    for ending in [Ending::CancelInExchange, Ending::CancelInDrain] {
        assert!(
            cells
                .iter()
                .any(|cell| cell.work == Work::Monitor && cell.ending == ending),
            "{ending:?}"
        );
    }
    assert!(
        PRUNE_RULES
            .iter()
            .any(|rule| rule.name == "monitor-companions")
    );
    assert!(
        !cells
            .iter()
            .any(|cell| cell.work == Work::Monitor && cell.followup.waits_for_a_wakeup())
    );

    // The follow-up and the turn's answer share a round only for the three single follow-ups
    // (REQ-AGENT-35, REQ-AGENT-64, REQ-AGENT-76), and only a same-round cell's name carries
    // the segment, after the follow-up's.
    assert!(
        PRUNE_RULES
            .iter()
            .any(|rule| rule.name == "same-round-over-a-single-followup")
    );
    assert!(!cells.iter().any(|cell| {
        cell.round == Round::SameRound
            && matches!(
                cell.followup,
                Followup::None
                    | Followup::ResumeTwice
                    | Followup::ResumeThenWakeup
                    | Followup::TaskStopUnawaited
            )
    }));
    for followup in [
        Followup::Resume,
        Followup::TaskStop,
        Followup::ScheduleWakeup,
    ] {
        assert!(
            cells
                .iter()
                .any(|cell| cell.round == Round::SameRound && cell.followup == followup),
            "{followup:?}"
        );
    }
    for cell in &cells {
        let same_round = format!("/{}/same-round/", cell.followup.slug());
        assert_eq!(
            cell.name().contains(&same_round),
            cell.round == Round::SameRound,
            "{}",
            cell.name()
        );
    }
    assert!(names.contains("result/background-bash/task-stop/same-round/settle"));

    // Every rule removes some combination of the product the generator would otherwise
    // produce.
    for rule in &PRUNE_RULES {
        assert!(!rule.reason.is_empty());
        let removes_one = Mode::ALL.into_iter().any(|mode| {
            Work::all().into_iter().any(|work| {
                Followup::ALL.into_iter().any(|followup| {
                    Round::ALL.into_iter().any(|round| {
                        Ending::ALL.into_iter().any(|ending| {
                            let cell = LifecycleCell {
                                mode,
                                work,
                                followup,
                                round,
                                ending,
                            };
                            (rule.forbids)(&cell)
                                && PRUNE_RULES
                                    .iter()
                                    .filter(|other| other.name != rule.name)
                                    .all(|other| !(other.forbids)(&cell))
                        })
                    })
                })
            })
        });
        assert!(removes_one, "rule {} removes nothing on its own", rule.name);
    }
}
