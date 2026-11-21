//! What the serving process keeps of its recent past for the overview (INV-RUNNER-46): a
//! fixed-capacity ring, allocated once, that drops its oldest entry when full, the bound of a
//! text an entry keeps, and the log of the runner's own events. Nothing here is written to
//! disk, sent to Cloud or read by a decision the runner makes.

use std::collections::VecDeque;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use super::control::milliseconds;
use crate::control_protocol::{
    AgentLoginSetupCause, AgentLoginSetupResult, ConnectionFailure, ControlError, OpenWord,
    OverviewEvent, RunnerEventKind,
};
use crate::telemetry::DroppedRecordObserver;

/// The assignments that ended most recently that the manager keeps (REQ-RUNNERENROLL-137).
pub(super) const RECENT_ASSIGNMENTS: usize = 32;

/// The runner's own events that the log keeps (REQ-RUNNERENROLL-138).
pub(super) const RUNNER_EVENTS: usize = 128;

/// The most characters of a text an entry keeps: a longer one is left out, since a step of
/// the run can write it.
const MAXIMUM_KEPT_TEXT: usize = 256;

/// `text` when it has at most `MAXIMUM_KEPT_TEXT` characters, none otherwise.
pub(super) fn kept_text(text: Option<String>) -> Option<String> {
    text.filter(|text| text.chars().nth(MAXIMUM_KEPT_TEXT).is_none())
}

/// At most `capacity` entries in the order they were pushed, its storage allocated once.
#[derive(Debug)]
pub(super) struct Ring<T> {
    entries: VecDeque<T>,
    capacity: usize,
}

impl<T> Ring<T> {
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            entries: VecDeque::with_capacity(capacity),
            capacity,
        }
    }

    /// Keeps `entry` as the newest, dropping the oldest when the ring is full.
    pub(super) fn push(&mut self, entry: T) {
        if self.capacity == 0 {
            return;
        }
        if self.entries.len() >= self.capacity {
            self.entries.pop_front();
        }
        self.entries.push_back(entry);
    }

    /// The entries, the newest first.
    pub(super) fn newest_first(&self) -> impl Iterator<Item = &T> {
        self.entries.iter().rev()
    }

    /// The newest entry, to be changed in place.
    fn newest_mut(&mut self) -> Option<&mut T> {
        self.entries.back_mut()
    }
}

/// One of the runner's own events, with the facts of its kind (REQ-RUNNERENROLL-138).
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum LoggedEvent {
    Connected,
    /// An attempt failed; `next_attempt` is the delay to the next one, none when there is
    /// none.
    ConnectionFailed {
        failure: ConnectionFailure,
        next_attempt: Option<Duration>,
    },
    CredentialReloaded {
        credential_id: String,
    },
    CredentialReloadFailed {
        error: ControlError,
    },
    /// An agent-login pass that `cause` asked for ended as `result`.
    AgentLoginsSetUp {
        cause: AgentLoginSetupCause,
        result: AgentLoginSetupResult,
    },
    /// The runner settled the run interrupted for its lapsed grant of time.
    LeaseLost {
        run_id: String,
        assignment_id: String,
    },
    /// The runner accepted an offer that continues an earlier attempt.
    RunContinued {
        run_id: String,
        assignment_id: String,
    },
    /// The work root held workspaces of an earlier process at the start.
    WorkspacesFound {
        workspaces: u64,
    },
    /// The observation outbox was full and refused an observation.
    ObservationRefused,
    /// The telemetry writer dropped a runner record.
    TelemetryDropped,
    /// The work root's disk turned low: `available_bytes` below `minimum_free_bytes`.
    DiskSpaceLow {
        available_bytes: u64,
        minimum_free_bytes: u64,
    },
    /// The work root's disk stopped being low.
    DiskSpaceRecovered {
        available_bytes: u64,
    },
    /// The runner declined an offer because the work root's disk was low.
    DiskLowOfferDeclined {
        run_id: String,
        assignment_id: String,
    },
    Stopping,
}

impl LoggedEvent {
    /// Whether `newer` is this event once more: the same kind and facts, the delay of a
    /// `connection_failed` aside, since it differs at every attempt.
    fn repeated_by(&self, newer: &Self) -> bool {
        match (self, newer) {
            (
                Self::ConnectionFailed { failure, .. },
                Self::ConnectionFailed {
                    failure: newer_failure,
                    ..
                },
            ) => failure == newer_failure,
            _ => self == newer,
        }
    }
}

#[derive(Clone, Debug)]
struct LoggedEntry {
    /// When the event last happened.
    at: OffsetDateTime,
    /// How many times in a row it happened.
    count: u64,
    event: LoggedEvent,
}

impl LoggedEntry {
    /// The entry as the overview lists it; none when its time cannot be written.
    fn listed(self) -> Option<OverviewEvent> {
        let event = |kind| listed_event(self.at.format(&Rfc3339).ok(), kind, self.count);
        Some(match self.event {
            LoggedEvent::Connected => event(RunnerEventKind::Connected)?,
            LoggedEvent::ConnectionFailed {
                failure,
                next_attempt,
            } => OverviewEvent {
                failure: Some(OpenWord::Known(failure)),
                next_attempt_in_milliseconds: next_attempt.map(milliseconds),
                ..event(RunnerEventKind::ConnectionFailed)?
            },
            LoggedEvent::CredentialReloaded { credential_id } => OverviewEvent {
                credential_id: Some(credential_id),
                ..event(RunnerEventKind::CredentialReloaded)?
            },
            LoggedEvent::CredentialReloadFailed { error } => OverviewEvent {
                error: Some(OpenWord::Known(error)),
                ..event(RunnerEventKind::CredentialReloadFailed)?
            },
            LoggedEvent::AgentLoginsSetUp { cause, result } => OverviewEvent {
                setup_cause: Some(OpenWord::Known(cause)),
                setup_result: Some(OpenWord::Known(result)),
                ..event(RunnerEventKind::AgentLoginsSetUp)?
            },
            LoggedEvent::LeaseLost {
                run_id,
                assignment_id,
            } => OverviewEvent {
                run_id: Some(run_id),
                assignment_id: Some(assignment_id),
                ..event(RunnerEventKind::LeaseLost)?
            },
            LoggedEvent::RunContinued {
                run_id,
                assignment_id,
            } => OverviewEvent {
                run_id: Some(run_id),
                assignment_id: Some(assignment_id),
                ..event(RunnerEventKind::RunContinued)?
            },
            LoggedEvent::WorkspacesFound { workspaces } => OverviewEvent {
                workspaces: Some(workspaces),
                ..event(RunnerEventKind::WorkspacesFound)?
            },
            LoggedEvent::ObservationRefused => event(RunnerEventKind::ObservationRefused)?,
            LoggedEvent::TelemetryDropped => event(RunnerEventKind::TelemetryDropped)?,
            LoggedEvent::DiskSpaceLow {
                available_bytes,
                minimum_free_bytes,
            } => OverviewEvent {
                available_bytes: Some(available_bytes),
                minimum_free_bytes: Some(minimum_free_bytes),
                ..event(RunnerEventKind::DiskSpaceLow)?
            },
            LoggedEvent::DiskSpaceRecovered { available_bytes } => OverviewEvent {
                available_bytes: Some(available_bytes),
                ..event(RunnerEventKind::DiskSpaceRecovered)?
            },
            LoggedEvent::DiskLowOfferDeclined {
                run_id,
                assignment_id,
            } => OverviewEvent {
                run_id: Some(run_id),
                assignment_id: Some(assignment_id),
                ..event(RunnerEventKind::DiskLowOfferDeclined)?
            },
            LoggedEvent::Stopping => event(RunnerEventKind::Stopping)?,
        })
    }
}

/// The listed event of `kind` at `at`, `count` times in a row, with no facts; none without
/// a time.
pub(super) fn listed_event(
    at: Option<String>,
    kind: RunnerEventKind,
    count: u64,
) -> Option<OverviewEvent> {
    Some(OverviewEvent {
        at: at?,
        kind: OpenWord::Known(kind),
        count,
        failure: None,
        next_attempt_in_milliseconds: None,
        credential_id: None,
        error: None,
        setup_cause: None,
        setup_result: None,
        run_id: None,
        assignment_id: None,
        workspaces: None,
        available_bytes: None,
        minimum_free_bytes: None,
    })
}

/// The runner's most recent events (REQ-RUNNERENROLL-138), an event that repeats the newest
/// one folded into it. Recording takes this lock alone, allocates nothing that grows and
/// awaits nothing (INV-RUNNER-46).
#[derive(Debug)]
pub(super) struct EventLog {
    entries: Mutex<Ring<LoggedEntry>>,
}

impl EventLog {
    pub(super) fn new() -> Self {
        Self {
            entries: Mutex::new(Ring::new(RUNNER_EVENTS)),
        }
    }

    /// Records `event` as happening now.
    pub(super) fn record(&self, event: LoggedEvent) {
        self.record_at(useful_support::utc_now(), event);
    }

    /// Records `event` as happening at `at`: once more of the newest entry when it repeats
    /// it, which then takes its time and its delay, and a new entry otherwise. Only the
    /// newest entry is compared, since the list is a timeline.
    pub(super) fn record_at(&self, at: OffsetDateTime, event: LoggedEvent) {
        let mut entries = self.lock();
        if let Some(newest) = entries
            .newest_mut()
            .filter(|newest| newest.event.repeated_by(&event))
        {
            newest.at = at;
            newest.count = newest.count.saturating_add(1);
            newest.event = event;
            return;
        }
        entries.push(LoggedEntry {
            at,
            count: 1,
            event,
        });
    }

    /// The events, the newest first, as the overview lists them.
    pub(super) fn newest_first(&self) -> Vec<OverviewEvent> {
        let entries: Vec<LoggedEntry> = self.lock().newest_first().cloned().collect();
        entries
            .into_iter()
            .filter_map(LoggedEntry::listed)
            .collect()
    }

    fn lock(&self) -> MutexGuard<'_, Ring<LoggedEntry>> {
        self.entries.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Each record the telemetry writer drops is one `telemetry_dropped`, the entry's count the
/// number of records; recording writes no telemetry.
impl DroppedRecordObserver for EventLog {
    fn record_dropped(&self) {
        self.record(LoggedEvent::TelemetryDropped);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // INV-RUNNER-46: a full ring drops its oldest entry, answers the rest newest first and
    // does not grow its storage.
    #[test]
    fn a_full_ring_drops_its_oldest_entry_without_growing() {
        let mut ring = Ring::new(RECENT_ASSIGNMENTS);
        let allocated = ring.entries.capacity();
        for entry in 0..=RECENT_ASSIGNMENTS {
            ring.push(entry);
        }
        let expected: Vec<usize> = (1..=RECENT_ASSIGNMENTS).rev().collect();
        assert_eq!(ring.newest_first().copied().collect::<Vec<_>>(), expected);
        assert_eq!(ring.entries.capacity(), allocated);
    }

    // INV-RUNNER-46: an entry keeps no text longer than 256 characters.
    #[test]
    fn a_text_past_its_bound_is_left_out() {
        let bound = "é".repeat(MAXIMUM_KEPT_TEXT);
        assert_eq!(kept_text(Some(bound.clone())), Some(bound.clone()));
        assert_eq!(kept_text(Some(format!("{bound}e"))), None);
        assert_eq!(kept_text(None), None);
    }

    fn at(second: i64) -> time::OffsetDateTime {
        time::OffsetDateTime::parse(
            "2026-11-19T14:00:00Z",
            &time::format_description::well_known::Rfc3339,
        )
        .unwrap()
            + time::Duration::seconds(second)
    }

    fn rfc3339(at: time::OffsetDateTime) -> String {
        at.format(&time::format_description::well_known::Rfc3339)
            .unwrap()
    }

    fn listed(at: time::OffsetDateTime, kind: RunnerEventKind, count: u64) -> OverviewEvent {
        listed_event(Some(rfc3339(at)), kind, count).unwrap()
    }

    fn connection_failed(failure: ConnectionFailure, delay_milliseconds: u64) -> LoggedEvent {
        LoggedEvent::ConnectionFailed {
            failure,
            next_attempt: Some(Duration::from_millis(delay_milliseconds)),
        }
    }

    // REQ-RUNNERENROLL-138, INV-RUNNER-46: an event that repeats the newest one, a
    // `connection_failed` whatever its delay, is that entry once more, at the later time and
    // with the later delay; one that does not is a new entry, listed first.
    #[test]
    fn a_repeated_event_is_one_entry_with_its_count_its_last_time_and_its_last_delay() {
        let log = EventLog::new();
        for (second, delay) in [(1, 1_000), (2, 2_000), (3, 4_200)] {
            log.record_at(
                at(second),
                connection_failed(ConnectionFailure::Network, delay),
            );
        }
        log.record_at(at(4), LoggedEvent::Connected);
        assert_eq!(
            log.newest_first(),
            [
                listed(at(4), RunnerEventKind::Connected, 1),
                OverviewEvent {
                    failure: Some(OpenWord::Known(ConnectionFailure::Network)),
                    next_attempt_in_milliseconds: Some(4_200),
                    ..listed(at(3), RunnerEventKind::ConnectionFailed, 3)
                },
            ]
        );
    }

    // REQ-RUNNERENROLL-138, INV-RUNNER-46: an event of another fact, or one that repeats an
    // entry other than the newest, is a new entry.
    #[test]
    fn an_event_of_other_facts_or_after_another_is_a_new_entry() {
        let log = EventLog::new();
        log.record_at(at(1), connection_failed(ConnectionFailure::Network, 1_000));
        log.record_at(
            at(2),
            connection_failed(ConnectionFailure::RateLimited, 2_000),
        );
        log.record_at(at(3), connection_failed(ConnectionFailure::Network, 4_000));
        for (second, credential_id) in [
            (4, "rrc_01k0z6r1w8f4jy2m7q9v3x5abd"),
            (5, "rrc_01k0z6r1w8f4jy2m7q9v3x5abe"),
        ] {
            log.record_at(
                at(second),
                LoggedEvent::CredentialReloaded {
                    credential_id: credential_id.to_owned(),
                },
            );
        }
        let listed: Vec<_> = log
            .newest_first()
            .into_iter()
            .map(|event| (event.count, event.failure, event.credential_id))
            .collect();
        assert_eq!(
            listed,
            [
                (1, None, Some("rrc_01k0z6r1w8f4jy2m7q9v3x5abe".to_owned())),
                (1, None, Some("rrc_01k0z6r1w8f4jy2m7q9v3x5abd".to_owned())),
                (1, Some(OpenWord::Known(ConnectionFailure::Network)), None),
                (
                    1,
                    Some(OpenWord::Known(ConnectionFailure::RateLimited)),
                    None
                ),
                (1, Some(OpenWord::Known(ConnectionFailure::Network)), None),
            ]
        );
    }

    // REQ-RUNNERENROLL-138, INV-RUNNER-46: the log keeps the 128 newest distinct events.
    #[test]
    fn the_log_keeps_its_newest_events_and_drops_the_oldest() {
        let log = EventLog::new();
        let distinct = u64::try_from(RUNNER_EVENTS).unwrap() + 1;
        for workspaces in 0..distinct {
            log.record_at(at(1), LoggedEvent::WorkspacesFound { workspaces });
        }
        let listed: Vec<_> = log
            .newest_first()
            .into_iter()
            .map(|event| event.workspaces)
            .collect();
        let expected: Vec<_> = (1..distinct).rev().map(Some).collect();
        assert_eq!(listed, expected);
    }

    // REQ-RUNNERENROLL-138: each record the telemetry writer drops is one occurrence of
    // `telemetry_dropped`, so three in a row are one entry with count 3.
    #[test]
    fn the_log_told_of_three_dropped_records_lists_one_entry_of_three() {
        let log = EventLog::new();
        for _ in 0..3 {
            DroppedRecordObserver::record_dropped(&log);
        }
        let listed: Vec<_> = log
            .newest_first()
            .into_iter()
            .map(|event| (event.kind, event.count))
            .collect();
        assert_eq!(
            listed,
            [(OpenWord::Known(RunnerEventKind::TelemetryDropped), 3)]
        );
    }
}
