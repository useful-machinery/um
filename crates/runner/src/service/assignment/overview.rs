//! The manager's overview of what it holds (REQ-RUNNERENROLL-134): one entry per assignment,
//! from what its offer said, which the manager keeps by assignment ID from the offer to the
//! assignment's end, the time Cloud granted a running one and the last read of its loop; and
//! the assignments that ended most recently, which the manager keeps in a ring of its own where
//! it ends their `runner.run` records (REQ-RUNNERENROLL-137, INV-RUNNER-46).
//!
//! The manager's read of one assignment gives the entry the overview lists for it, with where
//! a running tree run's directory and log are, as its activity published them, or a running
//! single-workflow run's view (REQ-RUNNERENROLL-144).
//!
//! Each read applies the manager's queued events as `status` does and changes nothing else:
//! under the manager's lock it takes only each activity's lock, each view's lock to copy its
//! steps in flight, each grant's watch value and the outbox's lock, projects no view, opens no
//! file and waits on nothing (INV-RUNNER-45).

use super::super::history::kept_text;
use super::super::tree_run::{RunPaths, RunRead};
use super::*;
use crate::control_protocol::{
    AssignmentPhase, HeldRunState, LeaseHealth, LiveStepState, MAXIMUM_OVERVIEW_ASSIGNMENTS,
    OpenWord, OverviewAssignment, OverviewCounts, OverviewLease, OverviewRecentAssignment,
    OverviewRunningStep, RunResult,
};
use serde::Deserialize as _;
use serde::de::IntoDeserializer as _;
use useful_execution::LiveRunningSteps;

/// What an offer said that the overview gives, kept from the offer to the assignment's end.
pub(super) struct OfferFacts {
    run_id: String,
    attempt_number: u64,
    workflow_path: String,
    run_url: Option<String>,
    continues_earlier_attempt: bool,
    /// When the runner held the offer, on the manager's clock.
    held_at: Instant,
    /// The subject the run's loop last read when the run ended; none before.
    subject: Option<String>,
    /// When the run settled, on the manager's clock and on the wall clock; none before.
    settled_at: Option<(Instant, time::OffsetDateTime)>,
}

/// The assignments of an overview, taken in one read of the manager.
#[derive(Debug)]
pub(in crate::service) struct AssignmentOverview {
    /// The counts `status` gives, from the same read.
    pub(in crate::service) counts: OverviewCounts,
    /// The working entries and then the waiting ones, each group by assignment ID, then the
    /// finishing ones and the reporting ones; at most `MAXIMUM_OVERVIEW_ASSIGNMENTS`.
    pub(in crate::service) assignments: Vec<OverviewAssignment>,
    /// The entries left out past the bound.
    pub(in crate::service) assignments_omitted: u64,
    /// The runner's reports Cloud has not acknowledged.
    pub(in crate::service) unacknowledged_reports: u64,
    /// The assignments that ended and that the manager holds no longer, newest first
    /// (REQ-RUNNERENROLL-137).
    pub(in crate::service) recent_assignments: Vec<OverviewRecentAssignment>,
}

impl AssignmentManager {
    /// Keeps what `offer` said, as the runner holds it now.
    pub(super) fn keep_offer_facts(&mut self, offer: &AssignmentOffer) {
        let spec = &offer.execution_spec;
        let facts = OfferFacts {
            run_id: offer.run_id.clone(),
            attempt_number: offer.attempt_number,
            workflow_path: spec
                .workflow_tree
                .as_ref()
                .map_or(&spec.workflow_definition_source.workflow_path, |tree| {
                    &tree.workflow_path
                })
                .clone(),
            run_url: spec
                .workflow_tree
                .as_ref()
                .and_then(|tree| tree.run_url.clone()),
            continues_earlier_attempt: offer.continuation.is_some(),
            held_at: self.sleeper.now(),
            subject: None,
            settled_at: None,
        };
        self.offer_facts.insert(offer.assignment_id.clone(), facts);
    }

    /// Keeps, beside its offer's facts as `running`'s run settles, the subject of its last
    /// read and when it settled.
    pub(super) fn keep_settled_run(&mut self, running: &RunningAssignment) {
        let subject = running
            .activity
            .as_ref()
            .and_then(|activity| activity.last_read())
            .and_then(|read| read.subject);
        let settled_at = (self.sleeper.now(), self.sleeper.utc_now());
        if let Some(facts) = self.offer_facts.get_mut(&running.identity.assignment_id) {
            facts.subject = subject;
            facts.settled_at = Some(settled_at);
        }
    }

    /// Lists `assignment_id`, whose `runner.run` record ended as `end`, as the newest recent
    /// assignment, from its offer's facts; one whose facts are not kept is not listed.
    pub(super) fn keep_recent_assignment(&mut self, assignment_id: &str, end: RunEnd) {
        let Some(facts) = self.offer_facts.get(assignment_id) else {
            return;
        };
        let (settled, settled_at) = facts
            .settled_at
            .unwrap_or_else(|| (self.sleeper.now(), self.sleeper.utc_now()));
        let Ok(ended_at) = settled_at.format(&time::format_description::well_known::Rfc3339) else {
            return;
        };
        self.recent_assignments.push(OverviewRecentAssignment {
            assignment_id: assignment_id.to_owned(),
            run_id: facts.run_id.clone(),
            workflow_path: facts.workflow_path.clone(),
            subject: kept_text(facts.subject.clone()),
            result: run_result(end.result),
            chosen_outcome: kept_text(end.chosen_outcome),
            failure_code: kept_text(end.failure_code),
            interruption_cause: kept_text(end.interruption_cause),
            ended_at,
            held_milliseconds: milliseconds(settled.saturating_duration_since(facts.held_at)),
        });
    }

    /// Drops what the offers of assignments neither held nor reporting said.
    pub(super) fn forget_ended_offer_facts(&mut self) {
        let (held, reporting) = (&self.held, &self.reporting);
        self.offer_facts.retain(|assignment_id, _| {
            held.contains_key(assignment_id) || reporting.contains_key(assignment_id)
        });
    }

    /// One entry per assignment the manager holds and per ended one still reporting, with
    /// the counts `status_assignments` gives, the running and waiting ones taken from the
    /// same read as the entries (REQ-RUNNERENROLL-134). An
    /// assignment whose offer's facts are not kept is left out.
    pub(in crate::service) fn overview_assignments(&mut self) -> AssignmentOverview {
        let (counts, _) = self.status_assignments();
        let now = self.sleeper.now();
        let lease_now = self.lease_clock.now().ok();
        let mut working = Vec::new();
        let mut waiting = Vec::new();
        let mut finishing = Vec::new();
        // One read of each run's wait flag decides both its count and its entry, so a loop that
        // begins or ends a wait during the walk cannot set the two apart.
        let mut running_count: u64 = 0;
        let mut waiting_count: u64 = 0;
        for (assignment_id, held) in &self.held {
            let is_working = held.is_working();
            if matches!(held, HeldAssignment::Running(_)) {
                let count = if is_working {
                    &mut running_count
                } else {
                    &mut waiting_count
                };
                *count = count.saturating_add(1);
            }
            let Some(facts) = self.offer_facts.get(assignment_id) else {
                continue;
            };
            let (group, entry) = held_entry(facts, assignment_id, held, is_working, now, lease_now);
            match group {
                EntryGroup::Working => working.push(entry),
                EntryGroup::Waiting => waiting.push(entry),
                EntryGroup::Finishing => finishing.push(entry),
            }
        }
        let reporting = self.reporting.keys().filter_map(|assignment_id| {
            self.offer_facts
                .get(assignment_id)
                .map(|facts| facts.entry(assignment_id, AssignmentPhase::Reporting, now))
        });
        let mut assignments: Vec<_> = working
            .into_iter()
            .chain(waiting)
            .chain(finishing)
            .chain(reporting)
            .collect();
        let omitted = assignments
            .len()
            .saturating_sub(MAXIMUM_OVERVIEW_ASSIGNMENTS);
        assignments.truncate(MAXIMUM_OVERVIEW_ASSIGNMENTS);
        let unacknowledged_reports = self.outbox.lock().entries.len();
        // An assignment whose record ended while it releases its workspace or reports is
        // listed as held until it is held no longer.
        let recent_assignments = self
            .recent_assignments
            .newest_first()
            .filter(|ended| {
                !self.held.contains_key(&ended.assignment_id)
                    && !self.reporting.contains_key(&ended.assignment_id)
            })
            .cloned()
            .collect();
        AssignmentOverview {
            counts: OverviewCounts {
                preparing: counts.preparing,
                accepted: counts.accepted,
                running: running_count,
                waiting: waiting_count,
                finishing: counts.finishing,
                reporting: counts.reporting,
            },
            assignments,
            assignments_omitted: u64::try_from(omitted).unwrap_or(u64::MAX),
            unacknowledged_reports: u64::try_from(unacknowledged_reports).unwrap_or(u64::MAX),
            recent_assignments,
        }
    }

    /// The entry of `assignment_id`, exactly as `overview_assignments` lists it, with where its
    /// run's directory and log are, its run's view and the wall-clock time of the read
    /// (REQ-RUNNERENROLL-144); `None` when the manager neither holds it nor still reports it,
    /// or its offer's facts are not kept. The paths are a running tree run's, as its activity
    /// published them, the view a running single-workflow run's, and both none for every other
    /// state. Like the overview, the read applies the queued events first, opens no file and
    /// projects no view (INV-RUNNER-45).
    pub(in crate::service) fn read_assignment(
        &mut self,
        assignment_id: &str,
    ) -> Option<AssignmentRead> {
        // Applies the queued events, as the overview's read does.
        self.status_assignments();
        let facts = self.offer_facts.get(assignment_id)?;
        let now = self.sleeper.now();
        let (entry, paths, view) = match self.held.get(assignment_id) {
            Some(held) => {
                let lease_now = self.lease_clock.now().ok();
                let (_, entry) = held_entry(
                    facts,
                    assignment_id,
                    held,
                    held.is_working(),
                    now,
                    lease_now,
                );
                let (paths, view) = match held {
                    HeldAssignment::Running(running) => (
                        running
                            .activity
                            .as_ref()
                            .map(|activity| activity.paths())
                            .unwrap_or_default(),
                        running.view.clone(),
                    ),
                    _ => (RunPaths::default(), None),
                };
                (entry, paths, view)
            }
            None if self.reporting.contains_key(assignment_id) => (
                facts.entry(assignment_id, AssignmentPhase::Reporting, now),
                RunPaths::default(),
                None,
            ),
            None => return None,
        };
        Some(AssignmentRead {
            entry,
            paths,
            view,
            now: self.sleeper.utc_now(),
        })
    }
}

/// One assignment as the manager's read gives it (REQ-RUNNERENROLL-144).
#[derive(Debug)]
pub(in crate::service) struct AssignmentRead {
    /// The entry the overview lists for it.
    pub(in crate::service) entry: OverviewAssignment,
    /// Where its run's directory and log are; none but a running tree run's.
    pub(in crate::service) paths: RunPaths,
    /// Its run's view, which the read hands on unprojected; none but a running
    /// single-workflow run's.
    pub(in crate::service) view: Option<AssignmentRunView>,
    /// The wall-clock time of the read.
    pub(in crate::service) now: time::OffsetDateTime,
}

/// Where an overview lists a held assignment's entry.
enum EntryGroup {
    Working,
    Waiting,
    Finishing,
}

/// The entry of `assignment_id`, held as `held` and working as `is_working` says, at `now` and,
/// for its lease, at `lease_now`, and where the overview lists it: the phase, `activity`, the
/// time Cloud granted a running one and its loop's last read, or a single-workflow run's steps
/// in flight as its view holds them (REQ-RUNNERENROLL-134).
fn held_entry(
    facts: &OfferFacts,
    assignment_id: &str,
    held: &HeldAssignment,
    is_working: bool,
    now: Instant,
    lease_now: Option<LeaseInstant>,
) -> (EntryGroup, OverviewAssignment) {
    let entry = |phase| facts.entry(assignment_id, phase, now);
    match held {
        HeldAssignment::Preparing(_) => (
            EntryGroup::Working,
            active(entry(AssignmentPhase::Preparing), HeldRunState::Working),
        ),
        HeldAssignment::Accepted(_) => (
            EntryGroup::Working,
            active(entry(AssignmentPhase::Accepted), HeldRunState::Working),
        ),
        HeldAssignment::Running(running) => {
            let lease = lease_now.and_then(|now| lease(&running.authority_updates.borrow(), now));
            let read = running
                .activity
                .as_ref()
                .and_then(|activity| activity.last_read());
            let (phase, state, group) = if is_working {
                (
                    AssignmentPhase::Running,
                    HeldRunState::Working,
                    EntryGroup::Working,
                )
            } else {
                (
                    AssignmentPhase::Waiting,
                    HeldRunState::Waiting,
                    EntryGroup::Waiting,
                )
            };
            let entry = with_loop_read(active(entry(phase), state), read);
            let entry = match &running.view {
                Some(view) => with_running_steps(entry, view.running_steps()),
                None => entry,
            };
            (group, OverviewAssignment { lease, ..entry })
        }
        HeldAssignment::Finishing(_) | HeldAssignment::Releasing(_) => {
            (EntryGroup::Finishing, entry(AssignmentPhase::Finishing))
        }
    }
}

/// The overview's word for a `runner.run` record's result, which is kept as written when this
/// release does not know it.
fn run_result(result: &str) -> OpenWord<RunResult> {
    match result {
        "succeeded" => RunResult::Succeeded.into(),
        "failed" => RunResult::Failed.into(),
        "cancelled" => RunResult::Cancelled.into(),
        "interrupted" => RunResult::Interrupted.into(),
        "aborted" => RunResult::Aborted.into(),
        "rejected" => RunResult::Rejected.into(),
        "fenced" => RunResult::Fenced.into(),
        other => OpenWord::Unknown(other.to_owned()),
    }
}

impl OfferFacts {
    /// The entry of `assignment_id` in `phase` at `now`: what the offer said, and the subject
    /// kept when its run ended.
    fn entry(
        &self,
        assignment_id: &str,
        phase: AssignmentPhase,
        now: Instant,
    ) -> OverviewAssignment {
        OverviewAssignment {
            assignment_id: assignment_id.to_owned(),
            run_id: self.run_id.clone(),
            attempt_number: self.attempt_number,
            workflow_path: self.workflow_path.clone(),
            phase: phase.into(),
            activity: None,
            held_milliseconds: milliseconds(now.saturating_duration_since(self.held_at)),
            continues_earlier_attempt: self.continues_earlier_attempt,
            lease: None,
            subject: self.subject.clone(),
            run_url: self.run_url.clone(),
            position: None,
            wait: None,
            running_steps: Vec::new(),
            running_steps_omitted: 0,
        }
    }
}

/// `entry` listed `working` or `waiting`, as `status` lists its run.
fn active(entry: OverviewAssignment, state: HeldRunState) -> OverviewAssignment {
    OverviewAssignment {
        activity: Some(state.into()),
        ..entry
    }
}

/// `entry` with what its run's loop last read; as it is before the loop's first read and for a
/// run on the embedded engine, which has no loop.
fn with_loop_read(entry: OverviewAssignment, read: Option<RunRead>) -> OverviewAssignment {
    let Some(read) = read else {
        return entry;
    };
    OverviewAssignment {
        subject: read.subject,
        position: Some(read.position),
        wait: read.wait,
        running_steps: read.running_steps,
        running_steps_omitted: read.running_steps_omitted,
        ..entry
    }
}

/// `entry` with a single-workflow run's steps in flight, the root's own: each in its run's one
/// attempt, its state read as the wire reads one.
fn with_running_steps(entry: OverviewAssignment, running: LiveRunningSteps) -> OverviewAssignment {
    let LiveRunningSteps { steps, omitted } = running;
    OverviewAssignment {
        running_steps: steps
            .into_iter()
            .map(|step| OverviewRunningStep {
                run: Vec::new(),
                run_attempt: 1,
                step: step.id,
                state: live_step_state(step.state),
            })
            .collect(),
        running_steps_omitted: u64::try_from(omitted).unwrap_or(u64::MAX),
        ..entry
    }
}

/// The wire's word for a step in flight's `state`, kept as written when this release does not
/// know it.
fn live_step_state(state: String) -> OpenWord<LiveStepState> {
    let read: Result<OpenWord<LiveStepState>, serde::de::value::Error> =
        OpenWord::deserialize(state.as_str().into_deserializer());
    read.unwrap_or(OpenWord::Unknown(state))
}

/// The health of `authority` at `now` and the time left before the runner would begin
/// cancelling the run, zero once it has begun; none when the clock cannot compare the two.
fn lease(authority: &LeaseAuthority, now: LeaseInstant) -> Option<OverviewLease> {
    let reached = |instant: LeaseInstant| {
        now.checked_cmp(instant)
            .ok()
            .map(|order| order != std::cmp::Ordering::Less)
    };
    let cancelling = reached(authority.cancellation_start)?;
    let health = if authority.revoked {
        LeaseHealth::Revoked
    } else if cancelling {
        LeaseHealth::Expiring
    } else if reached(authority.renewal_request)? {
        LeaseHealth::Renewing
    } else {
        LeaseHealth::Ok
    };
    let time_left = if cancelling {
        Duration::ZERO
    } else {
        authority
            .cancellation_start
            .checked_duration_since(now)
            .ok()?
    };
    Some(OverviewLease {
        health: health.into(),
        time_left_milliseconds: milliseconds(time_left),
    })
}

fn milliseconds(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}
