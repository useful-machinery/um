//! Whether a held tree run is working, as agent-login setup reads it (INV-RUNNER-15).
//!
//! A run is working unless its loop last reported a wait (REQ-GRANT-40): its `RunActivity`
//! starts working, reads waiting once the loop's report of a wait is queued, and working
//! again at the next report without one and when the loop ends. It also counts the run's
//! `useful` commands in flight, and a setup that installs or removes a login holds it, so
//! no command starts until the login is written. The assignment manager reads it too, to
//! take an offer only while every run it holds waits and to fence the reports of an ended
//! run only when it never reported a wait (INV-RUNNER-40), and an admission's own setup
//! reads the activities of the other runs held when its preparation began.

use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};

use crate::agent_login::setup::AssignmentGate;

/// One held run's activity, shared by its tree run and the assignment manager.
pub(in crate::service) struct RunActivity {
    state: Mutex<ActivityState>,
    /// Signalled when a setup lets go of the activity.
    released: Condvar,
}

#[derive(Default)]
struct ActivityState {
    /// Whether the loop's last report carried a wait.
    waiting: bool,
    /// Whether any report of the loop carried a wait; never cleared.
    reported_wait: bool,
    /// The run's `useful` commands started and not yet reaped or dropped.
    in_flight: usize,
    /// Whether a setup holds the activity while it installs or removes a login.
    held: bool,
}

impl RunActivity {
    /// A run's activity before its loop reports anything: working.
    pub(in crate::service) fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(ActivityState::default()),
            released: Condvar::new(),
        })
    }

    fn state(&self) -> MutexGuard<'_, ActivityState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Records the loop's last report: waiting exactly when it carried a wait.
    pub(in crate::service) fn reported(&self, waiting: bool) {
        let mut state = self.state();
        state.waiting = waiting;
        state.reported_wait |= waiting;
    }

    /// Sets the run working when the answer is dropped, as the loop's end does.
    pub(in crate::service) fn working_at_end(&self) -> WorkingAtEnd<'_> {
        WorkingAtEnd(self)
    }

    /// Whether the loop last reported a wait.
    pub(in crate::service) fn is_waiting(&self) -> bool {
        self.state().waiting
    }

    /// Whether the loop ever reported a wait, which Cloud may have answered by offering the
    /// runner another run before the end of this one is settled (INV-RUNNER-40).
    pub(in crate::service) fn has_reported_wait(&self) -> bool {
        self.state().reported_wait
    }

    /// Counts a command of the run in flight until the answer is dropped, first waiting,
    /// blocking the calling thread, while a setup holds the activity. Call it on a blocking
    /// worker, before the command's environment is built.
    pub(in crate::service) fn start_command(self: &Arc<Self>) -> CommandInFlight {
        let mut state = self
            .released
            .wait_while(self.state(), |state| state.held)
            .unwrap_or_else(PoisonError::into_inner);
        state.in_flight += 1;
        drop(state);
        CommandInFlight(Arc::clone(self))
    }

    /// Holds the activity for a setup's install or removal while the run waits with no
    /// command in flight, so none starts until the hold is dropped; `None` otherwise.
    fn hold(self: &Arc<Self>) -> Option<ActivityHold> {
        let mut state = self.state();
        if !state.waiting || state.in_flight > 0 || state.held {
            return None;
        }
        state.held = true;
        drop(state);
        Some(ActivityHold(Arc::clone(self)))
    }
}

/// The end of a run's loop, which leaves the run working when dropped.
pub(in crate::service) struct WorkingAtEnd<'a>(&'a RunActivity);

impl Drop for WorkingAtEnd<'_> {
    fn drop(&mut self) {
        self.0.reported(false);
    }
}

/// A command of a run in flight, counted until it is dropped.
pub(in crate::service) struct CommandInFlight(Arc<RunActivity>);

impl Drop for CommandInFlight {
    fn drop(&mut self) {
        let mut state = self.0.state();
        state.in_flight = state.in_flight.saturating_sub(1);
    }
}

/// A setup's hold on a waiting run's activity; dropping it lets the run's next command
/// start.
struct ActivityHold(Arc<RunActivity>);

impl Drop for ActivityHold {
    fn drop(&mut self) {
        self.0.state().held = false;
        self.0.released.notify_all();
    }
}

/// What the runner holds, as agent-login setup reads it from the assignment manager, and
/// the gate of a setup's pass over it (INV-RUNNER-15). The pass an admission makes for itself
/// reads the other tree runs the manager held when the preparation began, without its lock,
/// as `Waiting` of their activities, which answers at once when there is none
/// (REQ-GRANT-49).
pub(in crate::service) enum HeldRuns {
    /// No assignment in any state.
    Nothing,
    /// Only runs whose loops reported a wait when they were read, with their activities,
    /// each of which must still read waiting for the pass to act.
    Waiting(Vec<Arc<RunActivity>>),
    /// An assignment that is not a waiting tree run.
    Working,
}

impl HeldRuns {
    /// The read level, which a request for a delivery and a login check ask: no held run is
    /// working.
    pub(in crate::service) fn none_working(&self) -> bool {
        match self {
            Self::Nothing => true,
            Self::Waiting(activities) => activities.iter().all(|activity| activity.is_waiting()),
            Self::Working => false,
        }
    }

    /// The write level, which an install or a removal asks: runs `change` only while no held
    /// run is working and none has a command in flight, holding each run's activity while it
    /// runs, and answers whether it ran.
    pub(in crate::service) fn while_still(&self, change: &mut dyn FnMut()) -> bool {
        let holds = match self {
            Self::Nothing => Vec::new(),
            Self::Waiting(activities) => {
                let Some(holds) = activities
                    .iter()
                    .map(RunActivity::hold)
                    .collect::<Option<Vec<_>>>()
                else {
                    return false;
                };
                holds
            }
            Self::Working => return false,
        };
        change();
        drop(holds);
        true
    }
}

impl AssignmentGate for HeldRuns {
    fn none_working(&self) -> bool {
        Self::none_working(self)
    }

    fn while_still(&self, change: &mut dyn FnMut()) -> bool {
        Self::while_still(self, change)
    }
}
