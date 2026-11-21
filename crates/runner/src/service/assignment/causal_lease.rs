use super::*;

#[derive(Clone)]
pub(in crate::service) struct CausalLease {
    pub(super) state: Arc<Mutex<CausalLeaseState>>,
}

pub(super) struct CausalLeaseState {
    pub(super) bases: BTreeMap<u64, LeaseInstant>,
    /// The last request for each next lease sequence asked for.
    pub(super) renewal_requests: BTreeMap<u64, RenewalRequest>,
}

/// The last `LeaseRenewalRequested` enqueued for one next lease sequence.
#[derive(Clone, Copy)]
pub(super) struct RenewalRequest {
    /// The outbox id of the request's observation, retained until Cloud acknowledges it.
    pub(super) observation_id: u64,
    /// When, on the lease clock, the request was enqueued.
    pub(super) enqueued_at: LeaseInstant,
}

impl RenewalRequest {
    /// Enqueues `observation`, this request asked again, once Cloud has acknowledged the
    /// last one, and records it as the last request. A refused repeat leaves the last
    /// request standing to be asked again later.
    fn repeat(
        &mut self,
        observation: AssignmentObservation,
        now: LeaseInstant,
        outbox: &ObservationOutbox,
    ) {
        if outbox.contains(self.observation_id) {
            return;
        }
        if let Ok(observation_id) = outbox.enqueue(observation) {
            *self = Self {
                observation_id,
                enqueued_at: now,
            };
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::service) enum RenewalRequestFailure {
    LeaseClock,
    Outbox,
    Sequence,
}

impl CausalLease {
    pub(in crate::service) fn new(acceptance_basis: LeaseInstant) -> Self {
        Self {
            state: Arc::new(Mutex::new(CausalLeaseState {
                bases: BTreeMap::from([(1, acceptance_basis)]),
                renewal_requests: BTreeMap::new(),
            })),
        }
    }

    pub(super) fn basis(&self, sequence: u64) -> Option<LeaseInstant> {
        self.lock().bases.get(&sequence).copied()
    }

    /// Asks Cloud for lease `current_sequence + 1`, recording the lease clock's time as
    /// that lease's causal basis. Asked again for a sequence already requested, it enqueues
    /// the same request once more only when `retry_interval` has passed since the last one
    /// and Cloud has acknowledged it (its observation has left the outbox); otherwise it
    /// changes nothing. A repeat never moves the basis, and one the outbox refuses is not a
    /// failure: the earlier request may still be answered (INV-LIFECYCLE-22).
    pub(in crate::service) fn request_renewal(
        &self,
        current_sequence: u64,
        retry_interval: Duration,
        assignment_id: &str,
        attempt_id: &str,
        lease_clock: &LeaseClock,
        outbox: &ObservationOutbox,
    ) -> Result<(), RenewalRequestFailure> {
        let next_sequence = current_sequence
            .checked_add(1)
            .ok_or(RenewalRequestFailure::Sequence)?;
        let mut state = self.lock();
        let now = lease_clock
            .now()
            .map_err(|_| RenewalRequestFailure::LeaseClock)?;
        let observation = AssignmentObservation::LeaseRenewalRequested {
            assignment_id: assignment_id.to_owned(),
            attempt_id: attempt_id.to_owned(),
            current_lease_sequence: current_sequence,
        };
        if let Some(request) = state.renewal_requests.get_mut(&next_sequence) {
            let due = request
                .enqueued_at
                .checked_add(retry_interval)
                .and_then(|due| now.checked_cmp(due))
                .map_err(|_| RenewalRequestFailure::LeaseClock)?;
            if due != std::cmp::Ordering::Less {
                request.repeat(observation, now, outbox);
            }
            return Ok(());
        }
        state.bases.entry(next_sequence).or_insert(now);
        let observation_id = outbox
            .enqueue(observation)
            .map_err(|_| RenewalRequestFailure::Outbox)?;
        state.renewal_requests.insert(
            next_sequence,
            RenewalRequest {
                observation_id,
                enqueued_at: now,
            },
        );
        Ok(())
    }

    /// Asks Cloud again, without waiting for any retry interval, for lease
    /// `current_sequence + 1` when it has been requested and Cloud has acknowledged the last
    /// request (its observation has left the outbox); otherwise it changes nothing. It never
    /// makes a first request and never moves the basis (INV-RUNNER-48).
    pub(super) fn repeat_renewal_request(
        &self,
        current_sequence: u64,
        assignment_id: &str,
        attempt_id: &str,
        lease_clock: &LeaseClock,
        outbox: &ObservationOutbox,
    ) -> Result<(), LeaseClockError> {
        let Some(next_sequence) = current_sequence.checked_add(1) else {
            return Ok(());
        };
        let mut state = self.lock();
        let Some(request) = state.renewal_requests.get_mut(&next_sequence) else {
            return Ok(());
        };
        if outbox.contains(request.observation_id) {
            return Ok(());
        }
        let now = lease_clock.now()?;
        request.repeat(
            AssignmentObservation::LeaseRenewalRequested {
                assignment_id: assignment_id.to_owned(),
                attempt_id: attempt_id.to_owned(),
                current_lease_sequence: current_sequence,
            },
            now,
            outbox,
        );
        Ok(())
    }

    pub(super) fn lock(&self) -> std::sync::MutexGuard<'_, CausalLeaseState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}
