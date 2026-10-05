use super::*;

#[derive(Clone)]
pub(in crate::service) struct CausalLease {
    pub(super) state: Arc<Mutex<CausalLeaseState>>,
}

pub(super) struct CausalLeaseState {
    pub(super) bases: BTreeMap<u64, LeaseInstant>,
    pub(super) renewal_requests: BTreeSet<u64>,
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
                renewal_requests: BTreeSet::new(),
            })),
        }
    }

    pub(super) fn basis(&self, sequence: u64) -> Option<LeaseInstant> {
        self.lock().bases.get(&sequence).copied()
    }

    pub(in crate::service) fn request_renewal(
        &self,
        current_sequence: u64,
        assignment_id: &str,
        attempt_id: &str,
        lease_clock: &LeaseClock,
        outbox: &ObservationOutbox,
    ) -> Result<(), RenewalRequestFailure> {
        let next_sequence = current_sequence
            .checked_add(1)
            .ok_or(RenewalRequestFailure::Sequence)?;
        let mut state = self.lock();
        if state.renewal_requests.contains(&next_sequence) {
            return Ok(());
        }
        if let std::collections::btree_map::Entry::Vacant(entry) = state.bases.entry(next_sequence)
        {
            let basis = lease_clock
                .now()
                .map_err(|_| RenewalRequestFailure::LeaseClock)?;
            entry.insert(basis);
        }
        outbox
            .enqueue(AssignmentObservation::LeaseRenewalRequested {
                assignment_id: assignment_id.to_owned(),
                attempt_id: attempt_id.to_owned(),
                current_lease_sequence: current_sequence,
            })
            .map_err(|_| RenewalRequestFailure::Outbox)?;
        state.renewal_requests.insert(next_sequence);
        Ok(())
    }

    pub(super) fn lock(&self) -> std::sync::MutexGuard<'_, CausalLeaseState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}
