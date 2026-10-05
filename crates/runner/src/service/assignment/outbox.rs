use super::*;

pub(super) struct ObservationEntry {
    pub(super) id: u64,
    pub(super) observation: AssignmentObservation,
    encoded_bytes: usize,
    replayable: bool,
    encoded: bool,
    transport_owned: bool,
    retained_frame: Option<RetainedObservationFrame>,
}

pub(super) struct ObservationOutboxState {
    pub(super) entries: VecDeque<ObservationEntry>,
    next_id: u64,
}

#[derive(Clone)]
pub(in crate::service) struct ObservationOutbox {
    state: Arc<Mutex<ObservationOutboxState>>,
    changed: Arc<Notify>,
    pub(super) maximum_encoded_bytes: u64,
}

impl ObservationOutbox {
    pub(in crate::service) fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(ObservationOutboxState {
                entries: VecDeque::new(),
                next_id: 1,
            })),
            changed: Arc::new(Notify::new()),
            maximum_encoded_bytes: MAXIMUM_ENCODED_OUTBOX_BYTES,
        }
    }

    pub(super) fn reserve(
        &self,
        transition_entries: usize,
        encoded_outbox_bytes: u64,
    ) -> Result<usize, AssignmentDecline> {
        let reservation = transition_entries
            .checked_add(OBSERVATION_RESERVE_BASE)
            .ok_or_else(environment_unavailable)?;
        if encoded_outbox_bytes > self.maximum_encoded_bytes
            || reservation > MAXIMUM_SERVICE_OBSERVATIONS
        {
            return Err(environment_unavailable());
        }
        let mut state = self.lock();
        if state.entries.capacity() < reservation {
            let additional = reservation.saturating_sub(state.entries.len());
            state
                .entries
                .try_reserve_exact(additional)
                .map_err(|_| environment_unavailable())?;
        }
        Ok(transition_entries)
    }

    pub(in crate::service) fn enqueue(
        &self,
        observation: AssignmentObservation,
    ) -> Result<u64, OutboxFailure> {
        let encoded_bytes = encoded_observation_bytes(&observation, maximum_runner_envelope())?;
        let terminal = observation.is_terminal();
        let maximum_frame_bytes = if terminal {
            MAXIMUM_TERMINAL_FRAME_BYTES
        } else if observation.is_condition_evidence_transition() {
            MAXIMUM_CONDITION_TRANSITION_FRAME_BYTES
        } else {
            MAXIMUM_ORDINARY_FRAME_BYTES
        };
        if encoded_bytes > maximum_frame_bytes {
            return Err(OutboxFailure::Encoding);
        }

        let mut state = self.lock();
        let retained_bytes = state.entries.iter().try_fold(0_u64, |total, entry| {
            total.checked_add(u64::try_from(entry.encoded_bytes).ok()?)
        });
        if state.entries.len() == MAXIMUM_SERVICE_OBSERVATIONS
            || retained_bytes
                .and_then(|total| total.checked_add(u64::try_from(encoded_bytes).ok()?))
                .is_none_or(|total| total > self.maximum_encoded_bytes)
            || (terminal
                && state.entries.iter().any(|entry| {
                    entry.observation.is_terminal()
                        && entry.observation.assignment_id() == observation.assignment_id()
                }))
        {
            return Err(OutboxFailure::Capacity);
        }
        let id = state.next_id;
        state.next_id = state
            .next_id
            .checked_add(1)
            .ok_or(OutboxFailure::Sequence)?;
        state.entries.push_back(ObservationEntry {
            id,
            observation,
            encoded_bytes,
            replayable: true,
            encoded: false,
            transport_owned: false,
            retained_frame: None,
        });
        drop(state);
        self.changed.notify_waiters();
        Ok(id)
    }

    pub(in crate::service) fn pending(
        &self,
        in_flight: &BTreeSet<u64>,
        limit: usize,
    ) -> Vec<PendingAssignmentObservation> {
        self.lock()
            .entries
            .iter()
            .filter(|entry| {
                entry.replayable
                    && !entry.encoded
                    && !entry.transport_owned
                    && !in_flight.contains(&entry.id)
            })
            .take(limit)
            .map(|entry| PendingAssignmentObservation {
                id: entry.id,
                observation: entry.observation.clone(),
                retained_frame: entry.retained_frame.clone(),
            })
            .collect()
    }

    pub(super) fn acknowledge(&self, id: u64) -> Option<AssignmentObservation> {
        let mut state = self.lock();
        let index = state.entries.iter().position(|entry| entry.id == id)?;
        state.entries.remove(index).map(|entry| entry.observation)
    }

    pub(super) fn claim_for_transport(&self, id: u64) -> bool {
        let mut state = self.lock();
        let Some(entry) = state.entries.iter_mut().find(|entry| entry.id == id) else {
            return false;
        };
        if !entry.replayable || entry.encoded || entry.transport_owned {
            return false;
        }
        entry.transport_owned = true;
        true
    }

    pub(super) fn retain_frame(&self, id: u64, retained_frame: RetainedObservationFrame) {
        if let Some(entry) = self.lock().entries.iter_mut().find(|entry| entry.id == id)
            && entry.retained_frame.is_none()
        {
            entry.retained_frame = Some(retained_frame);
        }
    }

    pub(super) fn mark_encoded(&self, id: u64) {
        if let Some(entry) = self.lock().entries.iter_mut().find(|entry| entry.id == id) {
            entry.encoded = true;
        }
    }

    pub(super) fn is_encoded(&self, id: u64) -> bool {
        self.lock()
            .entries
            .iter()
            .any(|entry| entry.id == id && entry.encoded)
    }

    pub(super) fn retain_only(&self, id: u64) {
        let mut state = self.lock();
        for entry in &mut state.entries {
            entry.replayable = entry.id == id;
        }
        state
            .entries
            .retain(|entry| entry.replayable || entry.encoded || entry.transport_owned);
    }

    pub(super) fn fence_assignment(&self, assignment_id: &str) {
        let mut state = self.lock();
        for entry in &mut state.entries {
            if entry.observation.assignment_id() == assignment_id {
                entry.replayable = false;
            }
        }
        state
            .entries
            .retain(|entry| entry.replayable || entry.encoded || entry.transport_owned);
    }

    pub(in crate::service) fn finish_transport(&self) -> BTreeSet<u64> {
        let mut state = self.lock();
        let removed = state
            .entries
            .iter()
            .filter(|entry| !entry.replayable)
            .map(|entry| entry.id)
            .collect();
        state.entries.retain(|entry| entry.replayable);
        for entry in &mut state.entries {
            entry.encoded = false;
            entry.transport_owned = false;
        }
        removed
    }

    pub(super) fn contains(&self, id: u64) -> bool {
        self.lock().entries.iter().any(|entry| entry.id == id)
    }

    pub(super) fn replay_obligations_dispatched(&self) -> bool {
        self.lock()
            .entries
            .iter()
            .filter(|entry| entry.replayable)
            .all(|entry| entry.encoded)
    }

    pub(in crate::service) fn notification(&self) -> Arc<Notify> {
        Arc::clone(&self.changed)
    }

    pub(in crate::service) fn wake(&self) {
        self.changed.notify_waiters();
    }

    pub(super) fn lock(&self) -> std::sync::MutexGuard<'_, ObservationOutboxState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::service) enum OutboxFailure {
    Capacity,
    Encoding,
    Sequence,
}
// Envelope fields have fixed-width public IDs; only the sequence and timestamp
// grow. Size the complete frame with the longest sequence the protocol encoder
// accepts and the longest UTC timestamp the transport can format.
fn maximum_runner_envelope() -> RunnerEnvelope {
    RunnerEnvelope {
        message_id: "rmsg_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
        runner_id: "rnr_01k0z6r1w8f4jy2m7q9v3x5abd".to_owned(),
        boot_id: "rbt_01k0z6r1w8f4jy2m7q9v3x5abe".to_owned(),
        sequence: i64::MAX as u64,
        sent_at: "9999-12-31T23:59:59.999999999Z".to_owned(),
    }
}

fn encoded_observation_bytes(
    observation: &AssignmentObservation,
    envelope: RunnerEnvelope,
) -> Result<usize, OutboxFailure> {
    encode_runner_frame(&observation.runner_frame(envelope))
        .map(|frame| frame.len())
        .map_err(|_| OutboxFailure::Encoding)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn outbox_accounts_for_the_maximum_envelope_without_a_padding_guess() {
        let observation = AssignmentObservation::Preparing {
            effect_id: "eff_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
            assignment_id: "asn_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
            offered_execution_spec_id: "xsp_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
        };
        let maximum = maximum_runner_envelope();
        let mut compact = maximum.clone();
        compact.sequence = 1;
        compact.sent_at = "2026-07-23T00:00:00Z".to_owned();
        let overhead = encoded_observation_bytes(&observation, maximum.clone()).unwrap()
            - encoded_observation_bytes(&observation, compact).unwrap();
        assert_eq!(
            overhead,
            maximum.sequence.to_string().len() - 1 + maximum.sent_at.len()
                - "2026-07-23T00:00:00Z".len()
        );
        let outbox = ObservationOutbox::new();
        outbox.enqueue(observation).unwrap();
        let state = outbox.lock();
        assert_eq!(
            state.entries[0].encoded_bytes,
            encoded_observation_bytes(&state.entries[0].observation, maximum).unwrap()
        );
    }
}
