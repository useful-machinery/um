use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum GuardLifecycle {
    Prepared,
    Released,
    Quiesced,
}

pub(super) struct GuardRecord {
    identity: AuthenticatedProcessGroup,
    lifecycle: GuardLifecycle,
}

pub(super) trait GuardProcessControl: Send + Sync {
    fn observe(&self, identity: &AuthenticatedProcessGroup) -> ProcessIdentityObservation;
    fn terminate(&self, identity: &AuthenticatedProcessGroup) -> AuthenticatedSignalResult;
}

pub(super) struct SystemGuardProcessControl;

impl GuardProcessControl for SystemGuardProcessControl {
    fn observe(&self, identity: &AuthenticatedProcessGroup) -> ProcessIdentityObservation {
        SystemProcessIdentityInspector.observe(identity)
    }

    fn terminate(&self, identity: &AuthenticatedProcessGroup) -> AuthenticatedSignalResult {
        terminate_authenticated_process_group(identity)
    }
}

pub(super) struct ProcessGuardState {
    next_id: u64,
    control: Arc<dyn GuardProcessControl>,
    records: BTreeMap<String, GuardRecord>,
    forced_containment_started: bool,
    #[cfg(test)]
    quiescence_fixture: Option<Arc<std::sync::atomic::AtomicBool>>,
}

#[derive(Clone)]
pub(in crate::service) struct AssignmentProcessGuards {
    state: Arc<Mutex<ProcessGuardState>>,
}

impl AssignmentProcessGuards {
    pub(in crate::service) fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(ProcessGuardState {
                next_id: 1,
                control: Arc::new(SystemGuardProcessControl),
                records: BTreeMap::new(),
                forced_containment_started: false,
                #[cfg(test)]
                quiescence_fixture: None,
            })),
        }
    }

    pub(in crate::service) fn registry(&self, guarded: bool) -> ProcessGuardRegistry {
        if guarded {
            let store: Arc<dyn DurableProcessGuardStore> = Arc::new(self.clone());
            ProcessGuardRegistry::durable(store)
        } else {
            ProcessGuardRegistry::default()
        }
    }

    pub(super) fn begin_forced_containment(&self) {
        let (identities, control) = {
            let mut state = self.lock();
            state.forced_containment_started = true;
            let identities = state
                .records
                .values()
                .filter(|record| record.lifecycle != GuardLifecycle::Quiesced)
                .map(|record| record.identity.clone())
                .collect::<Vec<_>>();
            (identities, Arc::clone(&state.control))
        };
        for identity in identities {
            let _ = control.terminate(&identity);
        }
    }

    // Observe each registered identity once. The decision and the identities used in
    // the report must describe the same observation, not two racing inspections.
    pub(super) fn quiescence_snapshot(&self) -> (bool, Vec<String>) {
        let state = self.lock();
        let surviving = state
            .records
            .iter()
            .filter(|(_, record)| {
                record.lifecycle != GuardLifecycle::Quiesced
                    && !matches!(
                        state.control.observe(&record.identity),
                        ProcessIdentityObservation::Absent
                    )
            })
            .map(|(id, record)| {
                format!("{id} ({:?})", record.identity)
                    .chars()
                    .take(512)
                    .collect::<String>()
            })
            .take(255)
            .collect::<Vec<_>>();
        #[cfg(test)]
        if let Some(quiescent) = &state.quiescence_fixture {
            return (quiescent.load(Ordering::Acquire), surviving);
        }
        (surviving.is_empty(), surviving)
    }

    pub(in crate::service) fn is_quiescent(&self) -> bool {
        self.quiescence_snapshot().0
    }

    #[cfg(test)]
    pub(super) fn use_control(&self, control: Arc<dyn GuardProcessControl>) {
        self.lock().control = control;
    }

    #[cfg(test)]
    pub(in crate::service) fn use_quiescence_fixture(
        &self,
        quiescent: Arc<std::sync::atomic::AtomicBool>,
    ) {
        self.lock().quiescence_fixture = Some(quiescent);
    }

    pub(super) fn lock(&self) -> std::sync::MutexGuard<'_, ProcessGuardState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    #[cfg(test)]
    pub(super) fn forced_containment_started(&self) -> bool {
        self.lock().forced_containment_started
    }
}

impl DurableProcessGuardStore for AssignmentProcessGuards {
    fn register(
        &self,
        step: &str,
        action_id: u64,
        identity: &AuthenticatedProcessGroup,
    ) -> Result<String, ProcessGuardStoreError> {
        let mut state = self.lock();
        if state.forced_containment_started
            || state.records.values().any(|record| {
                record.lifecycle != GuardLifecycle::Quiesced && record.identity == *identity
            })
        {
            return Err(ProcessGuardStoreError);
        }
        let id = format!("{step}:{action_id}:{}", state.next_id);
        state.next_id = state.next_id.checked_add(1).ok_or(ProcessGuardStoreError)?;
        state.records.insert(
            id.clone(),
            GuardRecord {
                identity: identity.clone(),
                lifecycle: GuardLifecycle::Prepared,
            },
        );
        Ok(id)
    }

    fn mark_released(&self, guard_id: &str) -> Result<(), ProcessGuardStoreError> {
        let mut state = self.lock();
        if state.forced_containment_started {
            return Err(ProcessGuardStoreError);
        }
        let record = state
            .records
            .get_mut(guard_id)
            .ok_or(ProcessGuardStoreError)?;
        match record.lifecycle {
            GuardLifecycle::Prepared => record.lifecycle = GuardLifecycle::Released,
            GuardLifecycle::Released => {}
            GuardLifecycle::Quiesced => return Err(ProcessGuardStoreError),
        }
        Ok(())
    }

    fn mark_quiesced(&self, guard_id: &str) -> Result<(), ProcessGuardStoreError> {
        let mut state = self.lock();
        let record = state
            .records
            .get_mut(guard_id)
            .ok_or(ProcessGuardStoreError)?;
        record.lifecycle = GuardLifecycle::Quiesced;
        Ok(())
    }
}
