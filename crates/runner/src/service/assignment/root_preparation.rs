use super::*;

pub(super) const ROOT_PREPARATION_PENDING: u8 = 0;
pub(super) const ROOT_PREPARATION_DELIVERING: u8 = 1;
pub(super) const ROOT_PREPARATION_DETACHED: u8 = 2;

pub(super) struct AssignmentRootPreparationHandoff {
    pub(super) state: AtomicU8,
}

impl AssignmentRootPreparationHandoff {
    pub(in crate::service) fn new() -> Self {
        Self {
            state: AtomicU8::new(ROOT_PREPARATION_PENDING),
        }
    }

    pub(super) fn claim_delivery(&self) -> bool {
        self.state
            .compare_exchange(
                ROOT_PREPARATION_PENDING,
                ROOT_PREPARATION_DELIVERING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    pub(super) fn detach(&self) -> bool {
        self.state
            .compare_exchange(
                ROOT_PREPARATION_PENDING,
                ROOT_PREPARATION_DETACHED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }
}

pub(super) struct AssignmentRootPreparation {
    pub(super) offer: AssignmentOffer,
    pub(super) recorder: Option<Arc<crate::telemetry::Recorder>>,
    pub(super) root_preparer: Arc<dyn AssignmentRootPreparer>,
    pub(super) handoff: Arc<AssignmentRootPreparationHandoff>,
    pub(super) event_sender: mpsc::UnboundedSender<ManagerEvent>,
    pub(super) wake: ObservationOutbox,
}

pub(super) struct AssignmentRootPreparationWorker {
    pub(super) requests: Option<std::sync::mpsc::Sender<AssignmentRootPreparation>>,
}

impl AssignmentRootPreparationWorker {
    pub(in crate::service) fn new() -> Self {
        let (requests, pending) = std::sync::mpsc::channel::<AssignmentRootPreparation>();
        let worker = std::thread::Builder::new()
            .name("runner-assignment-root-preparation".to_owned())
            .spawn(move || {
                while let Ok(request) = pending.recv() {
                    prepare_assignment_root(request);
                }
            });
        Self {
            requests: worker.ok().map(|_| requests),
        }
    }

    pub(super) fn prepare(&self, request: AssignmentRootPreparation) -> Result<(), ()> {
        self.requests
            .as_ref()
            .ok_or(())?
            .send(request)
            .map_err(|_| ())
    }
}

pub(super) fn prepare_assignment_root(request: AssignmentRootPreparation) {
    let AssignmentRootPreparation {
        offer,
        recorder,
        root_preparer,
        handoff,
        event_sender,
        wake,
    } = request;
    let assignment_id = offer.assignment_id.clone();
    let root = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        root_preparer.prepare(&offer, recorder)
    })) {
        Ok(result) => result.map(Box::new),
        Err(_) => Err(AssignmentRootCreationError::CleanupFailed),
    };
    if !handoff.claim_delivery() {
        if let Ok(root) = root {
            let _ = root
                .release_pending(
                    ProcessQuiescence::Proven,
                    WorkspaceDisposition::Retain(RetentionReason::Failed),
                )
                .wait();
        }
        wake.wake();
        return;
    }
    let event = ManagerEvent::WorkspacePrepared {
        assignment_id,
        root,
    };
    if let Err(error) = event_sender.send(event)
        && let ManagerEvent::WorkspacePrepared { root: Ok(root), .. } = error.0
    {
        let _ = root
            .release_pending(
                ProcessQuiescence::Proven,
                WorkspaceDisposition::Retain(RetentionReason::Failed),
            )
            .wait();
    }
    wake.wake();
}
