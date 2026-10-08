use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::service) struct AssignmentPrepare {
    pub(in crate::service) effect_id: String,
    pub(in crate::service) assignment_id: String,
    pub(in crate::service) run_id: String,
    pub(in crate::service) attempt_id: String,
    pub(in crate::service) execution_spec_id: String,
    pub(in crate::service) preparation_expires_at: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::service) struct AssignmentStart {
    pub(in crate::service) effect_id: String,
    pub(in crate::service) assignment_id: String,
    pub(in crate::service) run_id: String,
    pub(in crate::service) attempt_id: String,
    pub(in crate::service) execution_spec_id: String,
    pub(in crate::service) lease: ExecutionLeaseGrant,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::service) struct AssignmentCancel {
    pub(in crate::service) effect_id: String,
    pub(in crate::service) assignment_id: String,
    pub(in crate::service) run_id: String,
    pub(in crate::service) attempt_id: String,
    pub(in crate::service) request_id: String,
    pub(in crate::service) mode: CancellationMode,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::service) struct AssignmentRelease {
    pub(in crate::service) effect_id: String,
    pub(in crate::service) assignment_id: String,
    pub(in crate::service) run_id: String,
    pub(in crate::service) attempt_id: String,
    pub(in crate::service) reason: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::service) struct AssignmentCancellationApplication {
    pub(in crate::service) effect_id: String,
    pub(in crate::service) request_id: String,
    pub(in crate::service) assignment_id: String,
    pub(in crate::service) attempt_id: String,
    pub(in crate::service) mode: CancellationMode,
    pub(in crate::service) effective_mode: CancellationMode,
    pub(in crate::service) disposition: CancellationApplicationDisposition,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct RetainedCancellation {
    pub(super) command: AssignmentCancel,
    pub(super) application: Option<AssignmentCancellationApplication>,
    pub(super) observation_id: Option<u64>,
    pub(super) ready: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::service) struct AssignmentRenewal {
    pub(in crate::service) effect_id: String,
    pub(in crate::service) assignment_id: String,
    pub(in crate::service) run_id: String,
    pub(in crate::service) attempt_id: String,
    pub(in crate::service) lease: ExecutionLeaseGrant,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::service) struct AssignmentStartAuthorization {
    pub(in crate::service) effect_id: String,
    pub(in crate::service) assignment_id: String,
    pub(in crate::service) run_id: String,
    pub(in crate::service) attempt_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::service) enum AssignmentDecision {
    Accepted {
        effect_id: String,
        assignment_id: String,
        offered_execution_spec_id: String,
    },
    Rejected {
        effect_id: String,
        assignment_id: String,
        decline: AssignmentDecline,
    },
}

impl AssignmentDecision {
    pub(in crate::service) fn assignment_id(&self) -> &str {
        match self {
            Self::Accepted { assignment_id, .. } | Self::Rejected { assignment_id, .. } => {
                assignment_id
            }
        }
    }

    pub(in crate::service) fn runner_frame(&self, envelope: RunnerEnvelope) -> RunnerFrame {
        match self {
            Self::Accepted {
                effect_id,
                assignment_id,
                offered_execution_spec_id,
            } => RunnerFrame::AssignmentAccepted {
                envelope,
                effect_id: effect_id.clone(),
                assignment_id: assignment_id.clone(),
                offered_execution_spec_id: offered_execution_spec_id.clone(),
            },
            Self::Rejected {
                effect_id,
                assignment_id,
                decline,
            } => RunnerFrame::AssignmentRejected {
                envelope,
                effect_id: effect_id.clone(),
                assignment_id: assignment_id.clone(),
                decline: decline.clone(),
            },
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum RenewalDisposition {
    Applied,
    ReplayApplied,
    ReplayRejected,
    UnknownAssignment,
    NotRunning,
    CancellationStarted,
    StaleSequence,
    MissingBasis,
}

impl RenewalDisposition {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Applied => "applied",
            Self::ReplayApplied => "replay_applied",
            Self::ReplayRejected => "replay_rejected",
            Self::UnknownAssignment => "unknown_assignment",
            Self::NotRunning => "not_running",
            Self::CancellationStarted => "cancellation_started",
            Self::StaleSequence => "stale_sequence",
            Self::MissingBasis => "missing_basis",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::service) struct RenewalDecision {
    pub(super) disposition: RenewalDisposition,
    pub(in crate::service) cancellation_headroom_ms: Option<i64>,
    pub(in crate::service) request_age_ms: Option<i64>,
}

impl RenewalDecision {
    pub(super) fn untimed(disposition: RenewalDisposition) -> Self {
        Self {
            disposition,
            cancellation_headroom_ms: None,
            request_age_ms: None,
        }
    }

    pub(in crate::service) fn record(&self, event: &TelemetryEvent) {
        use crate::telemetry::attribute;
        event.set(opentelemetry::KeyValue::new(
            attribute::LEASE_DISPOSITION,
            self.disposition.as_str(),
        ));
        for (key, value) in [
            (
                attribute::LEASE_CANCELLATION_HEADROOM_MS,
                self.cancellation_headroom_ms,
            ),
            (attribute::LEASE_REQUEST_AGE_MS, self.request_age_ms),
        ] {
            if let Some(value) = value {
                event.set(opentelemetry::KeyValue::new(key, value));
            }
        }
    }
}

impl AssignmentManager {
    pub(super) fn effective_cancellation_mode(
        &self,
        cancel: &AssignmentCancel,
    ) -> CancellationMode {
        if cancel.mode == CancellationMode::Force
            || self.cancellations.iter().any(|retained| {
                retained.command.assignment_id == cancel.assignment_id
                    && retained.application.as_ref().is_some_and(|application| {
                        application.effective_mode == CancellationMode::Force
                    })
            })
        {
            CancellationMode::Force
        } else {
            CancellationMode::Graceful
        }
    }

    pub(super) fn make_cancellation_room(&mut self) -> Result<(), AssignmentManagerFailure> {
        if self.cancellations.len() < MAXIMUM_RETAINED_DECISIONS {
            return Ok(());
        }
        let active_assignment_id = match &self.slot {
            Some(LocalSlot::Preparing(preparing)) => Some(preparing.offer.assignment_id.as_str()),
            Some(LocalSlot::Accepted(accepted)) => Some(accepted.identity.assignment_id.as_str()),
            Some(LocalSlot::Running(running)) => Some(running.identity.assignment_id.as_str()),
            Some(LocalSlot::Finishing(finishing)) => {
                Some(finishing.identity.assignment_id.as_str())
            }
            Some(LocalSlot::Releasing(releasing)) => Some(releasing.assignment_id.as_str()),
            None => self
                .reporting
                .as_ref()
                .map(|identity| identity.assignment_id.as_str()),
        };
        let Some(index) = self.cancellations.iter().position(|retained| {
            retained.observation_id.is_none()
                && retained.ready
                && active_assignment_id != Some(retained.command.assignment_id.as_str())
        }) else {
            return Err(AssignmentManagerFailure::DecisionCapacity);
        };
        self.cancellations.remove(index);
        Ok(())
    }

    pub(super) fn retain_cancellation(
        &mut self,
        command: AssignmentCancel,
        application: Option<AssignmentCancellationApplication>,
        ready: bool,
    ) -> Result<usize, AssignmentManagerFailure> {
        self.make_cancellation_room()?;
        self.cancellations.push_back(RetainedCancellation {
            command,
            application,
            observation_id: None,
            ready,
        });
        Ok(self.cancellations.len() - 1)
    }

    pub(super) fn emit_cancellation_application(
        &mut self,
        index: usize,
    ) -> Result<(), AssignmentManagerFailure> {
        let application = self.cancellations[index]
            .application
            .clone()
            .ok_or(AssignmentManagerFailure::ConflictingOffer)?;
        let observation_id = self
            .outbox
            .enqueue(AssignmentObservation::CancellationApplied(application))
            .map_err(|_| AssignmentManagerFailure::DecisionCapacity)?;
        self.cancellations[index].observation_id = Some(observation_id);
        self.cancellations[index].ready = true;
        Ok(())
    }

    pub(in crate::service) fn handle_start(
        &mut self,
        start: AssignmentStart,
    ) -> Result<Option<ExecutionJob>, AssignmentManagerFailure> {
        self.drain_events();
        if self.cancellation_uses_effect_id(&start.effect_id) {
            return Err(AssignmentManagerFailure::ConflictingOffer);
        }
        if self.decisions.iter().any(|decision| {
            decision
                .start
                .as_ref()
                .is_some_and(|known| known.effect_id == start.effect_id && known != &start)
        }) {
            return Err(AssignmentManagerFailure::ConflictingOffer);
        }
        let Some(index) = self
            .decisions
            .iter()
            .position(|decision| decision.offer.assignment_id == start.assignment_id)
        else {
            return Ok(None);
        };
        if !start_matches_offer(&start, &self.decisions[index].offer) {
            return Err(AssignmentManagerFailure::ConflictingOffer);
        }
        if let Some(known) = &self.decisions[index].start {
            return if known == &start {
                Ok(None)
            } else {
                Err(AssignmentManagerFailure::ConflictingOffer)
            };
        }
        if start.lease.sequence != 1 {
            return Err(AssignmentManagerFailure::ConflictingOffer);
        }
        self.decisions[index].start = Some(start.clone());

        let Some(slot) = self.slot.take() else {
            return Ok(None);
        };
        let LocalSlot::Accepted(accepted) = slot else {
            self.slot = Some(slot);
            return Ok(None);
        };
        if accepted.identity.assignment_id != start.assignment_id {
            self.slot = Some(LocalSlot::Accepted(accepted));
            return Ok(None);
        }
        let Some(causal_lease) = self.decisions[index].causal_lease.clone() else {
            let identity = accepted.identity.clone();
            let root = accepted.root;
            self.finish_before_execution(identity, root, "execution_lease_expired")?;
            return Ok(None);
        };
        let cancellation_grace = accepted.admitted.execution().cancellation().grace();
        let authority =
            match self.validate_grant(&start.lease, 1, cancellation_grace, &causal_lease) {
                Ok(authority) => authority,
                Err(GrantValidationFailure::MissingBasis) => {
                    let identity = accepted.identity.clone();
                    let root = accepted.root;
                    self.finish_before_execution(identity, root, "execution_lease_expired")?;
                    return Ok(None);
                }
                Err(GrantValidationFailure::Arithmetic(_)) => {
                    self.slot = Some(LocalSlot::Accepted(accepted));
                    self.lease_clock_failed = true;
                    return Err(AssignmentManagerFailure::LeaseClock);
                }
            };
        let now = match self.lease_clock.now() {
            Ok(now) => now,
            Err(_) => {
                self.slot = Some(LocalSlot::Accepted(accepted));
                self.lease_clock_failed = true;
                return Err(AssignmentManagerFailure::LeaseClock);
            }
        };
        match now.checked_cmp(authority.cancellation_start) {
            Ok(std::cmp::Ordering::Less) => {}
            Ok(std::cmp::Ordering::Equal | std::cmp::Ordering::Greater) => {
                let identity = accepted.identity.clone();
                let root = accepted.root;
                self.finish_before_execution(identity, root, "execution_lease_expired")?;
                return Ok(None);
            }
            Err(_) => {
                self.slot = Some(LocalSlot::Accepted(accepted));
                self.lease_clock_failed = true;
                return Err(AssignmentManagerFailure::LeaseClock);
            }
        }
        let cancellation = accepted
            .admitted
            .execution()
            .cancellation()
            .source()
            .clone();
        let (authority_updates, authority_receiver) = tokio::sync::watch::channel(authority);
        let (start_authority, start_authority_receiver) = tokio::sync::watch::channel(false);
        let (infrastructure_interruption, infrastructure_interruption_receiver) =
            tokio::sync::watch::channel(None);
        let workflow_git = accepted.workflow_git.clone();
        let engine_terminal = Arc::new(AtomicBool::new(false));
        let run_event = RunEvent::default();
        self.run_events
            .insert(accepted.assignment_id().to_owned(), run_event.clone());
        self.slot = Some(LocalSlot::Running(Box::new(RunningAssignment {
            identity: accepted.identity.clone(),
            cancellation,
            cancellation_grace,
            current_grant: start.lease,
            causal_lease: causal_lease.clone(),
            authority_updates,
            start_authority,
            infrastructure_interruption,
            workflow_git,
            engine_terminal: Arc::clone(&engine_terminal),
            workspace_release: None,
            run_event: run_event.clone(),
        })));
        Ok(Some(ExecutionJob::new(
            *accepted,
            self.outbox.clone(),
            self.artifact_delivery.clone(),
            self.event_sender.clone(),
            engine_terminal,
            run_event,
            ExecutionAuthority {
                lease_clock: self.lease_clock.clone(),
                causal_lease,
                updates: authority_receiver,
                start_authority: start_authority_receiver,
                infrastructure_interruption: infrastructure_interruption_receiver,
            },
        )))
    }

    pub(super) fn matching_decision_index(
        &self,
        assignment_id: &str,
        run_id: &str,
        attempt_id: &str,
    ) -> Result<Option<usize>, AssignmentManagerFailure> {
        let Some(index) = self
            .decisions
            .iter()
            .position(|decision| decision.offer.assignment_id == assignment_id)
        else {
            return Ok(None);
        };
        let decision = &self.decisions[index];
        if decision.offer.run_id != run_id || decision.offer.attempt_id != attempt_id {
            return Err(AssignmentManagerFailure::ConflictingOffer);
        }
        Ok(Some(index))
    }

    pub(in crate::service) fn handle_start_authorized(
        &mut self,
        authorization: AssignmentStartAuthorization,
    ) -> Result<(), AssignmentManagerFailure> {
        self.drain_events();
        if self.cancellation_uses_effect_id(&authorization.effect_id) {
            return Err(AssignmentManagerFailure::ConflictingOffer);
        }
        if self.decisions.iter().any(|decision| {
            decision.offer.effect_id == authorization.effect_id
                || decision
                    .start
                    .as_ref()
                    .is_some_and(|start| start.effect_id == authorization.effect_id)
                || decision.renewals.contains_key(&authorization.effect_id)
                || decision
                    .rejected_renewals
                    .contains_key(&authorization.effect_id)
        }) {
            return Err(AssignmentManagerFailure::ConflictingOffer);
        }
        if let Some(known) = self.decisions.iter().find_map(|decision| {
            decision
                .start_authorization
                .as_ref()
                .filter(|known| known.effect_id == authorization.effect_id)
        }) {
            return if known == &authorization {
                Ok(())
            } else {
                Err(AssignmentManagerFailure::ConflictingOffer)
            };
        }
        let Some(index) = self.matching_decision_index(
            &authorization.assignment_id,
            &authorization.run_id,
            &authorization.attempt_id,
        )?
        else {
            return Ok(());
        };
        let decision = &self.decisions[index];
        if decision.start.is_none() || decision.start_authorization.is_some() {
            return Err(AssignmentManagerFailure::ConflictingOffer);
        }
        self.decisions[index].start_authorization = Some(authorization.clone());
        if let Some(LocalSlot::Running(running)) = &self.slot
            && running.identity.assignment_id == authorization.assignment_id
            && running.identity.run_id == authorization.run_id
            && running.identity.attempt_id == authorization.attempt_id
            && !self.has_pending_pre_execution_cancellation(&authorization.assignment_id)
        {
            if let Some(recorder) = &self.recorder {
                running
                    .run_event
                    .start(recorder, &running.identity, &self.runner_id);
            }
            running.start_authority.send_replace(true);
        }
        Ok(())
    }

    pub(in crate::service) fn handle_renewal(
        &mut self,
        renewal: AssignmentRenewal,
    ) -> Result<RenewalDecision, AssignmentManagerFailure> {
        self.drain_events();
        if self.cancellation_uses_effect_id(&renewal.effect_id) {
            return Err(AssignmentManagerFailure::ConflictingOffer);
        }
        if self.decisions.iter().any(|decision| {
            decision.offer.effect_id == renewal.effect_id
                || decision
                    .start
                    .as_ref()
                    .is_some_and(|start| start.effect_id == renewal.effect_id)
        }) {
            return Err(AssignmentManagerFailure::ConflictingOffer);
        }
        if let Some((known, disposition)) = self.decisions.iter().find_map(|decision| {
            decision
                .renewals
                .get(&renewal.effect_id)
                .map(|known| (known, RenewalDisposition::ReplayApplied))
                .or_else(|| {
                    decision
                        .rejected_renewals
                        .get(&renewal.effect_id)
                        .map(|known| (known, RenewalDisposition::ReplayRejected))
                })
        }) {
            return if known == &renewal {
                Ok(RenewalDecision::untimed(disposition))
            } else {
                Err(AssignmentManagerFailure::ConflictingOffer)
            };
        }
        let Some(index) = self.matching_decision_index(
            &renewal.assignment_id,
            &renewal.run_id,
            &renewal.attempt_id,
        )?
        else {
            return Ok(RenewalDecision::untimed(
                RenewalDisposition::UnknownAssignment,
            ));
        };
        let decision = &self.decisions[index];
        if decision
            .renewals
            .values()
            .chain(decision.rejected_renewals.values())
            .any(|known| known.lease.sequence == renewal.lease.sequence)
        {
            return Err(AssignmentManagerFailure::ConflictingOffer);
        }
        let running_matches = matches!(
            &self.slot,
            Some(LocalSlot::Running(running))
                if running.identity.assignment_id == renewal.assignment_id
        );
        if !running_matches {
            // A later causal request must not change this grant's replay disposition.
            self.decisions[index]
                .rejected_renewals
                .insert(renewal.effect_id.clone(), renewal);
            return Ok(RenewalDecision::untimed(RenewalDisposition::NotRunning));
        }
        let Some(LocalSlot::Running(running)) = &self.slot else {
            return Ok(RenewalDecision::untimed(RenewalDisposition::NotRunning));
        };
        let now = match self.lease_clock.now() {
            Ok(now) => now,
            Err(error) => return Err(self.fail_lease_clock(error)),
        };
        let authority = running.authority_updates.borrow().clone();
        let cancellation_order = match now.checked_cmp(authority.cancellation_start) {
            Ok(ordering) => ordering,
            Err(error) => return Err(self.fail_lease_clock(error)),
        };
        // These local monotonic durations diagnose queueing and deadline pressure;
        // unavailable telemetry must not alter the authority decision.
        let milliseconds =
            |duration: Duration| crate::telemetry::integer_u128(duration.as_millis());
        let cancellation_headroom_ms = match cancellation_order {
            std::cmp::Ordering::Less | std::cmp::Ordering::Equal => authority
                .cancellation_start
                .checked_duration_since(now)
                .ok()
                .map(milliseconds),
            std::cmp::Ordering::Greater => now
                .checked_duration_since(authority.cancellation_start)
                .ok()
                .map(|duration| -milliseconds(duration)),
        };
        let request_age_ms = running
            .causal_lease
            .basis(renewal.lease.sequence)
            .and_then(|basis| now.checked_duration_since(basis).ok())
            .map(milliseconds);
        let timed = |disposition| RenewalDecision {
            disposition,
            cancellation_headroom_ms,
            request_age_ms,
        };
        let cancellation_started =
            authority.revoked || cancellation_order != std::cmp::Ordering::Less;
        if cancellation_started {
            let Some(LocalSlot::Running(running)) = &mut self.slot else {
                return Ok(timed(RenewalDisposition::NotRunning));
            };
            revoke_authority(running);
            return Ok(timed(RenewalDisposition::CancellationStarted));
        }
        if renewal.lease.sequence <= running.current_grant.sequence {
            return Ok(timed(RenewalDisposition::StaleSequence));
        }
        let expected_sequence = running
            .current_grant
            .sequence
            .checked_add(1)
            .ok_or(AssignmentManagerFailure::ConflictingOffer)?;
        if renewal.lease.sequence != expected_sequence {
            return Err(AssignmentManagerFailure::ConflictingOffer);
        }
        let cancellation_grace = running.cancellation_grace;
        let causal_lease = running.causal_lease.clone();
        let current_expiry = authority.local_expiry;
        let next_authority = match self.validate_grant(
            &renewal.lease,
            expected_sequence,
            cancellation_grace,
            &causal_lease,
        ) {
            Ok(authority) => authority,
            Err(GrantValidationFailure::MissingBasis) => {
                self.decisions[index]
                    .rejected_renewals
                    .insert(renewal.effect_id.clone(), renewal);
                return Ok(timed(RenewalDisposition::MissingBasis));
            }
            Err(GrantValidationFailure::Arithmetic(error)) => {
                return Err(self.fail_lease_clock(error));
            }
        };
        match next_authority.local_expiry.checked_cmp(current_expiry) {
            Ok(std::cmp::Ordering::Greater) => {}
            Ok(std::cmp::Ordering::Less | std::cmp::Ordering::Equal) => {
                return Err(AssignmentManagerFailure::ConflictingOffer);
            }
            Err(error) => return Err(self.fail_lease_clock(error)),
        }

        let Some(LocalSlot::Running(running)) = &mut self.slot else {
            return Ok(timed(RenewalDisposition::NotRunning));
        };
        running.current_grant = renewal.lease.clone();
        running.authority_updates.send_replace(next_authority);
        self.decisions[index]
            .renewals
            .insert(renewal.effect_id.clone(), renewal);
        Ok(timed(RenewalDisposition::Applied))
    }

    pub(in crate::service) fn handle_release(
        &mut self,
        release: AssignmentRelease,
    ) -> Result<(), AssignmentManagerFailure> {
        self.drain_events();
        if self.cancellation_uses_effect_id(&release.effect_id) {
            return Err(AssignmentManagerFailure::ConflictingOffer);
        }
        if let Some(known) = self
            .releases
            .iter()
            .find(|known| known.effect_id == release.effect_id)
        {
            return if known == &release {
                Ok(())
            } else {
                Err(AssignmentManagerFailure::ConflictingOffer)
            };
        }
        let retained_conflict = self.decisions.iter().any(|decision| {
            decision.offer.assignment_id == release.assignment_id
                && (decision.offer.run_id != release.run_id
                    || decision.offer.attempt_id != release.attempt_id)
        });
        if retained_conflict {
            return Err(AssignmentManagerFailure::ConflictingOffer);
        }
        let slot_conflict = match &self.slot {
            Some(LocalSlot::Preparing(preparing))
                if preparing.offer.assignment_id == release.assignment_id =>
            {
                preparing.offer.run_id != release.run_id
                    || preparing.offer.attempt_id != release.attempt_id
            }
            Some(LocalSlot::Accepted(accepted))
                if accepted.identity.assignment_id == release.assignment_id =>
            {
                accepted.identity.run_id != release.run_id
                    || accepted.identity.attempt_id != release.attempt_id
            }
            Some(LocalSlot::Running(running))
                if running.identity.assignment_id == release.assignment_id =>
            {
                running.identity.run_id != release.run_id
                    || running.identity.attempt_id != release.attempt_id
            }
            Some(LocalSlot::Finishing(finishing))
                if finishing.identity.assignment_id == release.assignment_id =>
            {
                finishing.identity.run_id != release.run_id
                    || finishing.identity.attempt_id != release.attempt_id
            }
            Some(
                LocalSlot::Preparing(_)
                | LocalSlot::Accepted(_)
                | LocalSlot::Running(_)
                | LocalSlot::Finishing(_)
                | LocalSlot::Releasing(_),
            )
            | None => false,
        };
        if slot_conflict {
            return Err(AssignmentManagerFailure::ConflictingOffer);
        }
        self.retain_release(release.clone());
        let assignment_id = release.assignment_id.as_str();
        let run_id = release.run_id.as_str();
        let attempt_id = release.attempt_id.as_str();
        let reason = release.reason.as_str();
        if let Some(LocalSlot::Preparing(preparing)) = &self.slot
            && preparing.offer.assignment_id == assignment_id
        {
            if preparing.offer.run_id != run_id || preparing.offer.attempt_id != attempt_id {
                return Err(AssignmentManagerFailure::ConflictingOffer);
            }
            let Some(LocalSlot::Preparing(mut preparing)) = self.slot.take() else {
                return Err(AssignmentManagerFailure::ConflictingOffer);
            };
            preparing.cancellation.cancel();
            if let Some(event) = preparing.preparation_event.take() {
                event.finish(TelemetryOutcome::Cancelled);
            }
            let offer = preparing.offer.clone();
            let response = rejected(
                &offer,
                AssignmentDecline::RunnerUnable(RunnerUnableReason::SourceServiceUnavailable),
            );
            self.retain_decision(offer, response)?;
            self.retire_assignment_observations(assignment_id);
            if let Some(root) = preparing.root.take() {
                self.begin_assignment_cleanup(
                    assignment_id.to_owned(),
                    root,
                    ProcessQuiescence::Proven,
                    ReleaseAfter::Idle,
                );
            } else {
                self.slot = Some(LocalSlot::Preparing(preparing));
            }
            return Ok(());
        }
        if matches!(
            &self.slot,
            Some(LocalSlot::Accepted(accepted))
                if accepted.identity.assignment_id == assignment_id
        ) {
            let Some(LocalSlot::Accepted(accepted)) = self.slot.take() else {
                return Err(AssignmentManagerFailure::ConflictingOffer);
            };
            self.retire_assignment_observations(assignment_id);
            self.begin_assignment_cleanup(
                assignment_id.to_owned(),
                accepted.root,
                ProcessQuiescence::Proven,
                ReleaseAfter::Idle,
            );
            return Ok(());
        }
        match &mut self.slot {
            Some(LocalSlot::Running(running))
                if running.identity.assignment_id == assignment_id
                    && reason == "execution_lease_expired" =>
            {
                revoke_authority(running);
            }
            _ => {}
        }
        Ok(())
    }

    pub(in crate::service) fn pending_observations(
        &mut self,
        in_flight: &BTreeSet<u64>,
        limit: usize,
    ) -> Vec<PendingAssignmentObservation> {
        self.drain_events();
        self.outbox.pending(in_flight, limit)
    }

    pub(in crate::service) fn acknowledge_observation(&mut self, id: u64) {
        self.drain_events();
        let Some(observation) = self.outbox.acknowledge(id) else {
            return;
        };
        if let AssignmentObservation::Artifact {
            delivery_id,
            request: ArtifactRequest::ConfirmResult { .. },
        } = &observation
        {
            self.artifact_delivery
                .acknowledged_result_confirmation(*delivery_id);
        }
        if let AssignmentObservation::CancellationApplied(application) = &observation
            && let Some(retained) = self.cancellations.iter_mut().find(|retained| {
                retained.command.request_id == application.request_id
                    && retained.observation_id == Some(id)
            })
        {
            retained.observation_id = None;
        }
        if let AssignmentObservation::Decision(decision) = &observation
            && let Some(retained) = self
                .decisions
                .iter_mut()
                .find(|retained| retained.offer.assignment_id == decision.assignment_id())
            && retained.response_observation_id == Some(id)
        {
            retained.response_observation_id = None;
        }
        if observation.is_terminal() {
            if self.cleanup_failure_report == Some(id) {
                self.cleanup_failure_report = None;
            }
            if let Some(event) = self.run_events.remove(observation.assignment_id()) {
                event.finish(None);
            }
            if self.lease_clock_failure_report == Some(id) {
                self.lease_clock_failure_report = None;
            }
            if matches!(
                &self.slot,
                Some(LocalSlot::Finishing(finishing)) if finishing.final_observation_id == id
            ) {
                let Some(LocalSlot::Finishing(finishing)) = self.slot.take() else {
                    return;
                };
                self.reporting = None;
                if let Some(root) = finishing.root {
                    self.begin_assignment_finalization(
                        finishing.identity.assignment_id,
                        root,
                        ProcessQuiescence::Proven,
                        finishing.workspace_disposition,
                        ReleaseAfter::Idle,
                    );
                }
            } else if self
                .reporting
                .as_ref()
                .is_some_and(|reporting| reporting.assignment_id == observation.assignment_id())
            {
                self.reporting = None;
            }
        }
        self.outbox.wake();
    }

    pub(in crate::service) fn retain_observation_frame(
        &self,
        id: u64,
        retained_frame: RetainedObservationFrame,
    ) {
        self.outbox.retain_frame(id, retained_frame);
    }

    pub(in crate::service) fn claim_observation_for_send(&self, id: u64) -> bool {
        self.outbox.claim_for_transport(id)
    }

    pub(in crate::service) fn mark_observation_encoded(&self, id: u64) {
        self.outbox.mark_encoded(id);
        self.outbox.wake();
    }

    pub(in crate::service) fn handle_artifact_response(
        &mut self,
        observation_id: u64,
        delivery_id: u64,
        response: ArtifactCloudResponse,
    ) -> Result<(), ArtifactDeliveryProtocolFailure> {
        self.drain_events();
        let acknowledged = self.outbox.contains(observation_id);
        if !acknowledged && response.request_kind() != ArtifactRequestKind::ConfirmResult {
            return Err(ArtifactDeliveryProtocolFailure);
        }
        self.artifact_delivery
            .handle_response(delivery_id, response)?;
        if acknowledged {
            self.outbox.acknowledge(observation_id);
        }
        Ok(())
    }

    pub(in crate::service) fn finish_transport(&mut self) {
        self.drain_events();
        let removed = self.outbox.finish_transport();
        for decision in &mut self.decisions {
            if decision
                .response_observation_id
                .is_some_and(|id| removed.contains(&id))
            {
                decision.response_observation_id = None;
            }
        }
        for cancellation in &mut self.cancellations {
            if cancellation
                .observation_id
                .is_some_and(|id| removed.contains(&id))
            {
                cancellation.observation_id = None;
            }
        }
    }

    pub(in crate::service) fn begin_shutdown(&mut self) -> Result<(), AssignmentManagerFailure> {
        self.drain_events();
        let cleanup_deadline = match self.shutdown_cleanup_deadline {
            Some(deadline) => deadline,
            None => self
                .lease_clock
                .now()
                .and_then(|now| now.checked_add(super::super::SHUTDOWN_CLEANUP_START_TIMEOUT))
                .map_err(|_| AssignmentManagerFailure::LeaseClock)?,
        };
        self.shutdown_cleanup_deadline = Some(cleanup_deadline);
        self.shutting_down = true;
        let Some(slot) = self.slot.take() else {
            self.outbox.wake();
            return Ok(());
        };
        match slot {
            LocalSlot::Preparing(preparing) => {
                preparing.cancellation.cancel();
                let detached_root_preparation = preparing.root.is_none()
                    && preparing.prepare_effect_id.is_none()
                    && preparing
                        .root_preparation
                        .as_ref()
                        .is_some_and(|handoff| handoff.detach());
                if !detached_root_preparation {
                    self.slot = Some(LocalSlot::Preparing(preparing));
                }
            }
            LocalSlot::Accepted(accepted) => {
                let identity = accepted.identity.clone();
                let root = accepted.root;
                self.finish_before_execution(identity, root, "graceful_shutdown")?;
            }
            LocalSlot::Running(running) => {
                running
                    .infrastructure_interruption
                    .send_replace(Some(InfrastructureInterruption::RunnerShutdown));
                disable_workflow_git_off_thread(&running.workflow_git);
                running
                    .cancellation
                    .request_cancellation(CancellationReason::RunnerShutdown);
                self.slot = Some(LocalSlot::Running(running));
            }
            LocalSlot::Finishing(finishing) => {
                self.start_final_grace(
                    finishing.identity.assignment_id.clone(),
                    finishing.final_observation_id,
                    cleanup_deadline,
                    false,
                )
                .map_err(|_| AssignmentManagerFailure::LeaseClock)?;
                self.slot = Some(LocalSlot::Finishing(finishing));
            }
            LocalSlot::Releasing(releasing) => {
                self.slot = Some(LocalSlot::Releasing(releasing));
            }
        }
        self.outbox.wake();
        Ok(())
    }
}
