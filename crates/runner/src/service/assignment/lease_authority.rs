use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::service) struct LeaseAuthority {
    pub(in crate::service) sequence: u64,
    pub(in crate::service) basis: LeaseInstant,
    pub(in crate::service) renewal_request: LeaseInstant,
    pub(in crate::service) cancellation_start: LeaseInstant,
    pub(in crate::service) force_stop_start: LeaseInstant,
    pub(in crate::service) force_stop_end: LeaseInstant,
    pub(in crate::service) local_expiry: LeaseInstant,
    pub(in crate::service) terminal_report_delivery_budget: Duration,
    /// How long after asking for the next lease the runner asks again while no grant
    /// has replaced this authority: the renewal lead, `max(renewal_delivery_budget, 30 s)`.
    pub(in crate::service) renewal_retry_interval: Duration,
    pub(in crate::service) revoked: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum GrantValidationFailure {
    MissingBasis,
    Arithmetic(LeaseClockError),
}

impl LeaseAuthority {
    pub(in crate::service) fn permits_artifact_delivery(
        &self,
        now: LeaseInstant,
    ) -> Result<bool, LeaseClockError> {
        Ok(!self.revoked
            && matches!(
                now.checked_cmp(self.local_expiry)?,
                std::cmp::Ordering::Less
            ))
    }

    pub(super) fn derive(
        sequence: u64,
        basis: LeaseInstant,
        policy: &ExecutionLeasePolicy,
        cancellation_grace: Duration,
    ) -> Result<Self, LeaseClockError> {
        let lease_duration = Duration::from_millis(policy.lease_duration_milliseconds);
        let fencing_margin = Duration::from_millis(policy.fencing_margin_milliseconds);
        let renewal_delivery_budget = Duration::from_millis(
            u64::try_from(policy.renewal_delivery_budget_milliseconds)
                .map_err(|_| LeaseClockError::ArithmeticOverflow)?,
        );
        let force_stop_reap_budget = Duration::from_millis(
            u64::try_from(policy.force_stop_and_reap_budget_milliseconds)
                .map_err(|_| LeaseClockError::ArithmeticOverflow)?,
        );
        let terminal_report_delivery_budget = Duration::from_millis(
            u64::try_from(policy.terminal_report_delivery_budget_milliseconds)
                .map_err(|_| LeaseClockError::ArithmeticOverflow)?,
        );
        let local_expiry = basis.checked_add(lease_duration)?;
        let force_stop_start = local_expiry.checked_sub(fencing_margin)?;
        let cancellation_start = force_stop_start.checked_sub(cancellation_grace)?;
        // Ask a third of the way into the cancellation window, so two thirds of it
        // remain for the answer: a connection lost after the request and back within
        // that remainder still delivers the grant before cancellation must begin. A
        // welcomed policy fits two renewal leads in the smallest window, so the
        // remainder always holds more than one.
        let cancellation_window = cancellation_start.checked_duration_since(basis)?;
        let renewal_request = basis.checked_add(cancellation_window / 3)?;
        let force_stop_end = force_stop_start.checked_add(force_stop_reap_budget)?;
        let renewal_retry_interval = renewal_delivery_budget.max(MINIMUM_RENEWAL_HEADROOM);
        Ok(Self {
            sequence,
            basis,
            renewal_request,
            cancellation_start,
            force_stop_start,
            force_stop_end,
            local_expiry,
            terminal_report_delivery_budget,
            renewal_retry_interval,
            revoked: false,
        })
    }
}
