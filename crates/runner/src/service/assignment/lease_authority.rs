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
        // The advertised delivery budget is a minimum, not a scheduling target.
        // Welcomed policies reserve two full leads in the maximum-grace window,
        // so every admitted grace can use the ordinary 30-second target.
        let cancellation_window = cancellation_start.checked_duration_since(basis)?;
        let renewal_lead = renewal_delivery_budget
            .max(MINIMUM_RENEWAL_HEADROOM)
            .min(cancellation_window / 2);
        let renewal_request = cancellation_start.checked_sub(renewal_lead)?;
        let force_stop_end = force_stop_start.checked_add(force_stop_reap_budget)?;
        Ok(Self {
            sequence,
            basis,
            renewal_request,
            cancellation_start,
            force_stop_start,
            force_stop_end,
            local_expiry,
            terminal_report_delivery_budget,
            revoked: false,
        })
    }
}
