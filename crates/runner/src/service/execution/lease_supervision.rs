use super::*;

#[derive(Clone, Copy)]
pub(super) struct LeaseFailureContext<'a> {
    cancellation: &'a CancellationSource,
    post_stop_fence: &'a PostStopFence,
    process_guards: &'a AssignmentProcessGuards,
}

pub(super) enum LeaseExecution<Output> {
    Completed {
        output: Output,
        infrastructure_interruption: Option<InfrastructureInterruption>,
    },
    ContainmentDeadline,
    LeaseClockFailed {
        quiescent: bool,
        error: LeaseClockError,
    },
}

#[expect(
    clippy::too_many_arguments,
    reason = "lease supervision receives every authority and containment boundary explicitly"
)]
pub(super) async fn run_under_lease<F, Output>(
    execution: F,
    cancellation: &CancellationSource,
    lease_clock: &LeaseClock,
    mut authority_updates: tokio::sync::watch::Receiver<LeaseAuthority>,
    mut infrastructure_updates: tokio::sync::watch::Receiver<Option<InfrastructureInterruption>>,
    mut initial_wait: Option<(u64, LeaseWait)>,
    causal_lease: &CausalLease,
    outbox: &ObservationOutbox,
    assignment_id: &str,
    attempt_id: &str,
    post_stop_fence: &PostStopFence,
    process_guards: &AssignmentProcessGuards,
) -> LeaseExecution<Output>
where
    F: Future<Output = Output>,
{
    tokio::pin!(execution);
    let mut infrastructure_interruption = *infrastructure_updates.borrow_and_update();
    loop {
        let authority = authority_updates.borrow_and_update().clone();
        if let Some(interruption) = *infrastructure_updates.borrow_and_update() {
            infrastructure_interruption = Some(interruption);
        }
        let failure = LeaseFailureContext {
            cancellation,
            post_stop_fence,
            process_guards,
        };
        let now = match lease_clock.now() {
            Ok(now) => now,
            Err(error) => {
                return fail_lease_clock(error, cancellation, post_stop_fence, process_guards);
            }
        };
        let cancellation_due = match now.checked_cmp(authority.cancellation_start) {
            Ok(ordering) => ordering != std::cmp::Ordering::Less,
            Err(error) => {
                return fail_lease_clock(error, cancellation, post_stop_fence, process_guards);
            }
        };
        if authority.revoked || cancellation_due {
            return finish_after_lease_loss(
                &mut execution,
                cancellation,
                lease_clock,
                &authority,
                post_stop_fence,
                process_guards,
            )
            .await;
        }
        let armed_wait = match initial_wait.take() {
            Some((sequence, wait)) if sequence == authority.sequence => Some(wait),
            Some(_) | None => None,
        };
        tokio::select! {
            biased;
            wait = wait_for_lease_deadline_or_armed(
                lease_clock,
                authority.renewal_request,
                armed_wait,
            ) => {
                if let Err(error) = wait {
                    return fail_lease_timer(&mut execution, failure, error).await;
                }
                let now = match lease_clock.now() {
                    Ok(now) => now,
                    Err(error) => return fail_lease_clock(error, cancellation, post_stop_fence, process_guards),
                };
                if !matches!(
                    now.checked_cmp(authority.cancellation_start),
                    Ok(std::cmp::Ordering::Less)
                ) {
                    return finish_after_lease_loss(
                        &mut execution,
                        cancellation,
                        lease_clock,
                        &authority,
                        post_stop_fence,
                        process_guards,
                    ).await;
                }
                match causal_lease.request_renewal(
                    authority.sequence,
                    assignment_id,
                    attempt_id,
                    lease_clock,
                    outbox,
                ) {
                    Ok(()) => {}
                    Err(RenewalRequestFailure::LeaseClock) => {
                        return fail_lease_clock(LeaseClockError::ClockUnavailable, cancellation, post_stop_fence, process_guards);
                    }
                    Err(RenewalRequestFailure::Outbox | RenewalRequestFailure::Sequence) => {
                        return finish_after_lease_loss(
                            &mut execution,
                            cancellation,
                            lease_clock,
                            &authority,
                            post_stop_fence,
                            process_guards,
                        ).await;
                    }
                }
                tokio::select! {
                    biased;
                    wait = wait_for_lease_deadline(lease_clock, authority.cancellation_start) => {
                        if let Err(error) = wait {
                            return fail_lease_timer(&mut execution, failure, error).await;
                        }
                        let latest_authority = authority_updates.borrow_and_update().clone();
                        if latest_authority != authority {
                            continue;
                        }
                        return finish_after_lease_loss(
                            &mut execution,
                            cancellation,
                            lease_clock,
                            &authority,
                            post_stop_fence,
                            process_guards,
                        ).await;
                    }
                    changed = authority_updates.changed() => {
                        if changed.is_err() {
                            return finish_after_lease_loss(
                                &mut execution,
                                cancellation,
                                lease_clock,
                                &authority,
                                post_stop_fence,
                                process_guards,
                            ).await;
                        }
                    }
                    changed = infrastructure_updates.changed() => {
                        if changed.is_ok()
                            && let Some(interruption) = *infrastructure_updates.borrow_and_update()
                        {
                            infrastructure_interruption = Some(interruption);
                        }
                    }
                    result = &mut execution => {
                        return complete_ready_execution(
                            result,
                            failure,
                            lease_clock,
                            &authority,
                            infrastructure_interruption,
                            false,
                        );
                    }
                }
            }
            changed = authority_updates.changed() => {
                if changed.is_err() {
                    return finish_after_lease_loss(
                        &mut execution,
                        cancellation,
                        lease_clock,
                        &authority,
                        post_stop_fence,
                        process_guards,
                    ).await;
                }
            }
            changed = infrastructure_updates.changed() => {
                if changed.is_ok()
                    && let Some(interruption) = *infrastructure_updates.borrow_and_update()
                {
                    infrastructure_interruption = Some(interruption);
                }
            }
            result = &mut execution => {
                return complete_ready_execution(
                    result,
                    failure,
                    lease_clock,
                    &authority,
                    infrastructure_interruption,
                    false,
                );
            }
        }
    }
}

pub(super) fn complete_ready_execution<Output>(
    output: Output,
    failure: LeaseFailureContext<'_>,
    lease_clock: &LeaseClock,
    authority: &LeaseAuthority,
    infrastructure_interruption: Option<InfrastructureInterruption>,
    lease_already_lost: bool,
) -> LeaseExecution<Output> {
    let LeaseFailureContext {
        cancellation,
        post_stop_fence,
        process_guards,
    } = failure;
    let now = match lease_clock.now() {
        Ok(now) => now,
        Err(error) => {
            return fail_lease_clock(error, cancellation, post_stop_fence, process_guards);
        }
    };
    if !lease_already_lost {
        match now.checked_cmp(authority.cancellation_start) {
            Ok(std::cmp::Ordering::Less) if !authority.revoked => {
                return LeaseExecution::Completed {
                    output,
                    infrastructure_interruption,
                };
            }
            Ok(_) => {}
            Err(error) => {
                return fail_lease_clock(error, cancellation, post_stop_fence, process_guards);
            }
        }
    }
    cancellation.request_cancellation(CancellationReason::ExecutionLeaseExpired);
    match now.checked_cmp(authority.force_stop_start) {
        Ok(std::cmp::Ordering::Less) => {
            return LeaseExecution::Completed {
                output,
                infrastructure_interruption: Some(
                    InfrastructureInterruption::ExecutionLeaseExpired,
                ),
            };
        }
        Ok(std::cmp::Ordering::Equal | std::cmp::Ordering::Greater) => {
            begin_forced_containment(cancellation, post_stop_fence, process_guards);
        }
        Err(error) => {
            return fail_lease_clock(error, cancellation, post_stop_fence, process_guards);
        }
    }
    match lease_clock
        .now()
        .and_then(|now| now.checked_cmp(authority.force_stop_end))
    {
        Ok(std::cmp::Ordering::Greater) => LeaseExecution::ContainmentDeadline,
        Ok(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
            if process_guards.is_quiescent() =>
        {
            LeaseExecution::Completed {
                output,
                infrastructure_interruption: Some(
                    InfrastructureInterruption::ExecutionLeaseExpired,
                ),
            }
        }
        Ok(std::cmp::Ordering::Less | std::cmp::Ordering::Equal) => {
            LeaseExecution::ContainmentDeadline
        }
        Err(error) => LeaseExecution::LeaseClockFailed {
            quiescent: process_guards.is_quiescent(),
            error,
        },
    }
}

pub(super) async fn finish_after_lease_loss<F, Output>(
    execution: &mut std::pin::Pin<&mut F>,
    cancellation: &CancellationSource,
    lease_clock: &LeaseClock,
    authority: &LeaseAuthority,
    post_stop_fence: &PostStopFence,
    process_guards: &AssignmentProcessGuards,
) -> LeaseExecution<Output>
where
    F: Future<Output = Output>,
{
    cancellation.request_cancellation(CancellationReason::ExecutionLeaseExpired);
    let failure = LeaseFailureContext {
        cancellation,
        post_stop_fence,
        process_guards,
    };
    let now = match lease_clock.now() {
        Ok(now) => now,
        Err(error) => {
            return fail_lease_clock(error, cancellation, post_stop_fence, process_guards);
        }
    };
    let before_force_stop = match now.checked_cmp(authority.force_stop_start) {
        Ok(std::cmp::Ordering::Less) => true,
        Ok(std::cmp::Ordering::Equal | std::cmp::Ordering::Greater) => false,
        Err(error) => {
            return fail_lease_clock(error, cancellation, post_stop_fence, process_guards);
        }
    };
    if before_force_stop {
        tokio::select! {
            biased;
            wait = wait_for_lease_deadline(lease_clock, authority.force_stop_start) => {
                if let Err(error) = wait {
                    return fail_lease_timer(execution, failure, error).await;
                }
            }
            output = execution.as_mut() => {
                return complete_ready_execution(
                    output,
                    failure,
                    lease_clock,
                    authority,
                    Some(InfrastructureInterruption::ExecutionLeaseExpired),
                    true,
                );
            }
        }
    }

    begin_forced_containment(cancellation, post_stop_fence, process_guards);
    let now = match lease_clock.now() {
        Ok(now) => now,
        Err(error) => {
            return LeaseExecution::LeaseClockFailed {
                quiescent: process_guards.is_quiescent(),
                error,
            };
        }
    };
    match now.checked_cmp(authority.force_stop_end) {
        Ok(std::cmp::Ordering::Greater) => return LeaseExecution::ContainmentDeadline,
        Ok(std::cmp::Ordering::Less | std::cmp::Ordering::Equal) => {}
        Err(error) => {
            return LeaseExecution::LeaseClockFailed {
                quiescent: process_guards.is_quiescent(),
                error,
            };
        }
    }
    tokio::select! {
        biased;
        output = execution.as_mut() => complete_ready_execution(
            output,
            failure,
            lease_clock,
            authority,
            Some(InfrastructureInterruption::ExecutionLeaseExpired),
            true,
        ),
        wait = wait_for_lease_deadline(lease_clock, authority.force_stop_end) => {
            match wait {
                Ok(()) => LeaseExecution::ContainmentDeadline,
                Err(error) => fail_lease_timer(execution, failure, error).await,
            }
        }
    }
}

pub(super) async fn fail_lease_timer<F, Output>(
    execution: &mut std::pin::Pin<&mut F>,
    failure: LeaseFailureContext<'_>,
    error: LeaseClockError,
) -> LeaseExecution<Output>
where
    F: Future<Output = Output>,
{
    let LeaseFailureContext {
        cancellation,
        post_stop_fence,
        process_guards,
    } = failure;
    begin_forced_containment(cancellation, post_stop_fence, process_guards);
    if !process_guards.is_quiescent() {
        let _ = execution.as_mut().await;
    }
    LeaseExecution::LeaseClockFailed {
        quiescent: process_guards.is_quiescent(),
        error,
    }
}

pub(super) fn fail_lease_clock<Output>(
    error: LeaseClockError,
    cancellation: &CancellationSource,
    post_stop_fence: &PostStopFence,
    process_guards: &AssignmentProcessGuards,
) -> LeaseExecution<Output> {
    begin_forced_containment(cancellation, post_stop_fence, process_guards);
    LeaseExecution::LeaseClockFailed {
        quiescent: process_guards.is_quiescent(),
        error,
    }
}

pub(super) fn begin_forced_containment(
    cancellation: &CancellationSource,
    post_stop_fence: &PostStopFence,
    process_guards: &AssignmentProcessGuards,
) {
    post_stop_fence.fence();
    cancellation.request_cancellation(CancellationReason::ExecutionLeaseExpired);
    process_guards.begin_forced_containment();
}

pub(super) async fn wait_for_lease_deadline(
    lease_clock: &LeaseClock,
    deadline: LeaseInstant,
) -> Result<(), LeaseClockError> {
    wait_for_lease_deadline_or_armed(lease_clock, deadline, None).await
}

pub(super) async fn wait_for_lease_deadline_or_armed(
    lease_clock: &LeaseClock,
    deadline: LeaseInstant,
    armed: Option<LeaseWait>,
) -> Result<(), LeaseClockError> {
    let wait = match armed {
        Some(wait) => wait,
        None => lease_clock.start_wait(deadline)?,
    };
    let cancellation = LeaseWaitCancellation::default();
    wait.wait(&cancellation).await.map(|_| ())
}
