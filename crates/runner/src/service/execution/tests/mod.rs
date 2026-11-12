// Runner execution and artifact delivery intentionally own separate broker fixtures;
// their matching imports keep each test module independently readable.
use std::collections::BTreeSet;
use std::sync::Arc;

use super::*;
use crate::service::lease_clock::{LeaseTimerRelease, controlled_lease_clock};
use crate::service::test_support::{controlled_sleeper, sleep_request, with_watchdog};
use um_execution::{
    CommitFailure, CommitFailureClass, ExecutionSettlement, ObservedExecutions,
    RecoveryFailureKind, SettledExecution, StartedExecution, StepFailureKind,
};
use um_runner_protocol::{MAXIMUM_ORDINARY_FRAME_BYTES, RunnerEnvelope, RunnerFrame};

#[test]
fn git_capture_diagnostic_is_sibling_only_for_the_matching_failed_node() {
    let diagnostics = StepDiagnosticLog::default();
    diagnostics.record_git_capture_failure(
        "capture",
        &um_execution::GitCaptureFailure::RequiredObjectsUnavailable,
    );
    let detail = json!({"phase": "output_capture", "code": "git_required_objects_unavailable", "output": "bundle"});
    let diagnostic = git_capture_diagnostic("capture", &detail, &diagnostics).unwrap();
    assert_eq!(diagnostic, json!({"stage": "git_capture"}));
    assert!(git_capture_diagnostic("other", &detail, &diagnostics).is_none());
    assert!(
        git_capture_diagnostic(
            "capture",
            &json!({
                "phase": "execution", "code": "command_exit",
            }),
            &diagnostics
        )
        .is_none()
    );
    let report = ExecutionReport::Finished {
        final_execution_event_sequence: 1,
        outcome: json!({"outcome":"failed", "primaryIssue": {
            "node": {"id":"capture", "role":"step"}, "state":"failed", "detail": detail,
        }, "forceAbort":null}),
        artifact_delivery: json!({"outcome":"prepared", "artifactSetId":"ats_01k0z6r1w8f4jy2m7q9v3x5abc"}),
        diagnostic: Some(diagnostic),
    };
    let frame = report.runner_frame(
        RunnerEnvelope {
            message_id: "rmsg_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
            runner_id: "rnr_01k0z6r1w8f4jy2m7q9v3x5abd".to_owned(),
            boot_id: "rbt_01k0z6r1w8f4jy2m7q9v3x5abe".to_owned(),
            sequence: 1,
            sent_at: "2026-07-23T00:00:00Z".to_owned(),
        },
        "asn_01k0z6r1w8f4jy2m7q9v3x5abh".to_owned(),
        "atm_01k0z6r1w8f4jy2m7q9v3x5abk".to_owned(),
    );
    let bytes = um_runner_protocol::encode_runner_frame(&frame).unwrap();
    let value: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(value["payload"]["diagnostic"]["stage"], "git_capture");
    assert!(
        value["payload"]["outcome"]["primaryIssue"]
            .get("diagnostic")
            .is_none()
    );
}

#[test]
fn upload_failure_diagnostic_is_sent_as_artifact_delivery_sibling() {
    let outcome = ArtifactDeliveryOutcome::Failed(
        crate::service::artifact_delivery::ClosedArtifactDeliveryFailure {
            phase: "upload".to_owned(),
            code: "result_upload_failed".to_owned(),
            diagnostic: Some(json!({"stage":"artifact_upload", "httpStatus":503})),
        },
    );
    assert_eq!(
        artifact_delivery_result(&outcome),
        json!({
            "outcome": "failed", "phase": "upload", "code": "result_upload_failed",
            "diagnostic": {"stage":"artifact_upload", "httpStatus":503},
        })
    );
}

#[test]
fn typed_outbox_and_staging_causes_keep_the_failure_identity() {
    for (error, suffix) in [
        (OutboxFailure::Encoding, "encoding_failed"),
        (OutboxFailure::Capacity, "capacity_exceeded"),
        (OutboxFailure::Sequence, "sequence_exhausted"),
    ] {
        assert_eq!(
            outbox_cause(error, false),
            format!("start_observation_{suffix}")
        );
        assert_eq!(
            outbox_cause(error, true),
            format!("terminal_observation_{suffix}")
        );
    }
    assert_eq!(
        artifact_staging_cause(ArtifactStagingFailure::StagingParentExposed),
        "artifact_staging_parent_exposed"
    );
    assert_eq!(
        input_staging_cause(InputStagingFailure::IdentityUnavailable),
        "input_staging_identity_unavailable"
    );
    assert_eq!(
        agent_input_staging_cause(AgentInputStagingFailure::ExecutionRootUnavailable),
        "agent_input_execution_root_unavailable"
    );
    let sentinel = "/private/sentinel/SECRET";
    let error = std::io::Error::new(std::io::ErrorKind::PermissionDenied, sentinel);
    assert_eq!(
        diagnostic_open_cause(&error),
        "diagnostic_directory_permission_denied"
    );
}

#[test]
fn collapse_causes_are_variant_based_and_ignore_error_text() {
    let sentinel = "/private/sentinel/runner secret ERROR TEXT";
    for (kind, expected) in [
        (
            std::io::ErrorKind::NotFound,
            "agent_dispatcher_resource_missing",
        ),
        (
            std::io::ErrorKind::PermissionDenied,
            "agent_dispatcher_permission_denied",
        ),
        (std::io::ErrorKind::Other, "agent_dispatcher_io_failure"),
    ] {
        let error = std::io::Error::new(kind, sentinel);
        let cause = dispatcher_cause(&error);
        assert_eq!(cause, expected);
        assert!(!cause.contains(sentinel));
    }
    let cases = [
        (
            CoordinationError::ArtifactStagingMismatch,
            "artifact_staging_mismatch",
        ),
        (
            CoordinationError::InputStagingMismatch,
            "input_staging_mismatch",
        ),
        (
            CoordinationError::AgentInputStagingMismatch,
            "agent_input_staging_mismatch",
        ),
        (
            CoordinationError::AgentRuntimeUnavailable,
            "agent_runtime_unavailable",
        ),
        (
            CoordinationError::CommitFailed(CommitFailure {
                class: CommitFailureClass::StorageFull,
                operation: Some("write temporary state"),
                os_error: Some(28),
            }),
            "coordination_commit_failed",
        ),
        (
            CoordinationError::OccurrenceChannelClosed,
            "occurrence_channel_closed",
        ),
        (CoordinationError::OccurrenceConflict, "occurrence_conflict"),
        (
            CoordinationError::OccurrenceIdentityCapacityExceeded,
            "occurrence_identity_capacity_exceeded",
        ),
        (
            CoordinationError::OccurrenceOrdinalExhausted,
            "occurrence_ordinal_exhausted",
        ),
        (
            CoordinationError::ReducerStateUnavailable,
            "reducer_state_unavailable",
        ),
        (
            CoordinationError::TransitionCapacityExceeded,
            "transition_capacity_exceeded",
        ),
    ];
    let (recorder, capture) = telemetry::test_recorder("rbt_fixture");
    let event = recorder.start("runner.run", []);
    event.set(KeyValue::new(
        telemetry::attribute::FAILURE_CAUSE_TYPE,
        dispatcher_cause(&std::io::Error::new(std::io::ErrorKind::NotFound, sentinel)),
    ));
    event.finish(telemetry::Outcome::Failure);
    let encoded = serde_json::to_string(&capture.event("runner.run")).unwrap();
    assert!(!encoded.contains(sentinel));
    let span = capture
        .spans()
        .into_iter()
        .find(|span| span.name == "runner.run")
        .unwrap();
    assert_eq!(
        span.attributes
            .iter()
            .find(|attribute| attribute.key.as_str() == telemetry::attribute::FAILURE_CAUSE_TYPE)
            .unwrap()
            .value
            .to_string(),
        "agent_dispatcher_resource_missing"
    );

    let mut slugs = BTreeSet::new();
    for (error, expected) in cases {
        assert_eq!(coordination_cause(error), expected);
        assert!(slugs.insert(expected));
    }
}

fn lease_authority(basis: LeaseInstant) -> LeaseAuthority {
    LeaseAuthority {
        sequence: 4,
        basis,
        renewal_request: basis.checked_add(Duration::from_secs(2)).unwrap(),
        cancellation_start: basis.checked_add(Duration::from_secs(4)).unwrap(),
        force_stop_start: basis.checked_add(Duration::from_secs(5)).unwrap(),
        force_stop_end: basis.checked_add(Duration::from_secs(8)).unwrap(),
        local_expiry: basis.checked_add(Duration::from_secs(12)).unwrap(),
        terminal_report_delivery_budget: Duration::from_secs(7),
        revoked: false,
    }
}

struct SupervisedLeaseFixture {
    result: tokio::sync::oneshot::Sender<&'static str>,
    task: tokio::task::JoinHandle<LeaseExecution<&'static str>>,
    waits: tokio::sync::mpsc::UnboundedReceiver<(Duration, LeaseTimerRelease)>,
    cancellation: CancellationSource,
    fence: PostStopFence,
    guards: AssignmentProcessGuards,
    _authority: tokio::sync::watch::Sender<LeaseAuthority>,
    _infrastructure_interruption: tokio::sync::watch::Sender<Option<InfrastructureInterruption>>,
}

struct SupervisedExecution<Output> {
    task: tokio::task::JoinHandle<LeaseExecution<Output>>,
    cancellation: CancellationSource,
    fence: PostStopFence,
    guards: AssignmentProcessGuards,
    authority: tokio::sync::watch::Sender<LeaseAuthority>,
    infrastructure_interruption: tokio::sync::watch::Sender<Option<InfrastructureInterruption>>,
    outbox: ObservationOutbox,
}

fn supervise_execution<Execution, Output>(
    lease_clock: LeaseClock,
    authority: LeaseAuthority,
    execution: Execution,
) -> SupervisedExecution<Output>
where
    Execution: Future<Output = Output> + Send + 'static,
    Output: Send + 'static,
{
    supervise_execution_with_guards(
        lease_clock,
        authority,
        execution,
        AssignmentProcessGuards::new(),
    )
}

fn supervise_execution_with_guards<Execution, Output>(
    lease_clock: LeaseClock,
    authority: LeaseAuthority,
    execution: Execution,
    guards: AssignmentProcessGuards,
) -> SupervisedExecution<Output>
where
    Execution: Future<Output = Output> + Send + 'static,
    Output: Send + 'static,
{
    let cancellation = CancellationSource::new();
    let observed_cancellation = cancellation.clone();
    let outbox = ObservationOutbox::new();
    let observed_outbox = outbox.clone();
    let fence = PostStopFence::with_workflow_git(None);
    let observed_fence = fence.clone();
    let observed_guards = guards.clone();
    let causal_lease = CausalLease::new(authority.basis);
    let (authority_sender, authority_updates) = tokio::sync::watch::channel(authority);
    let (infrastructure_interruption, infrastructure_updates) = tokio::sync::watch::channel(None);
    let observed_infrastructure_interruption = infrastructure_interruption.clone();
    let task = tokio::spawn(async move {
        run_under_lease(
            execution,
            &cancellation,
            &lease_clock,
            authority_updates,
            infrastructure_updates,
            None,
            &causal_lease,
            &outbox,
            "asn_01k0z6r1w8f4jy2m7q9v3x5abc",
            "atm_01k0z6r1w8f4jy2m7q9v3x5abc",
            &fence,
            &guards,
        )
        .await
    });
    SupervisedExecution {
        task,
        cancellation: observed_cancellation,
        fence: observed_fence,
        guards: observed_guards,
        authority: authority_sender,
        infrastructure_interruption: observed_infrastructure_interruption,
        outbox: observed_outbox,
    }
}

fn supervised_lease_fixture() -> SupervisedLeaseFixture {
    let (lease_clock, _control, waits) = controlled_lease_clock();
    let basis = lease_clock.now().unwrap();
    let (result_sender, result) = tokio::sync::oneshot::channel();
    let supervised = supervise_execution(lease_clock, lease_authority(basis), async {
        result.await.expect("fixture result")
    });
    SupervisedLeaseFixture {
        result: result_sender,
        task: supervised.task,
        waits,
        cancellation: supervised.cancellation,
        fence: supervised.fence,
        guards: supervised.guards,
        _authority: supervised.authority,
        _infrastructure_interruption: supervised.infrastructure_interruption,
    }
}

async fn lease_wait_request(
    waits: &mut tokio::sync::mpsc::UnboundedReceiver<(Duration, LeaseTimerRelease)>,
    expected: Duration,
) -> LeaseTimerRelease {
    loop {
        let (duration, release) = waits
            .recv()
            .await
            .expect("controlled lease clock closed before the expected timer");
        if duration == expected {
            return release;
        }
    }
}

fn assert_forced_containment(
    cancellation: &CancellationSource,
    fence: &PostStopFence,
    guards: &AssignmentProcessGuards,
) {
    assert_eq!(
        cancellation.cancellation_reason(),
        Some(CancellationReason::ExecutionLeaseExpired)
    );
    assert!(fence.is_fenced());
    assert!(guards.forced_containment_started());
}

fn unavailable_timer_fixture() -> (LeaseClock, LeaseAuthority) {
    let (lease_clock, control, _waits) = controlled_lease_clock();
    let authority = lease_authority(lease_clock.now().unwrap());
    control.make_timer_unavailable();
    (lease_clock, authority)
}

async fn lease_clock_failure_outcome<Output: Send + 'static>(
    task: tokio::task::JoinHandle<LeaseExecution<Output>>,
) -> LeaseExecution<Output> {
    with_watchdog(task)
        .await
        .expect("timer failure supervision timed out")
        .expect("timer failure supervision task failed")
}

async fn assert_lease_clock_failure<Output: Send + 'static>(
    supervised: SupervisedExecution<Output>,
) {
    assert!(matches!(
        lease_clock_failure_outcome(supervised.task).await,
        LeaseExecution::LeaseClockFailed {
            quiescent: true,
            ..
        }
    ));
    assert_forced_containment(
        &supervised.cancellation,
        &supervised.fence,
        &supervised.guards,
    );
}

#[tokio::test]
async fn lease_renewal_continues_while_command_launch_is_stalled() {
    let (lease_clock, _control, mut waits) = controlled_lease_clock();
    let basis = lease_clock.now().unwrap();
    let (launch_started, started) = tokio::sync::oneshot::channel();
    let (release_launch, released) = std::sync::mpsc::channel();
    let execution = async move {
        spawn_isolated_command_launch(move || {
            let _ = launch_started.send(());
            let _ = released.recv();
            "launch-completed"
        })
        .await
        .expect("blocking launch task failed")
    };
    let supervised = supervise_execution(lease_clock, lease_authority(basis), execution);

    started.await.expect("blocking launch did not start");
    let notification = supervised.outbox.notification();
    lease_wait_request(&mut waits, Duration::from_secs(2))
        .await
        .release();
    let renewal = with_watchdog(async {
        loop {
            let notified = notification.notified();
            tokio::pin!(notified);
            if let Some(renewal) = supervised
                .outbox
                .pending(&BTreeSet::new(), 4)
                .into_iter()
                .find(|pending| {
                    matches!(
                        pending.observation,
                        AssignmentObservation::LeaseRenewalRequested { .. }
                    )
                })
            {
                break renewal;
            }
            notified.await;
        }
    })
    .await
    .expect("lease renewal was delayed by the blocking launch");
    let renewal_frame = renewal.observation.runner_frame(RunnerEnvelope {
        message_id: "rmsg_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
        runner_id: "rnr_01k0z6r1w8f4jy2m7q9v3x5abd".to_owned(),
        boot_id: "rbt_01k0z6r1w8f4jy2m7q9v3x5abe".to_owned(),
        sequence: 9,
        sent_at: "2026-07-23T00:00:02Z".to_owned(),
    });
    assert!(matches!(
        renewal_frame,
        RunnerFrame::ExecutionLeaseRenewalRequested {
            current_lease_sequence: 4,
            ..
        }
    ));

    release_launch.send(()).unwrap();
    assert!(matches!(
        with_watchdog(supervised.task)
            .await
            .expect("lease supervision timed out")
            .expect("lease supervision task failed"),
        LeaseExecution::Completed {
            output: "launch-completed",
            ..
        }
    ));
}

#[tokio::test]
async fn sixty_second_lease_grace_allows_clean_exit_after_thirty_seconds() {
    let (lease_clock, _control, _lease_waits) = controlled_lease_clock();
    let basis = lease_clock.now().unwrap();
    let (execution_sleeper, mut execution_sleeps) = controlled_sleeper();
    let workflow_sleeper = Arc::clone(&execution_sleeper);
    let supervised = supervise_execution(
        lease_clock,
        LeaseAuthority {
            sequence: 1,
            basis,
            renewal_request: basis,
            cancellation_start: basis,
            force_stop_start: basis.checked_add(Duration::from_secs(60)).unwrap(),
            force_stop_end: basis.checked_add(Duration::from_secs(65)).unwrap(),
            local_expiry: basis.checked_add(Duration::from_secs(72)).unwrap(),
            terminal_report_delivery_budget: Duration::from_secs(7),
            revoked: false,
        },
        async move {
            workflow_sleeper.sleep(Duration::from_secs(30)).await;
            "clean-exit"
        },
    );

    assert_eq!(
        with_watchdog(supervised.cancellation.wait_for_cancellation())
            .await
            .expect("lease cancellation was not requested"),
        CancellationReason::ExecutionLeaseExpired
    );
    sleep_request(&mut execution_sleeps, Duration::from_secs(30))
        .await
        .release();

    assert!(matches!(
        with_watchdog(supervised.task)
            .await
            .expect("lease supervision timed out")
            .expect("lease supervision task failed"),
        LeaseExecution::Completed {
            output: "clean-exit",
            ..
        }
    ));
    assert!(!supervised.fence.is_fenced());
    assert!(!supervised.guards.forced_containment_started());
}

// REQ-RUNNERENROLL-73
#[tokio::test]
async fn runner_shutdown_is_retained_separately_from_sticky_user_cancellation() {
    let (lease_clock, _control, _waits) = controlled_lease_clock();
    let basis = lease_clock.now().unwrap();
    let (result_sender, result) = tokio::sync::oneshot::channel();
    let supervised = supervise_execution(lease_clock, lease_authority(basis), async {
        result.await.expect("fixture result")
    });
    assert!(
        supervised
            .cancellation
            .request_cancellation(CancellationReason::UserRequest)
    );
    supervised
        .infrastructure_interruption
        .send_replace(Some(InfrastructureInterruption::RunnerShutdown));
    result_sender.send("user-stopped").unwrap();

    assert!(matches!(
        with_watchdog(supervised.task)
            .await
            .expect("shutdown supervision timed out")
            .expect("shutdown supervision task failed"),
        LeaseExecution::Completed {
            output: "user-stopped",
            infrastructure_interruption: Some(InfrastructureInterruption::RunnerShutdown),
            ..
        }
    ));
    assert_eq!(
        supervised.cancellation.cancellation_reason(),
        Some(CancellationReason::UserRequest)
    );
}

#[tokio::test]
async fn lease_loss_is_retained_separately_from_sticky_user_cancellation() {
    let mut fixture = supervised_lease_fixture();
    assert!(
        fixture
            .cancellation
            .request_cancellation(CancellationReason::UserRequest)
    );
    lease_wait_request(&mut fixture.waits, Duration::from_secs(2))
        .await
        .release();
    lease_wait_request(&mut fixture.waits, Duration::from_secs(2))
        .await
        .release();
    let _force_stop = lease_wait_request(&mut fixture.waits, Duration::from_secs(1)).await;
    fixture.result.send("user-stopped").unwrap();

    assert!(matches!(
        with_watchdog(fixture.task)
            .await
            .expect("lease-loss supervision timed out")
            .expect("lease-loss supervision task failed"),
        LeaseExecution::Completed {
            output: "user-stopped",
            infrastructure_interruption: Some(InfrastructureInterruption::ExecutionLeaseExpired),
            ..
        }
    ));
    assert_eq!(
        fixture.cancellation.cancellation_reason(),
        Some(CancellationReason::UserRequest)
    );
}

#[tokio::test]
async fn lease_timer_failure_contains_without_accepting_ready_progress() {
    let (lease_clock, authority) = unavailable_timer_fixture();
    let (result_sender, result) = tokio::sync::oneshot::channel();
    let supervised = supervise_execution(lease_clock, authority, async {
        result.await.expect("fixture result")
    });

    assert_lease_clock_failure(supervised).await;
    assert!(result_sender.send("ready-late").is_err());
}

#[tokio::test]
async fn persistent_lease_timer_failure_waits_for_observed_quiescence() {
    let (lease_clock, authority) = unavailable_timer_fixture();
    let quiescent = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let guards = AssignmentProcessGuards::new();
    guards.use_quiescence_fixture(Arc::clone(&quiescent));
    let completion_quiescent = Arc::clone(&quiescent);
    let (result_sender, result) = tokio::sync::oneshot::channel();
    let supervised = supervise_execution_with_guards(
        lease_clock,
        authority,
        async move {
            result.await.expect("fixture result");
            completion_quiescent.store(true, std::sync::atomic::Ordering::Release);
        },
        guards,
    );

    assert_eq!(
        with_watchdog(supervised.cancellation.wait_for_cancellation())
            .await
            .expect("timer failure did not begin containment"),
        CancellationReason::ExecutionLeaseExpired
    );
    result_sender
        .send(())
        .expect("timer failure dropped execution before quiescence");
    assert_lease_clock_failure(supervised).await;
}

#[tokio::test]
async fn exact_stop_boundary_fences_before_a_ready_late_result() {
    let mut fixture = supervised_lease_fixture();
    lease_wait_request(&mut fixture.waits, Duration::from_secs(2))
        .await
        .release();
    lease_wait_request(&mut fixture.waits, Duration::from_secs(2))
        .await
        .release();
    assert_eq!(
        with_watchdog(fixture.cancellation.wait_for_cancellation())
            .await
            .expect("lease cancellation was not requested"),
        CancellationReason::ExecutionLeaseExpired
    );
    let stop_boundary = lease_wait_request(&mut fixture.waits, Duration::from_secs(1)).await;
    fixture.result.send("late-success").unwrap();
    stop_boundary.release();

    let result = with_watchdog(fixture.task)
        .await
        .expect("lease supervision timed out")
        .expect("lease supervision task failed");
    assert!(fixture.fence.is_fenced());
    assert!(fixture.guards.forced_containment_started());
    assert!(matches!(
        result,
        LeaseExecution::Completed {
            output: "late-success",
            infrastructure_interruption: Some(InfrastructureInterruption::ExecutionLeaseExpired),
        }
    ));
}

#[tokio::test]
async fn overlong_suspend_contains_before_ready_completion() {
    let (lease_clock, control, mut waits) = controlled_lease_clock();
    let basis = lease_clock.now().unwrap();
    let (result_sender, result) = tokio::sync::oneshot::channel();
    let supervised = supervise_execution(lease_clock, lease_authority(basis), async {
        result.await.expect("fixture result")
    });

    lease_wait_request(&mut waits, Duration::from_secs(2))
        .await
        .release();
    let _delayed_cancellation_wake = lease_wait_request(&mut waits, Duration::from_secs(2)).await;
    control.simulate_suspend(Duration::from_secs(4));
    result_sender.send("ready-after-suspend").unwrap();

    let outcome = with_watchdog(supervised.task)
        .await
        .expect("lease supervision timed out")
        .expect("lease supervision task failed");
    assert!(matches!(outcome, LeaseExecution::Completed { .. }));
    assert_eq!(
        supervised.cancellation.cancellation_reason(),
        Some(CancellationReason::ExecutionLeaseExpired),
        "the first runner action after a suspend past force-stop start must cancel"
    );
    assert!(
        supervised.fence.is_fenced(),
        "the first runner action after a suspend past force-stop start must fence output"
    );
    assert!(
        supervised.guards.forced_containment_started(),
        "the first runner action after a suspend past force-stop start must contain processes"
    );
}

#[tokio::test]
async fn force_reap_accepts_exact_boundary_and_rejects_late_completion() {
    let mut exact = supervised_lease_fixture();
    for duration in [
        Duration::from_secs(2),
        Duration::from_secs(2),
        Duration::from_secs(1),
    ] {
        lease_wait_request(&mut exact.waits, duration)
            .await
            .release();
    }
    let reap_boundary = lease_wait_request(&mut exact.waits, Duration::from_secs(3)).await;
    exact.result.send("exact-boundary").unwrap();
    reap_boundary.release();

    assert!(matches!(
        with_watchdog(exact.task)
            .await
            .expect("lease supervision timed out")
            .expect("lease supervision task failed"),
        LeaseExecution::Completed {
            output: "exact-boundary",
            ..
        }
    ));

    let mut late = supervised_lease_fixture();
    for duration in [
        Duration::from_secs(2),
        Duration::from_secs(2),
        Duration::from_secs(1),
        Duration::from_secs(3),
    ] {
        lease_wait_request(&mut late.waits, duration)
            .await
            .release();
    }
    assert!(matches!(
        with_watchdog(late.task)
            .await
            .expect("late lease supervision timed out")
            .expect("late lease supervision task failed"),
        LeaseExecution::ContainmentDeadline
    ));
    assert!(late.result.send("one-unit-late").is_err());
}

#[test]
fn recovery_agent_allocation_failure_retains_harness_stderr_without_native_session() {
    let observer = RunnerExecutionObserver::new(
        "asn_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
        "atm_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
        4,
        ObservationOutbox::new(),
        PostStopFence::with_workflow_git(None),
        CancellationSource::new(),
        RunnerInvocationEvidence {
            recovery_agent_steps: BTreeSet::from(["verify".to_owned()]),
            ..RunnerInvocationEvidence::default()
        },
        RunEvent::default(),
    );
    let diagnostic = StepDiagnostic::from_streams(
        um_execution::CapturedDiagnosticStream::from_parts(b"".as_slice(), 0, true),
        um_execution::CapturedDiagnosticStream::from_parts(
            b"allocation failed".as_slice(),
            0,
            true,
        ),
    );
    let now = RunnerExecutionClock.now();
    let handler = RunnerActiveInvocation {
        id: ActionId {
            transition_sequence: TransitionSequence(2),
        },
        role: ActiveStepInvocation::RecoveryHandler {
            round: RecoveryRoundNumber::fixture(1),
        },
        started_at: now,
    };
    let result = observer
        .invocation_evidence_with_diagnostic(
            "verify",
            handler,
            now,
            false,
            Some(diagnostic.clone()),
        )
        .expect("handler evidence");
    assert!(result.diagnostic_reference.is_none());
    assert_eq!(result.diagnostics.len(), 2);
    assert_eq!(
        result.diagnostics[1].kind,
        RecoveryDiagnosticKindV1::AgentHarnessStderr
    );
    assert_eq!(
        serde_json::to_value(&result.diagnostics[1]).unwrap()["stream"]["data"],
        BASE64_STANDARD.encode(b"allocation failed")
    );
    assert_eq!(
        result.diagnostics[1].reference,
        "runner/invocations/2/stderr"
    );

    let command = observer
        .invocation_evidence_with_diagnostic("other", handler, now, false, Some(diagnostic))
        .expect("command evidence");
    assert_eq!(
        command.diagnostics[1].kind,
        RecoveryDiagnosticKindV1::CommandStderr
    );
}

#[tokio::test]
async fn recovery_cancellation_preserves_bounded_invocation_evidence() {
    let outbox = ObservationOutbox::new();
    let observer = RunnerExecutionObserver::new(
        "asn_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
        "atm_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
        4,
        outbox.clone(),
        PostStopFence::with_workflow_git(None),
        CancellationSource::new(),
        RunnerInvocationEvidence::default(),
        RunEvent::default(),
    );
    let target = ActionId {
        transition_sequence: TransitionSequence(1),
    };
    let handler = ActionId {
        transition_sequence: TransitionSequence(2),
    };
    observer
        .observe(ExecutionObservation::Transition(Box::new(
            TransitionObservation {
                event: TransitionEvent::Step {
                    sequence: TransitionSequence(1),
                    step: "verify".to_owned(),
                    role: WorkflowNodeRole::Step,
                    failure_policy: FailurePolicy::Required,
                    from: StepStateKind::Pending,
                    to: StepStateKind::Starting,
                },
                step: None,
                executions: ObservedExecutions::default(),
            },
        )))
        .await;
    observer
        .observe(ExecutionObservation::Transition(Box::new(
            TransitionObservation {
                event: TransitionEvent::Step {
                    sequence: TransitionSequence(2),
                    step: "verify".to_owned(),
                    role: WorkflowNodeRole::Step,
                    failure_policy: FailurePolicy::Required,
                    from: StepStateKind::Running,
                    to: StepStateKind::Recovering,
                },
                step: Some(ObservedStepTransition::Recovery {
                    active: ActiveStepInvocation::RecoveryHandler {
                        round: RecoveryRoundNumber::fixture(1),
                    },
                    active_invocation_id: handler,
                    settled_invocation: Some((
                        target,
                        ActiveStepInvocation::Target {
                            execution_number: TargetExecutionNumber::fixture(1),
                        },
                    )),
                    configured_rounds: 1,
                    handler_kind: Some(RecoveryHandlerKind::Command),
                    handler_state: Some(RecoveryHandlerActivity::Starting),
                    decision: None,
                }),
                executions: ObservedExecutions::default(),
            },
        )))
        .await;

    let observations = outbox.pending(&BTreeSet::new(), 4);
    let AssignmentObservation::Execution {
        report: ExecutionReport::Transition { workflow_event, .. },
        ..
    } = &observations[1].observation
    else {
        panic!("recovery transition was not enqueued");
    };
    assert_eq!(
        workflow_event["recoveryProgress"]["activeRole"],
        "recovery_handler"
    );
    assert_eq!(workflow_event["invocationEvidence"]["invocationId"], 1);
    assert_eq!(workflow_event["invocationEvidence"]["role"], "target");
    assert!(serde_json::to_vec(workflow_event).unwrap().len() <= MAXIMUM_ORDINARY_FRAME_BYTES);

    let detail = serde_json::from_value(json!({"code": "user_request"})).unwrap();
    for (sequence, from, to, step) in [
        (
            3,
            StepStateKind::Recovering,
            StepStateKind::Cancelling,
            ObservedStepTransition::Cancelling { detail },
        ),
        (
            4,
            StepStateKind::Cancelling,
            StepStateKind::Cancelled,
            ObservedStepTransition::Cancelled { detail },
        ),
    ] {
        observer
            .observe(ExecutionObservation::Transition(Box::new(
                TransitionObservation {
                    event: TransitionEvent::Step {
                        sequence: TransitionSequence(sequence),
                        step: "verify".to_owned(),
                        role: WorkflowNodeRole::Step,
                        failure_policy: FailurePolicy::Required,
                        from,
                        to,
                    },
                    step: Some(step),
                    executions: ObservedExecutions::default(),
                },
            )))
            .await;
    }
    assert_eq!(observer.fault(), None);
    let observations = outbox.pending(&BTreeSet::new(), 4);
    assert_eq!(observations.len(), 4);
    let AssignmentObservation::Execution {
        report: ExecutionReport::Transition { workflow_event, .. },
        ..
    } = &observations[3].observation
    else {
        panic!("cancelled transition was not enqueued");
    };
    assert_eq!(workflow_event["to"], "cancelled");
    assert_eq!(workflow_event["detail"]["code"], "user_request");
    assert_eq!(workflow_event["invocationEvidence"]["invocationId"], 2);
    assert_eq!(
        workflow_event["invocationEvidence"]["role"],
        "recovery_handler"
    );
    assert_eq!(workflow_event["invocationEvidence"]["state"], "cancelled");
    assert_eq!(workflow_event["invocationEvidence"]["recoveryRound"], 1);
}

#[tokio::test]
async fn observer_retains_encoding_fault_and_stops_emitting_transitions() {
    let outbox = ObservationOutbox::new();
    let observer = RunnerExecutionObserver::new(
        "asn_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
        "atm_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
        2,
        outbox.clone(),
        PostStopFence::with_workflow_git(None),
        CancellationSource::new(),
        RunnerInvocationEvidence::default(),
        RunEvent::default(),
    );
    for sequence in [0, 1] {
        observer
            .observe(ExecutionObservation::Transition(Box::new(
                TransitionObservation {
                    event: TransitionEvent::Step {
                        sequence: TransitionSequence(sequence),
                        step: "prepare".to_owned(),
                        role: WorkflowNodeRole::Step,
                        failure_policy: FailurePolicy::Required,
                        from: StepStateKind::Pending,
                        to: StepStateKind::Starting,
                    },
                    step: None,
                    executions: ObservedExecutions::default(),
                },
            )))
            .await;
    }
    assert_eq!(
        observer.fault(),
        Some(ObserverFault::Outbox(OutboxFailure::Encoding))
    );
    assert_eq!(
        observer.fault().unwrap().cause(),
        "transition_observation_encoding_failed"
    );
    assert_eq!(observer.last_sequence(), 0);
    assert!(outbox.pending(&BTreeSet::new(), 2).is_empty());
}

#[tokio::test]
async fn post_stop_fence_rejects_late_success_but_allows_lease_terminal() {
    let outbox = ObservationOutbox::new();
    let fence = PostStopFence::with_workflow_git(None);
    let observer = RunnerExecutionObserver::new(
        "asn_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
        "atm_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
        2,
        outbox.clone(),
        fence.clone(),
        CancellationSource::new(),
        RunnerInvocationEvidence::default(),
        RunEvent::default(),
    );
    fence.fence();
    observer
        .observe(ExecutionObservation::Transition(Box::new(
            TransitionObservation {
                event: TransitionEvent::Workflow {
                    sequence: Default::default(),
                    from: WorkflowState::Executing {
                        gate: SchedulingGate::Open,
                    },
                    to: Box::new(WorkflowState::Succeeded),
                },
                step: None,
                executions: ObservedExecutions::default(),
            },
        )))
        .await;
    assert_eq!(observer.last_sequence(), 0);
    assert!(outbox.pending(&BTreeSet::new(), 1).is_empty());

    let lease_terminal = TransitionObservation {
        event: TransitionEvent::Workflow {
            sequence: Default::default(),
            from: WorkflowState::Executing {
                gate: SchedulingGate::Cancelling {
                    reason: CancellationReason::ExecutionLeaseExpired,
                    prior_issue: None,
                },
            },
            to: Box::new(WorkflowState::Cancelled {
                reason: CancellationReason::ExecutionLeaseExpired,
            }),
        },
        step: None,
        executions: ObservedExecutions::default(),
    };
    assert!(is_lease_loss_terminal_transition(&lease_terminal));
}

#[tokio::test]
async fn post_stop_fence_rearms_lease_loss_at_finalization_boundary() {
    let cancellation = CancellationSource::new();
    assert!(cancellation.request_cancellation(CancellationReason::ExecutionLeaseExpired));
    assert!(cancellation.fixture_begin_finalization_arm());

    let fence = PostStopFence::with_workflow_git(None);
    fence.fence();
    let observer = RunnerExecutionObserver::new(
        "asn_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
        "atm_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
        1,
        ObservationOutbox::new(),
        fence,
        cancellation.clone(),
        RunnerInvocationEvidence::default(),
        RunEvent::default(),
    );
    observer
        .observe(ExecutionObservation::Transition(Box::new(
            TransitionObservation {
                event: TransitionEvent::Workflow {
                    sequence: Default::default(),
                    from: WorkflowState::Executing {
                        gate: SchedulingGate::Cancelling {
                            reason: CancellationReason::ExecutionLeaseExpired,
                            prior_issue: None,
                        },
                    },
                    to: Box::new(WorkflowState::Finalizing {
                        trigger: FinalizationTrigger::Cancelled,
                        gate: FinalizationGate::Open,
                        primary_issue: None,
                    }),
                },
                step: None,
                executions: ObservedExecutions::default(),
            },
        )))
        .await;
    assert!(cancellation.fixture_complete_finalization_arm());

    assert_eq!(
        cancellation.cancellation_reason(),
        Some(CancellationReason::ExecutionLeaseExpired),
        "lease loss must close the newly committed finalization gate even after the post-stop observation fence",
    );
}

#[test]
fn assignment_supervisor_registers_before_release_and_closes_on_containment() {
    let guards = AssignmentProcessGuards::new();
    let registry = guards.registry(true);
    let identity = AuthenticatedProcessGroup::new(
        rustix::process::Pid::from_raw(41).unwrap(),
        "fixture-start".to_owned(),
    )
    .unwrap();
    let mut registration = registry.register("step", 9, &identity).unwrap();

    registration.mark_released().unwrap();
    guards.begin_forced_containment();
    assert!(guards.forced_containment_started());
    assert!(registry.register("later", 10, &identity).is_err());
    registration.mark_quiesced().unwrap();
    assert!(guards.is_quiescent());
}

#[test]
fn advisory_step_transition_preserves_policy_and_raw_disposition() {
    let transition = TransitionObservation::<RunnerExecutionInstant> {
        event: TransitionEvent::Step {
            sequence: TransitionSequence::default(),
            step: "lint".to_owned(),
            role: WorkflowNodeRole::Step,
            failure_policy: FailurePolicy::Advisory,
            from: StepStateKind::Pending,
            to: StepStateKind::Blocked,
        },
        step: Some(ObservedStepTransition::Blocked {
            detail: BlockedDetail::new([Prerequisite::control("analyze").unwrap()]).unwrap(),
        }),
        executions: ObservedExecutions::default(),
    };

    assert_eq!(
        workflow_event(&transition, None, None),
        json!({
            "eventVersion": 1,
            "eventType": "step_state_changed",
            "transitionSequence": 0,
            "stepId": "lint",
            "role": "step",
            "failurePolicy": "advisory",
            "from": "pending",
            "to": "blocked",
            "detail": {
                "code": "prerequisites_unsatisfied",
                "prerequisites": [{"kind": "control", "node": "analyze"}]
            },
        })
    );
}

#[test]
fn invalid_result_publication_retains_the_original_command_failure() {
    use std::os::unix::fs::PermissionsExt as _;

    let private_root = tempfile::tempdir().unwrap();
    let issue: PrimaryIssue = serde_json::from_value(json!({
        "node": {"id": "finish", "role": "step"},
        "state": "failed",
        "detail": {
            "phase": "execution",
            "code": "command_exit",
            "exitCode": 42,
        },
    }))
    .unwrap();
    let detail = serde_json::from_value(workflow_issue(&issue)["detail"].clone()).unwrap();
    let diagnostic = StepDiagnostic::from_streams(
        um_execution::CapturedDiagnosticStream::from_parts(b"".as_slice(), 0, true),
        um_execution::CapturedDiagnosticStream::from_parts(
            b"publication failed".as_slice(),
            0,
            true,
        ),
    );
    let step = WorkflowRunStep {
        id: "finish".to_owned(),
        role: WorkflowNodeRole::Step,
        kind: WorkflowRunStepKind::Command,
        failure_policy: FailurePolicy::Required,
        state: StepState::Failed { detail },
        timing: Some(WorkflowStepTiming {
            started_at: OffsetDateTime::UNIX_EPOCH,
            duration: Duration::from_millis(1),
        }),
        command_output: Some(diagnostic),
        recovery: None,
        invocations: Vec::new(),
    };
    retain_result_publication_failure(
        private_root.path(),
        &RunOutcome::Failed {
            primary_issue: issue,
            later_cancellation: None,
        },
        &[step],
        None,
        ("serialization", "invalid_run_result", Some("step_metadata")),
    )
    .unwrap();

    let path = private_root.path().join("result-publication-failure.json");
    assert_eq!(
        std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    let record: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    assert_eq!(record["primaryIssue"]["node"]["id"], "finish");
    assert_eq!(record["primaryIssue"]["detail"]["exitCode"], 42);
    assert_eq!(record["publicationFailure"]["invariant"], "step_metadata");
    assert_eq!(record["stepMetadata"][0]["state"], "failed");
    let stderr = record["failedCommandOutput"]["stderr"]["data"]
        .as_str()
        .unwrap();
    assert_eq!(
        BASE64_STANDARD.decode(stderr).unwrap(),
        b"publication failed"
    );
}

fn step_execution_observer(
    run_event: RunEvent,
    outbox: ObservationOutbox,
) -> RunnerExecutionObserver {
    RunnerExecutionObserver::new(
        "asn_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
        "atm_01k0z6r1w8f4jy2m7q9v3x5abc".to_owned(),
        32,
        outbox,
        PostStopFence::with_workflow_git(None),
        CancellationSource::new(),
        RunnerInvocationEvidence::default(),
        run_event,
    )
}

fn step_execution_run_event() -> (RunEvent, crate::telemetry::TestCapture) {
    let (recorder, capture) = crate::telemetry::test_recorder("rbt_fixture");
    let run_event = RunEvent::started_fixture(
        &recorder,
        "run_01k0z6r1w8f4jy2m7q9v3x5abc",
        "asn_01k0z6r1w8f4jy2m7q9v3x5abc",
        "atm_01k0z6r1w8f4jy2m7q9v3x5abc",
    );
    (run_event, capture)
}

fn invocation(sequence: u64) -> ActionId {
    ActionId {
        transition_sequence: TransitionSequence(sequence),
    }
}

fn target(execution_number: u8) -> ActiveStepInvocation {
    ActiveStepInvocation::Target {
        execution_number: TargetExecutionNumber::fixture(execution_number),
    }
}

fn handler(round: u8) -> ActiveStepInvocation {
    ActiveStepInvocation::RecoveryHandler {
        round: RecoveryRoundNumber::fixture(round),
    }
}

fn started_execution(
    sequence: u64,
    role: ActiveStepInvocation,
    recovery_failure_kind: Option<RecoveryFailureKind>,
) -> Option<StartedExecution> {
    Some(StartedExecution {
        invocation: invocation(sequence),
        role,
        recovery_failure_kind,
    })
}

fn settled_execution(
    sequence: u64,
    role: ActiveStepInvocation,
    settlement: ExecutionSettlement,
) -> Option<SettledExecution> {
    Some(SettledExecution {
        invocation: invocation(sequence),
        role,
        settlement,
    })
}

// One step transition at `sequence` whose wire projection is `observed` and
// whose physical executions are `settled` and `started`.
fn step_execution_transition(
    sequence: u64,
    (step, role): (&str, WorkflowNodeRole),
    (from, to): (StepStateKind, StepStateKind),
    observed: Option<ObservedStepTransition>,
    settled: Option<SettledExecution>,
    started: Option<StartedExecution>,
) -> TransitionObservation<RunnerExecutionInstant> {
    TransitionObservation {
        event: TransitionEvent::Step {
            sequence: TransitionSequence(sequence),
            step: step.to_owned(),
            role,
            failure_policy: FailurePolicy::Required,
            from,
            to,
        },
        step: observed,
        executions: ObservedExecutions { settled, started },
    }
}

fn recovery_progress(
    active: (u64, ActiveStepInvocation),
    settled: (u64, ActiveStepInvocation),
    decision: Option<RecoveryDecisionKind>,
) -> Option<ObservedStepTransition> {
    Some(ObservedStepTransition::Recovery {
        active: active.1,
        active_invocation_id: invocation(active.0),
        settled_invocation: Some((invocation(settled.0), settled.1)),
        configured_rounds: 1,
        handler_kind: Some(RecoveryHandlerKind::Command),
        handler_state: decision
            .is_none()
            .then_some(RecoveryHandlerActivity::Starting),
        decision,
    })
}

async fn observe_all(
    observer: &RunnerExecutionObserver,
    transitions: &[TransitionObservation<RunnerExecutionInstant>],
) {
    for transition in transitions {
        observer
            .observe(ExecutionObservation::Transition(Box::new(
                transition.clone(),
            )))
            .await;
    }
}

fn step_execution_records(
    capture: &crate::telemetry::TestCapture,
) -> Vec<serde_json::Map<String, Value>> {
    capture
        .events()
        .into_iter()
        .filter(|event| event["event.name"] == "runner.step_execution")
        .collect()
}

// The enqueued workflow events, without the wall-clock times of their
// invocation evidence.
fn enqueued_workflow_events(outbox: &ObservationOutbox) -> Vec<Value> {
    outbox
        .pending(&BTreeSet::new(), 64)
        .into_iter()
        .map(|entry| match entry.observation {
            AssignmentObservation::Execution {
                report:
                    ExecutionReport::Transition {
                        mut workflow_event, ..
                    },
                ..
            } => {
                if let Some(evidence) = workflow_event
                    .get_mut("invocationEvidence")
                    .and_then(Value::as_object_mut)
                {
                    for key in ["startedAt", "finishedAt", "durationMilliseconds"] {
                        evidence.remove(key);
                    }
                }
                workflow_event
            }
            _ => panic!("expected a transition observation"),
        })
        .collect()
}

// A finalizer `wrap` that succeeds, then a step `implement` whose first target
// fails its command, whose recovery handler succeeds and whose recheck succeeds.
fn finalizer_and_recovered_step() -> Vec<TransitionObservation<RunnerExecutionInstant>> {
    use StepStateKind::{CapturingOutputs, Pending, Recovering, Running, Starting, Succeeded};
    let wrap = ("wrap", WorkflowNodeRole::Finalizer);
    let implement = ("implement", WorkflowNodeRole::Step);
    vec![
        step_execution_transition(
            1,
            wrap,
            (Pending, Starting),
            None,
            None,
            started_execution(1, target(1), None),
        ),
        step_execution_transition(
            2,
            wrap,
            (CapturingOutputs, Succeeded),
            None,
            settled_execution(1, target(1), ExecutionSettlement::Succeeded),
            None,
        ),
        step_execution_transition(
            3,
            implement,
            (Pending, Starting),
            None,
            None,
            started_execution(3, target(1), None),
        ),
        step_execution_transition(
            4,
            implement,
            (Running, Recovering),
            recovery_progress((4, handler(1)), (3, target(1)), None),
            settled_execution(
                3,
                target(1),
                ExecutionSettlement::Failed(StepFailureKind::Command),
            ),
            started_execution(4, handler(1), None),
        ),
        step_execution_transition(
            5,
            implement,
            (Recovering, Starting),
            recovery_progress(
                (5, target(2)),
                (4, handler(1)),
                Some(RecoveryDecisionKind::Recheck),
            ),
            settled_execution(4, handler(1), ExecutionSettlement::Succeeded),
            started_execution(5, target(2), Some(RecoveryFailureKind::CommandExit)),
        ),
        step_execution_transition(
            6,
            implement,
            (CapturingOutputs, Succeeded),
            None,
            settled_execution(5, target(2), ExecutionSettlement::Succeeded),
            None,
        ),
    ]
}

fn span_attribute_json(value: &opentelemetry::Value) -> Value {
    match value {
        opentelemetry::Value::Bool(value) => json!(value),
        opentelemetry::Value::I64(value) => json!(value),
        opentelemetry::Value::String(value) => json!(value.as_str()),
        other => panic!("unexpected span attribute {other:?}"),
    }
}

fn is_step_execution_attribute(key: &str) -> bool {
    key.starts_with("um.") || key == "error.type"
}

// REQ-TELEMETRY-41
// REQ-TELEMETRY-42
// REQ-TELEMETRY-43
#[tokio::test]
async fn settled_step_executions_are_root_records_on_both_sinks() {
    let (run_event, capture) = step_execution_run_event();
    let observer = step_execution_observer(run_event, ObservationOutbox::new());
    observe_all(&observer, &finalizer_and_recovered_step()).await;
    assert_eq!(observer.fault(), None);
    assert_eq!(observer.last_sequence(), 6);

    let records = step_execution_records(&capture);
    assert_eq!(records.len(), 4);
    let expected = [
        ("wrap", "finalizer", 1, None, None, "success", None),
        (
            "implement",
            "ordinary",
            1,
            None,
            None,
            "failure",
            Some("step_command_failed"),
        ),
        (
            "implement",
            "recovery_handler",
            1,
            Some(1),
            None,
            "success",
            None,
        ),
        (
            "implement",
            "ordinary",
            2,
            None,
            Some("command_exit"),
            "success",
            None,
        ),
    ];
    for (record, (step, role, execution_number, round, failure_kind, outcome, error_type)) in
        records.iter().zip(expected)
    {
        assert_eq!(record["um.run.id"], "run_01k0z6r1w8f4jy2m7q9v3x5abc");
        assert_eq!(record["um.assignment.id"], "asn_01k0z6r1w8f4jy2m7q9v3x5abc");
        assert_eq!(record["um.attempt.id"], "atm_01k0z6r1w8f4jy2m7q9v3x5abc");
        assert_eq!(record["um.step.id"], step);
        assert_eq!(record["um.step.role"], role);
        assert_eq!(record["um.step.execution_number"], execution_number);
        assert_eq!(
            record.get("um.step.recovery_round").and_then(Value::as_i64),
            round
        );
        assert_eq!(
            record
                .get("um.step.recovery_failure_kind")
                .and_then(Value::as_str),
            failure_kind
        );
        assert_eq!(record["um.outcome"], outcome);
        assert_eq!(record.get("error.type").and_then(Value::as_str), error_type);
        assert!(record.get("parent_span_id").is_none());
        assert!(record.get("um.runner.id").is_none());
    }

    let spans = capture
        .spans()
        .into_iter()
        .filter(|span| span.name == "runner.step_execution")
        .collect::<Vec<_>>();
    assert_eq!(spans.len(), 4);
    for record in &records {
        let span = spans
            .iter()
            .find(|span| {
                span.span_context.span_id().to_string() == record["span_id"].as_str().unwrap()
            })
            .expect("each record has its span");
        assert_eq!(
            span.span_context.trace_id().to_string(),
            record["trace_id"].as_str().unwrap()
        );
        assert_eq!(span.parent_span_id, opentelemetry::trace::SpanId::INVALID);
        let span_attributes = span
            .attributes
            .iter()
            .filter(|attribute| is_step_execution_attribute(attribute.key.as_str()))
            .map(|attribute| {
                (
                    attribute.key.as_str().to_owned(),
                    span_attribute_json(&attribute.value),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let record_attributes = record
            .iter()
            .filter(|(key, _)| is_step_execution_attribute(key))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(span_attributes, record_attributes);
    }

    let first_implement = records
        .iter()
        .filter(|record| {
            record["um.step.role"] == "ordinary"
                && record["um.step.id"] == "implement"
                && record["um.step.execution_number"] == 1
        })
        .collect::<Vec<_>>();
    assert_eq!(first_implement.len(), 1);
    assert_eq!(first_implement[0]["um.outcome"], "failure");
}

// REQ-TELEMETRY-41
#[tokio::test]
async fn unexecuted_steps_and_starts_without_an_execution_write_nothing() {
    use StepStateKind::{Blocked, NotRun, Pending, Skipped, Starting};
    let (run_event, capture) = step_execution_run_event();
    let observer = step_execution_observer(run_event, ObservationOutbox::new());
    let step = WorkflowNodeRole::Step;
    let transitions = [
        step_execution_transition(
            1,
            ("blocked", step),
            (Pending, Blocked),
            Some(ObservedStepTransition::Blocked {
                detail: BlockedDetail::new([Prerequisite::control("analyze").unwrap()]).unwrap(),
            }),
            None,
            None,
        ),
        step_execution_transition(
            2,
            ("not_run", step),
            (Pending, NotRun),
            Some(ObservedStepTransition::NotRun {
                detail: serde_json::from_value(json!({"code": "failure_stop"})).unwrap(),
            }),
            None,
            None,
        ),
        step_execution_transition(
            3,
            ("skipped", step),
            (Pending, Skipped),
            Some(ObservedStepTransition::Skipped {
                detail: serde_json::from_value(json!({
                    "code": "condition_false",
                    "evaluatedPredicates": [{"path": "", "result": false}],
                }))
                .unwrap(),
            }),
            None,
            None,
        ),
        step_execution_transition(4, ("starting", step), (Pending, Starting), None, None, None),
    ];
    observe_all(&observer, &transitions).await;
    assert_eq!(observer.fault(), None);
    assert_eq!(observer.last_sequence(), 4);
    assert!(step_execution_records(&capture).is_empty());
    assert_eq!(capture.span_count("runner.step_execution"), 0);
}

// REQ-TELEMETRY-43
#[tokio::test]
async fn step_execution_outcome_follows_the_settlement_kind() {
    let cancelled = |code| {
        Some(ObservedStepTransition::Cancelled {
            detail: serde_json::from_value(json!({"code": code})).unwrap(),
        })
    };
    for (role, (from, to), observed, settlement, outcome, error_type) in [
        (
            target(1),
            (StepStateKind::Cancelling, StepStateKind::Cancelled),
            cancelled("execution_lease_expired"),
            ExecutionSettlement::Cancelled(CancellationReason::ExecutionLeaseExpired),
            "timeout",
            Some("step_execution_timeout"),
        ),
        (
            target(1),
            (StepStateKind::Cancelling, StepStateKind::Cancelled),
            cancelled("user_request"),
            ExecutionSettlement::Cancelled(CancellationReason::UserRequest),
            "cancelled",
            None,
        ),
        (
            handler(1),
            (StepStateKind::Running, StepStateKind::Failed),
            Some(ObservedStepTransition::Failed {
                detail: serde_json::from_value(json!({
                    "phase": "execution", "code": "command_exit", "exitCode": 1,
                }))
                .unwrap(),
            }),
            ExecutionSettlement::Failed(StepFailureKind::RecoveryHandler),
            "failure",
            Some("step_recovery_handler_failed"),
        ),
    ] {
        let (run_event, capture) = step_execution_run_event();
        let observer = step_execution_observer(run_event, ObservationOutbox::new());
        let verify = ("verify", WorkflowNodeRole::Step);
        observe_all(
            &observer,
            &[
                step_execution_transition(
                    1,
                    verify,
                    (StepStateKind::Pending, StepStateKind::Starting),
                    None,
                    None,
                    started_execution(1, role, None),
                ),
                step_execution_transition(
                    2,
                    verify,
                    (from, to),
                    observed,
                    settled_execution(1, role, settlement),
                    None,
                ),
            ],
        )
        .await;
        assert_eq!(observer.fault(), None, "{settlement:?}");
        assert_eq!(observer.last_sequence(), 2, "{settlement:?}");
        let records = step_execution_records(&capture);
        assert_eq!(records.len(), 1, "{settlement:?}");
        assert_eq!(records[0]["um.outcome"], outcome, "{settlement:?}");
        assert_eq!(
            records[0].get("error.type").and_then(Value::as_str),
            error_type,
            "{settlement:?}"
        );
    }
}

// REQ-TELEMETRY-41
#[tokio::test]
async fn step_executions_without_a_recorder_leave_the_transition_stream_unchanged() {
    let transitions = finalizer_and_recovered_step();
    let (run_event, _capture) = step_execution_run_event();
    let recorded_outbox = ObservationOutbox::new();
    let recorded = step_execution_observer(run_event, recorded_outbox.clone());
    observe_all(&recorded, &transitions).await;

    let outbox = ObservationOutbox::new();
    let observer = step_execution_observer(RunEvent::default(), outbox.clone());
    observe_all(&observer, &transitions).await;

    assert_eq!(observer.fault(), None);
    assert_eq!(observer.last_sequence(), 6);
    let events = enqueued_workflow_events(&outbox);
    assert_eq!(events.len(), 6);
    assert_eq!(events, enqueued_workflow_events(&recorded_outbox));
}
