use crate::workflow::agent::{AgentObservation, AgentObservationEnvelope, AgentObservationSink};
use crate::workflow::agent_process_driver::PROCESS_GROUP_QUIESCENCE_PROBE_INTERVAL;
use crate::workflow::coordinator::CoordinatorClock;
use crate::workflow::result_validation::{
    ResultValidationWorker, RunningResultValidation, ValidationWorkerDecision,
    ValidationWorkerRequest,
};
use std::future::{Future, pending, ready};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, watch};

async fn process_group_probe(deadline: Duration) -> bool {
    if deadline != PROCESS_GROUP_QUIESCENCE_PROBE_INTERVAL {
        return false;
    }
    // An OS process-group probe has no readiness channel. Yield once so the
    // anti-hang watchdog and cancellation can run between probes.
    let probe = tokio::spawn(async {});
    let _ = probe.await;
    true
}

async fn wait_for_release(mut released: watch::Receiver<bool>) {
    while !*released.borrow_and_update() {
        if released.changed().await.is_err() {
            return;
        }
    }
}

// Shared agent-driver fixtures. The process-group probe is an explicit scheduling point.
#[derive(Clone, Copy)]
pub(crate) struct InlineValidationWorker;

pub(crate) struct InlineValidation(pub(crate) Option<Result<ValidationWorkerDecision, ()>>);

impl ResultValidationWorker for InlineValidationWorker {
    type Running = InlineValidation;

    fn start(&self, request: ValidationWorkerRequest) -> Result<Self::Running, ()> {
        Ok(InlineValidation(Some(request.evaluate())))
    }
}

impl RunningResultValidation for InlineValidation {
    fn wait(&mut self) -> impl Future<Output = Result<ValidationWorkerDecision, ()>> + Send {
        ready(self.0.take().expect("inline validation is awaited once"))
    }

    fn request_stop(&mut self) {}

    fn quiesce(self) -> impl Future<Output = ()> + Send {
        ready(())
    }
}

#[derive(Clone, Copy)]
pub(crate) struct PendingClock;

// The pending clock deliberately never releases ordinary deadlines, unlike the
// controlled clock's explicit watch gate; keep their trait implementations separate.
impl CoordinatorClock for PendingClock {
    type Instant = Duration;

    fn now(&mut self) -> Self::Instant {
        Duration::ZERO
    }

    async fn wait_until(&self, deadline: Self::Instant) {
        if !process_group_probe(deadline).await {
            pending().await
        }
    }
}

#[derive(Clone)]
pub(crate) struct ControlledClock {
    pub(crate) registrations: mpsc::UnboundedSender<Duration>,
    pub(crate) release: watch::Receiver<bool>,
}

pub(crate) struct ClockControl {
    pub(crate) deadlines: mpsc::UnboundedReceiver<Duration>,
    pub(crate) expired: watch::Sender<bool>,
}

impl ControlledClock {
    pub(crate) fn new() -> (Self, ClockControl) {
        let (registrations, deadlines) = mpsc::unbounded_channel();
        let (expired, release) = watch::channel(false);
        (
            Self {
                registrations,
                release,
            },
            ClockControl { deadlines, expired },
        )
    }
}

impl CoordinatorClock for ControlledClock {
    type Instant = Duration;

    fn now(&mut self) -> Self::Instant {
        Duration::ZERO
    }

    async fn wait_until(&self, deadline: Self::Instant) {
        if process_group_probe(deadline).await {
            return;
        }
        let _ = self.registrations.send(deadline);
        wait_for_release(self.release.clone()).await;
    }
}

#[derive(Clone, Default)]
pub(crate) struct RecordingObservationSink(Arc<Mutex<Vec<AgentObservationEnvelope>>>);

impl RecordingObservationSink {
    /// Native text and reasoning arrive as arbitrarily split deltas.
    pub(crate) fn concatenated_text(
        &self,
        select: impl Fn(&AgentObservation) -> Option<&str>,
    ) -> String {
        self.snapshot()
            .iter()
            .filter_map(|envelope| select(envelope.observation()))
            .collect()
    }

    pub(crate) fn snapshot(&self) -> Vec<AgentObservationEnvelope> {
        self.0.lock().expect("observation sink lock").clone()
    }
}

impl AgentObservationSink for RecordingObservationSink {
    fn observe(&self, observation: AgentObservationEnvelope) -> impl Future<Output = ()> + Send {
        self.0
            .lock()
            .expect("observation sink lock")
            .push(observation);
        ready(())
    }
}

pub(crate) mod step_clock {
    use crate::workflow::coordinator::CoordinatorClock;
    use std::ops::Add;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::sync::{mpsc, watch};
    #[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
    pub(crate) struct TestInstant(pub(crate) Duration);

    impl Add<Duration> for TestInstant {
        type Output = Self;

        fn add(self, duration: Duration) -> Self::Output {
            Self(self.0 + duration)
        }
    }

    #[derive(Clone, Copy)]
    pub(crate) struct TestClock;

    impl CoordinatorClock for TestClock {
        type Instant = TestInstant;

        fn now(&mut self) -> Self::Instant {
            TestInstant(Duration::ZERO)
        }

        async fn wait_until(&self, _deadline: Self::Instant) {
            std::future::pending().await
        }
    }

    #[derive(Clone)]
    pub(crate) struct ControlledClock {
        now: TestInstant,
        release: watch::Receiver<bool>,
        registrations: mpsc::UnboundedSender<TestInstant>,
        active_waiters: Arc<AtomicUsize>,
    }

    pub(crate) struct DeadlineControl {
        release: watch::Sender<bool>,
        pub(crate) registrations: mpsc::UnboundedReceiver<TestInstant>,
        active_waiters: Arc<AtomicUsize>,
    }

    struct DeadlineWaiterGuard(Arc<AtomicUsize>);

    impl Drop for DeadlineWaiterGuard {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }

    impl ControlledClock {
        pub(crate) fn new(now: TestInstant) -> (Self, DeadlineControl) {
            let (release, released) = watch::channel(false);
            let (registrations, registered) = mpsc::unbounded_channel();
            let active_waiters = Arc::new(AtomicUsize::new(0));
            (
                Self {
                    now,
                    release: released,
                    registrations,
                    active_waiters: Arc::clone(&active_waiters),
                },
                DeadlineControl {
                    release,
                    registrations: registered,
                    active_waiters,
                },
            )
        }
    }

    impl CoordinatorClock for ControlledClock {
        type Instant = TestInstant;

        fn now(&mut self) -> Self::Instant {
            self.now
        }

        async fn wait_until(&self, deadline: Self::Instant) {
            self.active_waiters.fetch_add(1, Ordering::SeqCst);
            let _guard = DeadlineWaiterGuard(Arc::clone(&self.active_waiters));
            let _ = self.registrations.send(deadline);
            super::wait_for_release(self.release.clone()).await;
        }
    }

    impl DeadlineControl {
        pub(crate) async fn next_deadline(&mut self) -> TestInstant {
            self.registrations.recv().await.unwrap()
        }

        pub(crate) fn release(&self) {
            self.release.send(true).unwrap();
        }

        pub(crate) fn active_waiters(&self) -> usize {
            self.active_waiters.load(Ordering::SeqCst)
        }
    }

    #[derive(Clone)]
    pub(crate) struct AdvancingClock {
        now: Arc<Mutex<TestInstant>>,
        changed: watch::Receiver<TestInstant>,
        registrations: mpsc::UnboundedSender<TestInstant>,
        active_waiters: Arc<AtomicUsize>,
    }

    pub(crate) struct AdvancingClockControl {
        now: Arc<Mutex<TestInstant>>,
        changed: watch::Sender<TestInstant>,
        pub(crate) registrations: mpsc::UnboundedReceiver<TestInstant>,
        active_waiters: Arc<AtomicUsize>,
    }

    impl AdvancingClock {
        pub(crate) fn new(now: TestInstant) -> (Self, AdvancingClockControl) {
            let (changed, changes) = watch::channel(now);
            let (registrations, registered) = mpsc::unbounded_channel();
            let now = Arc::new(Mutex::new(now));
            let active_waiters = Arc::new(AtomicUsize::new(0));
            (
                Self {
                    now: Arc::clone(&now),
                    changed: changes,
                    registrations,
                    active_waiters: Arc::clone(&active_waiters),
                },
                AdvancingClockControl {
                    now,
                    changed,
                    registrations: registered,
                    active_waiters,
                },
            )
        }
    }

    impl CoordinatorClock for AdvancingClock {
        type Instant = TestInstant;

        fn now(&mut self) -> Self::Instant {
            *self.now.lock().unwrap()
        }

        async fn wait_until(&self, deadline: Self::Instant) {
            self.active_waiters.fetch_add(1, Ordering::SeqCst);
            let _guard = DeadlineWaiterGuard(Arc::clone(&self.active_waiters));
            let _ = self.registrations.send(deadline);
            let mut changed = self.changed.clone();
            while *changed.borrow_and_update() < deadline {
                if changed.changed().await.is_err() {
                    return;
                }
            }
        }
    }

    impl AdvancingClockControl {
        pub(crate) async fn next_deadline(&mut self) -> TestInstant {
            self.registrations.recv().await.unwrap()
        }

        pub(crate) fn advance_to(&self, now: TestInstant) {
            *self.now.lock().unwrap() = now;
            self.changed.send(now).unwrap();
        }

        pub(crate) fn active_waiters(&self) -> usize {
            self.active_waiters.load(Ordering::SeqCst)
        }
    }
}

pub(crate) mod validation_fixtures {
    pub(crate) use super::step_clock::{TestClock as NeverClock, TestInstant};
    use crate::workflow::coordinator::CoordinatorClock;
    use crate::workflow::result_validation::{ResultValidationWorker, ValidationWorkerRequest};
    use std::future::Future;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::sync::{mpsc, watch};

    #[derive(Clone)]
    pub(crate) struct CountingValidationWorker {
        starts: Arc<AtomicUsize>,
    }

    impl CountingValidationWorker {
        pub(crate) fn new() -> Self {
            Self {
                starts: Arc::new(AtomicUsize::new(0)),
            }
        }

        pub(crate) fn starts(&self) -> usize {
            self.starts.load(Ordering::SeqCst)
        }
    }

    impl ResultValidationWorker for CountingValidationWorker {
        type Running = super::InlineValidation;

        fn start(&self, request: ValidationWorkerRequest) -> Result<Self::Running, ()> {
            self.starts.fetch_add(1, Ordering::SeqCst);
            super::InlineValidationWorker.start(request)
        }
    }

    #[derive(Clone)]
    pub(crate) struct ControlledClock {
        deadline_registrations: mpsc::UnboundedSender<TestInstant>,
        expired: watch::Receiver<bool>,
    }

    pub(crate) struct ClockControl {
        pub(crate) deadline_registrations: mpsc::UnboundedReceiver<TestInstant>,
        pub(crate) expired: watch::Sender<bool>,
    }

    impl ControlledClock {
        pub(crate) fn new() -> (Self, ClockControl) {
            let (deadline_registrations, registrations) = mpsc::unbounded_channel();
            let (expired, expiration) = watch::channel(false);
            (
                Self {
                    deadline_registrations,
                    expired: expiration,
                },
                ClockControl {
                    deadline_registrations: registrations,
                    expired,
                },
            )
        }
    }

    // Validation starts at a nonzero instant and exposes registration via the
    // worker test's control channel; the step clock tracks active waiters instead.
    impl CoordinatorClock for ControlledClock {
        type Instant = TestInstant;

        fn now(&mut self) -> Self::Instant {
            TestInstant(Duration::from_secs(100))
        }

        fn wait_until(&self, deadline: Self::Instant) -> impl Future<Output = ()> + Send {
            let registrations = self.deadline_registrations.clone();
            let expired = self.expired.clone();
            async move {
                let _ = registrations.send(deadline);
                super::wait_for_release(expired).await
            }
        }
    }
}

pub(crate) mod pi_clock {
    use crate::workflow::coordinator::CoordinatorClock;
    use std::future::pending;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;
    use tokio::sync::{mpsc, oneshot, watch};
    #[derive(Clone)]
    pub(crate) enum TestClock {
        Pending,
        Yielding,
        Controlled {
            now_seconds: Arc<AtomicU64>,
            registrations: mpsc::UnboundedSender<Duration>,
            release: watch::Receiver<bool>,
        },
    }

    impl CoordinatorClock for TestClock {
        type Instant = Duration;

        fn now(&mut self) -> Self::Instant {
            match self {
                Self::Pending | Self::Yielding => Duration::ZERO,
                Self::Controlled { now_seconds, .. } => {
                    Duration::from_secs(now_seconds.load(Ordering::SeqCst))
                }
            }
        }

        async fn wait_until(&self, deadline: Self::Instant) {
            let (registrations, release) = match self {
                Self::Pending => return pending().await,
                Self::Yielding => return explicit_scheduling_point().await,
                Self::Controlled {
                    registrations,
                    release,
                    ..
                } => (registrations, release),
            };
            let _ = registrations.send(deadline);
            let mut release = release.clone();
            while !*release.borrow_and_update() {
                if release.changed().await.is_err() {
                    return;
                }
            }
            explicit_scheduling_point().await;
        }
    }

    async fn explicit_scheduling_point() {
        let (complete, completed) = oneshot::channel();
        tokio::spawn(async move {
            let _ = complete.send(());
        });
        let _ = completed.await;
    }
}

pub(crate) mod pi_sink {
    use crate::workflow::agent::{AgentObservationEnvelope, AgentObservationSink};
    use std::future::Future;
    use std::sync::{Arc, Mutex};
    use tokio::sync::{mpsc, watch};
    #[derive(Clone)]
    pub(crate) struct ObservationGate {
        pub(crate) reached: mpsc::UnboundedSender<()>,
        pub(crate) release: watch::Receiver<bool>,
    }

    #[derive(Clone)]
    pub(crate) struct RecordingObservationSink {
        pub(crate) observations: mpsc::UnboundedSender<AgentObservationEnvelope>,
        pub(crate) gate: Arc<Mutex<Option<ObservationGate>>>,
    }

    impl AgentObservationSink for RecordingObservationSink {
        fn observe(
            &self,
            observation: AgentObservationEnvelope,
        ) -> impl Future<Output = ()> + Send {
            let _ = self.observations.send(observation);
            let gate = self.gate.lock().unwrap().clone();
            async move {
                let Some(mut gate) = gate else {
                    return;
                };
                let _ = gate.reached.send(());
                while !*gate.release.borrow_and_update() {
                    if gate.release.changed().await.is_err() {
                        return;
                    }
                }
            }
        }
    }
}

pub(crate) mod codex_clock {
    use crate::workflow::coordinator::CoordinatorClock;
    use std::time::Duration;
    use tokio::sync::{mpsc, oneshot};
    #[derive(Clone)]
    pub(crate) struct ReleasedClock {
        pub(crate) deadlines: mpsc::UnboundedSender<(Duration, oneshot::Sender<()>)>,
    }

    // This clock carries Codex-specific stdin-deadline synchronization; sharing it with
    // another profile's fixture would couple independent protocol timing contracts.
    impl CoordinatorClock for ReleasedClock {
        type Instant = Duration;

        fn now(&mut self) -> Self::Instant {
            Duration::ZERO
        }

        async fn wait_until(&self, deadline: Self::Instant) {
            let (release, released) = oneshot::channel();
            if self.deadlines.send((deadline, release)).is_err() {
                std::future::pending::<()>().await;
            }
            if released.await.is_err() {
                std::future::pending::<()>().await;
            }
        }
    }
}

/// Streaming conformance assertions consume each observation through this channel.
#[derive(Clone)]
pub(crate) struct ChannelObservationSink {
    pub(crate) sender: mpsc::UnboundedSender<AgentObservationEnvelope>,
}

impl AgentObservationSink for ChannelObservationSink {
    fn observe(&self, observation: AgentObservationEnvelope) -> impl Future<Output = ()> + Send {
        let _ = self.sender.send(observation);
        ready(())
    }
}

pub(crate) mod blocking_validation {
    use crate::workflow::admission::{CancellationReason, CancellationSource};
    use crate::workflow::result_validation::{
        ResultValidationWorker, RunningResultValidation, ValidationWorkerDecision,
        ValidationWorkerRequest,
    };
    use tokio::sync::{mpsc, oneshot};
    #[derive(Clone)]
    pub(crate) struct BlockedWorker {
        started: mpsc::UnboundedSender<BlockedWorkerControl>,
    }

    pub(crate) struct BlockedWorkerControl {
        decision: Option<oneshot::Sender<Result<ValidationWorkerDecision, ()>>>,
        pub(crate) stopped: oneshot::Receiver<()>,
        quiesce: Option<oneshot::Sender<()>>,
    }

    impl BlockedWorkerControl {
        pub(crate) async fn wait_until_stopped(&mut self) {
            (&mut self.stopped).await.unwrap();
        }

        pub(crate) fn report_decision(&mut self, decision: Result<ValidationWorkerDecision, ()>) {
            self.decision.take().unwrap().send(decision).unwrap();
        }

        pub(crate) fn report_quiescence(mut self) {
            self.quiesce.take().unwrap().send(()).unwrap();
        }
    }

    pub(crate) struct BlockedValidation {
        decision: oneshot::Receiver<Result<ValidationWorkerDecision, ()>>,
        stop: Option<oneshot::Sender<()>>,
        quiesced: oneshot::Receiver<()>,
    }

    pub(crate) fn blocked_worker() -> (BlockedWorker, mpsc::UnboundedReceiver<BlockedWorkerControl>)
    {
        let (started, controls) = mpsc::unbounded_channel();
        (BlockedWorker { started }, controls)
    }

    #[derive(Clone)]
    pub(crate) struct CancellingWorker {
        pub(crate) cancellation: CancellationSource,
        pub(crate) blocked: BlockedWorker,
    }

    impl ResultValidationWorker for CancellingWorker {
        type Running = BlockedValidation;

        fn start(&self, request: ValidationWorkerRequest) -> Result<Self::Running, ()> {
            assert!(
                self.cancellation
                    .request_cancellation(CancellationReason::UserRequest)
            );
            self.blocked.start(request)
        }
    }

    impl ResultValidationWorker for BlockedWorker {
        type Running = BlockedValidation;

        fn start(&self, _request: ValidationWorkerRequest) -> Result<Self::Running, ()> {
            let (decision_guard, decision) = oneshot::channel();
            let (stop, stopped) = oneshot::channel();
            let (quiesce, quiesced) = oneshot::channel();
            self.started
                .send(BlockedWorkerControl {
                    decision: Some(decision_guard),
                    stopped,
                    quiesce: Some(quiesce),
                })
                .map_err(|_| ())?;
            Ok(BlockedValidation {
                decision,
                stop: Some(stop),
                quiesced,
            })
        }
    }

    impl RunningResultValidation for BlockedValidation {
        async fn wait(&mut self) -> Result<ValidationWorkerDecision, ()> {
            (&mut self.decision).await.map_err(|_| ())?
        }

        fn request_stop(&mut self) {
            if let Some(stop) = self.stop.take() {
                let _ = stop.send(());
            }
        }

        async fn quiesce(mut self) {
            let _ = (&mut self.quiesced).await;
        }
    }
}

pub(crate) mod claude_validation {
    use super::InlineValidation;
    use crate::workflow::result_validation::{
        ResultValidationWorker, RunningResultValidation, ValidationWorkerDecision,
        ValidationWorkerRequest,
    };
    use std::future::pending;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    #[derive(Clone)]
    pub(crate) struct PendingValidationWorker {
        pub(crate) stopped: Arc<AtomicBool>,
        pub(crate) quiesced: Arc<AtomicBool>,
    }

    pub(crate) struct PendingValidation {
        pub(crate) stopped: Arc<AtomicBool>,
        pub(crate) quiesced: Arc<AtomicBool>,
    }

    impl ResultValidationWorker for PendingValidationWorker {
        type Running = PendingValidation;

        fn start(&self, _request: ValidationWorkerRequest) -> Result<Self::Running, ()> {
            Ok(PendingValidation {
                stopped: Arc::clone(&self.stopped),
                quiesced: Arc::clone(&self.quiesced),
            })
        }
    }

    #[derive(Clone, Copy)]
    pub(crate) struct FailingValidationWorker;

    impl ResultValidationWorker for FailingValidationWorker {
        type Running = InlineValidation;

        fn start(&self, _request: ValidationWorkerRequest) -> Result<Self::Running, ()> {
            Err(())
        }
    }

    impl RunningResultValidation for PendingValidation {
        async fn wait(&mut self) -> Result<ValidationWorkerDecision, ()> {
            pending().await
        }

        fn request_stop(&mut self) {
            self.stopped.store(true, Ordering::SeqCst);
        }

        async fn quiesce(self) {
            self.quiesced.store(true, Ordering::SeqCst);
        }
    }
}

pub(crate) struct PendingResultValidation;

impl crate::workflow::result_validation::RunningResultValidation for PendingResultValidation {
    async fn wait(
        &mut self,
    ) -> Result<crate::workflow::result_validation::ValidationWorkerDecision, ()> {
        std::future::pending().await
    }

    fn request_stop(&mut self) {}

    fn quiesce(self) -> impl std::future::Future<Output = ()> + Send {
        std::future::ready(())
    }
}

pub(crate) mod pi_validation {
    use super::PendingResultValidation;
    use crate::workflow::result_validation::{ResultValidationWorker, ValidationWorkerRequest};
    #[derive(Clone, Copy)]
    pub(crate) struct DeadlineValidationWorker;

    impl ResultValidationWorker for DeadlineValidationWorker {
        type Running = PendingResultValidation;

        fn start(&self, _request: ValidationWorkerRequest) -> Result<Self::Running, ()> {
            Ok(PendingResultValidation)
        }
    }
}

pub(crate) mod pi_blocking_validation {
    use super::PendingResultValidation;
    use crate::workflow::result_validation::{ResultValidationWorker, ValidationWorkerRequest};
    use tokio::sync::mpsc;
    #[derive(Clone)]
    pub(crate) struct BlockingValidationWorker {
        pub(crate) reached: mpsc::UnboundedSender<()>,
    }

    impl ResultValidationWorker for BlockingValidationWorker {
        type Running = PendingResultValidation;

        fn start(&self, _request: ValidationWorkerRequest) -> Result<Self::Running, ()> {
            let _ = self.reached.send(());
            Ok(PendingResultValidation)
        }
    }
}
