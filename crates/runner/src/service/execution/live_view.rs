//! What a control socket read shows of a single-workflow assignment's run: the run
//! interface's model, kept in memory within the runner's bounds, and the observer pair that
//! feeds it only after the runner's own observer has seen each observation
//! (INV-RUNNER-47, REQ-RUNNERENROLL-147).

use useful_execution::{
    ExecutionSeed, LiveRunViewRequest, LiveRunningSteps, ObservationClock, ResolvedWorkflow,
    RunLogBudget, RunTimingObservation, RunView, WorkflowRunViewModel, live_run_view,
    live_running_steps,
};

use super::{
    ExecutionObservation, ExecutionObserver, Future, RunnerExecutionClock, RunnerExecutionInstant,
    RunnerExecutionObserver,
};
use crate::control_protocol::MAXIMUM_RUNNING_STEPS;

/// The most bytes of text a step of the view keeps.
const STEP_LOG_BYTES: usize = 262_144;
/// The most records a step of the view keeps.
const STEP_LOG_RECORDS: usize = 4_096;
/// The most bytes of text the view keeps across the run's steps.
const RUN_LOG_BYTES: usize = 16_777_216;
/// The most records the view keeps across the run's steps.
const RUN_LOG_RECORDS: usize = 65_536;

/// The run interface's model of a single-workflow assignment's run, within the runner's
/// budget. A clone is a handle on the same model: the manager holds one for its reads and
/// the execution job one for the run's observer pair. Nothing of it is written to disk or
/// sent to Cloud.
#[derive(Clone)]
pub(in crate::service) struct AssignmentRunView {
    model: WorkflowRunViewModel<RunnerExecutionClock>,
}

impl AssignmentRunView {
    /// A view of `workflow`, every step `pending`, run at most `maximum_parallel_steps` at
    /// once.
    pub(in crate::service) fn new(
        workflow: &ResolvedWorkflow,
        maximum_parallel_steps: usize,
    ) -> Self {
        let clock = RunnerExecutionClock;
        let timing = RunTimingObservation::new(clock.sample());
        Self {
            model: WorkflowRunViewModel::with_log_budget(
                workflow,
                maximum_parallel_steps,
                timing,
                clock,
                RunLogBudget {
                    step_bytes: STEP_LOG_BYTES,
                    run_bytes: RUN_LOG_BYTES,
                    step_records: STEP_LOG_RECORDS,
                    run_records: RUN_LOG_RECORDS,
                },
            ),
        }
    }

    /// The first steps in flight, at most `MAXIMUM_RUNNING_STEPS`, and the number of the
    /// others, copied in one hold of the model's lock (REQ-RUNNERENROLL-134).
    pub(in crate::service) fn running_steps(&self) -> LiveRunningSteps {
        live_running_steps(&self.model, MAXIMUM_RUNNING_STEPS)
    }

    /// The run as `request` asks for it (REQ-RUNNERENROLL-144, REQ-RUNNERENROLL-145).
    pub(in crate::service) fn run(&self, request: &LiveRunViewRequest) -> RunView {
        live_run_view(&self.model, request)
    }

    /// Marks the run's execution started now.
    pub(super) fn mark_execution_started(&self) {
        self.model
            .timing_observation()
            .mark_execution_started(RunnerExecutionClock.sample());
    }

    /// Shows the view the steps a continued attempt's `seed` inherits.
    pub(super) fn observe_seed<Output>(&self, seed: &ExecutionSeed<Output>) {
        self.model.observe_seed(seed);
    }
}

impl std::fmt::Debug for AssignmentRunView {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AssignmentRunView")
            .finish_non_exhaustive()
    }
}

impl ExecutionObserver<RunnerExecutionInstant> for AssignmentRunView {
    fn observe(
        &self,
        observation: ExecutionObservation<RunnerExecutionInstant>,
    ) -> impl Future<Output = ()> + Send {
        self.model.observe(observation)
    }
}

/// The runner's observer and a second observer the engine feeds as one: each observation
/// reaches the runner's observer first, and the second, the run's view in production, only
/// once the runner's has finished with it, so what the runner queues for Cloud and records
/// is what it would be without the second (INV-RUNNER-47).
#[derive(Clone)]
pub(super) struct ObserverThenView<Second> {
    runner: RunnerExecutionObserver,
    second: Second,
}

impl<Second> ObserverThenView<Second> {
    pub(super) fn new(runner: RunnerExecutionObserver, second: Second) -> Self {
        Self { runner, second }
    }
}

impl<Second> ExecutionObserver<RunnerExecutionInstant> for ObserverThenView<Second>
where
    Second: ExecutionObserver<RunnerExecutionInstant>,
{
    fn observe(
        &self,
        observation: ExecutionObservation<RunnerExecutionInstant>,
    ) -> impl Future<Output = ()> + Send {
        let runner = self.runner.clone();
        let second = self.second.clone();
        async move {
            let copy = observation.clone();
            runner.observe(observation).await;
            // The model does its work when `observe` is called, not when its future is
            // polled: the call stays after the runner's await.
            second.observe(copy).await;
        }
    }
}
