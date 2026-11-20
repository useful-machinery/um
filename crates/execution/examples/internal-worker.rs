use std::process::ExitCode;

use useful_execution::ExecutionOutcome;

fn main() -> ExitCode {
    let outcome = if useful_execution::child_guard_worker_requested() {
        useful_execution::run_child_guard_worker()
    } else if useful_execution::result_validation_worker_requested() {
        useful_execution::run_result_validation_worker()
    } else if useful_execution::result_tool_worker_requested() {
        useful_execution::run_result_tool_worker()
    } else {
        ExecutionOutcome::Failed
    };
    ExitCode::from(match outcome {
        ExecutionOutcome::Succeeded => 0,
        ExecutionOutcome::Failed => 1,
        ExecutionOutcome::Interrupted => 130,
        ExecutionOutcome::Terminated => 143,
    })
}
