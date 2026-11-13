use std::io::{self, Write};
use std::path::{Path, PathBuf};

use anyhow::anyhow;
use clap::Args;

use um_execution::{
    CONTROL_RETRY, CancellationSource, LocalRetryBeginError, LocalRetryOpen, PendingLocalRetry,
    WITHDRAWN_OUTCOME, WaitInspection, WaitKind, WorkflowRunOutput, acquire_local_retry,
    admit_local_workflow, await_begin_gate, await_run_lock_gate,
    reconcile_current_result_publication,
};

use super::control::{ControlFailure, ControlTarget, Refusal};
use crate::exit_code::ExitCode;

pub(super) const ABOUT: &str =
    "Retry a local workflow run, or a Cloud run waiting for repair with --cloud";
pub(super) const AFTER_HELP: &str = "Retry eligibility:
  Retry is available when the latest attempt did not succeed or end in rejection, no
  execution owner holds the run, and prior process ownership can be proven safe. A retry
  executes every workflow step as a new attempt and requires --execution-root.

A run waiting for repair is retried from the stage that failed: the retry is recorded,
with the replacement profiles of --agent-profiles when given, and the run continues as
resume continues it, in the previous attempt's execution root unless --execution-root
names another. A stage whose child run failed continues that child in place, inheriting
its completed steps and rerunning the rest; in another execution root the stage starts
afresh. No separate resume is needed.

With --cloud ORGANIZATION, RUN is a Cloud run ID waiting for repair: the command reads that
run and asks Cloud, under one request key sent again if the response is lost, to carry the
retry for its current wait to the runner executing it, which retries the run as this command
retries a local run. It takes no --execution-root or --agent-profiles. Cloud's acceptance is
not the runner's application. It reads and writes no local run.";

const COMMAND: &str = "um workflow run retry";

// Run, retry, and status intentionally compose different subsets of shared workflow options.
#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(flatten)]
    run: super::ExistingLocalRun,

    #[arg(
        long,
        value_name = "PATH",
        conflicts_with = "cloud",
        help = "Directory for workflow execution (must already exist); required unless the run waits for repair"
    )]
    execution_root: Option<PathBuf>,

    #[arg(
        long,
        value_name = "PATH",
        conflicts_with = "cloud",
        help = "Replace declared agent profiles with the closed replacement document at PATH"
    )]
    agent_profiles: Option<PathBuf>,

    #[command(flatten)]
    presentation: super::PresentationOptions,

    #[command(flatten)]
    cloud: super::CloudSelector,
}

impl Command {
    pub(super) fn execute(mut self) -> super::super::super::CommandResult {
        if let Some(organization) = self.cloud.organization.take() {
            return super::cloud::CloudRun::new(
                self.run,
                organization,
                self.cloud,
                self.presentation.output.json,
            )?
            .control(super::cloud::Control::Retry);
        }
        super::create::execute_with_runtime(
            "start local workflow retry runtime",
            self.execute_async(),
        )
    }

    async fn execute_async(self) -> super::super::super::CommandResult {
        let run_directory = self.run.resolve().map_err(anyhow::Error::new)?;
        let presentation_config = super::create::presentation_config(&self.presentation);
        let waiting = {
            let run_directory = run_directory.clone();
            tokio::task::spawn_blocking(move || super::control::open_if_waiting(&run_directory))
                .await
                .map_err(anyhow::Error::new)?
        };
        if let Some(target) = waiting {
            return self
                .retry_wait(&run_directory, target, presentation_config)
                .await;
        }
        // REQ-LOCALRUN-78, REQ-LOCALRUN-80: a run that does not wait starts a new attempt.
        if self.agent_profiles.is_some() {
            return Err(super::super::super::CommandFailure::with_exit_code(
                anyhow!("--agent-profiles is accepted only for a run waiting for repair"),
                ExitCode::UsageError,
            ));
        }
        let Some(execution_root) = self.execution_root.clone() else {
            return Err(super::super::super::CommandFailure::with_exit_code(
                anyhow!("--execution-root is required for a run that does not wait"),
                ExitCode::UsageError,
            ));
        };
        let cancellation = CancellationSource::new();
        let signal_task = super::create::start_signal_observation(
            cancellation.clone(),
            super::create::UnixSignals::new()?,
        );
        // REQ-LOCALRUN-135: a signal reads the run's control journal from here on, before the
        // attempt begins, as `continue_run` does for a resume.
        let _journal = signal_task.watch(&run_directory);
        let opened = match tokio::task::spawn_blocking(move || {
            reconcile_current_result_publication(&run_directory).map_err(|error| {
                anyhow::anyhow!("finalize workflow result publication: {error:?}")
            })?;
            // A test seam between the inspection and the lock; nothing outside `test-fixtures`.
            await_run_lock_gate()?;
            acquire_local_retry(&run_directory).map_err(anyhow::Error::new)
        })
        .await
        {
            Ok(opened) => opened,
            Err(_) => {
                signal_task.abort();
                return Err(anyhow::anyhow!("inspect local workflow retry state").into());
            }
        };
        let pending = match opened {
            Ok(LocalRetryOpen::Acquired(pending)) => *pending,
            Ok(LocalRetryOpen::Rejected(rejection)) => {
                signal_task.abort();
                return render_retry_rejection(presentation_config, &rejection);
            }
            Err(error) => {
                signal_task.abort();
                return Err(error.into());
            }
        };
        // REQ-LOCALRUN-213, REQ-LOCALRUN-186: a withdrawal no process honored over the run's
        // open latest attempt ends the run `withdrawn` without an attempt, as `resume` ends it;
        // the settlement `begin` makes would spend it.
        if matches!(pending.withdrawal_unhonored(), Ok(true)) {
            return end_withdrawn(pending, &signal_task, &presentation_config).await;
        }
        let (workflow, inputs, maximum_parallel_steps) = {
            let (workflow, inputs, maximum_parallel_steps) = pending.execution_specification();
            (workflow.clone(), inputs.clone(), maximum_parallel_steps)
        };
        // REQ-LOCALRUN-171: a run admitted from a tree retries only from an unchanged copy.
        let (pending, verified) = match tokio::task::spawn_blocking(move || {
            let verified = pending.verify_tree_copy();
            (pending, verified)
        })
        .await
        {
            Ok(verified) => verified,
            Err(_) => {
                signal_task.abort();
                return Err(anyhow::anyhow!("verify local workflow tree copy").into());
            }
        };
        let tree = match verified {
            None => None,
            Some(Ok(tree)) => Some(tree),
            Some(Err(mismatch)) => {
                signal_task.abort();
                let output =
                    WorkflowRunOutput::new(presentation_config, io::stdout(), io::stderr())
                        .for_retry(pending.run_directory());
                return super::create::rejection_exit(
                    output.render_tree_copy_rejection(&workflow.source.workflow_path, &mismatch),
                );
            }
        };
        let tree_copy = pending.tree_copy().map(|(copy_root, _)| copy_root);
        let workflow_for_context = workflow.clone();
        let context_cancellation = cancellation.clone();
        let mut context = match super::create::blocking_operation(move || {
            super::create::execution_context_for_workflow(
                &workflow_for_context,
                execution_root,
                maximum_parallel_steps,
                context_cancellation,
            )
        })
        .await
        {
            Ok(context) => context,
            Err(super::create::BlockingOperationError::Operation(failure)) => {
                signal_task.abort();
                let output =
                    WorkflowRunOutput::new(presentation_config, io::stdout(), io::stderr())
                        .for_retry(pending.run_directory());
                return super::create::rejection_exit(
                    output.render_agent_harness_installation_rejection(&workflow, &failure),
                );
            }
            Err(super::create::BlockingOperationError::WorkerUnavailable) => {
                signal_task.abort();
                return Err(anyhow::anyhow!("prepare local workflow retry context").into());
            }
        };
        if workflow.requires_git_capture() {
            context = context.with_local_git_baseline(pending.git_baseline().cloned());
        }
        if let Some(tree) = tree {
            context = context.with_tree(tree);
        }
        let untested_advisories = context.untested_harness_advisories();
        let workflow_for_admission = workflow.clone();
        let admitted = match super::create::blocking_operation(move || {
            admit_local_workflow(workflow_for_admission, inputs, context)
        })
        .await
        {
            Ok(admitted) => admitted,
            Err(super::create::BlockingOperationError::Operation(failure)) => {
                signal_task.abort();
                let output =
                    WorkflowRunOutput::new(presentation_config, io::stdout(), io::stderr())
                        .for_retry(pending.run_directory());
                return super::create::rejection_exit(
                    output.render_admission_rejection(&workflow, &failure),
                );
            }
            Err(super::create::BlockingOperationError::WorkerUnavailable) => {
                signal_task.abort();
                return Err(anyhow::anyhow!("admit local workflow retry").into());
            }
        };
        if workflow.source.source_root.to_str().is_none()
            || admitted.execution().root().to_str().is_none()
        {
            signal_task.abort();
            return Err(anyhow::anyhow!("prepare local workflow retry paths").into());
        }
        let admitted = match tree_copy {
            Some(copy_root) => match super::create::bind_tree_copy(admitted, &copy_root) {
                Ok(admitted) => admitted,
                Err(error) => {
                    signal_task.abort();
                    return Err(error.into());
                }
            },
            None => admitted,
        };
        super::create::write_untested_advisories(&untested_advisories);

        let reused_attempts = match pending.reused_execution_root_attempts(&admitted) {
            Ok(attempts) => attempts,
            Err(error) => {
                signal_task.abort();
                return super::create::diagnose(error);
            }
        };
        if !reused_attempts.is_empty()
            && let Err(error) = write_reuse_warning(admitted.execution().root(), &reused_attempts)
        {
            signal_task.abort();
            return Err(anyhow::Error::new(error)
                .context("write workflow retry warning")
                .into());
        }

        // REQ-LOCALRUN-135: an interruption held before the attempt begins starts none, and the
        // run is released as it was; a signal that carries a recorded `control.cancel` is a
        // cancellation, which begins the attempt and cancels it.
        if cancellation.interruption_requested() {
            drop(pending);
            signal_task.abort();
            return Err(super::create::signalled_before_attempt(
                cancellation.cancellation_reason(),
            ));
        }
        let admitted_for_begin = admitted.clone();
        // REQ-LOCALRUN-213: the attempt begins under the controls lock only once no withdrawal
        // is unhonored over the run, one recorded since the check above included; otherwise the
        // retry is handed back to end the run `withdrawn`.
        let begun = tokio::task::spawn_blocking(move || {
            // A test seam before the controls lock; nothing outside `test-fixtures`.
            await_begin_gate()?;
            let controls = pending.lock_controls()?;
            if matches!(pending.withdrawal_unhonored(), Ok(true)) {
                drop(controls);
                return Ok(super::resume::Begun::Withdrawn(Box::new(pending)));
            }
            let owned_run = pending.begin(&admitted_for_begin);
            drop(controls);
            owned_run.map(|owned_run| super::resume::Begun::Owned(Box::new(owned_run)))
        })
        .await;
        let owned_run = match begun {
            Ok(Ok(super::resume::Begun::Owned(owned_run))) => *owned_run,
            Ok(Ok(super::resume::Begun::Withdrawn(pending))) => {
                return end_withdrawn(*pending, &signal_task, &presentation_config).await;
            }
            Ok(Err(LocalRetryBeginError::Rejected(rejection))) => {
                signal_task.abort();
                return render_retry_rejection(presentation_config, &rejection);
            }
            Ok(Err(LocalRetryBeginError::Operational(error))) => {
                signal_task.abort();
                return super::create::diagnose(error);
            }
            Err(_) => {
                signal_task.abort();
                return Err(anyhow::anyhow!("commit local workflow retry attempt").into());
            }
        };
        super::create::execute_owned_attempt(
            workflow,
            admitted,
            owned_run,
            cancellation,
            signal_task,
            presentation_config,
            super::create::ExecutionLeaf::Retry,
        )
        .await
    }
}

impl Command {
    /// `retry` on a waiting run (REQ-LOCALRUN-184): on a `repair` wait, records
    /// `control.retry` naming it and continues the run as `resume` does.
    async fn retry_wait(
        &self,
        run_directory: &Path,
        target: Result<ControlTarget, Refusal>,
        config: um_execution::PresentationConfig,
    ) -> super::super::super::CommandResult {
        let json = self.presentation.output.json;
        let refuse = |run_directory: &Path, refusal: &Refusal| {
            super::control::write_refusal(COMMAND, Some(run_directory), refusal, json)
        };
        let target = match target {
            Ok(target) => target,
            Err(refusal) => return refuse(run_directory, &refusal),
        };
        // A wait whose rule the matcher refuses is left to the continuation to report.
        let wait = match &target.inspection {
            WaitInspection::Held { wait }
            | WaitInspection::Satisfied { wait, .. }
            | WaitInspection::RuleInvalid { wait, .. }
            | WaitInspection::ConditionUnavailable { wait, .. }
                if wait.kind != WaitKind::Repair =>
            {
                return refuse(
                    &target.run_directory,
                    &super::control::wait_kind_mismatch(wait),
                );
            }
            WaitInspection::Held { wait } => Some(wait),
            WaitInspection::RuleInvalid { .. } | WaitInspection::ConditionUnavailable { .. } => {
                None
            }
            WaitInspection::Satisfied { .. } => {
                return refuse(&target.run_directory, &super::control::wait_resolved());
            }
            WaitInspection::Locked(_) => {
                return refuse(&target.run_directory, &super::control::run_locked());
            }
            WaitInspection::NoWait { .. } | WaitInspection::Continued { .. } => {
                return refuse(
                    &target.run_directory,
                    &Refusal::new(
                        "wait_changed",
                        "The run's wait changed while the command inspected it; run the command again.",
                    ),
                );
            }
        };
        let cancellation = CancellationSource::new();
        if let Some(wait) = wait {
            let payload = match &self.agent_profiles {
                Some(path) => Some(replacement_payload(path, &cancellation)?),
                None => None,
            };
            match target.record(CONTROL_RETRY, Some(wait.id()), payload) {
                Ok(_) => {}
                Err(ControlFailure::Refused(refusal)) => {
                    return refuse(&target.run_directory, &refusal);
                }
                Err(ControlFailure::Failed(error)) => return Err(error.into()),
            }
        }
        let signal_task = super::create::start_signal_observation(
            cancellation.clone(),
            super::create::UnixSignals::new()?,
        );
        super::resume::continue_run(
            super::resume::Continuation {
                run_directory: target.run_directory,
                from_steps: Vec::new(),
                workflow: None,
                execution_root: self.execution_root.clone(),
                leaf: super::create::ExecutionLeaf::Retry,
                edge: None,
                bridge: None,
            },
            config,
            cancellation,
            signal_task,
        )
        .await
    }
}

/// The `control.retry` payload for the replacement document at `path` (REQ-AGENT-57): its
/// `agentProfiles` member as `agentProfiles`.
fn replacement_payload(
    path: &Path,
    cancellation: &CancellationSource,
) -> Result<serde_json::Map<String, serde_json::Value>, super::super::super::CommandFailure> {
    let document = super::create::acquire_agent_profile_replacement(path, cancellation)?;
    let profiles = serde_json::from_slice::<serde_json::Value>(document.bytes())
        .ok()
        .and_then(|mut document| document.get_mut("agentProfiles").map(serde_json::Value::take))
        .filter(serde_json::Value::is_object)
        .ok_or_else(|| {
            anyhow!(
                "agent profile replacement document: not a JSON document with an agentProfiles object"
            )
        })?;
    Ok(serde_json::Map::from_iter([(
        "agentProfiles".to_owned(),
        profiles,
    )]))
}

/// Ends the `running` run `pending` holds `withdrawn` without an attempt through
/// `PendingLocalRetry::end_running`, as `resume` ends it (`resume::end_withdrawn`), reported on
/// the retry's own documents. The caller holds no controls lock.
async fn end_withdrawn(
    pending: PendingLocalRetry,
    signal_task: &super::create::SignalObservation,
    config: &um_execution::PresentationConfig,
) -> super::super::super::CommandResult {
    let attempt_number = match pending.prior_attempt_number() {
        Ok(attempt_number) => attempt_number,
        Err(error) => {
            signal_task.abort();
            return super::create::diagnose(error);
        }
    };
    let (workflow, inputs, _) = pending.execution_specification();
    let unattempted = super::resume::Unattempted::new(
        pending.run_directory(),
        attempt_number,
        (workflow, inputs),
        WorkflowRunOutput::new(config.clone(), io::stdout(), io::stderr())
            .for_retry(pending.run_directory()),
    );
    super::resume::end_withdrawn(
        pending,
        unattempted,
        signal_task,
        config,
        super::create::ExecutionLeaf::Retry,
        |pending| pending.end_running(WITHDRAWN_OUTCOME),
    )
    .await
}

fn render_retry_rejection(
    config: um_execution::PresentationConfig,
    rejection: &um_execution::LocalRetryRejection,
) -> super::super::super::CommandResult {
    let output = WorkflowRunOutput::new(config, io::stdout(), io::stderr())
        .for_retry(rejection.run_directory());
    super::create::rejection_exit(output.render_retry_rejection(rejection))
}

fn write_reuse_warning(execution_root: &std::path::Path, attempts: &[u64]) -> io::Result<()> {
    let standard_error = io::stderr();
    let mut standard_error = standard_error.lock();
    let attempts = attempts
        .iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    writeln!(
        standard_error,
        "! Execution root {execution_root:?} was used by earlier attempt(s) {attempts} and may contain prior changes; Useful Machinery does not check cleanliness or mutations."
    )?;
    standard_error.flush()
}
