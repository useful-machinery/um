use std::io::{self, Write};
use std::path::{Path, PathBuf};

use anyhow::anyhow;
use clap::Args;

use um_execution::{
    CONTROL_RETRY, CancellationSource, LocalRetryBeginError, LocalRetryOpen, WaitInspection,
    WaitKind, WorkflowRunOutput, acquire_local_retry, admit_local_workflow,
    reconcile_current_result_publication,
};

use super::control::{ControlFailure, ControlTarget, Refusal};
use crate::exit_code::ExitCode;

pub(super) const ABOUT: &str = "Retry a local workflow run";
pub(super) const AFTER_HELP: &str = "Retry eligibility:
  Retry is available when the latest attempt did not succeed or end in rejection, no
  execution owner holds the run, and prior process ownership can be proven safe. A retry
  executes every workflow step as a new attempt and requires --execution-root.

A run waiting for repair is retried from the stage that failed: the retry is recorded,
with the replacement profiles of --agent-profiles when given, and the run continues as
resume continues it, in the previous attempt's execution root unless --execution-root
names another. A stage whose child run failed continues that child in place, inheriting
its completed steps and rerunning the rest; in another execution root the stage starts
afresh. No separate resume is needed.";

const COMMAND: &str = "um workflow run retry";

// Run, retry, and status intentionally compose different subsets of shared workflow options.
#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(flatten)]
    run: super::ExistingLocalRun,

    #[arg(
        long,
        value_name = "PATH",
        help = "Directory for workflow execution (must already exist); required unless the run waits for repair"
    )]
    execution_root: Option<PathBuf>,

    #[arg(
        long,
        value_name = "PATH",
        help = "Replace declared agent profiles with the closed replacement document at PATH"
    )]
    agent_profiles: Option<PathBuf>,

    #[command(flatten)]
    presentation: super::PresentationOptions,
}

impl Command {
    pub(super) fn execute(self) -> super::super::super::CommandResult {
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

        let admitted_for_begin = admitted.clone();
        let owned_run =
            match tokio::task::spawn_blocking(move || pending.begin(&admitted_for_begin)).await {
                Ok(Ok(owned_run)) => owned_run,
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
