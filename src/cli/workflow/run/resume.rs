use std::io::{self, Write};
use std::path::{Path, PathBuf};

use clap::Args;
use serde::Serialize;
use um_execution::{
    AdmittedWorkflow, CONTROL_WITHDRAW, CancellationSource, ContinuationDefinitionMode,
    ContinuationEdgeV1, ContinuationReasonV1, LocalContinuationOpen, PendingLocalContinuation,
    ResolvedBy, ResolvedInputs, ResolvedWorkflow, RunState, UntestedHarnessAdvisory, ValidatedTree,
    WITHDRAWN_OUTCOME, WaitInspection, WaitProbe, WaitRecord, WorkflowRunOutput,
    WorkflowRunPresentationResult, acquire_local_continuation, admit_local_continuation_workflow,
    inspect_wait, latest_child_run, normalize_terminal_path, normalize_terminal_scalar,
    positioned_element, read_local_run_status, reconcile_current_result_publication,
    resolve_workflow_file_with_agent_profiles, superseded_rejection_message,
};

use super::create::{ChildRecovery, ExecutionLeaf};
use crate::exit_code::ExitCode;

pub(super) const ABOUT: &str = "Continue a local workflow run";
pub(super) const AFTER_HELP: &str = "Select one or more ordinary steps with --from. Their downstream ordinary dependents run again; other eligible ordinary steps are inherited. Every finalizer runs again. The original run inputs and Git baseline remain authoritative.

Without --from, a waiting run continues once a control resolves its wait and otherwise reports the wait unchanged, and a run whose process died continues from the step it was running.";

const COMMAND: &str = "um workflow run resume";
const RETRY_COMMAND: &str = "um workflow run retry";
const RESTART_COMMAND: &str = "um workflow run restart";
const UPGRADE_COMMAND: &str = "um workflow run upgrade";

/// The command a continuation's own documents name.
pub(super) fn command_name(leaf: ExecutionLeaf) -> &'static str {
    match leaf {
        ExecutionLeaf::Retry => RETRY_COMMAND,
        ExecutionLeaf::Restart => RESTART_COMMAND,
        ExecutionLeaf::Upgrade => UPGRADE_COMMAND,
        ExecutionLeaf::Run | ExecutionLeaf::Resume => COMMAND,
    }
}

/// The output a refused continuation of the run at `run_directory` is reported on: as
/// `um workflow run resume` reports it, under `restart`'s or `upgrade`'s name for those
/// (REQ-LOCALRUN-184, REQ-LOCALRUN-194, REQ-LOCALRUN-197).
pub(super) fn continuation_output(
    config: um_execution::PresentationConfig,
    leaf: ExecutionLeaf,
    run_directory: &Path,
) -> WorkflowRunOutput<io::Stdout, io::Stderr> {
    let output = WorkflowRunOutput::new(config, io::stdout(), io::stderr());
    match leaf {
        ExecutionLeaf::Restart => output.for_restart(run_directory),
        ExecutionLeaf::Upgrade => output.for_upgrade(run_directory),
        ExecutionLeaf::Run | ExecutionLeaf::Retry | ExecutionLeaf::Resume => {
            output.for_resume(run_directory)
        }
    }
}

/// What the continuation starts from.
enum Selection {
    /// `--from`, as named.
    Explicit(Vec<String>),
    /// A wait a control resolved (REQ-CONTINUE-75): continue at `target` once the run still
    /// waits on the wait whose identity is `wait`.
    Wait {
        wait: String,
        target: String,
        reason: ContinuationReasonV1,
    },
    /// A wait a `control.withdraw` resolved (REQ-CONTINUE-75).
    Withdraw { wait: String },
    /// A backward or limited edge the prior attempt took (REQ-WFEXEC-64, REQ-CONTINUE-78):
    /// continue at its target once the run's latest attempt still ended `continued` on it.
    Edge(ContinuationEdgeV1),
    /// A run whose process died while it ran (REQ-CONTINUE-76).
    Abandoned,
}

/// The documents of a resume that creates no attempt (REQ-CONTINUE-74, REQ-CONTINUE-75).
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct UnattemptedDocument<'a> {
    schema_version: u8,
    command: &'static str,
    outcome: &'static str,
    exit_status: u8,
    run_directory: &'a str,
    attempt_number: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    wait: Option<&'a WaitRecord>,
}

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(flatten)]
    run: super::ExistingLocalRun,
    #[arg(
        long = "from",
        required = false,
        value_name = "STEP",
        help = "Ordinary step to reexecute (repeatable); without it, resume a waiting or abandoned run"
    )]
    from_steps: Vec<String>,
    #[arg(
        long,
        value_name = "FILE",
        help = "Replacement workflow file within the run's original source root"
    )]
    workflow: Option<PathBuf>,
    #[arg(
        long,
        value_name = "PATH",
        help = "Existing execution root; defaults to the previous attempt's root"
    )]
    execution_root: Option<PathBuf>,
    #[command(flatten)]
    presentation: super::PresentationOptions,
}

impl Command {
    pub(super) fn execute(self) -> super::super::super::CommandResult {
        super::create::execute_with_runtime(
            "start local workflow continuation runtime",
            self.execute_async(),
        )
    }

    async fn execute_async(self) -> super::super::super::CommandResult {
        let run_directory = self.run.resolve().map_err(anyhow::Error::new)?;
        let config = super::create::presentation_config(&self.presentation);
        let cancellation = CancellationSource::new();
        let signal_task = super::create::start_signal_observation(
            cancellation.clone(),
            super::create::UnixSignals::new()?,
        );
        continue_run(
            Continuation {
                run_directory,
                from_steps: self.from_steps,
                workflow: self.workflow,
                execution_root: self.execution_root,
                leaf: ExecutionLeaf::Resume,
                edge: None,
            },
            config,
            cancellation,
            signal_task,
        )
        .await
    }
}

/// What a continuation continues: the run, `--from`, `--workflow` and `--execution-root`, and
/// the leaf whose command its documents name. `edge` is the backward or limited edge the
/// attempt that just ended `continued` took, which the attempt loop continues from without
/// probing the run (REQ-WFEXEC-64).
pub(super) struct Continuation {
    pub(super) run_directory: PathBuf,
    pub(super) from_steps: Vec<String>,
    pub(super) workflow: Option<PathBuf>,
    pub(super) execution_root: Option<PathBuf>,
    pub(super) leaf: ExecutionLeaf,
    pub(super) edge: Option<ContinuationEdgeV1>,
}

/// Continues the run as `um workflow run resume` does (REQ-CONTINUE-74 to REQ-CONTINUE-78):
/// without `--from`, from the wait a control resolves, the step an abandoned attempt was
/// running or the target of the edge a `continued` attempt took, reporting a held wait
/// unchanged. `um workflow run retry` on a repair wait continues through here once it
/// recorded its control (REQ-LOCALRUN-184), and the attempt loop continues here from the
/// edge an attempt ended `continued` on (REQ-WFEXEC-64), so every command chains.
pub(super) async fn continue_run(
    continuation: Continuation,
    config: um_execution::PresentationConfig,
    cancellation: CancellationSource,
    signal_task: super::create::SignalObservation,
) -> super::super::super::CommandResult {
    let Continuation {
        run_directory,
        from_steps,
        workflow: replacement_workflow,
        execution_root,
        leaf,
        edge,
    } = continuation;
    // REQ-LOCALRUN-135: a signal reads the run's control journal.
    let _journal = signal_task.watch(&run_directory);
    // REQ-LOCALRUN-196: a successor whose move did not finish is settled before anything
    // else; an edge continuation chains inside the process that already settled it.
    if edge.is_none() {
        let lineage_directory = run_directory.clone();
        match super::create::blocking_operation(move || {
            super::successor::reconcile(&lineage_directory)
        })
        .await
        {
            Ok(super::successor::Lineage::Settled | super::successor::Lineage::Superseded) => {}
            Ok(super::successor::Lineage::Abandoned { attempt_number, .. }) => {
                signal_task.abort();
                return super::create::rejection_exit(
                    continuation_output(config, leaf, &run_directory).render_continuation_refusal(
                        &run_directory,
                        attempt_number,
                        super::successor::ABANDONED_CODE,
                        super::successor::ABANDONED_MESSAGE,
                    ),
                );
            }
            Ok(super::successor::Lineage::PredecessorLocked) => {
                signal_task.abort();
                let attempt_number = read_local_run_status(&run_directory)
                    .map(|snapshot| snapshot.current_attempt_number)
                    .unwrap_or_default();
                return super::create::rejection_exit(
                    continuation_output(config, leaf, &run_directory).render_continuation_refusal(
                        &run_directory,
                        attempt_number,
                        super::successor::PREDECESSOR_LOCKED_CODE,
                        super::successor::PREDECESSOR_LOCKED_MESSAGE,
                    ),
                );
            }
            Err(super::create::BlockingOperationError::Operation(error)) => {
                signal_task.abort();
                return Err(error.into());
            }
            Err(super::create::BlockingOperationError::WorkerUnavailable) => {
                signal_task.abort();
                return Err(anyhow::anyhow!("reconcile the run's predecessor").into());
            }
        }
    }
    let selection = if let Some(edge) = edge {
        Selection::Edge(edge)
    } else if from_steps.is_empty() {
        match probe(config.clone(), &run_directory, command_name(leaf)).await {
            Ok(Probed::Continue(selection)) => selection,
            Ok(Probed::Finished(result)) => {
                signal_task.abort();
                return result;
            }
            Err(error) => {
                signal_task.abort();
                return Err(error);
            }
        }
    } else {
        Selection::Explicit(from_steps)
    };
    let opened = tokio::task::spawn_blocking(move || {
        reconcile_current_result_publication(&run_directory)
            .map_err(|error| anyhow::anyhow!("finalize workflow result publication: {error:?}"))?;
        acquire_local_continuation(&run_directory).map_err(anyhow::Error::new)
    })
    .await
    .map_err(anyhow::Error::new)??;
    let pending = match opened {
        LocalContinuationOpen::Acquired(pending) => *pending,
        LocalContinuationOpen::Rejected(rejection) => {
            signal_task.abort();
            return super::create::rejection_exit(
                continuation_output(config, leaf, rejection.run_directory())
                    .render_retry_rejection(&rejection),
            );
        }
    };
    // The probe ran without the lock: the wait it evaluated must still be the run's. The reason
    // is the command's intent; `begin` records `restart` for any run-definition continuation,
    // including a `resume --from` that finishes an interrupted restart (REQ-CONTINUE-79).
    let (from, reason) = match &selection {
        Selection::Explicit(from) if leaf == ExecutionLeaf::Restart => {
            (from.clone(), ContinuationReasonV1::Restart)
        }
        Selection::Explicit(from) => (from.clone(), ContinuationReasonV1::Resume),
        Selection::Wait { wait, .. } | Selection::Withdraw { wait }
            if !still_waits(&pending, wait) =>
        {
            let result = refusal(
                &config,
                leaf,
                &pending,
                "wait_changed",
                "The run's wait changed while the command inspected it; run the command again.",
            );
            return reject_after_settlement(pending, &signal_task, result).await;
        }
        Selection::Withdraw { .. } => {
            let run_directory = pending.run_directory().to_owned();
            let attempt_number = pending.prior_attempt_number();
            // REQ-LOCALRUN-191: the root's `onOutcome.withdrawn`, read before `withdraw`
            // consumes the continuation and written once the record reads `exited`.
            let (workflow, inputs) = pending.root_specification();
            let mirror = super::mirror::read_run_record(&run_directory).and_then(|record| {
                super::mirror::run_mirror(&record, &workflow.definition, inputs, &run_directory)
            });
            // REQ-LOCALRUN-204: a write without credentials refuses before the run changes;
            // the run still waits, so the settlement writes nothing.
            if let Err(failure) =
                super::mirror::outcome_credentials(mirror.as_ref(), WITHDRAWN_OUTCOME)
            {
                let result = refusal(
                    &config,
                    leaf,
                    &pending,
                    failure.code,
                    super::mirror::CREDENTIALS_REFUSAL,
                );
                return reject_after_settlement(pending, &signal_task, result).await;
            }
            let withdrawn = tokio::task::spawn_blocking(move || {
                pending.withdraw()?;
                if let Some(mirror) = &mirror {
                    super::mirror::perform(
                        mirror,
                        Box::new(|mirror, target| mirror.outcome(target, WITHDRAWN_OUTCOME)),
                    );
                }
                Ok::<_, um_execution::LocalRunDirectoryError>(())
            })
            .await;
            signal_task.abort();
            withdrawn
                .map_err(anyhow::Error::new)?
                .map_err(anyhow::Error::new)?;
            return write_unattempted(
                &config,
                command_name(leaf),
                "withdrawn",
                &run_directory,
                attempt_number,
                None,
            );
        }
        Selection::Wait { target, reason, .. } => (vec![target.clone()], *reason),
        Selection::Edge(edge) if !still_continues(&pending, edge) => {
            let result = refusal(
                &config,
                leaf,
                &pending,
                "wait_changed",
                "The run's latest attempt changed while the command inspected it; run the command again.",
            );
            return reject_after_settlement(pending, &signal_task, result).await;
        }
        Selection::Edge(edge) => (vec![edge.target.clone()], ContinuationReasonV1::Edge),
        Selection::Abandoned => match pending.abandoned_steps().as_slice() {
            [step] => (vec![step.clone()], ContinuationReasonV1::Resume),
            _ => {
                let result = refusal(
                    &config,
                    leaf,
                    &pending,
                    "continuation_definition_invalid",
                    "The abandoned attempt left no single running step to continue from; continue the run with --from STEP.",
                );
                return reject_after_settlement(pending, &signal_task, result).await;
            }
        },
    };
    // REQ-CONTINUE-76, REQ-WFEXEC-68: the child run the abandoned step left, for a fan-out
    // that of the element the run's `position` names, is adopted or retried; a repair retry
    // adopts the child an independent repair completed (REQ-CONTINUE-75).
    let child_recovery = match (&selection, from.as_slice()) {
        (Selection::Abandoned, [step]) => {
            let run_directory = pending.run_directory().to_owned();
            let step = step.clone();
            // An element whose child was never created runs afresh (REQ-WFEXEC-68).
            let latest = tokio::task::spawn_blocking(move || {
                let latest = latest_child_run(&run_directory, &step)?;
                let fresh = match latest {
                    Some(_) => None,
                    None => positioned_element(&run_directory, &step)?,
                };
                Ok::<_, um_execution::LocalRunDirectoryError>((step, latest, fresh))
            })
            .await;
            match latest {
                Ok(Ok((step, Some((directory, RunState::Exited, item)), _))) => {
                    Some(ChildRecovery::Adopt {
                        step,
                        item,
                        directory,
                    })
                }
                Ok(Ok((
                    step,
                    Some((directory, RunState::Running | RunState::Waiting, item)),
                    _,
                ))) => Some(ChildRecovery::Retry {
                    step,
                    item,
                    directory,
                }),
                Ok(Ok((step, None, Some(item)))) => Some(ChildRecovery::Fresh {
                    step,
                    item: Some(item),
                }),
                Ok(Ok((_, None, None))) => None,
                Ok(Err(_)) | Err(_) => {
                    return fail_after_settlement(
                        pending,
                        &signal_task,
                        "read the abandoned step's child run",
                    )
                    .await;
                }
            }
        }
        (
            Selection::Wait {
                reason: ContinuationReasonV1::Retry,
                ..
            },
            [step],
        ) if replacement_workflow.is_none()
            && matches!(leaf, ExecutionLeaf::Resume | ExecutionLeaf::Retry) =>
        {
            match repaired_child(&pending, step) {
                Ok(recovery) => recovery,
                Err(error) => return error_after_settlement(pending, &signal_task, error).await,
            }
        }
        _ => None,
    };
    // REQ-LOCALRUN-171: a run admitted from a tree continues only from an unchanged copy.
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
            let result = continuation_output(config, leaf, pending.run_directory())
                .render_tree_copy_rejection(
                    &pending.previous_definition().source.workflow_path,
                    &mismatch,
                );
            return reject_after_settlement(pending, &signal_task, result).await;
        }
    };
    let tree_copy = pending.tree_copy().map(|(copy_root, _)| copy_root);
    let original_root = pending.previous_definition().source.source_root.clone();
    // REQ-CONTINUE-79: a successor's first own attempt runs under the successor's own
    // definition, the run's, rather than the imported prior attempt's.
    let run_definition = replacement_workflow.is_none() && pending.imported_prior().is_some();
    let workflow = if let Some(path) = &replacement_workflow {
        let root = original_root.clone();
        let selected = path.clone();
        // A continuation inherits the run's retained replacement, even under a
        // replaced definition (Local Workflow Run CLI section 3.6.5).
        let replacement = pending
            .previous_definition()
            .agent_profile_replacement
            .as_ref()
            .map(|replacement| replacement.document.clone());
        match super::create::blocking_operation(move || {
            resolve_workflow_file_with_agent_profiles(&root, &selected, replacement.as_ref())
        })
        .await
        {
            Ok(workflow) => workflow,
            Err(super::create::BlockingOperationError::Operation(failure)) => {
                let result = continuation_output(config, leaf, pending.run_directory())
                    .render_continuation_resolution_rejection(
                        pending.run_directory(),
                        pending.prior_attempt_number(),
                        &failure,
                    );
                return reject_after_settlement(pending, &signal_task, result).await;
            }
            Err(super::create::BlockingOperationError::WorkerUnavailable) => {
                return fail_after_settlement(
                    pending,
                    &signal_task,
                    "resolve continuation definition",
                )
                .await;
            }
        }
    } else if run_definition {
        pending.root_specification().0.clone()
    } else {
        pending.previous_definition().clone()
    };
    let selection_edge = selection;
    let explicit_root = execution_root.is_some();
    let candidate = ContinuationCandidate {
        inputs: pending.candidate_inputs(&workflow),
        workflow: &workflow,
        from: &from,
        execution_root: execution_root.unwrap_or_else(|| pending.prior_execution_root().to_owned()),
        tree,
    };
    let (admitted, untested_advisories) =
        match admit_continuation(&pending, candidate, &cancellation, (&config, leaf)).await {
            Ok(admitted) => admitted,
            Err(Unadmitted::Rejected(result)) => {
                return reject_after_settlement(pending, &signal_task, result).await;
            }
            Err(Unadmitted::Failed(message)) => {
                return fail_after_settlement(pending, &signal_task, message).await;
            }
        };
    let admitted = match tree_copy {
        Some(copy_root) => match super::create::bind_tree_copy(admitted, &copy_root) {
            Ok(admitted) => admitted,
            Err(_) => {
                return fail_after_settlement(
                    pending,
                    &signal_task,
                    super::create::TREE_COPY_PATH_FAILURE,
                )
                .await;
            }
        },
        None => admitted,
    };
    super::create::write_untested_advisories(&untested_advisories);
    if let Err(error) = pending.validate_execution_root(&admitted) {
        return diagnose_after_settlement(pending, &signal_task, error).await;
    }
    // REQ-CONTINUE-81, REQ-CONTINUE-82, INV-ENGINE-27: a repair wait's retry at the waiting
    // step continues the failed child the wait names, verified and admitted here, under both
    // locks, before `begin` consumes the parent's wait.
    let child_recovery = match (child_recovery, &selection_edge) {
        (Some(recovery), _) => Some(recovery),
        (
            None,
            Selection::Wait {
                target,
                reason: ContinuationReasonV1::Retry,
                ..
            },
        ) if replacement_workflow.is_none() => {
            let repair = super::child_recovery::Repair {
                parent: &pending,
                admitted: &admitted,
                target,
                explicit_root,
            };
            match super::child_recovery::prepare(repair, &cancellation, (&config, leaf)).await {
                Ok(recovery) => recovery,
                Err(super::child_recovery::Unprepared::Rejected(result)) => {
                    return reject_after_settlement(pending, &signal_task, result).await;
                }
                Err(super::child_recovery::Unprepared::Failed(message)) => {
                    return fail_after_settlement(pending, &signal_task, message).await;
                }
                Err(super::child_recovery::Unprepared::Diagnosed(error)) => {
                    return diagnose_after_settlement(pending, &signal_task, error).await;
                }
            }
        }
        (None, _) => None,
    };
    let definition_mode = if replacement_workflow.is_some() {
        ContinuationDefinitionMode::Replaced
    } else if run_definition {
        ContinuationDefinitionMode::Run
    } else {
        ContinuationDefinitionMode::Inherited
    };
    let admitted_for_begin = admitted.clone();
    let taken_edge = match selection_edge {
        Selection::Edge(edge) => Some(edge),
        _ => None,
    };
    let owned = tokio::task::spawn_blocking(move || {
        pending.begin(
            &admitted_for_begin,
            from,
            definition_mode,
            reason,
            taken_edge,
        )
    })
    .await
    .map_err(anyhow::Error::new)?
    .map_err(anyhow::Error::new)?;
    let record = owned.continuation_record().map_err(anyhow::Error::new)?;
    let Some(record) = record else {
        signal_task.abort();
        return Err(anyhow::anyhow!("claimed continuation has no continuation record").into());
    };
    let attempt_number = owned.attempt_number();
    {
        let mut stderr = io::stderr().lock();
        if config.mode() == um_execution::PresentationMode::Json {
            let event = serde_json::json!({
                "event": "continuation_partition", "attemptNumber": attempt_number,
                "continuation": record,
            });
            serde_json::to_writer(&mut stderr, &event).map_err(anyhow::Error::new)?;
            writeln!(stderr).map_err(anyhow::Error::new)?;
        } else {
            writeln!(
                stderr,
                "Continuation attempt {attempt_number}: reexecute [{}], inherit [{}]",
                record.reexecuted_steps().join(", "),
                record.inherited_step_ids().collect::<Vec<_>>().join(", ")
            )
            .map_err(anyhow::Error::new)?;
        }
        stderr.flush().map_err(anyhow::Error::new)?;
    }
    let (owned, admitted) = tokio::task::spawn_blocking(move || {
        let admitted = owned.bind_continuation_context(admitted)?;
        Ok::<_, um_execution::LocalRunDirectoryError>((owned, admitted))
    })
    .await
    .map_err(anyhow::Error::new)?
    .map_err(anyhow::Error::new)?;
    super::create::execute_recovering_attempt(
        workflow,
        admitted,
        owned,
        cancellation,
        signal_task,
        (config, leaf),
        child_recovery,
    )
    .await
}

enum Probed {
    Continue(Selection),
    Finished(super::super::super::CommandResult),
}

/// Evaluates the run without taking its lock (REQ-CONTINUE-74): a held wait is reported and
/// the command ends, a resolved wait or an abandoned run continues, and anything else is
/// refused.
async fn probe(
    config: um_execution::PresentationConfig,
    requested: &Path,
    command: &'static str,
) -> Result<Probed, super::super::super::CommandFailure> {
    let requested = requested.to_owned();
    let WaitProbe {
        run_directory,
        attempt_number,
        inspection,
    } = tokio::task::spawn_blocking(move || {
        inspect_wait(&requested, &super::condition::Conditions::default())
    })
    .await
    .map_err(anyhow::Error::new)?
    .map_err(anyhow::Error::new)?;
    let refuse = |code: &'static str, message: &str| {
        Ok(Probed::Finished(super::create::rejection_exit(
            WorkflowRunOutput::new(config.clone(), io::stdout(), io::stderr())
                .for_resume(&run_directory)
                .render_continuation_refusal(&run_directory, attempt_number, code, message),
        )))
    };
    match inspection {
        WaitInspection::Locked(rejection) => Ok(Probed::Finished(super::create::rejection_exit(
            WorkflowRunOutput::new(config.clone(), io::stdout(), io::stderr())
                .for_resume(rejection.run_directory())
                .render_retry_rejection(&rejection),
        ))),
        WaitInspection::Held { wait } => Ok(Probed::Finished(write_unattempted(
            &config,
            command,
            "waiting",
            &run_directory,
            attempt_number,
            Some(&wait),
        ))),
        WaitInspection::Satisfied {
            wait,
            by: ResolvedBy::Control(control),
        } if control.event == CONTROL_WITHDRAW => {
            Ok(Probed::Continue(Selection::Withdraw { wait: wait.id() }))
        }
        // REQ-CONTINUE-75: an observed external event continues at the wait's `then`.
        WaitInspection::Satisfied {
            wait,
            by: ResolvedBy::External(_),
        } => Ok(Probed::Continue(Selection::Wait {
            wait: wait.id(),
            target: wait.then.clone(),
            reason: ContinuationReasonV1::Resume,
        })),
        WaitInspection::Satisfied {
            wait,
            by: ResolvedBy::Control(control),
        } => {
            let (target, reason) = match control.event.as_str() {
                "control.hand_back" => (
                    wait.on_hand_back
                        .clone()
                        .unwrap_or_else(|| wait.then.clone()),
                    ContinuationReasonV1::Resume,
                ),
                "control.retry" => (wait.then.clone(), ContinuationReasonV1::Retry),
                _ => (wait.then.clone(), ContinuationReasonV1::Resume),
            };
            Ok(Probed::Continue(Selection::Wait {
                wait: wait.id(),
                target,
                reason,
            }))
        }
        WaitInspection::Continued { edge } => Ok(Probed::Continue(Selection::Edge(edge))),
        WaitInspection::RuleInvalid { .. } => refuse(
            um_execution::RuleError::CODE,
            "The wait's rule is not a conjunction of supported equalities; continue the run with --from STEP.",
        ),
        // REQ-CONTINUE-74: a condition that cannot be read is refused, never reported held.
        WaitInspection::ConditionUnavailable { error, .. } => refuse(error.code, &error.message),
        WaitInspection::NoWait {
            state: Some(RunState::Running),
            ..
        } => Ok(Probed::Continue(Selection::Abandoned)),
        // REQ-CONTINUE-29: a superseded run's refusal names its successor.
        WaitInspection::NoWait {
            state: Some(RunState::Exited),
            successor: Some(successor),
        } => refuse(
            "continuation_disposition_ineligible",
            &superseded_rejection_message(Some(&successor)),
        ),
        WaitInspection::NoWait {
            state: Some(RunState::Exited),
            successor: None,
        } => refuse(
            "continuation_disposition_ineligible",
            "An exited run cannot be continued without --from; create a new run.",
        ),
        WaitInspection::NoWait {
            state: Some(RunState::Waiting),
            ..
        } => refuse(
            "wait_record_missing",
            "The run reads waiting but holds no wait record; continue it with --from STEP.",
        ),
        WaitInspection::NoWait { state: None, .. } => refuse(
            "continuation_definition_invalid",
            "This run does not wait; name the steps to continue from with --from STEP.",
        ),
    }
}

/// Whether the run's latest attempt, under the continuation's lock, still ended `continued`
/// on `edge` (REQ-CONTINUE-78).
fn still_continues(pending: &PendingLocalContinuation, edge: &ContinuationEdgeV1) -> bool {
    pending.continued_edge().is_some_and(|taken| {
        (&taken.step, &taken.outcome, &taken.target) == (&edge.step, &edge.outcome, &edge.target)
    })
}

/// Whether the run, under the continuation's lock, still waits on the wait `wait` names.
fn still_waits(pending: &PendingLocalContinuation, wait: &str) -> bool {
    matches!(pending.wait(), Ok(Some(current)) if current.id() == wait)
}

/// REQ-CONTINUE-75: consume an independently completed repair only for the exact child
/// named by this wait. A normal retry or requested profile replacement still runs afresh.
fn repaired_child(
    pending: &PendingLocalContinuation,
    step: &str,
) -> anyhow::Result<Option<ChildRecovery>> {
    let Some(wait) = pending.wait()? else {
        return Ok(None);
    };
    if wait.kind != um_execution::WaitKind::Repair
        || wait.then != step
        || wait.step.first().and_then(serde_json::Value::as_str) != Some(step)
    {
        return Ok(None);
    }
    let Some(control) = pending.resolving_control(&wait)? else {
        return Ok(None);
    };
    if control.event != "control.retry"
        || control
            .payload
            .as_ref()
            .is_some_and(|payload| payload.contains_key("agentProfiles"))
    {
        return Ok(None);
    }
    let Some((directory, RunState::Exited, item)) =
        latest_child_run(pending.run_directory(), step)?
    else {
        return Ok(None);
    };
    if wait
        .payload
        .get("child")
        .and_then(serde_json::Value::as_str)
        .map(Path::new)
        != directory.strip_prefix(pending.run_directory()).ok()
    {
        return Ok(None);
    }
    let child = read_local_run_status(&directory).map_err(|error| {
        anyhow::anyhow!(
            "inspect repaired child {}: {}: {}",
            directory.display(),
            error.code.as_str(),
            error.code.message()
        )
    })?;
    if !matches!(child.recovery, um_execution::LocalRecoveryStatus::Settled)
        || !child.status_state.succeeded_after(&wait.since)?
    {
        return Ok(None);
    }
    Ok(Some(ChildRecovery::Adopt {
        step: step.to_owned(),
        item,
        directory,
    }))
}

fn refusal(
    config: &um_execution::PresentationConfig,
    leaf: ExecutionLeaf,
    pending: &PendingLocalContinuation,
    code: &'static str,
    message: &'static str,
) -> WorkflowRunPresentationResult {
    continuation_output(config.clone(), leaf, pending.run_directory()).render_continuation_refusal(
        pending.run_directory(),
        pending.prior_attempt_number(),
        code,
        message,
    )
}

/// Reports a resume that created no attempt and exits 0: a held wait (REQ-CONTINUE-74),
/// with `--json` the waiting document, or a withdrawn run (REQ-CONTINUE-75).
fn write_unattempted(
    config: &um_execution::PresentationConfig,
    command: &'static str,
    outcome: &'static str,
    run_directory: &Path,
    attempt_number: u64,
    wait: Option<&WaitRecord>,
) -> super::super::super::CommandResult {
    if config.mode() == um_execution::PresentationMode::Json {
        let run_directory = run_directory
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("normalized run path is not UTF-8"))?;
        super::super::super::write_pretty_json(&UnattemptedDocument {
            schema_version: 1,
            command,
            outcome,
            exit_status: 0,
            run_directory,
            attempt_number,
            wait,
        })
        .map_err(anyhow::Error::new)?;
    } else {
        let mut stdout = io::stdout().lock();
        write!(
            stdout,
            "run {} {outcome} · attempt {attempt_number}",
            normalize_terminal_path(run_directory)
        )
        .map_err(anyhow::Error::new)?;
        if let Some(wait) = wait {
            write!(
                stdout,
                " · {} at step {} since {}",
                normalize_terminal_scalar(wait.kind_name().as_bytes()),
                normalize_terminal_scalar(wait.step_path().as_bytes()),
                normalize_terminal_scalar(wait.since.as_bytes()),
            )
            .map_err(anyhow::Error::new)?;
        }
        writeln!(stdout).map_err(anyhow::Error::new)?;
        stdout.flush().map_err(anyhow::Error::new)?;
    }
    Ok(ExitCode::Success)
}

/// A continuation to admit against `pending`'s run: `workflow` from `from` with `inputs`, in
/// `execution_root`, with the run's tree when it has one.
pub(super) struct ContinuationCandidate<'a> {
    pub(super) workflow: &'a ResolvedWorkflow,
    pub(super) from: &'a [String],
    pub(super) inputs: ResolvedInputs,
    pub(super) execution_root: PathBuf,
    pub(super) tree: Option<ValidatedTree>,
}

/// Why a continuation was not admitted: its refusal, rendered for the leaf, or an
/// operational failure.
pub(super) enum Unadmitted {
    Rejected(WorkflowRunPresentationResult),
    Failed(&'static str),
}

/// Admits `candidate` as the continuation of `pending` (REQ-CONTINUE-3, REQ-CONTINUE-9,
/// REQ-CONTINUE-13): the partition, the projected inputs, the harness installations and the
/// workflow's admission, every failure reported together at phase `continuation`. The
/// admitted workflow is not bound to a tree copy; the advisories are for the caller to print
/// once it goes on.
pub(super) async fn admit_continuation(
    pending: &PendingLocalContinuation,
    candidate: ContinuationCandidate<'_>,
    cancellation: &CancellationSource,
    output: (&um_execution::PresentationConfig, ExecutionLeaf),
) -> Result<(AdmittedWorkflow, Vec<UntestedHarnessAdvisory>), Unadmitted> {
    admit_continuation_in(pending, candidate, None, cancellation, output).await
}

/// `admit_continuation` launched from `environment` rather than the host's: a child's repair
/// continuation keeps its parent's admitted environment (REQ-WFEXEC-59, INV-ENGINE-27).
pub(super) async fn admit_continuation_in(
    pending: &PendingLocalContinuation,
    candidate: ContinuationCandidate<'_>,
    environment: Option<um_execution::EnvironmentSnapshot>,
    cancellation: &CancellationSource,
    (config, leaf): (&um_execution::PresentationConfig, ExecutionLeaf),
) -> Result<(AdmittedWorkflow, Vec<UntestedHarnessAdvisory>), Unadmitted> {
    let ContinuationCandidate {
        workflow,
        from,
        inputs,
        execution_root,
        tree,
    } = candidate;
    let selection = pending.partition(workflow, from);
    let input_admission_failures =
        um_execution::local_continuation_input_failures(workflow, &inputs);
    // Graph selection is independent of predecessor dispositions. Continue collecting
    // applicable profile and Git-admission failures even if inheritance was rejected.
    let candidate_reexecuted = pending.candidate_reexecuted_steps(workflow, from);
    let maximum_parallel_steps = pending.maximum_parallel_steps();
    let baseline = pending.git_baseline();
    let workflow_for_context = workflow.clone();
    let context_cancellation = cancellation.clone();
    // Discovery is shared with retry, while rejection here must settle abandonment.
    let (context, installation_failures) = tokio::task::spawn_blocking(move || {
        super::create::continuation_execution_context(
            &workflow_for_context,
            execution_root,
            maximum_parallel_steps,
            context_cancellation,
            environment,
        )
    })
    .await
    .map_err(|_| Unadmitted::Failed("prepare continuation execution context"))?;
    let context = if workflow.requires_git_capture() {
        context.with_local_git_baseline(baseline)
    } else {
        context
    };
    let context = match tree {
        Some(tree) => context.with_tree(tree),
        None => context,
    };
    let untested_advisories = context.untested_harness_advisories();
    let rejection = |admission: &[&um_execution::AdmissionFailure]| {
        Unadmitted::Rejected(
            continuation_output(config.clone(), leaf, pending.run_directory())
                .render_continuation_admission_rejection(
                    pending.run_directory(),
                    pending.prior_attempt_number(),
                    selection
                        .as_ref()
                        .err()
                        .map(Vec::as_slice)
                        .unwrap_or_default(),
                    &[],
                    &installation_failures,
                    admission,
                ),
        )
    };
    let admitted = if let Some(reexecuted) = candidate_reexecuted {
        let context = context.with_continuation_reexecuted_steps(&reexecuted);
        let candidate = workflow.clone();
        match super::create::blocking_operation(move || {
            admit_local_continuation_workflow(candidate, inputs, context)
        })
        .await
        {
            Ok(admitted) => Some(admitted),
            Err(super::create::BlockingOperationError::Operation(failures)) => {
                let admission_failures = failures
                    .iter()
                    .filter(|failure| {
                        installation_failures.is_empty()
                            || failure.kind()
                                != um_execution::AdmissionFailureKind::AgentStepRuntimeUnsupported
                    })
                    .collect::<Vec<_>>();
                return Err(rejection(&admission_failures));
            }
            Err(super::create::BlockingOperationError::WorkerUnavailable) => {
                return Err(Unadmitted::Failed("admit continuation execution"));
            }
        }
    } else {
        None
    };
    if selection.is_err() || !installation_failures.is_empty() {
        return Err(rejection(
            &input_admission_failures.iter().collect::<Vec<_>>(),
        ));
    }
    admitted
        .map(|admitted| (admitted, untested_advisories))
        .ok_or(Unadmitted::Failed("complete continuation admission"))
}

async fn fail_after_settlement(
    pending: PendingLocalContinuation,
    signal_task: &super::create::SignalObservation,
    message: &'static str,
) -> super::super::super::CommandResult {
    error_after_settlement(pending, signal_task, anyhow::anyhow!(message)).await
}

/// `fail_after_settlement` with the cause the caller read.
async fn error_after_settlement(
    pending: PendingLocalContinuation,
    signal_task: &super::create::SignalObservation,
    error: anyhow::Error,
) -> super::super::super::CommandResult {
    settle(pending, signal_task).await?;
    Err(error.into())
}

async fn diagnose_after_settlement(
    pending: PendingLocalContinuation,
    signal_task: &super::create::SignalObservation,
    error: um_execution::LocalRunDirectoryError,
) -> super::super::super::CommandResult {
    settle(pending, signal_task).await?;
    super::create::diagnose(error)
}

async fn reject_after_settlement(
    pending: PendingLocalContinuation,
    signal_task: &super::create::SignalObservation,
    result: WorkflowRunPresentationResult,
) -> super::super::super::CommandResult {
    settle(pending, signal_task).await?;
    super::create::rejection_exit(result)
}

/// Settles an abandoned owner for a continuation that ends without claiming an attempt.
async fn settle(
    pending: PendingLocalContinuation,
    signal_task: &super::create::SignalObservation,
) -> anyhow::Result<()> {
    let settlement = tokio::task::spawn_blocking(move || pending.settle_abandoned()).await;
    signal_task.abort();
    settlement
        .map_err(anyhow::Error::new)?
        .map_err(anyhow::Error::new)
}
