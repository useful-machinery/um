//! The tree-run job (REQ-RUNNERENROLL-91): an offer whose spec carries `workflowTree`
//! runs the published tree with this runner's own `um` release. Preparation downloads
//! and verifies the tree's files, publishes them with the revision's bindings into a
//! run-scoped home under the assignment's private directory, checks the digest the
//! local publish reports against the spec's and writes the revision's resolution record
//! beside the published snapshot (REQ-WFDEF-32); execution runs `um workflow run create`
//! under the assignment's lease, resumes the run through its waits and attempts
//! (`resume_loop`), reporting its root progress and the step states of the run and its
//! child runs (`steps`, REQ-RUNNERENROLL-88), and reports the record's exit once
//! (REQ-RUNNERENROLL-99). It applies each trigger delivery for the assignment with `um
//! workflow run event`, those the offer carries before the run's first step
//! (REQ-TRIGGER-136), and each authorized Cloud control with the `um workflow run` control
//! command it names (REQ-RUNNERENROLL-90). A continuation attempt adopts the run an earlier
//! attempt created instead (`adopt`, REQ-RUNNERENROLL-89): it downloads and publishes
//! nothing, keeps the resolution record the first attempt wrote and drives the run directory
//! the retained home holds, applying its deliveries and controls in the same loop.

use std::collections::VecDeque;
use std::ffi::OsString;
use std::fs::{self, DirBuilder};
use std::io::Write as _;
use std::os::unix::fs::{DirBuilderExt as _, OpenOptionsExt as _};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use tokio::sync::Notify;
use um_execution::{
    CancellationPolicy, CancellationSource, CaptureCancellation, EnvironmentSnapshot,
    ResolvedInput, ResolvedInputs,
};
use um_runner_protocol::{
    AssignmentDecline, ExecutionSpecInvalidReason, ExecutionSpecV1RunnerProjection, RunControl,
    RunnerUnableReason, TriggerDelivery, WorkflowTreeV1RunnerProjection,
};

use super::Sleeper;
use super::run_inputs::PreparationDeadline;

mod command;
mod download;
mod records;
mod resume_loop;
mod steps;
#[cfg(test)]
mod tests;

pub(super) use download::{HttpTreeBroker, TreeBroker, is_safe_tree_path};
pub(super) use records::{TransportResult, TreeConclusion};
pub(super) use resume_loop::{DriveFailure, ReportSink, TreeEnd};

use command::{PublishFailure, RunLog, UmCommand};
use download::{TreeDownloadFailure, TreeRequest};
use resume_loop::ResumeIntervals;

const ROOT_WORKFLOW_FILE: &str = "workflow.yaml";
const RESOLUTION_FILE: &str = "resolution.json";
/// The binding kinds whose resolved provider ids the mirror uses (REQ-WFDEF-32).
const RESOLVED_KINDS: [&str; 2] = ["linear.team", "linear.state"];

/// The executable a tree run supervises: this runner's own `um` release.
pub(super) type UmExecutable = Arc<dyn Fn() -> std::io::Result<PathBuf> + Send + Sync>;

/// What the runner needs to execute published trees.
#[derive(Clone)]
pub(super) struct TreeRunDependencies {
    broker: Option<Arc<dyn TreeBroker>>,
    executable: UmExecutable,
    intervals: ResumeIntervals,
    command_bound: Duration,
    deliveries: Inbox<TriggerDelivery>,
    controls: Inbox<RunControl>,
}

impl TreeRunDependencies {
    pub(super) fn new(broker: Option<Arc<dyn TreeBroker>>) -> Self {
        Self {
            broker,
            executable: Arc::new(std::env::current_exe),
            intervals: ResumeIntervals::default(),
            command_bound: command::SHORT_COMMAND_BOUND,
            deliveries: Inbox::default(),
            controls: Inbox::default(),
        }
    }

    /// Queues `delivery` for the tree run of `assignment_id`, which applies it once
    /// (REQ-TRIGGER-136).
    pub(super) fn deliver(&self, assignment_id: &str, delivery: TriggerDelivery) {
        self.deliveries.push(assignment_id, delivery);
    }

    /// Queues `control` for the tree run of `assignment_id`, which applies it once
    /// (REQ-RUNNERENROLL-90).
    pub(super) fn control(&self, assignment_id: &str, control: RunControl) {
        self.controls.push(assignment_id, control);
    }

    /// Supervises `executable` in place of the running `um` executable.
    #[cfg(test)]
    pub(super) fn with_executable(mut self, executable: PathBuf) -> Self {
        self.executable = Arc::new(move || Ok(executable.clone()));
        self
    }

    /// Re-evaluates held waits, and reads the record, at these intervals.
    #[cfg(test)]
    pub(super) fn with_resume_intervals(mut self, intervals: ResumeIntervals) -> Self {
        self.intervals = intervals;
        self
    }

    /// Abandons each short `um` command once `bound` passes.
    #[cfg(test)]
    pub(super) fn with_command_bound(mut self, bound: Duration) -> Self {
        self.command_bound = bound;
        self
    }
}

/// An item the connection hands a tree run, by the id Cloud redelivers it under.
trait Inbound {
    fn id(&self) -> &str;
}

impl Inbound for TriggerDelivery {
    fn id(&self) -> &str {
        &self.delivery_id
    }
}

impl Inbound for RunControl {
    fn id(&self) -> &str {
        &self.control_id
    }
}

/// The trigger deliveries, or the controls, the connection received and the tree run of
/// their assignment has not yet taken, in arrival order and each id once. The runner holds
/// one assignment at a time, so an item for another assignment drops those still queued
/// for an earlier one.
#[derive(Clone)]
struct Inbox<T> {
    queued: Arc<Mutex<VecDeque<(String, T)>>>,
    arrived: Arc<Notify>,
}

impl<T> Default for Inbox<T> {
    fn default() -> Self {
        Self {
            queued: Arc::default(),
            arrived: Arc::default(),
        }
    }
}

impl<T: Inbound> Inbox<T> {
    fn push(&self, assignment_id: &str, item: T) {
        {
            let mut queued = self.queued.lock().unwrap_or_else(PoisonError::into_inner);
            queued.retain(|(queued_for, _)| queued_for == assignment_id);
            if queued.iter().any(|(_, queued)| queued.id() == item.id()) {
                return;
            }
            queued.push_back((assignment_id.to_owned(), item));
        }
        self.arrived.notify_one();
    }

    /// Removes and returns the items queued for `assignment_id`.
    fn take(&self, assignment_id: &str) -> Vec<T> {
        let mut queued = self.queued.lock().unwrap_or_else(PoisonError::into_inner);
        let (taken, kept) = std::mem::take(&mut *queued)
            .into_iter()
            .partition::<VecDeque<_>, _>(|(queued_for, _)| queued_for == assignment_id);
        *queued = kept;
        taken.into_iter().map(|(_, item)| item).collect()
    }

    /// Completes once an item has been queued since the last completion.
    async fn arrived(&self) {
        self.arrived.notified().await;
    }
}

/// The start's own stop step of a run with pending deliveries, which is created with a stop
/// before its first step instead: the runner puts it back when it lifts its own.
struct PendingStop {
    start: Option<String>,
}

/// The assignment's tree-run files, all under its private directory.
struct TreeLayout {
    tree: PathBuf,
    home: PathBuf,
    bindings: PathBuf,
    inputs: PathBuf,
    log: PathBuf,
    documents: PathBuf,
}

impl TreeLayout {
    fn under(private: &Path) -> Self {
        Self {
            tree: private.join("tree"),
            home: private.join("home"),
            bindings: private.join("bindings.json"),
            inputs: private.join("tree-inputs"),
            log: private.join("tree-run.log"),
            documents: private.join("tree-run.json"),
        }
    }
}

/// The run-scoped home a tree run's assignment creates under its private directory: the
/// home an adopting attempt looks for in each earlier attempt of the run.
pub(super) fn tree_home(private: &Path) -> PathBuf {
    TreeLayout::under(private).home
}

/// How a tree run's first command starts it.
enum TreeStart {
    /// `um workflow run create` with these arguments (REQ-RUNNERENROLL-91), the root
    /// workflow's first step, and the stop it was given before that step when the offer
    /// carries pending deliveries.
    Create {
        arguments: Vec<OsString>,
        first: String,
        pending_stop: Option<PendingStop>,
    },
    /// The home's run, resumed from these steps, or, with none, from where its record
    /// stands (REQ-RUNNERENROLL-89).
    Adopt(Vec<String>),
}

/// What a tree run takes from its assignment's offer: the assignment, under which the
/// connection queues its trigger deliveries and controls, the spec, for its cancellation grace, and the
/// deliveries the offer carried (REQ-TRIGGER-136).
struct TreeOffer<'a> {
    assignment_id: &'a str,
    execution_spec: &'a ExecutionSpecV1RunnerProjection,
    pending: &'a [TriggerDelivery],
}

/// A prepared tree run: published into its home and ready to `create`, or adopted from
/// the home an earlier attempt created.
pub(super) struct TreeRun {
    cancellation: CancellationPolicy,
    sleeper: Arc<dyn Sleeper>,
    um: UmCommand,
    start: TreeStart,
    runs: PathBuf,
    documents: PathBuf,
    intervals: ResumeIntervals,
    assignment_id: String,
    deliveries: Inbox<TriggerDelivery>,
    /// The Cloud controls for the run; a wait whose rule source is `um` is evaluated again
    /// only once one is applied.
    controls: Inbox<RunControl>,
    /// The deliveries the offer carried, applied before the run's first step: after
    /// `create`, or before an adopted run's first command.
    pending: Vec<TriggerDelivery>,
}

impl TreeRun {
    /// The run `start` begins for `offer` in the home `um` names, its runs under `runs`
    /// and the assignment's documents in `documents`.
    fn new(
        dependencies: &TreeRunDependencies,
        offer: &TreeOffer<'_>,
        sleeper: &Arc<dyn Sleeper>,
        um: UmCommand,
        start: TreeStart,
        runs: PathBuf,
        documents: PathBuf,
    ) -> Self {
        Self {
            cancellation: CancellationPolicy::new(
                CancellationSource::new(),
                Duration::from_secs(
                    offer
                        .execution_spec
                        .execution_limits
                        .cancellation_grace_seconds,
                ),
            ),
            sleeper: Arc::clone(sleeper),
            um,
            start,
            runs,
            documents,
            intervals: dependencies.intervals,
            assignment_id: offer.assignment_id.to_owned(),
            deliveries: dependencies.deliveries.clone(),
            controls: dependencies.controls.clone(),
            pending: offer.pending.to_vec(),
        }
    }

    pub(super) fn cancellation(&self) -> &CancellationPolicy {
        &self.cancellation
    }

    /// Runs `um workflow run create`, or adopts the home's run, and resumes the run until
    /// its record reads `exited` or the assignment's cancellation ends it, applying the
    /// cancellation to each process and each trigger delivery and control to the run, and
    /// queuing each new root progress and step report and each application through `reports`. The
    /// loop's future is boxed so it does not enlarge the job's stack frame, which in a
    /// debug build holds the tree job's future before boxing it.
    pub(super) async fn drive(&self, reports: ReportSink<'_>) -> Result<TreeEnd, DriveFailure> {
        Box::pin(resume_loop::drive(self, reports)).await
    }
}

/// The identity a tree run's preparation acts for.
pub(super) struct TreePreparation<'a> {
    pub(super) assignment_id: &'a str,
    pub(super) execution_spec: &'a ExecutionSpecV1RunnerProjection,
    pub(super) tree: &'a WorkflowTreeV1RunnerProjection,
    pub(super) private: &'a Path,
    pub(super) workspace: &'a Path,
    pub(super) environment: &'a EnvironmentSnapshot,
    pub(super) deadline: PreparationDeadline,
    pub(super) cancellation: &'a CaptureCancellation,
    pub(super) sleeper: Arc<dyn Sleeper>,
}

/// Downloads, verifies and publishes the tree and stages the run inputs, on the
/// calling blocking worker. A published digest other than the spec's declines the
/// assignment `workflow_source_digest_mismatch` (cause `workflow_tree_digest_mismatch`).
pub(super) fn prepare(
    dependencies: &TreeRunDependencies,
    preparation: &TreePreparation<'_>,
    inputs: &ResolvedInputs,
) -> Result<TreeRun, AssignmentDecline> {
    let layout = TreeLayout::under(preparation.private);
    let broker = dependencies.broker.as_deref().ok_or_else(|| {
        AssignmentDecline::RunnerUnable(RunnerUnableReason::SourceServiceUnavailable).diagnosed(
            "workflow_tree_download",
            "workflow_tree_service_unavailable",
        )
    })?;
    download::download_tree(
        broker,
        TreeRequest {
            assignment_id: preparation.assignment_id,
            execution_spec_id: &preparation.execution_spec.execution_spec_id,
            digest: &preparation.tree.digest,
        },
        preparation.tree,
        &layout.tree,
        preparation.cancellation,
        preparation.deadline,
    )
    .map_err(download_decline)?;
    let staged = (|| {
        DirBuilder::new().mode(0o700).create(&layout.home)?;
        write_private(
            &layout.bindings,
            &serde_json::to_vec(&preparation.tree.bindings).map_err(std::io::Error::other)?,
        )?;
        stage_inputs(&layout.inputs, inputs)
    })()
    .map_err(|_| environment_decline("workflow_tree_publication"))?;
    let um = UmCommand {
        executable: (dependencies.executable)()
            .map_err(|_| environment_decline("workflow_tree_publication"))?,
        environment: preparation.environment.clone(),
        home: layout.home.clone(),
        log: RunLog::new(layout.log.clone()),
        sleeper: Arc::clone(&preparation.sleeper),
        bound: dependencies.command_bound,
    };
    let fenced = || {
        AssignmentDecline::RunnerUnable(RunnerUnableReason::SourceServiceUnavailable)
            .diagnosed("workflow_tree_publication", "assignment_fenced")
    };
    if preparation.cancellation.is_cancelled() {
        return Err(fenced());
    }
    let published = command::publish(
        &um,
        &layout.tree,
        &layout.bindings,
        preparation.cancellation,
        preparation.deadline,
    )
    .map_err(|failure| match failure {
        PublishFailure::Launch => environment_decline("workflow_tree_publication"),
        PublishFailure::Refused => AssignmentDecline::ExecutionSpecInvalid(
            ExecutionSpecInvalidReason::WorkflowSourceInvalid,
        )
        .diagnosed(
            "workflow_tree_publication",
            "workflow_tree_publication_refused",
        ),
        PublishFailure::Abandoned if preparation.cancellation.is_cancelled() => fenced(),
        PublishFailure::Abandoned => {
            AssignmentDecline::RunnerUnable(RunnerUnableReason::ExecutionEnvironmentUnavailable)
                .diagnosed(
                    "workflow_tree_publication",
                    "workflow_tree_publication_abandoned",
                )
        }
    })?;
    if published.digest != preparation.tree.digest {
        return Err(AssignmentDecline::ExecutionSpecInvalid(
            ExecutionSpecInvalidReason::WorkflowSourceDigestMismatch,
        )
        .diagnosed("workflow_tree_publication", "workflow_tree_digest_mismatch"));
    }
    let definition = layout.home.join("workflows").join(&published.definition);
    write_private(
        &definition
            .join("revisions")
            .join(published.revision.to_string())
            .join(RESOLUTION_FILE),
        &serde_json::to_vec(&resolution_record(&preparation.tree.resolution))
            .map_err(|_| environment_decline("workflow_tree_publication"))?,
    )
    .map_err(|_| environment_decline("workflow_tree_publication"))?;
    let runs = definition.join("runs");
    let first = first_step(&layout.tree, &preparation.tree.workflow_path)?;
    let pending_stop = (!preparation.tree.pending_deliveries.is_empty()).then(|| PendingStop {
        start: preparation.tree.stop_before.clone(),
    });
    let create_arguments = create_arguments(
        &published.definition,
        preparation.execution_spec,
        preparation.tree,
        preparation.workspace,
        staged,
        pending_stop
            .as_ref()
            .map(|_| &first)
            .or(preparation.tree.stop_before.as_ref()),
    );
    Ok(TreeRun::new(
        dependencies,
        &TreeOffer {
            assignment_id: preparation.assignment_id,
            execution_spec: preparation.execution_spec,
            pending: &preparation.tree.pending_deliveries,
        },
        &preparation.sleeper,
        um,
        TreeStart::Create {
            arguments: create_arguments,
            first,
            pending_stop,
        },
        runs,
        layout.documents,
    ))
}

/// What a tree run's adoption acts for (REQ-RUNNERENROLL-89): the run-scoped home the
/// attempt that created the run left, and the adopting assignment's own id, private
/// directory, environment, spec, sleeper, named steps and the trigger deliveries its offer
/// carried.
pub(super) struct TreeAdoption<'a> {
    pub(super) assignment_id: &'a str,
    pub(super) home: &'a Path,
    pub(super) private: &'a Path,
    pub(super) environment: &'a EnvironmentSnapshot,
    pub(super) execution_spec: &'a ExecutionSpecV1RunnerProjection,
    pub(super) sleeper: Arc<dyn Sleeper>,
    pub(super) steps: &'a [String],
    pub(super) pending_deliveries: &'a [TriggerDelivery],
}

/// Adopts the run in `adoption.home`, on the calling blocking worker: its runs are those
/// of the home's only definition, which must hold exactly one run, or the offer is
/// declined `retained_workspace_unavailable`. Its commands run with the adopting
/// attempt's environment and that home as `UM_HOME`, their output in the adopting
/// assignment's own log and documents; the runner itself writes nothing in the home.
pub(super) fn adopt(
    dependencies: &TreeRunDependencies,
    adoption: &TreeAdoption<'_>,
) -> Result<TreeRun, AssignmentDecline> {
    let runs = command::only_entry(&adoption.home.join("workflows"))
        .map(|definition| definition.join("runs"))
        .filter(|runs| command::only_entry(runs).is_some())
        .ok_or_else(|| {
            AssignmentDecline::RunnerUnable(RunnerUnableReason::RetainedWorkspaceUnavailable)
                .diagnosed("workflow_tree_adoption", "tree_run_directory_unavailable")
        })?;
    let layout = TreeLayout::under(adoption.private);
    let um = UmCommand {
        executable: (dependencies.executable)()
            .map_err(|_| environment_decline("workflow_tree_adoption"))?,
        environment: adoption.environment.clone(),
        home: adoption.home.to_owned(),
        log: RunLog::new(layout.log),
        sleeper: Arc::clone(&adoption.sleeper),
        bound: dependencies.command_bound,
    };
    Ok(TreeRun::new(
        dependencies,
        &TreeOffer {
            assignment_id: adoption.assignment_id,
            execution_spec: adoption.execution_spec,
            pending: adoption.pending_deliveries,
        },
        &adoption.sleeper,
        um,
        TreeStart::Adopt(adoption.steps.to_vec()),
        runs,
        layout.documents,
    ))
}

/// The first step, in document order, of the tree's workflow at `workflow_path`: a run with
/// pending deliveries stops before it, earlier than any step the start names, and a refused
/// `create` names it as the step that did not start.
fn first_step(tree: &Path, workflow_path: &str) -> Result<String, AssignmentDecline> {
    um_execution::resolve_workflow_file(tree, &tree.join(workflow_path))
        .ok()
        .and_then(|workflow| workflow.definition.source_order.first().cloned())
        .ok_or_else(|| {
            AssignmentDecline::ExecutionSpecInvalid(
                ExecutionSpecInvalidReason::WorkflowSourceInvalid,
            )
            .diagnosed(
                "workflow_tree_publication",
                "workflow_tree_first_step_unavailable",
            )
        })
}

/// The revision's `resolution.json` (REQ-WFDEF-32): each `linear.team` and `linear.state`
/// key the spec's resolution gives an id, as exactly `{id, name}`. A key resolved without
/// a display name is named by its bound value; a key without an id is left out, so the
/// mirror reports it `mirror_state_unknown` when it writes that state.
fn resolution_record(resolution: &serde_json::Value) -> serde_json::Value {
    let record = resolution
        .as_object()
        .into_iter()
        .flatten()
        .filter(|(_, resolved)| {
            resolved["kind"]
                .as_str()
                .is_some_and(|kind| RESOLVED_KINDS.contains(&kind))
        })
        .filter_map(|(key, resolved)| {
            let id = resolved["id"].as_str().filter(|id| !id.is_empty())?;
            let name = [&resolved["name"], &resolved["value"]]
                .into_iter()
                .find_map(|name| name.as_str().filter(|name| !name.is_empty()))?;
            Some((key.clone(), serde_json::json!({ "id": id, "name": name })))
        })
        .collect();
    serde_json::Value::Object(record)
}

/// `um workflow run create` for the run: the run inputs, the start's owner, the stop step,
/// and the Cloud run page as `external.url` (REQ-LOCALRUN-209).
fn create_arguments(
    definition: &str,
    execution_spec: &ExecutionSpecV1RunnerProjection,
    tree: &WorkflowTreeV1RunnerProjection,
    workspace: &Path,
    inputs: Vec<OsString>,
    stop_before: Option<&String>,
) -> Vec<OsString> {
    let mut arguments: Vec<OsString> = vec![
        "workflow".into(),
        "run".into(),
        "create".into(),
        definition.into(),
        "--execution-root".into(),
        workspace.into(),
        "--max-parallel".into(),
        execution_spec
            .execution_limits
            .maximum_parallel_steps
            .to_string()
            .into(),
    ];
    if tree.workflow_path != ROOT_WORKFLOW_FILE {
        arguments.extend(["--workflow".into(), tree.workflow_path.clone().into()]);
    }
    arguments.extend(inputs);
    // One argument, so an owner whose name begins with `-` is not read as a flag.
    if let Some(owner) = &tree.owner {
        arguments.push(format!("--owner={owner}").into());
    }
    if let Some(url) = &tree.run_url {
        arguments.extend(["--external".into(), format!("url={url}").into()]);
    }
    if let Some(step) = stop_before {
        arguments.extend(["--stop-before".into(), step.into()]);
    }
    arguments.push("--json".into());
    arguments
}

/// Writes each materialized run input to a private file and returns the `--input-*`
/// arguments that supply it.
fn stage_inputs(directory: &Path, inputs: &ResolvedInputs) -> std::io::Result<Vec<OsString>> {
    DirBuilder::new().mode(0o700).create(directory)?;
    let mut arguments = Vec::new();
    for (index, (name, input)) in inputs.values().iter().enumerate() {
        let file = |member: usize, bytes: &[u8]| -> std::io::Result<OsString> {
            let path = directory.join(format!("{index:03}-{member:03}"));
            write_private(&path, bytes)?;
            Ok(path.into_os_string())
        };
        match input {
            ResolvedInput::Text(text) => arguments.extend([
                "--input-text-file".into(),
                name.into(),
                file(0, text.as_bytes())?,
            ]),
            ResolvedInput::Json(json) => arguments.extend([
                "--input-json-file".into(),
                name.into(),
                file(0, json.source())?,
            ]),
            ResolvedInput::File(value) => arguments.extend([
                "--input-file".into(),
                name.into(),
                value.media_type().into(),
                file(0, value.bytes())?,
            ]),
            ResolvedInput::Attachments(members) if members.is_empty() => {
                arguments.extend(["--input-attachments-empty".into(), name.into()]);
            }
            ResolvedInput::Attachments(members) => {
                for (member, attachment) in members.iter().enumerate() {
                    arguments.extend([
                        "--input-attachment".into(),
                        name.into(),
                        attachment.media_type().into(),
                        file(member, attachment.bytes())?,
                    ]);
                }
            }
        }
    }
    Ok(arguments)
}

fn write_private(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(bytes)?;
    file.sync_all()
}

fn environment_decline(stage: &'static str) -> AssignmentDecline {
    AssignmentDecline::RunnerUnable(RunnerUnableReason::ExecutionEnvironmentUnavailable)
        .diagnosed(stage, "execution_root_unavailable")
}

fn download_decline(failure: TreeDownloadFailure) -> AssignmentDecline {
    let decline = match failure {
        TreeDownloadFailure::Unavailable
        | TreeDownloadFailure::Fenced
        | TreeDownloadFailure::InvalidResponse => {
            AssignmentDecline::RunnerUnable(RunnerUnableReason::SourceServiceUnavailable)
        }
        TreeDownloadFailure::ContentMismatch => AssignmentDecline::ExecutionSpecInvalid(
            ExecutionSpecInvalidReason::WorkflowSourceDigestMismatch,
        ),
        TreeDownloadFailure::Environment => {
            AssignmentDecline::RunnerUnable(RunnerUnableReason::ExecutionEnvironmentUnavailable)
        }
    };
    decline.diagnosed("workflow_tree_download", failure.cause())
}
