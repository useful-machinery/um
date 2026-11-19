//! The tree-run job (REQ-RUNNERENROLL-91): an offer whose spec carries `workflowTree`
//! runs the published tree with this runner's own `useful` release. Preparation downloads
//! and verifies the tree's files, publishes them with the revision's bindings into a
//! run-scoped home under the assignment's private directory, checks the digest the
//! local publish reports against the spec's and writes the revision's resolution record
//! beside the published snapshot (REQ-WFDEF-32); execution runs `useful workflow run create`
//! under the assignment's lease, resumes the run through its waits and attempts
//! (`resume_loop`), reporting its root progress and the step states of the run and its
//! child runs (`steps`, REQ-RUNNERENROLL-88), and reports the record's exit once
//! (REQ-RUNNERENROLL-99), with the root's text exports its current attempt published
//! (`exports`, REQ-RUNNERENROLL-131). It applies each trigger delivery for the assignment with `useful
//! workflow run event`, those the offer carries before the run's first step
//! (REQ-TRIGGER-136), and each authorized Cloud control with the `useful workflow run` control
//! command it names (REQ-RUNNERENROLL-90). A continuation attempt adopts the run an earlier
//! attempt created instead (`adopt`, REQ-RUNNERENROLL-89): it downloads and publishes
//! nothing, keeps the resolution record the first attempt wrote and drives the run directory
//! the retained home holds, applying its deliveries and controls in the same loop.

use std::collections::{BTreeMap, VecDeque};
use std::ffi::OsString;
use std::fs::DirBuilder;
use std::io::Write as _;
use std::os::unix::fs::DirBuilderExt as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use tokio::sync::Notify;
use useful_execution::{
    CancellationPolicy, CancellationSource, CaptureCancellation, ResolvedInput, ResolvedInputs,
};
use useful_runner_protocol::{
    AssignmentDecline, ExecutionSpecInvalidReason, ExecutionSpecV1RunnerProjection, RunControl,
    RunnerUnableReason, TriggerDelivery, ValidationFacts, WorkflowTreeV1RunnerProjection,
};

use super::Sleeper;
use super::run_inputs::PreparationDeadline;
use crate::agent_login::CommandEnvironment;

mod activity;
mod command;
mod download;
mod exports;
mod records;
mod resume_loop;
mod steps;
#[cfg(test)]
mod tests;

pub(super) use activity::{HeldRuns, RunActivity};
pub(super) use download::{HttpTreeBroker, TreeBroker, is_safe_tree_path};
pub(super) use records::{TransportResult, TreeConclusion};
pub(super) use resume_loop::{DriveFailure, ReportSink, TreeEnd};

use command::{PublishFailure, RunLog, UsefulCommand};
use download::{TreeDownloadFailure, TreeRequest};
use resume_loop::ResumeIntervals;

const ROOT_WORKFLOW_FILE: &str = "workflow.yaml";
const RESOLUTION_FILE: &str = "resolution.json";
/// The flat binding kinds whose resolved provider ids the mirror uses (REQ-WFDEF-32).
const RESOLVED_KINDS: [&str; 2] = ["linear.team", "linear.state"];
/// The kinds of the `linear` integration's bindings whose resolved provider ids the mirror
/// uses, recorded under the member `linear` (REQ-WFDEF-32).
const RESOLVED_LINEAR_KINDS: [&str; 2] = ["team", "state"];

/// The executable a tree run supervises, or the one its workflow-Git helper runs: this
/// runner's own `useful` release.
pub(super) type UsefulExecutable = Arc<dyn Fn() -> std::io::Result<PathBuf> + Send + Sync>;

/// What the runner needs to execute published trees.
#[derive(Clone)]
pub(super) struct TreeRunDependencies {
    broker: Option<Arc<dyn TreeBroker>>,
    executable: UsefulExecutable,
    helper_executable: UsefulExecutable,
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
            helper_executable: Arc::new(std::env::current_exe),
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

    /// Drops the deliveries and controls still queued for `assignment_id`, which has ended,
    /// so the runner keeps nothing of an ended assignment's for the life of its boot.
    pub(super) fn forget(&self, assignment_id: &str) {
        self.deliveries.forget(assignment_id);
        self.controls.forget(assignment_id);
    }

    /// The executable the workspace's workflow-Git helper runs in its internal helper mode
    /// (REQ-RUNNERENROLL-121).
    pub(super) fn helper_executable(&self) -> std::io::Result<PathBuf> {
        (self.helper_executable)()
    }

    /// Supervises `executable` in place of the running `useful` executable.
    #[cfg(test)]
    pub(super) fn with_executable(mut self, executable: PathBuf) -> Self {
        self.executable = Arc::new(move || Ok(executable.clone()));
        self
    }

    /// Runs the workflow-Git helper with `executable` in place of the running `useful`
    /// executable.
    #[cfg(test)]
    pub(super) fn with_helper_executable(mut self, executable: PathBuf) -> Self {
        self.helper_executable = Arc::new(move || Ok(executable.clone()));
        self
    }

    /// Re-evaluates held waits, and reads the record, at these intervals.
    #[cfg(test)]
    pub(super) fn with_resume_intervals(mut self, intervals: ResumeIntervals) -> Self {
        self.intervals = intervals;
        self
    }

    /// Abandons each short `useful` command once `bound` passes.
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

/// The trigger deliveries, or the controls, the connection received for each assignment the
/// runner holds and its tree run has not yet taken. Each assignment has a queue of its own,
/// so an item for one neither drops nor wakes another's (INV-RUNNER-40).
#[derive(Clone)]
struct Inbox<T> {
    queues: Arc<Mutex<BTreeMap<String, Arc<AssignmentInbox<T>>>>>,
}

impl<T> Default for Inbox<T> {
    fn default() -> Self {
        Self {
            queues: Arc::default(),
        }
    }
}

impl<T: Inbound> Inbox<T> {
    fn push(&self, assignment_id: &str, item: T) {
        self.of(assignment_id).push(item);
    }

    /// The queue of `assignment_id`, made empty when it has none: an item can arrive
    /// before the assignment's tree run exists, and it is kept for that run.
    fn of(&self, assignment_id: &str) -> Arc<AssignmentInbox<T>> {
        let mut queues = self.queues.lock().unwrap_or_else(PoisonError::into_inner);
        Arc::clone(queues.entry(assignment_id.to_owned()).or_default())
    }

    /// Drops the queue of `assignment_id`, which has ended.
    fn forget(&self, assignment_id: &str) {
        self.queues
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(assignment_id);
    }
}

/// One assignment's trigger deliveries, or its controls, in arrival order and each id once.
struct AssignmentInbox<T> {
    queued: Mutex<VecDeque<T>>,
    arrived: Notify,
}

impl<T> Default for AssignmentInbox<T> {
    fn default() -> Self {
        Self {
            queued: Mutex::default(),
            arrived: Notify::new(),
        }
    }
}

impl<T: Inbound> AssignmentInbox<T> {
    fn push(&self, item: T) {
        {
            let mut queued = self.queued.lock().unwrap_or_else(PoisonError::into_inner);
            if queued.iter().any(|queued| queued.id() == item.id()) {
                return;
            }
            queued.push_back(item);
        }
        self.arrived.notify_one();
    }

    /// Removes and returns the items queued.
    fn take(&self) -> Vec<T> {
        std::mem::take(&mut *self.queued.lock().unwrap_or_else(PoisonError::into_inner)).into()
    }

    /// Completes once an item has been queued since the last completion.
    async fn arrived(&self) {
        self.arrived.notified().await;
    }
}

/// How a created run meets the pending deliveries its offer carries (REQ-TRIGGER-136).
enum PendingStop {
    /// The root is edge-scheduled, so its first step is a `workflow` step the runtime holds
    /// before: the run is created with a stop of the runner's own before that step in place
    /// of the start's own stop step `start`, which the runner puts back when it lifts its own.
    Held { start: Option<String> },
    /// The root is not edge-scheduled, so the runtime holds before none of its steps
    /// (REQ-WFEXEC-62, REQ-WFFORMAT-93): the run is created with the start's own stop as
    /// named, which `create` refuses `step_not_holdable`, and no stop of the runner's, and
    /// the deliveries are applied once `create` returns, with no `stop`, `lift` or `resume`.
    Unheld,
}

/// The assignment's tree-run files, all under its private directory.
struct TreeLayout {
    tree: PathBuf,
    home: PathBuf,
    bindings: PathBuf,
    /// The origin a GitHub project's workspace checkout cloned, recorded when the runner
    /// installs its Git relay for it, which a continuation reinstalls for the same origin
    /// (REQ-RUNNERENROLL-122).
    origin: PathBuf,
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
            origin: private.join("tree-origin"),
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

/// Records in the assignment's private directory, beside the run-scoped home, the origin
/// its workspace checkout cloned and its Git relay serves: an attempt that adopts the run
/// clones nothing, and reinstalls the relay for this origin (REQ-RUNNERENROLL-122).
pub(super) fn record_origin(private: &Path, origin: &str) -> std::io::Result<()> {
    write_private(&TreeLayout::under(private).origin, origin.as_bytes())
}

/// The origin the attempt whose private directory is `private` recorded, or none when it
/// recorded none: its project is on GitLab, or an earlier release created the run.
pub(super) fn recorded_origin(private: &Path) -> std::io::Result<Option<Arc<str>>> {
    match std::fs::read_to_string(TreeLayout::under(private).origin) {
        Ok(origin) => Ok(Some(Arc::from(origin))),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

/// How a tree run's first command starts it.
enum TreeStart {
    /// `useful workflow run create` with these arguments (REQ-RUNNERENROLL-91), the root
    /// workflow's first step, and, when the offer carries pending deliveries, how they meet
    /// the run: before that step, behind a stop of the runner's own, on an edge-scheduled
    /// root, or once `create` returns on any other.
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
    useful: UsefulCommand,
    start: TreeStart,
    runs: PathBuf,
    documents: PathBuf,
    intervals: ResumeIntervals,
    /// The trigger deliveries for the run's assignment.
    deliveries: Arc<AssignmentInbox<TriggerDelivery>>,
    /// The Cloud controls for the run's assignment; a wait whose rule source is `um` is
    /// evaluated again only once one is applied.
    controls: Arc<AssignmentInbox<RunControl>>,
    /// The deliveries the offer carried, applied before the run's first step: after
    /// `create`, or before an adopted run's first command.
    pending: Vec<TriggerDelivery>,
}

impl TreeRun {
    /// The run `start` begins for `offer` in the home `useful` names, its runs under `runs`
    /// and the assignment's documents in `documents`.
    fn new(
        dependencies: &TreeRunDependencies,
        offer: &TreeOffer<'_>,
        sleeper: &Arc<dyn Sleeper>,
        useful: UsefulCommand,
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
            useful,
            start,
            runs,
            documents,
            intervals: dependencies.intervals,
            deliveries: dependencies.deliveries.of(offer.assignment_id),
            controls: dependencies.controls.of(offer.assignment_id),
            pending: offer.pending.to_vec(),
        }
    }

    pub(super) fn cancellation(&self) -> &CancellationPolicy {
        &self.cancellation
    }

    /// Whether the run is working, which the assignment manager shares with it so
    /// agent-login setup and the offer rule can read it (INV-RUNNER-15, INV-RUNNER-40).
    pub(super) fn activity(&self) -> &Arc<RunActivity> {
        &self.useful.activity
    }

    /// Runs `useful workflow run create`, or adopts the home's run, and resumes the run until
    /// its record reads `exited` or the assignment's cancellation ends it, applying the
    /// cancellation to each process and each trigger delivery and control to the run, and
    /// queuing each new root progress and step report and each application through `reports`. The
    /// loop's future is boxed so it does not enlarge the job's stack frame, which in a
    /// debug build holds the tree job's future before boxing it. The run is working again
    /// once the loop returns or is dropped, whatever ended it, so its release and report
    /// count as work (INV-RUNNER-15).
    pub(super) async fn drive(&self, reports: ReportSink<'_>) -> Result<TreeEnd, DriveFailure> {
        let _working = self.activity().working_at_end();
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
    pub(super) environment: &'a CommandEnvironment,
    pub(super) deadline: PreparationDeadline,
    pub(super) cancellation: &'a CaptureCancellation,
    pub(super) sleeper: Arc<dyn Sleeper>,
}

/// Downloads, verifies and publishes the tree and stages the run inputs, on the
/// calling blocking worker. A publish that refuses the tree declines the assignment
/// `workflow_source_invalid` (cause `workflow_tree_publication_refused`) with the validation
/// facts its report gives, the path only when it is one of the offered tree's files; a
/// published digest other than the spec's declines it `workflow_source_digest_mismatch`
/// (cause `workflow_tree_digest_mismatch`).
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
    let useful = UsefulCommand {
        executable: (dependencies.executable)()
            .map_err(|_| environment_decline("workflow_tree_publication"))?,
        environment: preparation.environment.clone(),
        activity: RunActivity::new(),
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
        &useful,
        &layout.tree,
        &layout.bindings,
        preparation.cancellation,
        preparation.deadline,
    )
    .map_err(|failure| match failure {
        PublishFailure::Launch => environment_decline("workflow_tree_publication"),
        PublishFailure::Refused(report) => {
            let path = report
                .path
                .as_deref()
                .filter(|path| preparation.tree.files.iter().any(|file| file.path == *path));
            AssignmentDecline::ExecutionSpecInvalid(
                ExecutionSpecInvalidReason::WorkflowSourceInvalid,
            )
            .diagnosed(
                "workflow_tree_publication",
                "workflow_tree_publication_refused",
            )
            .with_validation_facts(ValidationFacts::new(
                report.code.as_deref(),
                report.location.as_deref(),
                path,
                report.step.as_deref(),
            ))
        }
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
    let FirstStep {
        step: first,
        edge_scheduled,
    } = first_step(&layout.tree, &preparation.tree.workflow_path)?;
    let pending_stop = (!preparation.tree.pending_deliveries.is_empty()).then(|| {
        if edge_scheduled {
            PendingStop::Held {
                start: preparation.tree.stop_before.clone(),
            }
        } else {
            PendingStop::Unheld
        }
    });
    let stop_before = match pending_stop {
        Some(PendingStop::Held { .. }) => Some(&first),
        Some(PendingStop::Unheld) | None => preparation.tree.stop_before.as_ref(),
    };
    let create_arguments = create_arguments(
        &published.definition,
        preparation.execution_spec,
        preparation.tree,
        preparation.workspace,
        staged,
        stop_before,
    );
    Ok(TreeRun::new(
        dependencies,
        &TreeOffer {
            assignment_id: preparation.assignment_id,
            execution_spec: preparation.execution_spec,
            pending: &preparation.tree.pending_deliveries,
        },
        &preparation.sleeper,
        useful,
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
    pub(super) environment: &'a CommandEnvironment,
    pub(super) execution_spec: &'a ExecutionSpecV1RunnerProjection,
    pub(super) sleeper: Arc<dyn Sleeper>,
    pub(super) steps: &'a [String],
    pub(super) pending_deliveries: &'a [TriggerDelivery],
}

/// Adopts the run in `adoption.home`, on the calling blocking worker: its runs are those
/// of the home's only definition, which must hold exactly one run, or the offer is
/// declined `retained_workspace_unavailable`. Its commands run with the environment the
/// adopting attempt's offer builds when each starts and that home as `USEFUL_HOME`, their
/// output in the adopting assignment's own log and documents; the runner itself writes
/// nothing in the home.
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
    let useful = UsefulCommand {
        executable: (dependencies.executable)()
            .map_err(|_| environment_decline("workflow_tree_adoption"))?,
        environment: adoption.environment.clone(),
        activity: RunActivity::new(),
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
        useful,
        TreeStart::Adopt(adoption.steps.to_vec()),
        runs,
        layout.documents,
    ))
}

/// The first step, in document order, of a tree's root workflow, and whether the root is
/// edge-scheduled.
struct FirstStep {
    step: String,
    /// The root's steps are `workflow` steps, the only steps the runtime holds before
    /// (REQ-WFEXEC-62); a root is all `workflow` steps or has none (REQ-WFFORMAT-93).
    edge_scheduled: bool,
}

/// The first step of the tree's workflow at `workflow_path`: on an edge-scheduled root a run
/// with pending deliveries stops before it, earlier than any step the start names, and a
/// refused `create` names it as the step that did not start.
fn first_step(tree: &Path, workflow_path: &str) -> Result<FirstStep, AssignmentDecline> {
    useful_execution::resolve_workflow_file(tree, &tree.join(workflow_path))
        .ok()
        .and_then(|workflow| {
            Some(FirstStep {
                step: workflow.definition.source_order.first().cloned()?,
                edge_scheduled: matches!(
                    workflow.definition.scheduling,
                    useful_execution::Scheduling::Edges { .. }
                ),
            })
        })
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

/// The revision's `resolution.json` (REQ-WFDEF-32): each flat `linear.team` and
/// `linear.state` key the spec's resolution gives an id and, under the member `linear`, each
/// `team` and `state` binding of the `linear` integration's entry it gives one, each as
/// exactly `{id, name}`. A member of the resolution is a flat entry exactly when its `kind`
/// is a string; any other is an integration's entry keyed by its id. A binding resolved
/// without a display name is named by its bound value; one without an id is left out, so the
/// mirror reports it `mirror_state_unknown` when it writes that state. An integration's
/// `account`, and every other integration's entry, are not written: the store refuses a
/// record holding anything but these.
fn resolution_record(resolution: &serde_json::Value) -> serde_json::Value {
    let linear = useful_execution::IntegrationProvider::Linear.as_str();
    let mut record = serde_json::Map::new();
    for (key, resolved) in resolution.as_object().into_iter().flatten() {
        let entry = match resolved["kind"].as_str() {
            Some(kind) if RESOLVED_KINDS.contains(&kind) => resolved_entry(resolved),
            Some(_) => None,
            None if key == linear => linear_bindings(resolved),
            None => None,
        };
        if let Some(entry) = entry {
            record.insert(key.clone(), entry);
        }
    }
    serde_json::Value::Object(record)
}

/// The `linear` integration entry's `team` and `state` bindings that have an id, as name to
/// `{id, name}`, or none when no binding has one.
fn linear_bindings(entry: &serde_json::Value) -> Option<serde_json::Value> {
    let bindings: serde_json::Map<_, _> = entry["bindings"]
        .as_object()
        .into_iter()
        .flatten()
        .filter(|(_, binding)| {
            binding["kind"]
                .as_str()
                .is_some_and(|kind| RESOLVED_LINEAR_KINDS.contains(&kind))
        })
        .filter_map(|(name, binding)| Some((name.clone(), resolved_entry(binding)?)))
        .collect();
    (!bindings.is_empty()).then_some(serde_json::Value::Object(bindings))
}

/// A resolved binding's `{id, name}`, named by its bound value when it has no display name,
/// or none when it has no id.
fn resolved_entry(resolved: &serde_json::Value) -> Option<serde_json::Value> {
    let id = resolved["id"].as_str().filter(|id| !id.is_empty())?;
    let name = [&resolved["name"], &resolved["value"]]
        .into_iter()
        .find_map(|name| name.as_str().filter(|name| !name.is_empty()))?;
    Some(serde_json::json!({ "id": id, "name": name }))
}

/// `useful workflow run create` for the run: the run inputs, the start's owner, the stop step,
/// the Cloud run page as `external.url` (REQ-LOCALRUN-209), and the pinned source as the
/// engine path derives it (REQ-RUNNERENROLL-120). The stop step is the
/// start's own as named, which the runtime refuses when it cannot hold before it, or, on an
/// edge-scheduled root with pending deliveries, the runner's own before the first step.
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
    // Each value inside its name's argument, so a branch that begins with `-` is not read
    // as a flag.
    let source = super::assignment::source_revision(execution_spec);
    let mut names = vec![("commit", source.commit_oid())];
    names.extend(source.branch().map(|branch| ("branch", branch)));
    if let Some((branch, commit_oid)) = source.definition() {
        names.extend([("workflowBranch", branch), ("workflowCommit", commit_oid)]);
    }
    for (name, value) in names {
        arguments.extend(["--source-revision".into(), format!("{name}={value}").into()]);
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
    let mut file = crate::enrollment::create_new_private_file(path)?;
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
