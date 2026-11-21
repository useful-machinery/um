//! Reading the step states of a tree run and of every child run under it
//! (REQ-RUNNERENROLL-88). Each run directory's `state.json` holds `attempts[]`, each with
//! `attemptNumber` and `progress.steps[]` of `{id, state}`; a `workflow` step's child runs
//! are complete run directories under `children/<step>/<NNNNNN>/`, or
//! `children/<step>/<n>/<NNNNNN>/` for element `n` of a fan-out (REQ-LOCALRUN-177), and
//! the engine's `child_run_directories` finds them, so the runner and the engine tell a
//! child run from an element by the same `run.json`. The records are read as untyped
//! JSON, tolerant of members this reader does not know, and a record, step or child
//! number outside the wire's grammar is skipped rather than reported. The steps running
//! now, which an overview lists (REQ-RUNNERENROLL-134), are reduced from one scan's
//! observations by `running_now`.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use serde_json::Value;
use useful_runner_protocol::{TreeStepObservation, TreeStepRunElement, TreeStepState};
use useful_support::is_identifier;

use crate::control_protocol::{
    LiveStepState, MAXIMUM_RUNNING_STEPS, OpenWord, OverviewRunElement, OverviewRunningStep,
};

/// The deepest child run reported: the wire's `run` holds at most 16 levels.
const MAXIMUM_DEPTH: usize = 16;
/// The wire's bound on an attempt, a fan-out element and a child ordinal.
const MAXIMUM_NUMBER: u64 = 2_147_483_647;

const STATE_FILE: &str = "state.json";

const STATES: [TreeStepState; 12] = [
    TreeStepState::Pending,
    TreeStepState::Starting,
    TreeStepState::Running,
    TreeStepState::CapturingOutputs,
    TreeStepState::Cancelling,
    TreeStepState::Succeeded,
    TreeStepState::Inherited,
    TreeStepState::Failed,
    TreeStepState::Blocked,
    TreeStepState::Skipped,
    TreeStepState::NotRun,
    TreeStepState::Cancelled,
];

/// An observation's identity on the wire: its run path (step, fan-out element and
/// ordinal at each level), its run's engine attempt and its step.
pub(super) type StepKey = (Vec<(String, Option<u64>, u64)>, u64, String);

pub(super) fn key(observation: &TreeStepObservation) -> StepKey {
    (
        observation
            .run
            .iter()
            .map(|element| (element.step.clone(), element.item, element.ordinal))
            .collect(),
        observation.run_attempt,
        observation.step.clone(),
    )
}

/// One run directory's steps as its `state.json` last read: `(attempt, step, state)`.
type RunSteps = Vec<(u64, String, TreeStepState)>;

/// A parsed `state.json`, kept while its size and modification time are unchanged.
struct Parsed {
    length: u64,
    modified: SystemTime,
    steps: RunSteps,
}

/// Scans a run directory tree, re-parsing a run's `state.json` only when its size or
/// modification time changed since the last scan.
#[derive(Default)]
pub(super) struct StepReader {
    parsed: HashMap<PathBuf, Parsed>,
}

impl StepReader {
    /// Every step observation of the run at `directory` and of its child runs, the run's
    /// own first and each child after its parent, in directory order.
    pub(super) fn scan(&mut self, directory: &Path) -> Vec<TreeStepObservation> {
        let mut previous = std::mem::take(&mut self.parsed);
        let mut observations = Vec::new();
        self.walk(&mut previous, directory, &[], &mut observations);
        observations
    }

    fn walk(
        &mut self,
        previous: &mut HashMap<PathBuf, Parsed>,
        directory: &Path,
        run: &[TreeStepRunElement],
        observations: &mut Vec<TreeStepObservation>,
    ) {
        observations.extend(self.read(previous, directory).into_iter().map(
            |(run_attempt, step, state)| TreeStepObservation {
                run: run.to_vec(),
                run_attempt,
                step,
                state,
            },
        ));
        if run.len() == MAXIMUM_DEPTH {
            return;
        }
        // A run whose `children/` the engine cannot list reports no child run this scan.
        let children = useful_execution::child_run_directories(directory).unwrap_or_default();
        for child in children.into_iter().filter(|child| {
            child.ordinal <= MAXIMUM_NUMBER && child.item.is_none_or(|item| item <= MAXIMUM_NUMBER)
        }) {
            let mut path = run.to_vec();
            path.push(TreeStepRunElement {
                step: child.step,
                item: child.item,
                ordinal: child.ordinal,
            });
            self.walk(previous, &child.directory, &path, observations);
        }
    }

    /// The steps of `directory`'s `state.json`; none when it is absent or not JSON. It is
    /// read as the engine reads it (`useful_execution::read_durable_record`): a step of the
    /// run can replace the file, so a symbolic link is not followed, a FIFO is not waited
    /// on and a record over the engine's bound is not read. One that could not be read is
    /// not kept, so the next scan reads it again.
    fn read(&mut self, previous: &mut HashMap<PathBuf, Parsed>, directory: &Path) -> RunSteps {
        let path = directory.join(STATE_FILE);
        let Some((length, modified)) = fs::symlink_metadata(&path)
            .ok()
            .filter(fs::Metadata::is_file)
            .and_then(|metadata| Some((metadata.len(), metadata.modified().ok()?)))
        else {
            return Vec::new();
        };
        let steps = match previous.remove(directory) {
            Some(parsed) if parsed.length == length && parsed.modified == modified => parsed.steps,
            _ => match useful_execution::read_durable_record(directory, STATE_FILE) {
                Ok(bytes) => parse(&bytes),
                Err(_) => return Vec::new(),
            },
        };
        self.parsed.insert(
            directory.to_owned(),
            Parsed {
                length,
                modified,
                steps: steps.clone(),
            },
        );
        steps
    }
}

/// The steps of each attempt in `bytes`, each `(attempt, step)` once.
fn parse(bytes: &[u8]) -> RunSteps {
    let Ok(state) = serde_json::from_slice::<Value>(bytes) else {
        return Vec::new();
    };
    let mut seen = HashSet::new();
    let mut steps = Vec::new();
    for attempt in state["attempts"].as_array().into_iter().flatten() {
        let Some(number) = attempt["attemptNumber"]
            .as_u64()
            .filter(|number| (1..=MAXIMUM_NUMBER).contains(number))
        else {
            continue;
        };
        for step in attempt["progress"]["steps"]
            .as_array()
            .into_iter()
            .flatten()
        {
            let Some(id) = step["id"].as_str().filter(|id| is_identifier(id)) else {
                continue;
            };
            let Some(state) = step["state"]
                .as_str()
                .and_then(|name| STATES.into_iter().find(|state| state.as_str() == name))
            else {
                continue;
            };
            if seen.insert((number, id)) {
                steps.push((number, id.to_owned(), state));
            }
        }
    }
    steps
}

/// One level of a run path: the `workflow` step, its fan-out element and the child ordinal.
type Level<'a> = (&'a str, Option<u64>, u64);

fn levels(run: &[TreeStepRunElement]) -> Vec<Level<'_>> {
    run.iter()
        .map(|element| (element.step.as_str(), element.item, element.ordinal))
        .collect()
}

/// The steps running now among `observations`, as `StepReader::scan` yields them
/// (REQ-RUNNERENROLL-134): the steps in a live state of each run's newest attempt, in runs
/// whose every level is the newest child run of its `workflow` step and fan-out element, for
/// an attempt or a child run that a killed process left behind keeps its steps live. A
/// `workflow` step is left out when a step listed in a child run beneath it names it in its
/// path, so the leaf is kept. At most 16 in scan order, with the number left out.
pub(super) fn running_now(observations: &[TreeStepObservation]) -> (Vec<OverviewRunningStep>, u64) {
    let mut newest_attempt = HashMap::<Vec<Level<'_>>, u64>::new();
    let mut newest_child = HashMap::<(Vec<Level<'_>>, &str, Option<u64>), u64>::new();
    for observation in observations {
        let attempt = newest_attempt.entry(levels(&observation.run)).or_default();
        *attempt = (*attempt).max(observation.run_attempt);
        for (depth, element) in observation.run.iter().enumerate() {
            let parent = levels(&observation.run[..depth]);
            let ordinal = newest_child
                .entry((parent, element.step.as_str(), element.item))
                .or_default();
            *ordinal = (*ordinal).max(element.ordinal);
        }
    }
    let current = |observation: &TreeStepObservation| {
        newest_attempt.get(&levels(&observation.run)) == Some(&observation.run_attempt)
            && observation.run.iter().enumerate().all(|(depth, element)| {
                let parent = levels(&observation.run[..depth]);
                newest_child.get(&(parent, element.step.as_str(), element.item))
                    == Some(&element.ordinal)
            })
    };
    let live = observations
        .iter()
        .filter_map(|observation| Some((observation, live_state(observation.state)?)))
        .filter(|(observation, _)| current(observation))
        .collect::<Vec<_>>();
    // Each `workflow` step, by its run's path, that a live step beneath it names.
    let occupied =
        live.iter()
            .flat_map(|(observation, _)| {
                observation.run.iter().enumerate().map(|(depth, element)| {
                    (levels(&observation.run[..depth]), element.step.as_str())
                })
            })
            .collect::<HashSet<_>>();
    let mut listed = live.iter().filter(|(observation, _)| {
        !occupied.contains(&(levels(&observation.run), observation.step.as_str()))
    });
    let steps = listed
        .by_ref()
        .take(MAXIMUM_RUNNING_STEPS)
        .map(|(observation, state)| OverviewRunningStep {
            run: observation
                .run
                .iter()
                .map(|element| OverviewRunElement {
                    step: element.step.clone(),
                    item: element.item,
                    ordinal: element.ordinal,
                })
                .collect(),
            run_attempt: observation.run_attempt,
            step: observation.step.clone(),
            state: OpenWord::Known(*state),
        })
        .collect();
    (steps, u64::try_from(listed.count()).unwrap_or(u64::MAX))
}

/// The live state a step in `state` is in, or `None` for one that is not running now.
fn live_state(state: TreeStepState) -> Option<LiveStepState> {
    match state {
        TreeStepState::Starting => Some(LiveStepState::Starting),
        TreeStepState::Running => Some(LiveStepState::Running),
        TreeStepState::CapturingOutputs => Some(LiveStepState::CapturingOutputs),
        TreeStepState::Cancelling => Some(LiveStepState::Cancelling),
        TreeStepState::Pending
        | TreeStepState::Succeeded
        | TreeStepState::Inherited
        | TreeStepState::Failed
        | TreeStepState::Blocked
        | TreeStepState::Skipped
        | TreeStepState::NotRun
        | TreeStepState::Cancelled => None,
    }
}
