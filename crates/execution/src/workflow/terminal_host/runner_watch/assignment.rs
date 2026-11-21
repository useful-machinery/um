//! An opened assignment (REQ-RUNNERENROLL-146): the run levels the keys walked from the root,
//! each with its selected step, and what the runner last answered, which an answer replaces
//! only for the query it answered.

use super::log_document::{LogDocument, LogPlace};
use super::run_steps::{phase_boundary, step_state_kind};
use super::*;

/// The kind word of a step that starts child runs, which has no output of its own.
pub(super) const WORKFLOW_KIND: &str = "workflow";
/// The state word of a child run that has ended.
const EXITED_STATE: &str = "exited";

/// The keys' place in an opened assignment: its ID, one level per run from the root to the
/// open one and, while the log pane is open full-screen, its viewport.
pub(super) struct OpenAssignment {
    pub(super) assignment_id: String,
    pub(super) levels: Vec<RunLevel>,
    pub(super) full_log: Option<OutputViewport<LogPlace>>,
}

/// One run of the way down: the path element that names it, none for the root, and its
/// selected step's ID, unset until the run is first drawn.
pub(super) struct RunLevel {
    pub(super) element: Option<RunnerWatchRunElement>,
    pub(super) selected: Option<String>,
}

impl OpenAssignment {
    pub(super) fn new(assignment_id: String) -> Self {
        Self {
            assignment_id,
            levels: vec![RunLevel {
                element: None,
                selected: None,
            }],
            full_log: None,
        }
    }

    /// The open run's path from the root.
    pub(super) fn path(&self) -> Vec<RunnerWatchRunElement> {
        self.levels
            .iter()
            .filter_map(|level| level.element.clone())
            .collect()
    }

    fn selected(&self) -> Option<&str> {
        self.levels.last()?.selected.as_deref()
    }

    /// The selected step's row in `run`: the step held by ID, or else the step a run opens
    /// on.
    pub(super) fn selected_index(&self, run: &RunnerWatchRun) -> usize {
        self.selected()
            .and_then(|id| position(run, id))
            .unwrap_or_else(|| opening_index(run))
    }

    /// The step whose output the log pane wants: the selected step unless it is a
    /// `workflow` step, beside which the pane shows the run log.
    pub(super) fn wanted_step(&self, shown: &AssignmentScreen) -> Option<String> {
        let id = self.selected()?;
        let run = &shown.run_at(&self.path())?.run;
        let step = run.steps.iter().find(|step| step.id == id)?;
        (step.kind != WORKFLOW_KIND).then(|| id.to_owned())
    }

    /// The read the surface wants now, or none once the runner holds the assignment no
    /// longer.
    pub(super) fn query(&self, shown: &AssignmentScreen) -> Option<RunnerWatchAssignmentQuery> {
        (!shown.not_held).then(|| RunnerWatchAssignmentQuery {
            assignment_id: self.assignment_id.clone(),
            path: self.path(),
            step: self.wanted_step(shown),
        })
    }

    /// The document the log pane shows now, when an answer gave it one.
    pub(super) fn shown_document<'a>(
        &self,
        shown: &'a AssignmentScreen,
    ) -> Option<&'a LogDocument> {
        shown
            .log_for(&self.path(), self.wanted_step(shown).as_deref())?
            .document
            .as_ref()
    }

    /// Acts on a key of the assignment surface. Answers `false` only for Esc at the root,
    /// which leaves the assignment.
    pub(super) fn handle_key(
        &mut self,
        event: TerminalInputEvent,
        shown: &mut AssignmentScreen,
    ) -> bool {
        if self.full_log.is_some() {
            self.handle_full_log_key(event, shown);
            return true;
        }
        match event {
            TerminalInputEvent::Escape => {
                if self.levels.len() <= 1 {
                    return false;
                }
                self.levels.pop();
                let path = self.path();
                shown.runs.retain(|drawn| path.starts_with(&drawn.path));
            }
            TerminalInputEvent::Enter => {
                if !self.open_child_run(shown) && self.selected_step(shown).is_some() {
                    self.full_log = Some(OutputViewport::following());
                }
            }
            TerminalInputEvent::Character('o') => {
                self.full_log = Some(OutputViewport::following());
            }
            TerminalInputEvent::Up | TerminalInputEvent::Down => {
                let Some(drawn) = shown.run_at(&self.path()) else {
                    return true;
                };
                let steps = &drawn.run.steps;
                let current = self.selected_index(&drawn.run);
                let next = if event == TerminalInputEvent::Up {
                    current.saturating_sub(1)
                } else {
                    current.saturating_add(1).min(steps.len().saturating_sub(1))
                };
                if let (Some(step), Some(level)) = (steps.get(next), self.levels.last_mut()) {
                    level.selected = Some(step.id.clone());
                }
            }
            _ => {}
        }
        true
    }

    /// The full-screen log's keys (REQ-LOCALRUN-44 to REQ-LOCALRUN-46): the movement keys,
    /// any of which pauses, `F` to follow again, Left and Right to pan, and Esc to leave,
    /// which forgets the position (REQ-LOCALRUN-49).
    fn handle_full_log_key(&mut self, event: TerminalInputEvent, shown: &AssignmentScreen) {
        if event == TerminalInputEvent::Escape {
            self.full_log = None;
            return;
        }
        let document = self.shown_document(shown);
        let Some(viewport) = &mut self.full_log else {
            return;
        };
        if let Some(navigation) = vertical_navigation(event) {
            viewport.navigate(&document, navigation);
            return;
        }
        match event {
            TerminalInputEvent::Follow => viewport.follow(),
            TerminalInputEvent::PanLeft => viewport.pan_left(),
            TerminalInputEvent::PanRight => {
                viewport.pan_right(document.map_or(0, |document| document.width));
            }
            _ => {}
        }
    }

    pub(super) fn selected_step<'a>(
        &self,
        shown: &'a AssignmentScreen,
    ) -> Option<&'a RunnerWatchRunStep> {
        let drawn = shown.run_at(&self.path())?;
        drawn.run.steps.get(self.selected_index(&drawn.run))
    }

    /// Opens the selected `workflow` step's newest child run: the last listed that has not
    /// exited, or else the last listed. Answers whether it opened one.
    fn open_child_run(&mut self, shown: &AssignmentScreen) -> bool {
        let Some(step) = self.selected_step(shown) else {
            return false;
        };
        if step.kind != WORKFLOW_KIND {
            return false;
        }
        let Some(child) = step
            .child_runs
            .iter()
            .rev()
            .find(|child| child.state != EXITED_STATE)
            .or_else(|| step.child_runs.last())
        else {
            return false;
        };
        let element = RunnerWatchRunElement {
            step: step.id.clone(),
            item: child.item,
            ordinal: child.ordinal,
        };
        if let Some(level) = self.levels.last_mut() {
            level.selected = Some(step.id.clone());
        }
        self.levels.push(RunLevel {
            element: Some(element),
            selected: None,
        });
        true
    }
}

/// What the screen shows of the open assignment: its entry, each run of the way down as
/// last answered, the log the pane was last given and whether the runner still holds it.
pub(super) struct AssignmentScreen {
    pub(super) entry: RunnerWatchAssignment,
    pub(super) runs: Vec<DrawnRun>,
    pub(super) log: Option<ShownLog>,
    pub(super) not_held: bool,
}

/// A run as last answered, with its graph built once per answer.
pub(super) struct DrawnRun {
    /// The path the read named, by which the run is kept.
    pub(super) path: Vec<RunnerWatchRunElement>,
    pub(super) run: RunnerWatchRun,
    pub(super) graph: DagLayout,
    pub(super) phase_boundary: Option<StepPhaseBoundary>,
}

/// The log an answer gave the pane, with the run and the step its read named.
pub(super) struct ShownLog {
    pub(super) path: Vec<RunnerWatchRunElement>,
    pub(super) step: Option<String>,
    /// None when the answer had no log.
    pub(super) document: Option<LogDocument>,
}

impl AssignmentScreen {
    /// An assignment just opened, drawn from the overview's entry until the runner answers.
    pub(super) fn new(entry: RunnerWatchAssignment) -> Self {
        Self {
            entry,
            runs: Vec::new(),
            log: None,
            not_held: false,
        }
    }

    pub(super) fn run_at(&self, path: &[RunnerWatchRunElement]) -> Option<&DrawnRun> {
        self.runs.iter().find(|drawn| drawn.path == path)
    }

    /// The log the pane shows for `wanted` in the run at `path`, when an answer for it came.
    pub(super) fn log_for(
        &self,
        path: &[RunnerWatchRunElement],
        wanted: Option<&str>,
    ) -> Option<&ShownLog> {
        self.log.as_ref().filter(|shown| {
            shown.step.as_deref() == wanted && (wanted.is_none() || shown.path == path)
        })
    }

    /// Takes an answer against the query it answered. One for another assignment or another
    /// run is dropped. One for the open run replaces the entry, replaces the run when it
    /// has one and holds the selection by step ID, and gives the pane its log only when it
    /// asked about the step the pane wants now.
    pub(super) fn take(
        &mut self,
        open: &mut OpenAssignment,
        query: RunnerWatchAssignmentQuery,
        answer: RunnerWatchAssignmentAnswer,
    ) {
        if query.assignment_id != open.assignment_id {
            return;
        }
        let held = match answer {
            RunnerWatchAssignmentAnswer::NotHeld => {
                self.not_held = true;
                return;
            }
            RunnerWatchAssignmentAnswer::Held(held) => *held,
        };
        if query.path != open.path() {
            return;
        }
        let RunnerWatchHeldAssignment { entry, run, log } = held;
        self.entry = entry;
        if let Some(run) = run {
            let previous = self
                .run_at(&query.path)
                .and_then(|drawn| position(&drawn.run, open.selected()?));
            if let Some(level) = open.levels.last_mut() {
                level.selected = reselect(&run, level.selected.take(), previous);
            }
            let drawn = DrawnRun {
                graph: DagLayout::for_steps(&run.steps),
                phase_boundary: phase_boundary(&run.steps),
                path: query.path.clone(),
                run,
            };
            match self.runs.iter_mut().find(|kept| kept.path == drawn.path) {
                Some(kept) => *kept = drawn,
                None => self.runs.push(drawn),
            }
        }
        // Never another step's output under the step asked about.
        let names_another_step = log.as_ref().is_some_and(|log| {
            matches!(&log.source,
                RunnerWatchLogSource::Step { step, .. }
                | RunnerWatchLogSource::StepRecords { step: Some(step), .. }
                if query.step.as_deref() != Some(step.as_str()))
        });
        if query.step == open.wanted_step(self) && !names_another_step {
            self.log = Some(ShownLog {
                path: query.path,
                step: query.step,
                document: log.as_ref().map(LogDocument::new),
            });
        }
    }
}

fn position(run: &RunnerWatchRun, id: &str) -> Option<usize> {
    run.steps.iter().position(|step| step.id == id)
}

/// A run opens on its first step in flight, or else its first step.
fn opening_index(run: &RunnerWatchRun) -> usize {
    run.steps
        .iter()
        .position(|step| step_state_kind(&step.state).is_some_and(step_state_is_active))
        .unwrap_or(0)
}

/// The selection in a new answer's run: the same step, or the nearest row when it went, or
/// the step the run opens on when none was selected.
fn reselect(
    run: &RunnerWatchRun,
    selected: Option<String>,
    previous: Option<usize>,
) -> Option<String> {
    if run.steps.is_empty() {
        return selected;
    }
    let index = match &selected {
        Some(id) => position(run, id)
            .unwrap_or_else(|| previous.unwrap_or(0).min(run.steps.len().saturating_sub(1))),
        None => opening_index(run),
    };
    run.steps
        .get(index)
        .map(|step| step.id.clone())
        .or(selected)
}
