//! `um workflow run answer` (REQ-LOCALRUN-182); with `--cloud ORGANIZATION`, the answer is
//! carried to the Cloud run's runner instead (REQ-LOCALRUN-210).

use clap::Args;
use serde_json::{Map, Value};
use um_execution::{CONTROL_ANSWER, WaitKind, read_controls};

use super::control::{Acted, ControlFailure, ControlTarget, Refusal};

pub(super) const ABOUT: &str = "Answer a question a waiting run asks";
const COMMAND: &str = "um workflow run answer";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(flatten)]
    run: super::ExistingLocalRun,

    #[arg(
        long,
        value_name = "ID",
        help = "Question to answer, as the wait lists it"
    )]
    question: String,

    #[arg(long, value_name = "TEXT", help = "The answer")]
    text: String,

    #[command(flatten)]
    output: super::control::JsonOutput,

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
                self.output.json,
            )?
            .control(super::cloud::Control::Answer {
                question: self.question,
                text: self.text,
            });
        }
        super::control::execute(COMMAND, &self.run, self.output.json, |target| {
            self.answer(target)
        })
    }

    fn answer(&self, target: &ControlTarget) -> Result<Acted, ControlFailure> {
        let wait = target.wait_to_resolve(WaitKind::Answer)?;
        let questions = question_ids(&wait.payload);
        if let Some(questions) = &questions
            && !questions.contains(&self.question)
        {
            return Err(Refusal::new(
                "question_unknown",
                "The wait asks no question with this id.",
            )
            .into());
        }
        let wait_id = wait.id();
        // The answers recorded for this wait so far, the latest text per question.
        let mut answers = Map::new();
        for control in read_controls(&target.run_directory)?
            .iter()
            .filter(|control| {
                control.event == CONTROL_ANSWER && control.wait.as_deref() == Some(&wait_id)
            })
        {
            if let Some(payload) = &control.payload
                && let (Some(Value::String(question)), Some(text)) =
                    (payload.get("question"), payload.get("text"))
            {
                answers.insert(question.clone(), text.clone());
            }
        }
        answers.insert(self.question.clone(), Value::String(self.text.clone()));
        let complete = questions.is_none_or(|questions| {
            questions
                .iter()
                .all(|question| answers.contains_key(question))
        });
        let payload = Map::from_iter([
            ("question".to_owned(), Value::String(self.question.clone())),
            ("text".to_owned(), Value::String(self.text.clone())),
            ("answers".to_owned(), Value::Object(answers)),
            ("complete".to_owned(), Value::Bool(complete)),
        ]);
        let control = target.record(CONTROL_ANSWER, Some(wait_id), Some(payload))?;
        Ok(Acted::recorded(control, Some(wait.clone())))
    }
}

/// The ids of the questions a wait's payload lists (REQ-LOCALRUN-182): each element of a
/// `questions` array by its string `id`, else its one-based position; `None` for a payload
/// without such an array, which asks exactly one question any id answers.
fn question_ids(payload: &Map<String, Value>) -> Option<Vec<String>> {
    let questions = payload.get("questions")?.as_array()?;
    Some(
        questions
            .iter()
            .enumerate()
            .map(|(index, question)| {
                question
                    .get("id")
                    .and_then(Value::as_str)
                    .map_or_else(|| (index + 1).to_string(), str::to_owned)
            })
            .collect(),
    )
}
