//! `um workflow definition cloud evaluation list` and `show` (REQ-CLOUDDEF-15): a Cloud
//! definition's retained start evaluations (REQ-TRIGGER-132), listed with the exact `state`
//! and `runId` filters and read one at a time, each with its stored reason.

use std::io::{self, Write};

use clap::{Args, Subcommand};
use serde::Serialize;
use serde_json::Value;

use super::super::super::super::{self as cli, OrganizationArg, PaginationArgs, ProjectArg};
use super::super::report::display;
use super::report::{self, Scope};
use super::{Options, with_api};
use um_api::{WorkflowDefinitionEvaluation, WorkflowEvaluationFilter};
use um_human_auth::Deployment;

pub(super) const ABOUT: &str = "List and show a Cloud workflow definition's start evaluations";
const COMMAND_PATH: &[&str] = &["workflow", "definition", "cloud", "evaluation"];

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(subcommand)]
    command: Option<Leaf>,
}

#[derive(Debug, Subcommand)]
enum Leaf {
    #[command(about = "List a Cloud workflow definition's start evaluations, newest first")]
    List(List),
    #[command(about = "Show one start evaluation of a Cloud workflow definition")]
    Show(Show),
}

#[derive(Debug, Args)]
struct List {
    #[arg(value_name = OrganizationArg::VALUE_NAME, help = OrganizationArg::HELP)]
    organization: OrganizationArg,
    #[arg(value_name = "DEFINITION", help = "Cloud workflow definition ID")]
    definition: String,
    #[arg(long, value_name = ProjectArg::VALUE_NAME, help = ProjectArg::HELP)]
    project_id: ProjectArg,
    #[arg(
        long,
        value_parser = [
            "pending",
            "processing",
            "retry_waiting",
            "run_created",
            "not_matched",
            "skipped",
            "skipped_active",
            "stopped",
            "failed",
        ],
        help = "List only evaluations in this state"
    )]
    state: Option<String>,
    #[arg(
        long,
        value_name = "RUN_ID",
        help = "List only the evaluation that created this run"
    )]
    run_id: Option<String>,
    #[command(flatten)]
    page: PaginationArgs<100>,
    #[command(flatten)]
    options: Options,
}

#[derive(Debug, Args)]
struct Show {
    #[arg(value_name = OrganizationArg::VALUE_NAME, help = OrganizationArg::HELP)]
    organization: OrganizationArg,
    #[arg(value_name = "DEFINITION", help = "Cloud workflow definition ID")]
    definition: String,
    #[arg(value_name = "EVALUATION", help = "Start evaluation ID")]
    evaluation: String,
    #[arg(long, value_name = ProjectArg::VALUE_NAME, help = ProjectArg::HELP)]
    project_id: ProjectArg,
    #[command(flatten)]
    options: Options,
}

impl Command {
    pub(super) fn execute(self) -> cli::CommandResult {
        cli::execute_deployment_command(
            self.command,
            COMMAND_PATH,
            "configure Useful Machinery workflow definitions",
            |leaf, deployment| match leaf {
                Leaf::List(list) => list.run(deployment),
                Leaf::Show(show) => show.run(deployment),
            },
        )
    }
}

impl List {
    fn run(self, deployment: &Deployment) -> cli::CommandResult {
        let filter = WorkflowEvaluationFilter {
            state: self.state.as_deref(),
            run_id: self.run_id.as_deref(),
        };
        let page = with_api(deployment, &self.options, |api| {
            api.evaluations(
                &self.organization,
                &self.project_id,
                &self.definition,
                filter,
                self.page.limit.map(i32::from),
                self.page.cursor.as_deref(),
            )
        })?;
        let scope = Scope {
            leaf: "evaluation list",
            organization: &self.organization,
            project: &self.project_id,
            json: self.options.json,
        };
        report::finish(&scope, page.as_ref(), |(evaluations, cursor)| {
            if scope.json {
                return scope.write(
                    "listed",
                    ListedBody {
                        definition_id: &self.definition,
                        evaluations,
                        next_cursor: cursor.as_deref(),
                    },
                );
            }
            let mut stdout = io::stdout().lock();
            if evaluations.is_empty() {
                writeln!(stdout, "No start evaluations.")?;
            }
            for evaluation in evaluations {
                writeln!(stdout, "{}", summary(evaluation))?;
            }
            if let Some(cursor) = cursor {
                writeln!(stdout, "next cursor: {}", display(cursor))?;
            }
            Ok(())
        })
    }
}

impl Show {
    fn run(self, deployment: &Deployment) -> cli::CommandResult {
        let read = with_api(deployment, &self.options, |api| {
            api.evaluation(
                &self.organization,
                &self.project_id,
                &self.definition,
                &self.evaluation,
            )
        })?;
        let scope = Scope {
            leaf: "evaluation show",
            organization: &self.organization,
            project: &self.project_id,
            json: self.options.json,
        };
        report::finish(&scope, read.as_ref(), |evaluation| {
            if scope.json {
                return scope.write(
                    "shown",
                    ShownBody {
                        definition_id: &self.definition,
                        evaluation,
                    },
                );
            }
            write_evaluation(&mut io::stdout().lock(), evaluation)?;
            Ok(())
        })
    }
}

/// One evaluation on a line: id, state with its reason, trigger, revision, event and run.
fn summary(evaluation: &WorkflowDefinitionEvaluation) -> String {
    format!(
        "{} · {} · reason {} · trigger {} · revision {} · {} {} · accepted {} · run {}",
        display(&evaluation.id),
        report::wire(evaluation.state),
        or_dash(evaluation.reason.as_deref()),
        display(&evaluation.trigger_name),
        evaluation.revision,
        report::wire(evaluation.event_name),
        issue(evaluation),
        display(&evaluation.accepted_at),
        or_dash(evaluation.run_id.as_deref()),
    )
}

fn write_evaluation(
    out: &mut impl Write,
    evaluation: &WorkflowDefinitionEvaluation,
) -> io::Result<()> {
    writeln!(out, "evaluation: {}", display(&evaluation.id))?;
    writeln!(out, "definition: {}", display(&evaluation.definition_id))?;
    writeln!(out, "revision: {}", evaluation.revision)?;
    writeln!(out, "trigger: {}", display(&evaluation.trigger_name))?;
    writeln!(
        out,
        "event: {} {}",
        report::wire(evaluation.event_name),
        issue(evaluation)
    )?;
    writeln!(
        out,
        "connection: {}",
        or_dash(evaluation.connection_id.as_deref())
    )?;
    writeln!(
        out,
        "execution principal: {}",
        display(&evaluation.execution_principal_id)
    )?;
    writeln!(out, "state: {}", report::wire(evaluation.state))?;
    writeln!(out, "reason: {}", or_dash(evaluation.reason.as_deref()))?;
    writeln!(out, "run: {}", or_dash(evaluation.run_id.as_deref()))?;
    writeln!(
        out,
        "blocking run: {}",
        or_dash(evaluation.blocking_run_id.as_deref())
    )?;
    writeln!(
        out,
        "cycle: {} · attempt {} · started {} · deadline {}",
        evaluation.cycle_number,
        evaluation.attempt_number,
        display(&evaluation.cycle_started_at),
        display(&evaluation.cycle_deadline)
    )?;
    writeln!(
        out,
        "next attempt: {}",
        or_dash(evaluation.next_attempt_at.as_deref())
    )?;
    writeln!(out, "accepted: {}", display(&evaluation.accepted_at))?;
    writeln!(
        out,
        "settled: {}",
        or_dash(evaluation.settled_at.as_deref())
    )?;
    writeln!(out, "expires: {}", display(&evaluation.expires_at))
}

/// The issue identifier the evaluation's event names, `-` for a schedule's event.
fn issue(evaluation: &WorkflowDefinitionEvaluation) -> String {
    let event = serde_json::to_value(&evaluation.event).unwrap_or(Value::Null);
    or_dash(event["issue"]["identifier"].as_str())
}

fn or_dash(value: Option<&str>) -> String {
    value.map_or_else(|| "-".to_owned(), display)
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ListedBody<'a> {
    definition_id: &'a str,
    evaluations: &'a [WorkflowDefinitionEvaluation],
    #[serde(skip_serializing_if = "Option::is_none")]
    next_cursor: Option<&'a str>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ShownBody<'a> {
    definition_id: &'a str,
    evaluation: &'a WorkflowDefinitionEvaluation,
}
