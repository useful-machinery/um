use std::path::PathBuf;

use anyhow::{Context, anyhow};
use clap::Args;
use um_api::{EvaluationFilters, HttpTransportPolicy, LinearApi, TriggerAction};
use um_human_auth::Deployment;

use super::super::super::{self as cli, OrganizationArg, ProjectArg};
use super::super::resource::{self, Run, Vocabulary};
use super::{Options, config};

const LINEAR: Vocabulary = Vocabulary {
    path: &["project", "trigger"],
    purpose: "manage Linear triggers",
    noun: "Linear trigger",
    nouns: &[("evaluation", "Linear evaluation")],
    mutation: "Linear trigger mutation",
    networking: "prepare Linear networking",
};

fn linear(api_url: &str, token: &str, policy: HttpTransportPolicy) -> anyhow::Result<LinearApi> {
    LinearApi::new(api_url, token, policy)
        .map_err(|error| anyhow!(error))
        .context("prepare Linear API")
}

pub(super) fn execute<T: Run<Management>>(cmd: T, action: Management) -> cli::CommandResult {
    resource::execute(&LINEAR, cmd, action)
}

#[derive(Clone, Copy)]
pub(super) enum Management {
    Create,
    List,
    Show,
    Update,
    Enable,
    Disable,
    Delete,
    EvaluationList,
    EvaluationShow,
}

#[derive(Debug, Args)]
pub(super) struct Project {
    #[arg(value_name = OrganizationArg::VALUE_NAME, help = OrganizationArg::HELP)]
    organization: OrganizationArg,
    #[arg(value_name = ProjectArg::VALUE_NAME, help = ProjectArg::HELP)]
    project: ProjectArg,
}
#[derive(Debug, Args)]
pub(super) struct Create {
    #[command(flatten)]
    project: Project,
    #[arg(
        long,
        value_name = "PATH",
        help = "Read complete trigger configuration from a JSON file or - for stdin"
    )]
    config_file: PathBuf,
    #[command(flatten)]
    options: Options,
}
#[derive(Debug, Args)]
pub(super) struct List {
    #[command(flatten)]
    project: Project,
    #[command(flatten)]
    page: cli::PaginationArgs<100>,
    #[command(flatten)]
    options: Options,
}
#[derive(Debug, Args)]
pub(super) struct Target {
    #[command(flatten)]
    project: Project,
    #[arg(value_name = "TRIGGER", help = "Linear trigger ID")]
    trigger: String,
    #[command(flatten)]
    options: Options,
}
pub(super) type Delete = resource::Delete<Target>;
pub(super) type Update = resource::Update<Target>;
#[derive(Debug, Args)]
pub(super) struct EvaluationList {
    #[command(flatten)]
    target: Target,
    #[command(flatten)]
    page: cli::PaginationArgs<100>,
    #[arg(long, value_enum, help = "Filter evaluations by exact state")]
    state: Option<EvaluationState>,
    #[arg(long, value_name = "RUN_ID", help = "Filter by the resulting run ID")]
    run_id: Option<String>,
}
#[derive(Debug, Clone, Copy, clap::ValueEnum)]
#[value(rename_all = "snake_case")]
enum EvaluationState {
    Pending,
    Processing,
    RetryWaiting,
    RunCreated,
    NotMatched,
    SkippedActive,
    Stopped,
    Failed,
}
impl EvaluationState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Processing => "processing",
            Self::RetryWaiting => "retry_waiting",
            Self::RunCreated => "run_created",
            Self::NotMatched => "not_matched",
            Self::SkippedActive => "skipped_active",
            Self::Stopped => "stopped",
            Self::Failed => "failed",
        }
    }
}
#[derive(Debug, Args)]
pub(super) struct EvaluationTarget {
    #[command(flatten)]
    target: Target,
    #[arg(value_name = "EVALUATION", help = "Linear evaluation ID")]
    evaluation: String,
}

impl Run<Management> for Create {
    fn run(self, d: Deployment, _: Management) -> cli::CommandResult {
        let config = config::read(
            &self.config_file,
            None,
            self.options.authentication.uses_stdin(),
        )
        .and_then(config::create);
        let config = match config {
            Ok(config) => config,
            Err(error) => return resource::configuration_error(&LINEAR, error, self.options.json),
        };
        let org = self.project.organization.to_string();
        let project = self.project.project.to_string();
        resource::mutate(
            &LINEAR,
            d,
            org.clone(),
            self.options,
            "created",
            "trigger",
            linear,
            move |api, key| api.create_trigger(&org, &project, key, config.clone()),
        )
    }
}
impl Run<Management> for Update {
    fn run(self, d: Deployment, _: Management) -> cli::CommandResult {
        let config = config::read(
            &self.config_file,
            Some(self.expected_version),
            self.target.options.authentication.uses_stdin(),
        )
        .and_then(config::update);
        let config = match config {
            Ok(config) => config,
            Err(error) => {
                return resource::configuration_error(&LINEAR, error, self.target.options.json);
            }
        };
        let org = self.target.project.organization.to_string();
        let project = self.target.project.project.to_string();
        let id = self.target.trigger;
        resource::mutate(
            &LINEAR,
            d,
            org.clone(),
            self.target.options,
            "updated",
            "trigger",
            linear,
            move |api, key| api.update_trigger(&org, &project, &id, key, config.clone()),
        )
    }
}
impl Run<Management> for Target {
    fn run(self, d: Deployment, action: Management) -> cli::CommandResult {
        let org = self.project.organization.to_string();
        let project = self.project.project.to_string();
        let id = self.trigger;
        if matches!(action, Management::Show) {
            return resource::result(
                &LINEAR,
                &d,
                &org,
                "shown",
                "trigger",
                resource::with_api(&LINEAR, &d, &self.options, linear, |api| {
                    api.trigger(&org, &project, &id)
                })?,
                |_| None,
                self.options.json,
            );
        }
        let (kind, label) = match action {
            Management::Enable => (TriggerAction::Enable, "enabled"),
            Management::Disable => (TriggerAction::Disable, "disabled"),
            Management::Delete => (TriggerAction::Delete, "deleted"),
            _ => return Err(anyhow!("invalid Linear trigger action").into()),
        };
        resource::mutate(
            &LINEAR,
            d,
            org.clone(),
            self.options,
            label,
            "trigger",
            linear,
            move |api, key| api.trigger_action(&org, &project, &id, key, kind),
        )
    }
}
impl Run<Management> for List {
    fn run(self, d: Deployment, _: Management) -> cli::CommandResult {
        let org = self.project.organization.to_string();
        let project = self.project.project.to_string();
        let page = resource::with_api(&LINEAR, &d, &self.options, linear, |api| {
            api.triggers(
                &org,
                &project,
                self.page.limit.map(i32::from),
                self.page.cursor.as_deref(),
            )
        })?;
        resource::list_output(
            &LINEAR,
            &d,
            &org,
            page.map(|page| (page.items, page.next_cursor)),
            self.options.json,
        )
    }
}
impl Run<Management> for EvaluationList {
    fn run(self, d: Deployment, _: Management) -> cli::CommandResult {
        let org = self.target.project.organization.to_string();
        let project = self.target.project.project.to_string();
        let page = resource::with_api(&LINEAR, &d, &self.target.options, linear, |api| {
            api.evaluations(
                &org,
                &project,
                &self.target.trigger,
                EvaluationFilters {
                    limit: self.page.limit.map(i32::from),
                    cursor: self.page.cursor.as_deref(),
                    state: self.state.map(EvaluationState::as_str),
                    run_id: self.run_id.as_deref(),
                },
            )
        })?;
        resource::list_output(
            &LINEAR,
            &d,
            &org,
            page.map(|page| (page.items, page.next_cursor)),
            self.target.options.json,
        )
    }
}
impl Run<Management> for EvaluationTarget {
    fn run(self, d: Deployment, _: Management) -> cli::CommandResult {
        let org = self.target.project.organization.to_string();
        let project = self.target.project.project.to_string();
        let value = resource::with_api(&LINEAR, &d, &self.target.options, linear, |api| {
            api.evaluation(&org, &project, &self.target.trigger, &self.evaluation)
        })?;
        resource::result(
            &LINEAR,
            &d,
            &org,
            "shown",
            "evaluation",
            value,
            |_| None,
            self.target.options.json,
        )
    }
}
