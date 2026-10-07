use std::io::{self, Write};
use std::path::PathBuf;

use anyhow::{Context, anyhow};
use clap::Args;
use serde::Serialize;
use um_api::{EvaluationFilters, HttpClient, LinearApi, LinearFailure, TriggerAction};
use um_human_auth::Deployment;

use super::super::super::{self as cli, OrganizationArg, ProjectArg};
use super::{ObservationControl, Options, config};
use crate::exit_code::ExitCode;

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
#[derive(Debug, Args)]
pub(super) struct Delete {
    #[command(flatten)]
    target: Target,
    #[command(flatten)]
    confirmation: cli::ConfirmationArgs,
}
#[derive(Debug, Args)]
pub(super) struct Update {
    #[command(flatten)]
    target: Target,
    #[arg(
        long,
        value_name = "PATH",
        help = "Read replacement configuration fields from a JSON file or - for stdin"
    )]
    config_file: PathBuf,
    #[arg(long, value_name = "VERSION", value_parser = clap::value_parser!(i32).range(1..), help = "Required current configuration version")]
    expected_version: i32,
}
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

pub(super) fn execute<T: Run>(cmd: T, action: Management) -> cli::CommandResult {
    cli::execute_deployment_command(
        Some(cmd),
        &["project", "trigger"],
        "manage Linear triggers",
        |cmd, deployment| cmd.run(deployment.clone(), action),
    )
}
pub(super) trait Run {
    fn run(self, d: Deployment, action: Management) -> cli::CommandResult;
}

fn with_api<T>(
    d: &Deployment,
    options: &Options,
    mut operation: impl FnMut(&LinearApi) -> Result<T, LinearFailure>,
) -> anyhow::Result<Result<T, LinearFailure>> {
    let policy = options.http.transport_policy();
    let client = HttpClient::new(policy)
        .map_err(|error| anyhow!(error))
        .context("prepare Linear networking")?;
    cli::execute_selected_api_operation(
        cli::principal_api_context(&client, d, &options.authentication, "acquire human session"),
        |token| {
            let api = LinearApi::new(d.fingerprint().api_url(), token, policy)
                .map_err(|error| anyhow!(error))
                .context("prepare Linear API")?;
            Ok(operation(&api))
        },
        LinearFailure::credential_rejected,
        || LinearFailure::Unauthenticated,
        |category| LinearFailure::Unreachable { category },
    )
}

fn output(
    d: &Deployment,
    org: &str,
    label: (&str, &str),
    resource: Option<&impl Serialize>,
    cursor: Option<&str>,
    failure: Option<&LinearFailure>,
    json: bool,
) -> cli::CommandResult {
    let (action, field) = label;
    let (outcome, exit) = match failure {
        None => (action, ExitCode::Success),
        Some(LinearFailure::Unauthenticated) => {
            ("authentication_required", ExitCode::AuthenticationRequired)
        }
        Some(LinearFailure::Forbidden) => ("forbidden", ExitCode::GeneralFailure),
        Some(LinearFailure::NotFound) => ("not_found", ExitCode::GeneralFailure),
        Some(LinearFailure::Conflict) => ("conflict", ExitCode::GeneralFailure),
        Some(LinearFailure::InvalidInput) => ("invalid_input", ExitCode::UsageError),
        Some(LinearFailure::RateLimited { .. } | LinearFailure::Unreachable { .. }) => {
            ("unavailable", ExitCode::Unavailable)
        }
        Some(LinearFailure::InvalidResponse { .. }) => {
            ("invalid_response", ExitCode::GeneralFailure)
        }
    };
    let write = || -> anyhow::Result<()> {
        if json {
            let mut out = io::stdout().lock();
            let mut document = serde_json::json!({"schemaVersion":1, "deployment":d.fingerprint().api_url(), "organizationRef":org, "outcome":outcome});
            if let Some(cursor) = cursor {
                document["nextCursor"] = cursor.into();
            }
            document[field] = serde_json::to_value(resource)?;
            serde_json::to_writer(&mut out, &document)?;
            writeln!(out)?;
        } else if failure.is_some() {
            writeln!(
                io::stderr().lock(),
                "error: Linear trigger {outcome}\n\nInspect the trigger and your access before trying again."
            )?;
        } else {
            let mut out = io::stdout().lock();
            let name = if field == "evaluation" {
                "Linear evaluation"
            } else {
                "Linear trigger"
            };
            writeln!(out, "{name}: {outcome}")?;
            if let Some(value) = resource {
                serde_json::to_writer_pretty(&mut out, value)?;
                writeln!(out)?;
            }
            if let Some(cursor) = cursor {
                writeln!(out, "next cursor: {cursor}")?;
            }
        }
        Ok(())
    };
    write().map_err(cli::CommandFailure::from)?;
    Ok(exit)
}
fn result<T: Serialize>(
    d: &Deployment,
    org: &str,
    action: &str,
    field: &str,
    value: Result<T, LinearFailure>,
    cursor: impl Fn(&T) -> Option<&str>,
    json: bool,
) -> cli::CommandResult {
    match value {
        Ok(value) => output(
            d,
            org,
            (action, field),
            Some(&value),
            cursor(&value),
            None,
            json,
        ),
        Err(failure) => output(
            d,
            org,
            (action, field),
            None::<&T>,
            None,
            Some(&failure),
            json,
        ),
    }
}

fn uncertain(
    d: &Deployment,
    org: &str,
    key: &str,
    json: bool,
    exit: ExitCode,
    replay_failure: Option<&LinearFailure>,
) -> cli::CommandResult {
    let replay_outcome = replay_failure.map(|failure| match failure {
        LinearFailure::Unauthenticated => "authentication_required",
        LinearFailure::Forbidden => "forbidden",
        LinearFailure::RateLimited { .. } => "rate_limited",
        LinearFailure::Unreachable { .. } => "unavailable",
        LinearFailure::InvalidResponse { .. } => "invalid_response",
        LinearFailure::Conflict => "conflict",
        LinearFailure::NotFound => "not_found",
        LinearFailure::InvalidInput => "invalid_input",
    });
    let write = || -> anyhow::Result<()> {
        if json {
            let mut out = io::stdout().lock();
            let mut document = serde_json::json!({"schemaVersion":1,"deployment":d.fingerprint().api_url(),"organizationRef":org,"outcome":"commitment_unknown","idempotencyKey":key});
            if let Some(reason) = replay_outcome {
                document["replayOutcome"] = reason.into();
            }
            serde_json::to_writer(&mut out, &document)?;
            writeln!(out)?;
        } else {
            let mut out = io::stderr().lock();
            writeln!(
                out,
                "error: Linear trigger commitment unknown\nrequest key: {key}"
            )?;
            if let Some(reason) = replay_outcome {
                writeln!(out, "replay outcome: {reason}")?;
            }
            writeln!(
                out,
                "\nInspect the trigger and your access before repeating this operation."
            )?;
        }
        Ok(())
    };
    write().map_err(cli::CommandFailure::from)?;
    Ok(exit)
}

// Mutations reuse a single key for a lost response. Never continue with another key after
// an uncertain result or a signal; a read is required before any deliberate new write.
fn mutate<T: Serialize + Send + 'static>(
    d: Deployment,
    org: String,
    options: Options,
    action: &'static str,
    field: &'static str,
    mut call: impl FnMut(&LinearApi, &str) -> Result<T, LinearFailure> + Send + 'static,
) -> cli::CommandResult {
    let key =
        um_support::generate_idempotency_key().context("generate trigger request identity")?;
    let signal_deployment = d.clone();
    let signal_org = org.clone();
    let signal_key = key.clone();
    let json = options.json;
    cli::execute_mutation_with_signals(
        "Linear trigger mutation",
        (),
        move |control| {
            if !control.begin_dispatch() {
                return Ok(ExitCode::Interrupted);
            }
            let mut response_lost = false;
            let mut replay_failure = None;
            let value = with_api(&d, &options, |api| match call(api, &key) {
                Err(LinearFailure::Unreachable { .. }) if !control.is_stopped() => {
                    response_lost = true;
                    let replay = call(api, &key);
                    replay_failure = replay.as_ref().err().cloned();
                    replay
                }
                value => value,
            });
            // A credential refresh can fail after a rejected replay. It does not
            // settle the first dispatch; retain the key even without a final API result.
            let value = match value {
                Ok(value) => value,
                Err(_) if response_lost => {
                    return cli::complete_operation(control, || {
                        uncertain(
                            &d,
                            &org,
                            &key,
                            json,
                            ExitCode::Unavailable,
                            replay_failure.as_ref(),
                        )
                    });
                }
                Err(error) => return Err(error.into()),
            };
            cli::complete_operation(control, || {
                if matches!(
                    value,
                    Err(LinearFailure::Unreachable { .. } | LinearFailure::InvalidResponse { .. })
                ) || (response_lost && value.is_err())
                {
                    uncertain(
                        &d,
                        &org,
                        &key,
                        json,
                        ExitCode::Unavailable,
                        if response_lost {
                            value.as_ref().err()
                        } else {
                            None
                        },
                    )
                } else {
                    result(&d, &org, action, field, value, |_| None, json)
                }
            })
        },
        move |exit, snapshot| {
            if snapshot.dispatched {
                uncertain(
                    &signal_deployment,
                    &signal_org,
                    &signal_key,
                    json,
                    exit,
                    None,
                )
            } else {
                Ok(exit)
            }
        },
    )
}
fn configuration_error(error: anyhow::Error, json: bool) -> cli::CommandResult {
    if json {
        let mut out = io::stdout().lock();
        serde_json::to_writer(
            &mut out,
            &serde_json::json!({"schemaVersion":1,"outcome":"invalid_configuration"}),
        )
        .map_err(|e| cli::CommandFailure::from(anyhow!(e).context("write configuration error")))?;
        writeln!(out).map_err(|e| {
            cli::CommandFailure::from(anyhow!(e).context("write configuration error"))
        })?;
        writeln!(io::stderr().lock(), "error: trigger configuration rejected: {error}\n\nCheck the configuration and try again.")
            .map_err(|e| cli::CommandFailure::from(anyhow!(e).context("write configuration diagnostic")))?;
        Ok(ExitCode::UsageError)
    } else {
        Err(cli::CommandFailure::with_exit_code(
            error.context("Check the trigger configuration file and try again"),
            ExitCode::UsageError,
        ))
    }
}

impl Run for Create {
    fn run(self, d: Deployment, _: Management) -> cli::CommandResult {
        let config = config::read(
            &self.config_file,
            false,
            None,
            self.options.authentication.uses_stdin(),
        )
        .and_then(config::create);
        let config = match config {
            Ok(config) => config,
            Err(error) => return configuration_error(error, self.options.json),
        };
        let org = self.project.organization.to_string();
        let project = self.project.project.to_string();
        mutate(
            d,
            org.clone(),
            self.options,
            "created",
            "trigger",
            move |api, key| api.create_trigger(&org, &project, key, config.clone()),
        )
    }
}
impl Run for Update {
    fn run(self, d: Deployment, _: Management) -> cli::CommandResult {
        let config = config::read(
            &self.config_file,
            true,
            Some(self.expected_version),
            self.target.options.authentication.uses_stdin(),
        )
        .and_then(config::update);
        let config = match config {
            Ok(config) => config,
            Err(error) => return configuration_error(error, self.target.options.json),
        };
        let org = self.target.project.organization.to_string();
        let project = self.target.project.project.to_string();
        let id = self.target.trigger;
        mutate(
            d,
            org.clone(),
            self.target.options,
            "updated",
            "trigger",
            move |api, key| api.update_trigger(&org, &project, &id, key, config.clone()),
        )
    }
}
impl Run for Delete {
    fn run(self, d: Deployment, action: Management) -> cli::CommandResult {
        let _ = self.confirmation;
        self.target.run(d, action)
    }
}
impl Run for Target {
    fn run(self, d: Deployment, action: Management) -> cli::CommandResult {
        let org = self.project.organization.to_string();
        let project = self.project.project.to_string();
        let id = self.trigger;
        if matches!(action, Management::Show) {
            return result(
                &d,
                &org,
                "shown",
                "trigger",
                with_api(&d, &self.options, |api| api.trigger(&org, &project, &id))?,
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
        mutate(
            d,
            org.clone(),
            self.options,
            label,
            "trigger",
            move |api, key| api.trigger_action(&org, &project, &id, key, kind),
        )
    }
}
fn list_output<T: Serialize>(
    d: &Deployment,
    org: &str,
    page: Result<(Vec<T>, Option<String>), LinearFailure>,
    json: bool,
) -> cli::CommandResult {
    match page {
        Ok((items, cursor)) => output(
            d,
            org,
            ("listed", "items"),
            Some(&items),
            cursor.as_deref(),
            None,
            json,
        ),
        Err(failure) => output(
            d,
            org,
            ("listed", "items"),
            None::<&Vec<T>>,
            None,
            Some(&failure),
            json,
        ),
    }
}

impl Run for List {
    fn run(self, d: Deployment, _: Management) -> cli::CommandResult {
        let org = self.project.organization.to_string();
        let project = self.project.project.to_string();
        let page = with_api(&d, &self.options, |api| {
            api.triggers(
                &org,
                &project,
                self.page.limit.map(i32::from),
                self.page.cursor.as_deref(),
            )
        })?;
        list_output(
            &d,
            &org,
            page.map(|page| (page.items, page.next_cursor)),
            self.options.json,
        )
    }
}
impl Run for EvaluationList {
    fn run(self, d: Deployment, _: Management) -> cli::CommandResult {
        let org = self.target.project.organization.to_string();
        let project = self.target.project.project.to_string();
        let page = with_api(&d, &self.target.options, |api| {
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
        list_output(
            &d,
            &org,
            page.map(|page| (page.items, page.next_cursor)),
            self.target.options.json,
        )
    }
}
impl Run for EvaluationTarget {
    fn run(self, d: Deployment, _: Management) -> cli::CommandResult {
        let org = self.target.project.organization.to_string();
        let project = self.target.project.project.to_string();
        let value = with_api(&d, &self.target.options, |api| {
            api.evaluation(&org, &project, &self.target.trigger, &self.evaluation)
        })?;
        result(
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
