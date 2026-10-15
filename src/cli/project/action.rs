mod config;

use super::{Options, ProjectReference};
use crate::exit_code::{ExitCode, OutcomeClass};
use anyhow::{Context, anyhow};
use clap::{Args, Subcommand};
use serde::Serialize;
use std::{
    io::{self, Write},
    path::PathBuf,
};
use um_api::{ActionApi, ActionFailure, ActionLifecycle, ActionRead, GoogleDocsAction};
use um_human_auth::Deployment;

type ResultCode = anyhow::Result<ExitCode>;
#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(subcommand)]
    command: Option<Leaf>,
}
#[derive(Debug, Subcommand)]
enum Leaf {
    #[command(about = "Create a project action")]
    Create(Create),
    #[command(about = "Delete a project action")]
    Delete(Delete),
    #[command(about = "Disable a project action")]
    Disable(Target),
    #[command(about = "Enable a project action")]
    Enable(Target),
    #[command(about = "List project actions")]
    List(List),
    #[command(about = "Show a project action or retained tombstone")]
    Show(Target),
    #[command(about = "Update selected configuration fields")]
    Update(Update),
}
#[derive(Debug, Args)]
struct Source {
    #[arg(
        long,
        value_name = "PATH",
        help = "Read action configuration from a JSON file or - for standard input"
    )]
    config_file: PathBuf,
}
#[derive(Debug, Args)]
struct Create {
    #[command(flatten)]
    project: ProjectReference,
    #[command(flatten)]
    source: Source,
    #[command(flatten)]
    options: Options,
}
#[derive(Debug, Args)]
struct List {
    #[command(flatten)]
    project: ProjectReference,
    #[command(flatten)]
    page: super::super::PaginationArgs<100>,
    #[command(flatten)]
    options: Options,
}
#[derive(Debug, Args)]
struct TargetRef {
    #[command(flatten)]
    project: ProjectReference,
    #[arg(value_name = "ACTION", help = "Project action ID")]
    action: String,
}
#[derive(Debug, Args)]
struct Target {
    #[command(flatten)]
    target: TargetRef,
    #[command(flatten)]
    options: Options,
}
#[derive(Debug, Args)]
struct Update {
    #[command(flatten)]
    target: TargetRef,
    #[command(flatten)]
    source: Source,
    #[arg(long, value_name = "VERSION", value_parser = clap::value_parser!(i32).range(1..), help = "Current action configuration version")]
    expected_version: i32,
    #[command(flatten)]
    options: Options,
}
#[derive(Debug, Args)]
struct Delete {
    #[command(flatten)]
    target: Target,
    #[command(flatten)]
    confirmation: super::super::ConfirmationArgs,
}
impl Command {
    pub(super) fn execute(self) -> super::super::CommandResult {
        let Some(command) = self.command else {
            return super::super::print_help(&["project", "action"]);
        };
        super::execute_leaf(command, Leaf::execute)
    }
}
impl Leaf {
    fn execute(self, d: &Deployment) -> ResultCode {
        match self {
            Self::Create(cmd) => {
                let body = match config::read(
                    &cmd.source.config_file,
                    false,
                    None,
                    cmd.options.authentication.uses_stdin(),
                )
                .and_then(config::create)
                {
                    Ok(body) => body,
                    Err(error) => return input_error(d, &cmd.options, &error),
                };
                let key = key()?;
                let result = call(d, &cmd.options, |api| {
                    api.create(
                        &cmd.project.organization,
                        &cmd.project.project_id,
                        &key,
                        &body,
                    )
                })?;
                action_result(d, &cmd.options, result, "created", Some(&key))
            }
            Self::Update(cmd) => {
                let body = match config::read(
                    &cmd.source.config_file,
                    true,
                    Some(cmd.expected_version),
                    cmd.options.authentication.uses_stdin(),
                )
                .and_then(config::update)
                {
                    Ok(body) => body,
                    Err(error) => return input_error(d, &cmd.options, &error),
                };
                let key = key()?;
                let result = call(d, &cmd.options, |api| {
                    api.update(
                        &cmd.target.project.organization,
                        &cmd.target.project.project_id,
                        &cmd.target.action,
                        &key,
                        &body,
                    )
                })?;
                action_result(d, &cmd.options, result, "updated", Some(&key))
            }
            Self::List(cmd) => {
                let page = call(d, &cmd.options, |api| {
                    api.list(
                        &cmd.project.organization,
                        &cmd.project.project_id,
                        cmd.page.limit,
                        cmd.page.cursor.as_deref(),
                    )
                })?;
                match page {
                    Ok(page) => {
                        if cmd.options.json {
                            super::super::write_cloud_list_json(
                                d.fingerprint().api_url(),
                                &page.items,
                                page.next_cursor.as_deref(),
                            )?;
                        } else {
                            println!("✓ Actions listed.\n");
                            for item in &page.items {
                                println!("{}\n", summary(item));
                            }
                            if let Some(cursor) = page.next_cursor {
                                println!("next cursor: {cursor}");
                            }
                            println!("deployment: {}", d.fingerprint().api_url());
                        }
                        Ok(ExitCode::Success)
                    }
                    Err(failure) => failure_result(d, &cmd.options, failure, None),
                }
            }
            Self::Show(cmd) => {
                let result = call(d, &cmd.options, |api| {
                    api.get(
                        &cmd.target.project.organization,
                        &cmd.target.project.project_id,
                        &cmd.target.action,
                    )
                })?;
                match result {
                    Ok(ActionRead::Active(item)) => write_value(
                        d,
                        &cmd.options,
                        "found",
                        "action",
                        &item,
                        &format!("✓ Action found.\n\n{}", summary(&item)),
                    ),
                    Ok(ActionRead::Deleted(item)) => write_value(
                        d,
                        &cmd.options,
                        "deleted",
                        "tombstone",
                        &item,
                        &format!(
                            "✓ Action deleted.\n\naction: {}\nproject: {}\ndeleted: {}",
                            item.id, item.project_id, item.deleted_at
                        ),
                    ),
                    Err(failure) => failure_result(d, &cmd.options, failure, None),
                }
            }
            Self::Enable(cmd) => lifecycle(d, cmd, ActionLifecycle::Enable, "enabled"),
            Self::Disable(cmd) => lifecycle(d, cmd, ActionLifecycle::Disable, "disabled"),
            Self::Delete(cmd) => {
                let _ = cmd.confirmation;
                lifecycle(d, cmd.target, ActionLifecycle::Delete, "deleted")
            }
        }
    }
}
fn key() -> anyhow::Result<String> {
    um_support::generate_idempotency_key().context("generate action request identity")
}
fn lifecycle(
    d: &Deployment,
    cmd: Target,
    kind: ActionLifecycle,
    outcome: &'static str,
) -> ResultCode {
    let key = key()?;
    let result = call(d, &cmd.options, |api| {
        api.lifecycle(
            &cmd.target.project.organization,
            &cmd.target.project.project_id,
            &cmd.target.action,
            &key,
            kind,
        )
    })?;
    match result {
        Ok(Some(item)) if outcome != "deleted" => write_value(
            d,
            &cmd.options,
            outcome,
            "action",
            &item,
            &format!("✓ Action {outcome}.\n\n{}", summary(&item)),
        ),
        Ok(None) if outcome == "deleted" => write_value(
            d,
            &cmd.options,
            outcome,
            "action",
            &serde_json::json!({"id":cmd.target.action}),
            &format!("✓ Action deleted.\n\naction: {}", cmd.target.action),
        ),
        Ok(_) => failure_result(
            d,
            &cmd.options,
            ActionFailure::Protocol {
                credential_rejected: false,
            },
            Some(&key),
        ),
        Err(failure) => failure_result(d, &cmd.options, failure, Some(&key)),
    }
}
fn call<T>(
    d: &Deployment,
    options: &Options,
    mut operation: impl FnMut(&ActionApi) -> Result<T, ActionFailure>,
) -> anyhow::Result<Result<T, ActionFailure>> {
    let policy = options.http.transport_policy();
    let client = um_api::HttpClient::new(policy)
        .map_err(|error| anyhow!(error))
        .context("prepare human session networking")?;
    let mut ambiguous = false;
    let result = super::super::execute_selected_api_operation(
        super::super::principal_api_context(
            &client,
            d,
            &options.authentication,
            "acquire human session for action operation",
        ),
        |token| {
            let api = ActionApi::new(d.fingerprint().api_url(), token, policy)
                .map_err(|error| anyhow!(error))
                .context("prepare action management networking")?;
            Ok(match operation(&api) {
                Err(ActionFailure::Uncertain(replay)) => {
                    ambiguous = true;
                    // Let the normal credential refresh run; a successful replay
                    // settles the dispatch, but a later rejection does not.
                    Err(replay.map_or(ActionFailure::Unauthenticated, |failure| *failure))
                }
                result => result,
            })
        },
        ActionFailure::credential_rejected,
        || ActionFailure::Unauthenticated,
        ActionFailure::Unreachable,
    );
    if !ambiguous {
        return result;
    }
    // A refresh failure is not proof the first dispatch failed. Do not echo
    // authentication error text, which can contain credentials or private paths.
    Ok(Err(ActionFailure::Uncertain(match result {
        Ok(Ok(value)) => return Ok(Ok(value)),
        Ok(Err(failure)) => Some(Box::new(failure)),
        Err(_) => None,
    })))
}
fn summary(item: &GoogleDocsAction) -> String {
    // Never print literal input bindings as human-readable diagnostics.
    format!(
        "action: {}\nproject: {}\nname: {}\nworkflow: {}\nenabled: {}\nversion: {}",
        item.id, item.project_id, item.display_name, item.workflow_path, item.enabled, item.version
    )
}
fn write_value(
    d: &Deployment,
    options: &Options,
    outcome: &'static str,
    field: &str,
    item: &impl Serialize,
    human: &str,
) -> ResultCode {
    if options.json {
        let mut doc = serde_json::json!({"schemaVersion":1,"deployment":d.fingerprint().api_url(),"outcome":outcome});
        doc[field] = serde_json::to_value(item)?;
        super::super::write_pretty_json(&doc)?;
    } else {
        println!("{human}\ndeployment: {}", d.fingerprint().api_url());
    }
    Ok(ExitCode::Success)
}
fn action_result(
    d: &Deployment,
    options: &Options,
    result: Result<GoogleDocsAction, ActionFailure>,
    outcome: &'static str,
    key: Option<&str>,
) -> ResultCode {
    match result {
        Ok(item) => write_value(
            d,
            options,
            outcome,
            "action",
            &item,
            &format!("✓ Action {outcome}.\n\n{}", summary(&item)),
        ),
        Err(failure) => failure_result(d, options, failure, key),
    }
}
fn failure_result(
    d: &Deployment,
    options: &Options,
    failure: ActionFailure,
    key: Option<&str>,
) -> ResultCode {
    if let ActionFailure::Uncertain(replay) = &failure {
        let replay_outcome = match replay.as_deref() {
            Some(ActionFailure::Unauthenticated) => "unauthenticated",
            Some(ActionFailure::Forbidden) => "forbidden",
            Some(ActionFailure::NotFound) => "not_found",
            Some(ActionFailure::InvalidInput) => "invalid_input",
            Some(ActionFailure::Conflict) => "conflict",
            Some(ActionFailure::RateLimited(_)) => "rate_limited",
            Some(ActionFailure::Unreachable(_)) => "unreachable",
            Some(ActionFailure::Protocol { .. }) => "invalid_response",
            Some(ActionFailure::Uncertain(_)) | None => "session_unavailable",
        };
        let key = key.ok_or_else(|| anyhow!("missing action request identity"))?;
        if options.json {
            super::super::write_pretty_json(&serde_json::json!({
                "schemaVersion":1, "deployment":d.fingerprint().api_url(),
                "outcome":"result_uncertain", "idempotencyKey":key,
                "nextAction":"inspect_resource", "replayOutcome":replay_outcome,
            }))?;
        } else {
            writeln!(
                io::stderr().lock(),
                "error: action result uncertain (replay: {replay_outcome})\n\nInspect the action before a new mutation.\nidempotency key: {key}"
            )?;
        }
        return Ok(ExitCode::Unavailable);
    }
    let retry_after = match &failure {
        ActionFailure::RateLimited(delay) => *delay,
        _ => None,
    };
    let (outcome, class, remedy) = match &failure {
        ActionFailure::Unauthenticated => (
            "unauthenticated",
            OutcomeClass::Unauthenticated,
            "Sign in or check the service API key.",
        ),
        ActionFailure::Forbidden => (
            "forbidden",
            OutcomeClass::Forbidden,
            "Ask an organization owner to perform this operation.",
        ),
        ActionFailure::NotFound => (
            "not_found",
            OutcomeClass::GeneralFailure,
            "Check the organization, project and action IDs.",
        ),
        ActionFailure::InvalidInput => (
            "invalid_input",
            OutcomeClass::GeneralFailure,
            "Check the action configuration and try again.",
        ),
        ActionFailure::Conflict => (
            "conflict",
            OutcomeClass::GeneralFailure,
            "Inspect the current action before trying again.",
        ),
        ActionFailure::RateLimited(_) => (
            "rate_limited",
            OutcomeClass::RateLimited,
            "Wait before trying again.",
        ),
        ActionFailure::Unreachable(category) => (
            "unreachable",
            super::super::unreachable_outcome_class(*category),
            "The result may be uncertain. Inspect the action before a new mutation.",
        ),
        ActionFailure::Uncertain(_) => return Err(anyhow!("unexpected uncertain result")),
        ActionFailure::Protocol { .. } => (
            "invalid_response",
            OutcomeClass::Protocol,
            "The result may be uncertain. Inspect the action before trying again.",
        ),
    };
    if let Some(key) = key.filter(|_| {
        matches!(
            failure,
            ActionFailure::Unreachable(_) | ActionFailure::Protocol { .. }
        )
    }) {
        if options.json {
            super::super::write_pretty_json(
                &serde_json::json!({"schemaVersion":1,"deployment":d.fingerprint().api_url(),"outcome":outcome,"idempotencyKey":key,"nextAction":"inspect_resource"}),
            )?;
        } else {
            writeln!(
                io::stderr().lock(),
                "error: action {outcome}\n\n{remedy}\nidempotency key: {key}"
            )?;
        }
        return Ok(class.exit_code());
    }
    super::super::write_api_failure(
        d.fingerprint().api_url(),
        outcome,
        None,
        retry_after,
        &format!("error: action {outcome}\n\n{remedy}"),
        class,
        options.json,
    )
}
fn input_error(d: &Deployment, options: &Options, error: &anyhow::Error) -> ResultCode {
    // Never echo parser or file errors: paths and literals may be private.
    let message = if error.to_string()
        == "configuration and authentication cannot both read standard input"
    {
        "error: standard input is already in use\n\nRead the action configuration or the service API key from a regular file."
    } else {
        "error: invalid action configuration\n\nCheck the config file, standard input use and field names."
    };
    super::super::write_api_failure(
        d.fingerprint().api_url(),
        "invalid_input",
        None,
        None,
        message,
        OutcomeClass::GeneralFailure,
        options.json,
    )
}
