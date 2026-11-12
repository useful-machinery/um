mod definition;
mod reference;
mod run;
mod schema;
pub(in crate::cli) mod validate;

use std::future::Future;
use std::io::{self, Write};
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context;
use clap::{Args, Subcommand, ValueEnum};

use crate::exit_code::ExitCode;

pub(super) const ABOUT: &str = "Work with local workflow definitions and runs";
const NAME: &str = "workflow";

#[derive(Clone, Copy, Debug, ValueEnum)]
pub(super) enum ColorArgument {
    Auto,
    Always,
    Never,
}

#[derive(Debug, Args)]
pub(super) struct PresentationOptions {
    #[arg(long, conflicts_with = "json", help = "Force plain human presentation")]
    pub(super) plain: bool,

    #[command(flatten)]
    output: super::JsonArgs<super::WorkflowJson>,

    #[arg(
        long,
        value_enum,
        value_name = "WHEN",
        default_value_t = ColorArgument::Auto,
        help = "Select renderer color behavior"
    )]
    pub(super) color: ColorArgument,
}

#[derive(Debug, Args)]
pub(super) struct ExistingLocalRun {
    #[arg(
        value_name = "RUN",
        help = "Run directory, or the id or subject of a run under the um home"
    )]
    pub(super) run_dir: PathBuf,
}

impl ExistingLocalRun {
    /// The run directory `RUN` names (REQ-LOCALRUN-180): a single component of a run id's
    /// form is the run with that id under the `um` home; a single component of a subject's
    /// form that names no existing path is the run the home's subject index holds for it;
    /// anything else is the directory itself.
    pub(super) fn resolve(&self) -> Result<PathBuf, RunAddressError> {
        let Some(value) = self.run_dir.to_str() else {
            return Ok(self.run_dir.clone());
        };
        if um_execution::is_run_id(value) {
            return locate_by_id(&home()?, value);
        }
        if um_execution::is_subject(value) && std::fs::symlink_metadata(&self.run_dir).is_err() {
            return locate_by_subject(&home()?, value);
        }
        Ok(self.run_dir.clone())
    }
}

fn home() -> Result<um_execution::WorkflowHome, RunAddressError> {
    um_execution::WorkflowHome::from_environment().map_err(|error| {
        RunAddressError::new(
            error.code(),
            "The um home cannot be located: neither UM_HOME nor HOME is set.",
        )
    })
}

fn locate_by_id(home: &um_execution::WorkflowHome, id: &str) -> Result<PathBuf, RunAddressError> {
    um_execution::locate_run(home, id).map_err(|error| match error {
        um_execution::LocalRunDirectoryError::RunNotFound => {
            RunAddressError::new(RUN_NOT_FOUND, "No run under the um home has this id.")
        }
        other => RunAddressError::unavailable(other),
    })
}

/// The run the one definition's slot for `subject` names; `subject_ambiguous` with the ids
/// when several do.
fn locate_by_subject(
    home: &um_execution::WorkflowHome,
    subject: &str,
) -> Result<PathBuf, RunAddressError> {
    let not_found =
        || RunAddressError::new(RUN_NOT_FOUND, "No run under the um home has this subject.");
    let mut holders =
        um_execution::SlotHolder::of_subject(home, subject).map_err(|error| match error {
            um_execution::SubjectError::Unavailable(source) => RunAddressError::unavailable(source),
            // `of_subject` refuses only a value outside the subject form, which `resolve`
            // never passes, and reads no slot as busy or settled.
            _ => not_found(),
        })?;
    match holders.len() {
        0 => Err(not_found()),
        1 => Ok(holders.remove(0).directory),
        _ => Err(RunAddressError::new(
            SUBJECT_AMBIGUOUS,
            format!(
                "Several runs under the um home hold this subject: {}.",
                holders
                    .iter()
                    .map(|holder| holder.run.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        )),
    }
}

const RUN_NOT_FOUND: &str = "run_not_found";
const RUN_DIRECTORY_UNAVAILABLE: &str = "run_directory_unavailable";
const SUBJECT_AMBIGUOUS: &str = "subject_ambiguous";

/// A run id or subject `RUN` the `um` home does not answer (REQ-LOCALRUN-180): `code` is
/// `run_not_found`, `subject_ambiguous` (the message names the runs), `home_unavailable`, or
/// `run_directory_unavailable` when the home's runs cannot be read, with that failure as its
/// source.
#[derive(Debug)]
pub(super) struct RunAddressError {
    pub(super) code: &'static str,
    pub(super) message: String,
    source: Option<um_execution::LocalRunDirectoryError>,
}

impl RunAddressError {
    fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            source: None,
        }
    }

    fn unavailable(source: um_execution::LocalRunDirectoryError) -> Self {
        Self {
            code: RUN_DIRECTORY_UNAVAILABLE,
            message: "The runs under the um home could not be read.".to_owned(),
            source: Some(source),
        }
    }

    /// Prints the `error` document `command` writes under `--json` for this failure: no
    /// `runDirectory`, since none was resolved, and exit status 1.
    pub(super) fn write_error_document(&self, command: &'static str) -> io::Result<ExitCode> {
        #[derive(serde::Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Document<'a> {
            schema_version: u8,
            command: &'static str,
            outcome: &'static str,
            exit_status: u8,
            error: Detail<'a>,
        }
        #[derive(serde::Serialize)]
        struct Detail<'a> {
            code: &'static str,
            message: &'a str,
        }
        super::write_pretty_json(&Document {
            schema_version: 1,
            command,
            outcome: "error",
            exit_status: ExitCode::GeneralFailure.as_u8(),
            error: Detail {
                code: self.code,
                message: &self.message,
            },
        })?;
        Ok(ExitCode::GeneralFailure)
    }
}

impl std::fmt::Display for RunAddressError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for RunAddressError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_ref()
            .map(|source| source as &(dyn std::error::Error + 'static))
    }
}

#[derive(Debug, Args)]
pub(super) struct LocalExecutionRoot {
    #[arg(
        long,
        value_name = "PATH",
        help = "Directory for workflow execution (must already exist)"
    )]
    pub(super) execution_root: PathBuf,
}

#[derive(Debug, Args)]
pub(super) struct LocalWorkflowSource {
    #[arg(
        long,
        value_name = "ROOT",
        help = "Directory boundary for workflow source files"
    )]
    pub(super) source_root: PathBuf,

    #[arg(
        value_name = "WORKFLOW_FILE",
        help = "Workflow definition path, resolved from the initial working directory"
    )]
    pub(super) workflow_file: PathBuf,
}

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(subcommand)]
    command: Option<WorkflowCommand>,
}

#[derive(Debug, Subcommand)]
enum WorkflowCommand {
    #[command(about = definition::ABOUT)]
    Definition(definition::Command),
    #[command(about = reference::ABOUT)]
    Reference(reference::Command),
    #[command(about = run::ABOUT)]
    Run(run::Command),
    #[command(about = schema::ABOUT, after_help = schema::AFTER_HELP)]
    Schema(schema::Command),
    #[command(about = validate::ABOUT)]
    Validate(validate::Command),
}

/// The run record's state as `run.json` spells it.
pub(super) const fn state_name(state: um_execution::RunState) -> &'static str {
    match state {
        um_execution::RunState::Running => "running",
        um_execution::RunState::Waiting => "waiting",
        um_execution::RunState::Exited => "exited",
    }
}

/// The invoking user's login name: `USER`, then `LOGNAME`, else `unknown` (REQ-WFDEF-22,
/// REQ-LOCALRUN-175).
pub(super) fn login_name() -> String {
    ["USER", "LOGNAME"]
        .into_iter()
        .filter_map(std::env::var_os)
        .find(|value| !value.is_empty())
        .map_or_else(
            || "unknown".to_owned(),
            |value| value.to_string_lossy().into_owned(),
        )
}

pub(super) fn write_embedded_asset(
    asset: &'static str,
    context: &'static str,
) -> super::CommandResult {
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    stdout
        .write_all(asset.as_bytes())
        .and_then(|()| stdout.flush())
        .context(context)?;
    Ok(ExitCode::Success)
}

// Read-only commands use a current-thread runtime so signal exit can abandon a blocked
// filesystem worker without delaying process shutdown.
pub(super) fn execute_with_abandonable_runtime(
    leaf: &'static str,
    execution: impl Future<Output = super::CommandResult>,
) -> super::CommandResult {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .with_context(|| format!("start workflow {leaf} runtime"))?;
    let result = runtime.block_on(execution);
    runtime.shutdown_timeout(Duration::ZERO);
    result
}

pub(super) fn observe_workflow_signals(
    leaf: &'static str,
) -> anyhow::Result<(tokio::signal::unix::Signal, tokio::signal::unix::Signal)> {
    (|| -> io::Result<_> {
        let interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
        let terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        Ok((interrupt, terminate))
    })()
    .with_context(|| format!("install workflow {leaf} signal observation"))
}

impl Command {
    pub(super) fn execute(self) -> super::CommandResult {
        match self.command {
            None => super::print_help(&[NAME]),
            Some(WorkflowCommand::Definition(command)) => command.execute(),
            Some(WorkflowCommand::Reference(command)) => command.execute(),
            Some(WorkflowCommand::Run(command)) => command.execute(),
            Some(WorkflowCommand::Schema(command)) => command.execute(),
            Some(WorkflowCommand::Validate(command)) => command.execute(),
        }
    }
}
