//! `um workflow run restart RUN --at STEP` (REQ-LOCALRUN-194).

use std::ffi::OsString;
use std::path::PathBuf;

use clap::Args;

use super::successor::SuccessorRequest;
use crate::exit_code::ExitCode;

pub(super) const ABOUT: &str = "Move a waiting run to its definition's current revision";
pub(super) const AFTER_HELP: &str = "Starts a successor of a waiting definition run on the definition's current revision. The successor inherits the results of the steps completed before --at STEP, runs STEP and its dependents again under the current revision, and takes the run's subject; the run exits superseded. A fan-out STEP runs again from its first element; um workflow run upgrade keeps a run waiting inside one at its element.

The run's inputs carry over to the inputs the current revision declares; --input-text sets a Text input on top of them, except the one the workflow names as its subject.";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(flatten)]
    run: super::ExistingLocalRun,
    #[arg(
        long = "at",
        value_name = "STEP",
        help = "Ordinary step the successor runs again from"
    )]
    at: String,
    #[arg(
        long,
        value_names = ["NAME", "TEXT"],
        num_args = 2,
        action = clap::ArgAction::Append,
        help = "Set one named Text input of the current revision"
    )]
    input_text: Vec<OsString>,
    #[arg(
        long,
        value_name = "PATH",
        help = "Existing execution root; defaults to the run's last attempt's root"
    )]
    execution_root: Option<PathBuf>,
    #[command(flatten)]
    presentation: super::PresentationOptions,
}

impl Command {
    pub(super) fn execute(self) -> super::super::super::CommandResult {
        super::create::execute_with_runtime(
            "start local workflow restart runtime",
            self.execute_async(),
        )
    }

    async fn execute_async(self) -> super::super::super::CommandResult {
        let input_text = super::successor::text_inputs(&self.input_text).map_err(|error| {
            super::super::super::CommandFailure::with_exit_code(error, ExitCode::UsageError)
        })?;
        let run_directory = self.run.resolve().map_err(anyhow::Error::new)?;
        let config = super::create::presentation_config(&self.presentation);
        super::successor::start(
            run_directory,
            SuccessorRequest::Restart {
                from: self.at,
                input_text,
                execution_root: self.execution_root,
            },
            config,
        )
        .await
    }
}
