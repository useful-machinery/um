//! `um workflow run upgrade RUN` (REQ-LOCALRUN-197).

use clap::Args;

use super::successor::SuccessorRequest;

pub(super) const ABOUT: &str =
    "Move a waiting run to its definition's current revision with its wait";
pub(super) const AFTER_HELP: &str = "Starts a successor of a waiting definition run on the definition's current revision that keeps the run's pending wait, so no step runs again. The successor takes the run's attempts, position and subject, and waits where the run waited; the run exits superseded. Answer, approve or otherwise resolve the successor's wait, then resume it.

The command refuses, and changes nothing, when the run is already on the current revision, when a recorded control already resolves its wait, or when the current revision no longer has a step, outcome or element the run's state names.";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(flatten)]
    run: super::ExistingLocalRun,
    #[command(flatten)]
    presentation: super::PresentationOptions,
}

impl Command {
    pub(super) fn execute(self) -> super::super::super::CommandResult {
        super::create::execute_with_runtime(
            "start local workflow upgrade runtime",
            self.execute_async(),
        )
    }

    async fn execute_async(self) -> super::super::super::CommandResult {
        let run_directory = self.run.resolve().map_err(anyhow::Error::new)?;
        let config = super::create::presentation_config(&self.presentation);
        super::successor::start(run_directory, SuccessorRequest::Upgrade, config).await
    }
}
