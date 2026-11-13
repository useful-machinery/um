//! `um workflow run create ID --cloud ORGANIZATION --project PROJECT` (REQ-RUN-94): a Cloud
//! run of the Cloud definition `ID`, created and reported as every Cloud run creation is.

use std::path::PathBuf;

use anyhow::anyhow;
use clap::Args;
use um_api::RunDefinitionSelection;

use crate::cli::run::{
    Creation, CreationOptions, INTEGRATION_CONTEXT_FILE_HELP, RunOptions, RunWaitArgs,
};
use crate::cli::{CommandFailure, CommandResult, OrganizationArg, ProjectArg};
use crate::exit_code::ExitCode;

/// The most simultaneous steps a Cloud run admits (REQ-RUN-84).
const MAXIMUM_CLOUD_PARALLEL_STEPS: usize = 64;

/// The options only a Cloud creation reads; each needs `--cloud` (REQ-LOCALRUN-80).
#[derive(Debug, Args)]
#[command(
    mut_arg("source_branch", |arg| arg.requires("cloud")),
    mut_arg("workspace_branch", |arg| arg.requires("cloud")),
    mut_arg("workspace_commit", |arg| arg.requires("cloud")),
    mut_arg("display_name", |arg| arg.requires("cloud")),
    mut_arg("input_set_id", |arg| arg.requires("cloud")),
    mut_arg("wait", |arg| arg.requires("cloud")),
    mut_arg("timeout", |arg| arg.requires("cloud"))
)]
pub(super) struct Options {
    #[arg(
        long,
        value_name = ProjectArg::VALUE_NAME,
        requires = "cloud",
        help = "Cloud project ID of the run --cloud creates"
    )]
    project: Option<ProjectArg>,

    #[command(flatten)]
    selection: CreationOptions,

    #[arg(long, value_name = "PATH|-", requires = "cloud", help = INTEGRATION_CONTEXT_FILE_HELP)]
    integration_context_file: Option<PathBuf>,

    #[arg(
        long,
        requires = "cloud",
        conflicts_with = "owner",
        help = "Start the Cloud run with no owner, in the project's runner pools"
    )]
    no_owner: bool,

    #[command(flatten)]
    wait: RunWaitArgs,
}

impl Options {
    /// The first Cloud-only option given, by its flag. Clap waives a `requires` whose target
    /// conflicts with an option present, so a local run checks again (REQ-LOCALRUN-80).
    pub(super) fn given(&self) -> Option<&'static str> {
        if self.project.is_some() {
            return Some("--project");
        }
        self.selection.given().or_else(|| {
            [
                (
                    self.integration_context_file.is_some(),
                    "--integration-context-file",
                ),
                (self.no_owner, "--no-owner"),
                (self.wait.wait, "--wait"),
                (self.wait.timeout.is_some(), "--timeout"),
            ]
            .into_iter()
            .find_map(|(given, flag)| given.then_some(flag))
        })
    }
}

impl super::Command {
    /// Asks Cloud to create a run of the Cloud definition `ID`, sent as given, and the
    /// workflow file `--workflow` names in its tree. Nothing under the `um` home and no
    /// execution root is read.
    pub(super) fn create_in_cloud(self, organization: OrganizationArg) -> CommandResult {
        let usage = |error| CommandFailure::with_exit_code(error, ExitCode::UsageError);
        let id = self
            .id
            .ok_or_else(|| usage(anyhow!("name the Cloud definition id ID")))?;
        let project_id = self
            .cloud_creation
            .project
            .ok_or_else(|| usage(anyhow!("--cloud needs --project PROJECT")))?;
        let workflow_path = match self.workflow.as_deref() {
            None => None,
            Some(workflow) => {
                super::tree_workflow_file(Some(workflow)).map_err(usage)?;
                if workflow.to_str().is_none() {
                    return Err(usage(anyhow!("--workflow must be valid UTF-8")));
                }
                Some(super::tree_relative_workflow(Some(workflow)))
            }
        };
        let max_parallel = self
            .max_parallel
            .map(|count| {
                u8::try_from(count)
                    .ok()
                    .filter(|_| count <= MAXIMUM_CLOUD_PARALLEL_STEPS)
                    .ok_or_else(|| {
                        usage(anyhow!(
                            "invalid value '{count}' for '--max-parallel <COUNT>': with --cloud, value must be between 1 and {MAXIMUM_CLOUD_PARALLEL_STEPS}"
                        ))
                    })
            })
            .transpose()?;
        let creation = Creation {
            organization,
            project_id,
            definition: RunDefinitionSelection {
                definition_id: id,
                workflow_path,
            },
            selection: self.cloud_creation.selection,
            inputs: self.inputs,
            integration_context_file: self.cloud_creation.integration_context_file,
            max_parallel,
            unowned: self.cloud_creation.no_owner,
            wait: self.cloud_creation.wait,
            options: RunOptions::new(
                self.presentation.output.json,
                self.cloud.authentication,
                self.cloud.http,
            ),
        };
        crate::cli::execute_deployment_command(
            Some(creation),
            &["workflow", "run", "create"],
            "configure Useful Machinery run creation",
            |creation, deployment| creation.submit(deployment.clone()),
        )
    }
}
