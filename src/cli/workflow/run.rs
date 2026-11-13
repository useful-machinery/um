mod answer;
mod approve;
mod child_recovery;
mod cloud;
mod condition;
mod control;
mod create;
mod end;
mod event;
mod interrupt;
mod list;
mod mirror;
mod restart;
mod resume;
mod retry;
mod show;
mod status;
mod stop;
mod successor;
mod upgrade;
mod view;

use clap::{Args, Subcommand};

use super::super::{HttpOptions, OrganizationArg, PrincipalAuthenticationArgs};
pub(super) use super::{
    ColorArgument, ExistingLocalRun, LocalExecutionRoot, PresentationOptions,
    execute_with_abandonable_runtime, observe_workflow_signals,
};
pub(super) use create::acquire_agent_profile_replacement;

/// `--cloud ORGANIZATION`, which sends a `um workflow run` leaf to Cloud in that organization
/// instead of the `um` home: `resume`, `show`, `release` and the control leaves `answer`,
/// `approve`, `hand-back`, `lift`, `stop` and `retry` to the Cloud run `RUN` names, and `create`
/// to a new Cloud run of the Cloud definition `ID` (REQ-RUN-94), with the authentication and
/// transport options only a Cloud request reads; a leaf's local-only options conflict with the
/// argument id `cloud`.
#[derive(Debug, Args)]
#[command(
    mut_arg("service_api_key_file", |arg| arg.requires("cloud")),
    mut_arg("allow_insecure_http", |arg| arg.requires("cloud"))
)]
pub(super) struct CloudSelector {
    #[arg(
        id = "cloud",
        long = "cloud",
        value_name = OrganizationArg::VALUE_NAME,
        help = "Act in Cloud in this organization (ID or exact slug) instead of locally"
    )]
    pub(super) organization: Option<OrganizationArg>,
    #[command(flatten)]
    pub(super) authentication: PrincipalAuthenticationArgs,
    #[command(flatten)]
    pub(super) http: HttpOptions,
}

pub(super) const ABOUT: &str =
    "Create, control and inspect local workflow runs, and Cloud runs with --cloud";
const COMMAND_PATH: &[&str] = &["workflow", "run"];

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(subcommand)]
    command: Option<RunCommand>,
}

#[derive(Debug, Subcommand)]
enum RunCommand {
    #[command(about = answer::ABOUT, after_help = cloud::CONTROL_AFTER_HELP)]
    Answer(answer::Command),
    #[command(about = approve::APPROVE_ABOUT, after_help = cloud::CONTROL_AFTER_HELP)]
    Approve(approve::Approve),
    #[command(about = end::CANCEL_ABOUT)]
    Cancel(end::Command),
    #[command(about = create::ABOUT, after_help = create::AFTER_HELP)]
    Create(Box<create::Command>),
    #[command(about = event::ABOUT)]
    Event(event::Command),
    #[command(about = approve::HAND_BACK_ABOUT, after_help = cloud::CONTROL_AFTER_HELP)]
    HandBack(approve::HandBack),
    #[command(about = interrupt::ABOUT)]
    Interrupt(interrupt::Command),
    #[command(about = stop::LIFT_ABOUT, after_help = cloud::CONTROL_AFTER_HELP)]
    Lift(stop::Lift),
    #[command(about = list::ABOUT)]
    List(list::Command),
    #[command(about = end::RELEASE_ABOUT, after_help = end::RELEASE_AFTER_HELP)]
    Release(end::Release),
    #[command(about = restart::ABOUT, after_help = restart::AFTER_HELP)]
    Restart(restart::Command),
    #[command(about = resume::ABOUT, after_help = resume::AFTER_HELP)]
    Resume(resume::Command),
    #[command(about = retry::ABOUT, after_help = retry::AFTER_HELP)]
    Retry(retry::Command),
    #[command(about = show::ABOUT, after_help = show::AFTER_HELP)]
    Show(show::Command),
    #[command(about = status::ABOUT)]
    Status(status::Command),
    #[command(about = stop::STOP_ABOUT, after_help = cloud::CONTROL_AFTER_HELP)]
    Stop(stop::Stop),
    #[command(about = upgrade::ABOUT, after_help = upgrade::AFTER_HELP)]
    Upgrade(upgrade::Command),
    #[command(about = view::ABOUT, after_help = view::AFTER_HELP)]
    View(view::Command),
    #[command(about = end::WITHDRAW_ABOUT)]
    Withdraw(end::Command),
}

impl Command {
    pub(super) fn execute(self) -> super::super::CommandResult {
        match self.command {
            None => super::super::print_help(COMMAND_PATH),
            Some(RunCommand::Answer(command)) => command.execute(),
            Some(RunCommand::Approve(command)) => command.execute(),
            Some(RunCommand::Cancel(command)) => command.cancel(),
            Some(RunCommand::Create(command)) => command.execute(),
            Some(RunCommand::Event(command)) => command.execute(),
            Some(RunCommand::HandBack(command)) => command.execute(),
            Some(RunCommand::Interrupt(command)) => command.execute(),
            Some(RunCommand::Lift(command)) => command.execute(),
            Some(RunCommand::List(command)) => command.execute(),
            Some(RunCommand::Release(command)) => command.execute(),
            Some(RunCommand::Restart(command)) => command.execute(),
            Some(RunCommand::Resume(command)) => command.execute(),
            Some(RunCommand::Retry(command)) => command.execute(),
            Some(RunCommand::Show(command)) => command.execute(),
            Some(RunCommand::Status(command)) => command.execute(),
            Some(RunCommand::Stop(command)) => command.execute(),
            Some(RunCommand::Upgrade(command)) => command.execute(),
            Some(RunCommand::View(command)) => command.execute(),
            Some(RunCommand::Withdraw(command)) => command.withdraw(),
        }
    }
}
