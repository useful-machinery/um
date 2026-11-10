mod answer;
mod approve;
mod child_recovery;
mod condition;
mod control;
mod create;
mod end;
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

pub(super) use super::{
    ColorArgument, ExistingLocalRun, LocalExecutionRoot, PresentationOptions,
    execute_with_abandonable_runtime, observe_workflow_signals,
};
pub(super) use create::acquire_agent_profile_replacement;

pub(super) const ABOUT: &str = "Create, control and inspect local workflow runs";
const COMMAND_PATH: &[&str] = &["workflow", "run"];

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(subcommand)]
    command: Option<RunCommand>,
}

#[derive(Debug, Subcommand)]
enum RunCommand {
    #[command(about = answer::ABOUT)]
    Answer(answer::Command),
    #[command(about = approve::APPROVE_ABOUT)]
    Approve(approve::Approve),
    #[command(about = end::CANCEL_ABOUT)]
    Cancel(end::Command),
    #[command(about = create::ABOUT, after_help = create::AFTER_HELP)]
    Create(Box<create::Command>),
    #[command(about = approve::HAND_BACK_ABOUT)]
    HandBack(approve::HandBack),
    #[command(about = stop::LIFT_ABOUT)]
    Lift(stop::Lift),
    #[command(about = list::ABOUT)]
    List(list::Command),
    #[command(about = end::RELEASE_ABOUT)]
    Release(end::Command),
    #[command(about = restart::ABOUT, after_help = restart::AFTER_HELP)]
    Restart(restart::Command),
    #[command(about = resume::ABOUT, after_help = resume::AFTER_HELP)]
    Resume(resume::Command),
    #[command(about = retry::ABOUT, after_help = retry::AFTER_HELP)]
    Retry(retry::Command),
    #[command(about = show::ABOUT)]
    Show(show::Command),
    #[command(about = status::ABOUT)]
    Status(status::Command),
    #[command(about = stop::STOP_ABOUT)]
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
            Some(RunCommand::HandBack(command)) => command.execute(),
            Some(RunCommand::Lift(command)) => command.execute(),
            Some(RunCommand::List(command)) => command.execute(),
            Some(RunCommand::Release(command)) => command.release(),
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
