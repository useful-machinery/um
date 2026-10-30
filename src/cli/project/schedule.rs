mod config;
mod management;

use clap::{Args, Subcommand};

use super::Options;

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(subcommand)]
    command: Option<Leaf>,
}
#[derive(Debug, Subcommand)]
enum Leaf {
    #[command(about = "Create a schedule")]
    Create(management::Create),
    #[command(about = "Delete a schedule")]
    Delete(management::Delete),
    #[command(about = "Disable a schedule")]
    Disable(management::Target),
    #[command(about = "Enable a schedule")]
    Enable(management::Target),
    #[command(about = "List schedules")]
    List(management::List),
    #[command(about = "Manage a schedule's occurrences")]
    Occurrence(OccurrenceCommand),
    #[command(about = "Show a schedule")]
    Show(management::Target),
    #[command(about = "Update a schedule")]
    Update(management::Update),
}
#[derive(Debug, Args)]
struct OccurrenceCommand {
    #[command(subcommand)]
    command: Option<OccurrenceLeaf>,
}
#[derive(Debug, Subcommand)]
enum OccurrenceLeaf {
    #[command(about = "List retained occurrences")]
    List(management::OccurrenceList),
    #[command(about = "Show an occurrence")]
    Show(management::OccurrenceTarget),
}

impl Command {
    pub(super) fn execute(self) -> super::super::CommandResult {
        use management::{Management, execute};
        match self.command {
            None => super::super::print_help(&["project", "schedule"]),
            Some(Leaf::Create(cmd)) => execute(cmd, Management::Create),
            Some(Leaf::List(cmd)) => execute(cmd, Management::List),
            Some(Leaf::Show(cmd)) => execute(cmd, Management::Show),
            Some(Leaf::Update(cmd)) => execute(cmd, Management::Update),
            Some(Leaf::Enable(cmd)) => execute(cmd, Management::Enable),
            Some(Leaf::Disable(cmd)) => execute(cmd, Management::Disable),
            Some(Leaf::Delete(cmd)) => execute(cmd, Management::Delete),
            Some(Leaf::Occurrence(cmd)) => match cmd.command {
                None => super::super::print_help(&["project", "schedule", "occurrence"]),
                Some(OccurrenceLeaf::List(cmd)) => execute(cmd, Management::OccurrenceList),
                Some(OccurrenceLeaf::Show(cmd)) => execute(cmd, Management::OccurrenceShow),
            },
        }
    }
}
