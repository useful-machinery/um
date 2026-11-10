//! `um workflow definition enable` and `disable` (REQ-WFDEF-24): one command whose
//! target value the leaf supplies.

use std::io::{self, Write};

use clap::Args;
use serde::Serialize;

use super::report::{display, write_json};
use um_execution::{EnabledChange, WorkflowHome, set_enabled};

pub(super) const ENABLE_ABOUT: &str = "Enable a local workflow definition";
pub(super) const DISABLE_ABOUT: &str = "Disable a local workflow definition";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[arg(value_name = "ID", help = super::ID_HELP)]
    id: String,

    #[command(flatten)]
    output: super::super::super::JsonArgs<super::super::super::WorkflowJson>,
}

/// What one of the two leaves sets and how it names itself.
struct Leaf {
    enabled: bool,
    command: &'static str,
    outcome: &'static str,
    refused: &'static str,
    done: &'static str,
}

const ENABLE: Leaf = Leaf {
    enabled: true,
    command: "um workflow definition enable",
    outcome: "enabled",
    refused: "Enable refused.",
    done: "Enabled",
};

const DISABLE: Leaf = Leaf {
    enabled: false,
    command: "um workflow definition disable",
    outcome: "disabled",
    refused: "Disable refused.",
    done: "Disabled",
};

impl Command {
    pub(super) fn execute(self, enabled: bool) -> super::super::super::CommandResult {
        let leaf = if enabled { &ENABLE } else { &DISABLE };
        let result = WorkflowHome::from_environment()
            .and_then(|home| set_enabled(&home, &self.id, leaf.enabled));
        super::finish(
            leaf.command,
            leaf.refused,
            result,
            self.output.json,
            |change, json| {
                if json {
                    write_json(&enabled_report(leaf, change), "workflow definition state")
                } else {
                    write_human(leaf, change)
                }
            },
        )
    }
}

fn write_human(leaf: &Leaf, change: &EnabledChange) -> anyhow::Result<()> {
    let record = &change.definition.record;
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    if change.changed {
        writeln!(
            stdout,
            "✓ {} definition {}.",
            leaf.done,
            display(&record.name)
        )?;
    } else {
        writeln!(
            stdout,
            "✓ Definition {} is already {}.",
            display(&record.name),
            leaf.outcome
        )?;
    }
    writeln!(stdout, "definition: {}", display(&record.id))?;
    writeln!(stdout, "name: {}", display(&record.name))?;
    writeln!(stdout, "enabled: {}", super::yes_no(record.enabled))?;
    Ok(())
}

fn enabled_report<'a>(leaf: &Leaf, change: &'a EnabledChange) -> JsonEnabled<'a> {
    let record = &change.definition.record;
    JsonEnabled {
        schema_version: 1,
        command: leaf.command,
        outcome: leaf.outcome,
        definition: JsonDefinition {
            id: &record.id,
            name: &record.name,
            enabled: record.enabled,
            current: record.current,
            changed: change.changed,
            path: change.definition.path.to_string_lossy().into_owned(),
        },
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JsonEnabled<'a> {
    schema_version: u8,
    command: &'static str,
    outcome: &'static str,
    definition: JsonDefinition<'a>,
}

#[derive(Serialize)]
struct JsonDefinition<'a> {
    id: &'a str,
    name: &'a str,
    enabled: bool,
    current: u64,
    changed: bool,
    path: String,
}
