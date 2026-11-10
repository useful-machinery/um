//! `um workflow definition list` (REQ-WFDEF-25).

use std::io::{self, Write};

use clap::Args;
use serde::Serialize;

use super::report::{display, write_json};
use um_execution::{StoredDefinition, WorkflowHome, list_definitions};

pub(super) const ABOUT: &str = "List local workflow definitions";
const COMMAND_NAME: &str = "um workflow definition list";
const REFUSED: &str = "List refused.";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(flatten)]
    output: super::super::super::JsonArgs<super::super::super::WorkflowJson>,
}

impl Command {
    pub(super) fn execute(self) -> super::super::super::CommandResult {
        let result = WorkflowHome::from_environment().and_then(|home| list_definitions(&home));
        super::finish(
            COMMAND_NAME,
            REFUSED,
            result,
            self.output.json,
            |definitions, json| {
                if json {
                    write_json(&list_report(definitions), "workflow definition list")
                } else {
                    write_human(definitions)
                }
            },
        )
    }
}

fn published_at(definition: &StoredDefinition) -> Option<&str> {
    definition
        .record
        .current_revision()
        .map(|revision| revision.published_at.as_str())
}

/// One line per definition, `No workflow definitions.` for none.
fn write_human(definitions: &[StoredDefinition]) -> anyhow::Result<()> {
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    if definitions.is_empty() {
        writeln!(stdout, "No workflow definitions.")?;
        return Ok(());
    }
    for definition in definitions {
        let record = &definition.record;
        let count = record.revisions.len();
        write!(
            stdout,
            "{} · {} · {} · current {} · {} revision{}",
            display(&record.id),
            display(&record.name),
            if record.enabled {
                "enabled"
            } else {
                "disabled"
            },
            record.current,
            count,
            if count == 1 { "" } else { "s" }
        )?;
        if let Some(published_at) = published_at(definition) {
            write!(stdout, " · published at {}", display(published_at))?;
        }
        writeln!(stdout)?;
    }
    Ok(())
}

fn list_report(definitions: &[StoredDefinition]) -> JsonList<'_> {
    JsonList {
        schema_version: 1,
        command: COMMAND_NAME,
        definitions: definitions
            .iter()
            .map(|definition| JsonListed {
                id: &definition.record.id,
                name: &definition.record.name,
                enabled: definition.record.enabled,
                current: definition.record.current,
                revisions: definition.record.revisions.len(),
                published_at: published_at(definition),
                path: definition.path.to_string_lossy().into_owned(),
            })
            .collect(),
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JsonList<'a> {
    schema_version: u8,
    command: &'static str,
    definitions: Vec<JsonListed<'a>>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JsonListed<'a> {
    id: &'a str,
    name: &'a str,
    enabled: bool,
    current: u64,
    revisions: usize,
    published_at: Option<&'a str>,
    path: String,
}
