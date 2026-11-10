use std::io::{self, Write};
use std::path::PathBuf;

use anyhow::Context;
use clap::Args;
use serde::Serialize;

use super::report::{display, write_invalid, write_json};
use crate::exit_code::ExitCode;
use um_execution::{
    BindingsFile, PublishTarget, Published, WorkflowHome, publish, validate_workflow_tree,
};

pub(super) const ABOUT: &str = "Publish a local workflow tree as a definition revision";
const COMMAND_NAME: &str = "um workflow definition publish";
const REFUSED: &str = "Publish refused.";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[arg(
        value_name = "DIR",
        help = "Tree directory holding workflow.yaml, resolved from the initial working directory"
    )]
    directory: PathBuf,

    #[arg(
        long,
        value_name = "FILE",
        help = "JSON object of binding values, resolved from the initial working directory"
    )]
    bindings: Option<PathBuf>,

    #[arg(
        long,
        value_name = "ID",
        conflicts_with = "new",
        help = "Publish to this definition id, whatever its name"
    )]
    to: Option<String>,

    #[arg(long, help = "Create a new definition even when the name is taken")]
    new: bool,

    #[command(flatten)]
    output: super::super::super::JsonArgs<super::super::super::WorkflowJson>,
}

impl Command {
    pub(super) fn execute(self) -> super::super::super::CommandResult {
        let json = self.output.json;
        let tree = match validate_workflow_tree(&self.directory) {
            Ok(tree) => tree,
            Err(diagnostics) => {
                write_invalid(COMMAND_NAME, &self.directory, &diagnostics, json)
                    .context("write workflow definition publish result")?;
                return Ok(ExitCode::GeneralFailure);
            }
        };
        let target = match (self.to, self.new) {
            (Some(id), _) => PublishTarget::To(id),
            (None, true) => PublishTarget::New,
            (None, false) => PublishTarget::Resolve,
        };
        let published = WorkflowHome::from_environment().and_then(|home| {
            let bindings = self
                .bindings
                .as_deref()
                .map(BindingsFile::read)
                .transpose()?;
            publish(
                &home,
                &tree,
                target,
                bindings.as_ref(),
                &super::super::login_name(),
            )
        });
        super::finish(COMMAND_NAME, REFUSED, published, json, |published, json| {
            if json {
                write_json(&published_report(published), "workflow definition publish")
            } else {
                write_human_published(published)
            }
        })
    }
}

fn write_human_published(published: &Published) -> anyhow::Result<()> {
    let record = &published.record;
    let revision = &published.revision;
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    writeln!(
        stdout,
        "✓ Published {} revision {}.",
        display(&record.name),
        revision.n
    )?;
    writeln!(stdout, "definition: {}", display(&record.id))?;
    writeln!(stdout, "name: {}", display(&record.name))?;
    writeln!(stdout, "created: {}", super::yes_no(published.created))?;
    writeln!(stdout, "revision: {}", revision.n)?;
    writeln!(stdout, "tree: {}", display(&revision.tree))?;
    writeln!(stdout, "bindings: {}", display(&revision.bindings))?;
    writeln!(stdout, "published at: {}", display(&revision.published_at))?;
    writeln!(stdout, "by: {}", display(&revision.by))?;
    Ok(())
}

fn published_report(published: &Published) -> JsonPublished<'_> {
    let record = &published.record;
    let revision = &published.revision;
    JsonPublished {
        schema_version: 1,
        command: COMMAND_NAME,
        outcome: "published",
        definition: JsonDefinition {
            id: &record.id,
            name: &record.name,
            enabled: record.enabled,
            current: record.current,
            created: published.created,
            path: published.path.to_string_lossy().into_owned(),
        },
        revision: JsonRevision {
            n: revision.n,
            tree: &revision.tree,
            bindings: &revision.bindings,
            published_at: &revision.published_at,
            by: &revision.by,
        },
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JsonPublished<'a> {
    schema_version: u8,
    command: &'static str,
    outcome: &'static str,
    definition: JsonDefinition<'a>,
    revision: JsonRevision<'a>,
}

#[derive(Serialize)]
struct JsonDefinition<'a> {
    id: &'a str,
    name: &'a str,
    enabled: bool,
    current: u64,
    created: bool,
    path: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JsonRevision<'a> {
    n: u64,
    tree: &'a str,
    bindings: &'a str,
    published_at: &'a str,
    by: &'a str,
}
