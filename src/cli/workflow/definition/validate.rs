use std::collections::BTreeMap;
use std::io::{self, Write};
use std::path::PathBuf;

use anyhow::Context;
use clap::Args;
use serde::Serialize;

use super::report::{JsonTree, display, display_path, write_invalid, write_json};
use crate::exit_code::ExitCode;
use um_execution::{ValidatedTree, validate_workflow_tree};

pub(super) const ABOUT: &str = "Validate a local workflow tree and print its digest";
const COMMAND_NAME: &str = "um workflow definition validate";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[arg(
        value_name = "DIR",
        help = "Tree directory holding workflow.yaml, resolved from the initial working directory"
    )]
    directory: PathBuf,

    #[command(flatten)]
    output: super::super::super::JsonArgs<super::super::super::WorkflowJson>,
}

impl Command {
    pub(super) fn execute(self) -> super::super::super::CommandResult {
        let (result, exit_code) = match validate_workflow_tree(&self.directory) {
            Ok(tree) => (
                if self.output.json {
                    write_json(&valid_report(&tree), "workflow tree validation result")
                } else {
                    write_human_valid(&tree)
                },
                ExitCode::Success,
            ),
            Err(diagnostics) => (
                write_invalid(
                    COMMAND_NAME,
                    &self.directory,
                    &diagnostics,
                    self.output.json,
                ),
                ExitCode::GeneralFailure,
            ),
        };
        result.context("write workflow tree validation result")?;
        Ok(exit_code)
    }
}

fn display_list(values: &[String]) -> String {
    if values.is_empty() {
        return "none".to_owned();
    }
    values
        .iter()
        .map(|value| display(value))
        .collect::<Vec<_>>()
        .join(", ")
}

fn write_human_valid(tree: &ValidatedTree) -> anyhow::Result<()> {
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    writeln!(stdout, "✓ Workflow tree is valid.")?;
    writeln!(stdout, "tree: {}", display_path(&tree.root))?;
    if let Some(name) = &tree.name {
        writeln!(stdout, "name: {}", display(name))?;
    }
    if let Some(version) = &tree.version {
        writeln!(stdout, "version: {}", display(version))?;
    }
    writeln!(
        stdout,
        "digest: {}:{}",
        tree.digest.algorithm.as_str(),
        tree.digest.value
    )?;
    writeln!(
        stdout,
        "workflow files: {}",
        display_list(&tree.workflow_files)
    )?;
    writeln!(stdout, "programs: {}", display_list(&tree.programs))?;
    writeln!(
        stdout,
        "required tools: {}",
        display_list(&tree.required_tools)
    )?;
    writeln!(
        stdout,
        "triggers:{}",
        if tree.triggers.is_empty() {
            " none"
        } else {
            ""
        }
    )?;
    for (name, trigger) in &tree.triggers {
        writeln!(
            stdout,
            "  {}: {} on {} {}",
            display(name),
            trigger.kind.as_str(),
            display(&trigger.source),
            display(&trigger.event)
        )?;
    }
    writeln!(
        stdout,
        "bindings:{}",
        if tree.bindings.is_empty() {
            " none"
        } else {
            ""
        }
    )?;
    for (key, binding) in &tree.bindings {
        let value = binding.default.as_deref().map_or_else(
            || "(required)".to_owned(),
            |default| format!("(default \"{}\")", display(default)),
        );
        writeln!(
            stdout,
            "  {}: {} {value}",
            display(key),
            binding.kind.as_str()
        )?;
    }
    Ok(())
}

fn valid_report(tree: &ValidatedTree) -> JsonReport<'_> {
    JsonReport {
        schema_version: 1,
        command: COMMAND_NAME,
        tree: JsonTree::new(&tree.root),
        outcome: "valid",
        name: tree.name.as_deref(),
        version: tree.version.as_deref(),
        digest: JsonDigest {
            algorithm: tree.digest.algorithm.as_str(),
            value: &tree.digest.value,
        },
        workflow_files: &tree.workflow_files,
        programs: &tree.programs,
        requires_tools: &tree.required_tools,
        triggers: tree
            .triggers
            .iter()
            .map(|(name, trigger)| {
                (
                    name.as_str(),
                    JsonTrigger {
                        kind: trigger.kind.as_str(),
                        source: &trigger.source,
                        event: &trigger.event,
                    },
                )
            })
            .collect(),
        bindings: tree
            .bindings
            .iter()
            .map(|(key, binding)| {
                (
                    key.as_str(),
                    JsonBinding {
                        kind: binding.kind.as_str(),
                        default: binding.default.as_deref(),
                        required: binding.default.is_none(),
                    },
                )
            })
            .collect(),
    }
}

/// `outcome` `valid`; an invalid tree is `report::write_invalid`'s.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JsonReport<'a> {
    schema_version: u8,
    command: &'static str,
    tree: JsonTree,
    outcome: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    version: Option<&'a str>,
    digest: JsonDigest<'a>,
    workflow_files: &'a [String],
    programs: &'a [String],
    requires_tools: &'a [String],
    triggers: BTreeMap<&'a str, JsonTrigger<'a>>,
    bindings: BTreeMap<&'a str, JsonBinding<'a>>,
}

#[derive(Serialize)]
struct JsonDigest<'a> {
    algorithm: &'static str,
    value: &'a str,
}

#[derive(Serialize)]
struct JsonTrigger<'a> {
    kind: &'static str,
    source: &'a str,
    event: &'a str,
}

#[derive(Serialize)]
struct JsonBinding<'a> {
    kind: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    default: Option<&'a str>,
    required: bool,
}
