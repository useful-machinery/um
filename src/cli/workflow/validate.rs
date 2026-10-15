use std::collections::BTreeMap;
use std::io::{self, Write};
use std::path::PathBuf;

use anyhow::Context;
use clap::Args;
use serde::Serialize;

use crate::exit_code::ExitCode;
use um_execution::{
    CancellationSource, RejectionDiagnostic as Diagnostic, ReplacedAgentProfileV1,
    ReplacedHarnessV1, ResolutionFailure, ResolvedWorkflow, human_resolution_remedy,
    resolve_workflow_file_with_agent_profiles,
};

pub(super) const ABOUT: &str = "Validate a local workflow definition";
const COMMAND_NAME: &str = "um workflow validate";

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(flatten)]
    source: super::LocalWorkflowSource,

    #[arg(
        long,
        value_name = "PATH",
        help = "Replace declared agent profiles with the closed replacement document at PATH"
    )]
    agent_profiles: Option<PathBuf>,

    #[command(flatten)]
    output: super::super::JsonArgs<super::super::WorkflowJson>,
}

impl Command {
    pub(super) fn execute(self) -> super::super::CommandResult {
        let replacement = match &self.agent_profiles {
            Some(path) => Some(super::run::acquire_agent_profile_replacement(
                path,
                &CancellationSource::new(),
            )?),
            None => None,
        };
        match resolve_workflow_file_with_agent_profiles(
            &self.source.source_root,
            &self.source.workflow_file,
            replacement.as_ref(),
        ) {
            Ok(workflow) => {
                let result = if self.output.json {
                    write_json_valid(&workflow)
                } else {
                    write_human_valid(&workflow)
                };
                finish_output(result, ExitCode::Success)
            }
            Err(failure) => {
                let result = if self.output.json {
                    write_json_invalid(&failure)
                } else {
                    write_human_invalid(&failure)
                };
                finish_output(result, ExitCode::GeneralFailure)
            }
        }
    }
}

fn finish_output(result: anyhow::Result<()>, exit_code: ExitCode) -> super::super::CommandResult {
    result.context("write workflow validation result")?;
    Ok(exit_code)
}

fn write_human_valid(workflow: &ResolvedWorkflow) -> anyhow::Result<()> {
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    writeln!(stdout, "✓ Workflow definition is valid.")?;
    writeln!(stdout, "workflow: {}", workflow.source.workflow_path)?;
    writeln!(
        stdout,
        "digest: {}:{}",
        workflow.content_digest.algorithm.as_str(),
        workflow.content_digest.value
    )?;
    writeln!(stdout, "steps: {}", workflow.definition.steps.len())?;
    writeln!(
        stdout,
        "finalizers: {}",
        workflow.definition.finalizers.len()
    )?;
    writeln!(
        stdout,
        "required inputs: {}",
        human_required_inputs(workflow)
    )?;
    if let Some(replacement) = &workflow.agent_profile_replacement {
        writeln!(
            stdout,
            "replaced agent profiles: {}",
            replacement
                .profiles
                .keys()
                .map(String::as_str)
                .collect::<Vec<_>>()
                .join(", ")
        )?;
    }
    Ok(())
}

fn write_human_invalid(failure: &ResolutionFailure) -> anyhow::Result<()> {
    let diagnostic = Diagnostic::from_resolution(failure);
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    writeln!(stdout, "✗ Workflow definition is invalid.")?;
    if let Some(workflow_path) = failure.workflow_path() {
        writeln!(stdout, "workflow: {workflow_path}")?;
    }
    writeln!(stdout, "code: {}", diagnostic.code)?;
    writeln!(stdout, "location: {}", diagnostic.location)?;
    writeln!(stdout, "No workflow steps were executed.")?;
    writeln!(stdout)?;
    writeln!(stdout, "{}", human_resolution_remedy(failure))?;
    Ok(())
}

fn human_required_inputs(workflow: &ResolvedWorkflow) -> String {
    if workflow.required_inputs().is_empty() {
        return "none".to_owned();
    }
    workflow
        .required_inputs()
        .iter()
        .map(|(name, kind)| format!("{name}:{}", input_kind(*kind)))
        .collect::<Vec<_>>()
        .join(", ")
}

fn input_kind(kind: um_execution::WorkflowValueType) -> &'static str {
    match kind {
        um_execution::WorkflowValueType::Text => "text",
        um_execution::WorkflowValueType::AttachmentCollection => "attachments",
        um_execution::WorkflowValueType::Json => "json",
        um_execution::WorkflowValueType::File => "file",
        um_execution::WorkflowValueType::GitBranch => "git_branch",
    }
}

fn write_json_valid(workflow: &ResolvedWorkflow) -> anyhow::Result<()> {
    let required_inputs = workflow
        .required_inputs()
        .iter()
        .map(|(name, kind)| (name.as_str(), input_kind(*kind)))
        .collect::<BTreeMap<_, _>>();
    let report = JsonReport {
        schema_version: 1,
        command: COMMAND_NAME,
        result: JsonResult::Valid {
            workflow: WorkflowIdentity {
                path: &workflow.source.workflow_path,
            },
            digest: JsonDigest {
                algorithm: workflow.content_digest.algorithm.as_str(),
                value: &workflow.content_digest.value,
            },
            step_count: workflow.definition.steps.len(),
            finalizer_count: workflow.definition.finalizers.len(),
            required_inputs,
            replaced_agent_profiles: workflow.agent_profile_replacement.as_ref().map(
                |replacement| {
                    replacement
                        .profiles
                        .iter()
                        .map(|(name, harness)| {
                            (
                                name.as_str(),
                                ReplacedAgentProfileV1 {
                                    harness: ReplacedHarnessV1::from(harness),
                                },
                            )
                        })
                        .collect()
                },
            ),
        },
    };
    write_json(&report)
}

fn write_json_invalid(failure: &ResolutionFailure) -> anyhow::Result<()> {
    let report = JsonReport {
        schema_version: 1,
        command: COMMAND_NAME,
        result: JsonResult::Invalid {
            workflow: failure
                .workflow_path()
                .map(|path| WorkflowIdentity { path }),
            diagnostics: [Diagnostic::from_resolution(failure)],
        },
    };
    write_json(&report)
}

fn write_json(report: &JsonReport<'_>) -> anyhow::Result<()> {
    super::super::write_pretty_json(report).context("write workflow validation result")
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JsonReport<'a> {
    schema_version: u8,
    command: &'static str,
    #[serde(flatten)]
    result: JsonResult<'a>,
}

#[derive(Serialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
enum JsonResult<'a> {
    Valid {
        workflow: WorkflowIdentity<'a>,
        digest: JsonDigest<'a>,
        #[serde(rename = "stepCount")]
        step_count: usize,
        #[serde(rename = "finalizerCount")]
        finalizer_count: usize,
        #[serde(rename = "requiredInputs")]
        required_inputs: BTreeMap<&'a str, &'static str>,
        #[serde(
            rename = "replacedAgentProfiles",
            skip_serializing_if = "Option::is_none"
        )]
        replaced_agent_profiles: Option<BTreeMap<&'a str, ReplacedAgentProfileV1>>,
    },
    Invalid {
        workflow: Option<WorkflowIdentity<'a>>,
        diagnostics: [Diagnostic<'a>; 1],
    },
}

#[derive(Serialize)]
struct WorkflowIdentity<'a> {
    path: &'a str,
}

#[derive(Serialize)]
struct JsonDigest<'a> {
    algorithm: &'static str,
    value: &'a str,
}
