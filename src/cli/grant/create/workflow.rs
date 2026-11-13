//! The plan `um grant create --workflow` prints before any sign-in (REQ-GRANT-76): the providers
//! a workflow tree calls for, derived by `um runner doctor --workflow`'s grouping
//! ([`Roots`](crate::cli::runner::doctor::roots::Roots)).

use std::io::{self, Write};

use anyhow::Context;
use serde::Serialize;

use crate::cli::runner::doctor::workflow::{
    self as doctor, JsonDeclaration, describe, group_label,
};
use um_runner::{CredentialRequirement, Declaration, WorkflowCredentials};

/// A provider the tree calls for: what the run does about it, and the requirement whose origins
/// call for it.
pub(super) struct Planned<'a> {
    pub(super) provider: &'static str,
    pub(super) state: State,
    pub(super) requirement: &'a CredentialRequirement,
}

/// What the run does about a provider the tree calls for (REQ-GRANT-76, REQ-GRANT-79).
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum State {
    /// Selected for a sign-in, since the member lacks it.
    SignIn,
    /// Signed in to again, since every grant held of it needs reconnecting and the member
    /// settled its replacement.
    Reconnect,
    /// Passed over as held, including a grant that needs reconnecting the member kept.
    Held,
}

impl State {
    const fn label(self) -> &'static str {
        match self {
            Self::SignIn => "sign in",
            Self::Reconnect => "reconnect",
            Self::Held => "held",
        }
    }
}

/// Prints, before any sign-in, the workflow files covered, each provider selected, each to
/// reconnect and each passed over as held with the origins that call for it, and what the
/// declarations cannot show: as the first JSON event, or as a report on standard output
/// (REQ-GRANT-76, REQ-GRANT-79).
pub(super) fn write_plan(
    deployment: &str,
    credentials: &WorkflowCredentials,
    planned: &[Planned<'_>],
    json: bool,
) -> anyhow::Result<()> {
    if json {
        let providers = |state: State| {
            planned
                .iter()
                .filter(|entry| entry.state == state)
                .map(|entry| JsonProvider {
                    provider: entry.provider,
                    origins: entry
                        .requirement
                        .origins
                        .iter()
                        .map(JsonDeclaration::from_declaration)
                        .collect(),
                })
                .collect()
        };
        return crate::cli::write_json_line(&JsonPlan {
            schema_version: 1,
            event: "workflow_requirements",
            deployment,
            workflows: &credentials.workflows,
            selected: providers(State::SignIn),
            reconnect: providers(State::Reconnect),
            held: providers(State::Held),
            unknown: credentials
                .unknown
                .iter()
                .map(JsonDeclaration::from_declaration)
                .collect(),
        })
        .context("write JSON grant event");
    }
    let mut stdout = io::stdout().lock();
    write_human_plan_to(&mut stdout, credentials, planned)?;
    stdout.flush()?;
    Ok(())
}

fn write_human_plan_to(
    output: &mut impl Write,
    credentials: &WorkflowCredentials,
    planned: &[Planned<'_>],
) -> io::Result<()> {
    for workflow in &credentials.workflows {
        writeln!(output, "workflow: {workflow}")?;
    }
    writeln!(output)?;
    for entry in planned {
        writeln!(output, "{}: {}", entry.state.label(), entry.provider)?;
        for origin in &entry.requirement.origins {
            write_declaration(output, "  needed by", origin)?;
        }
    }
    if !planned.is_empty() {
        writeln!(output)?;
    }
    if !credentials.unknown.is_empty() {
        writeln!(
            output,
            "These declarations may use credentials the workflows do not name, and no sign-in is run for them:"
        )?;
        for entry in &credentials.unknown {
            write_declaration(output, "unknown", entry)?;
        }
        writeln!(output)?;
    }
    Ok(())
}

/// `<key>: <workflow file> · <node> · <declaration> [<name>]`, as the doctor names a declaration.
fn write_declaration<K: um_runner::DeclarationKind + doctor::HumanLabel>(
    output: &mut impl Write,
    key: &str,
    entry: &Declaration<K>,
) -> io::Result<()> {
    writeln!(
        output,
        "{key}: {} · {}",
        group_label(entry),
        describe(entry)
    )
}

/// The first event of `um grant create --workflow --json`.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JsonPlan<'a> {
    schema_version: u8,
    event: &'static str,
    deployment: &'a str,
    workflows: &'a [String],
    selected: Vec<JsonProvider<'a>>,
    reconnect: Vec<JsonProvider<'a>>,
    held: Vec<JsonProvider<'a>>,
    unknown: Vec<JsonDeclaration<'a>>,
}

#[derive(Serialize)]
struct JsonProvider<'a> {
    provider: &'static str,
    origins: Vec<JsonDeclaration<'a>>,
}
