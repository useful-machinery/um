use std::io::{self, Write};

use anyhow::Context;
use serde::Serialize;

use crate::cli::grant::consent::{GrantView, report};
use crate::exit_code::ExitCode;
use um_api::{GrantFailure, GrantList, GrantUse, GrantUseList};

type Stdout = io::StdoutLock<'static>;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct UseView<'a> {
    id: &'a str,
    grant_id: &'a str,
    run_id: &'a str,
    assignment_id: &'a str,
    organization_id: &'a str,
    provider: &'static str,
    issued_at: &'a str,
}

impl<'a> UseView<'a> {
    fn new(value: &'a GrantUse) -> Self {
        use um_api::provider_grant_use::Provider;
        Self {
            id: &value.id,
            grant_id: &value.grant_id,
            run_id: &value.run_id,
            assignment_id: &value.assignment_id,
            organization_id: &value.organization_id,
            provider: match value.provider {
                Provider::ProviderGrantUseProviderGithub => "github",
                Provider::ProviderGrantUseProviderGitlabCom => "gitlab.com",
                Provider::ProviderGrantUseProviderLinear => "linear",
            },
            issued_at: &value.issued_at,
        }
    }

    fn write_human(&self, output: &mut impl Write) -> io::Result<()> {
        writeln!(output, "use: {}", self.id)?;
        writeln!(output, "run: {}", self.run_id)?;
        writeln!(output, "assignment: {}", self.assignment_id)?;
        writeln!(output, "organization: {}", self.organization_id)?;
        writeln!(output, "provider: {}", self.provider)?;
        writeln!(output, "issued: {}", self.issued_at)
    }
}

pub(super) fn write_list(
    deployment: &str,
    page: &GrantList,
    json: bool,
) -> anyhow::Result<ExitCode> {
    let items: Vec<_> = page.items.iter().map(GrantView::new).collect();
    write_page(
        deployment,
        &items,
        page.next_cursor.as_deref(),
        json,
        |output| writeln!(output, "✓ Provider grants listed."),
        "grant",
        GrantView::write_human,
    )
}

pub(super) fn write_uses(
    deployment: &str,
    grant_id: &str,
    page: &GrantUseList,
    json: bool,
) -> anyhow::Result<ExitCode> {
    let items: Vec<_> = page.items.iter().map(UseView::new).collect();
    write_page(
        deployment,
        &items,
        page.next_cursor.as_deref(),
        json,
        |output| {
            writeln!(output, "✓ Provider grant uses listed.")?;
            writeln!(output, "grant: {grant_id}")
        },
        "use",
        UseView::write_human,
    )
}

fn write_page<T: Serialize>(
    deployment: &str,
    items: &[T],
    next_cursor: Option<&str>,
    json: bool,
    heading: impl FnOnce(&mut Stdout) -> io::Result<()>,
    section: &str,
    item: impl Fn(&T, &mut Stdout) -> io::Result<()>,
) -> anyhow::Result<ExitCode> {
    if json {
        write_json(&super::super::super::CloudListResult {
            schema_version: 1,
            deployment,
            outcome: "listed",
            items,
            next_cursor,
        })?;
        return Ok(ExitCode::Success);
    }
    let mut stdout = io::stdout().lock();
    heading(&mut stdout)?;
    writeln!(stdout, "deployment: {deployment}")?;
    writeln!(stdout, "count: {}", items.len())?;
    for value in items {
        writeln!(stdout, "\n── {section} ──")?;
        item(value, &mut stdout)?;
    }
    if let Some(next_cursor) = next_cursor {
        writeln!(stdout, "\nnext cursor: {next_cursor}")?;
    }
    stdout.flush()?;
    Ok(ExitCode::Success)
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RevokeResult<'a> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    grant_id: &'a str,
}

pub(super) fn write_revoke(
    deployment: &str,
    grant_id: &str,
    json: bool,
) -> anyhow::Result<ExitCode> {
    if json {
        write_json(&RevokeResult {
            schema_version: 1,
            deployment,
            outcome: "revoked",
            grant_id,
        })?;
    } else {
        let mut stdout = io::stdout().lock();
        writeln!(stdout, "✓ Provider grant revoked.")?;
        writeln!(stdout, "grant: {grant_id}")?;
        writeln!(stdout, "deployment: {deployment}")?;
    }
    Ok(ExitCode::Success)
}

/// Reports a grant operation's failure with the remedy a member can act on.
pub(in crate::cli::auth) fn failure(
    deployment: &str,
    failure: &GrantFailure,
    json: bool,
) -> anyhow::Result<ExitCode> {
    report(deployment, failure).write(deployment, json)
}

fn write_json(value: &impl Serialize) -> anyhow::Result<()> {
    super::super::super::write_pretty_json(value).context("write JSON provider grant result")
}
