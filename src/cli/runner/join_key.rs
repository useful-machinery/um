use std::io::{self, Write};

use anyhow::{Context, anyhow};
use clap::{Args, Subcommand};
use serde::Serialize;
use zeroize::Zeroizing;

use super::{
    CloudOptions, OrganizationArg, PaginationArgs, PoolArg, cloud, completed_cloud_result,
};
use crate::exit_code::ExitCode;
use um_human_auth::Deployment;
use um_runner::ArtifactDestination;
use um_support::generate_idempotency_key;

pub(super) const ABOUT: &str = "Manage runner pool join keys";
const COMMAND_PATH: &[&str] = &["runner", "join-key"];
// Largest unit first, so a removal time prints as it is usually written.
const DURATION_UNITS: [(&str, u32); 3] = [("d", 86_400), ("h", 3_600), ("m", 60)];

// um-api does not name its join-key models, so these documents borrow them generically.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JoinKeyResult<'a, K> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    join_key: &'a K,
    #[serde(skip_serializing_if = "Option::is_none")]
    join_key_file: Option<&'a str>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JoinKeyListResult<'a, K> {
    schema_version: u8,
    deployment: &'a str,
    outcome: &'static str,
    items: &'a [K],
    #[serde(skip_serializing_if = "Option::is_none")]
    next_cursor: Option<&'a str>,
}

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(subcommand)]
    command: Option<JoinKeyCommand>,
}

#[derive(Debug, Subcommand)]
enum JoinKeyCommand {
    #[command(
        about = "Issue a runner pool join key",
        after_help = "Join key:\n  Any number of hosts can join the pool with one key until it expires or is revoked."
    )]
    Issue(IssueCommand),
    #[command(about = "List runner pool join keys")]
    List(ListCommand),
    #[command(
        about = "Revoke a runner pool join key",
        after_help = "Revocation:\n  The runners that joined with the key stay in the pool."
    )]
    Revoke(RevokeCommand),
}

#[derive(Debug, Args)]
struct IssueCommand {
    #[arg(value_name = OrganizationArg::VALUE_NAME, help = OrganizationArg::HELP)]
    organization: OrganizationArg,

    // With no pool named, the key is for the caller's own personal pool.
    #[arg(long = "pool-id", value_name = PoolArg::VALUE_NAME, help = PoolArg::HELP)]
    pool: Option<PoolArg>,

    #[arg(
        long,
        value_name = "DURATION",
        value_parser = parse_expiry,
        help = "Set how long the key admits hosts, from 1m to 365d"
    )]
    expires_in: i32,

    #[arg(
        long,
        value_name = "DURATION",
        value_parser = parse_removal_time,
        help = "Set how long a runner that joined may stay offline before removal, from 5m to 30d"
    )]
    remove_after: i32,

    #[arg(
        long,
        value_name = "PATH|-",
        help = "Create the protected join key file; with - or when omitted, write only the join key to stdout"
    )]
    join_key_file: Option<String>,

    #[command(flatten)]
    options: CloudOptions,
}

#[derive(Debug, Args)]
struct ListCommand {
    #[arg(value_name = OrganizationArg::VALUE_NAME, help = OrganizationArg::HELP)]
    organization: OrganizationArg,

    #[arg(long = "pool-id", value_name = PoolArg::VALUE_NAME, help = PoolArg::HELP)]
    pool: Option<PoolArg>,

    #[command(flatten)]
    pagination: PaginationArgs,

    #[command(flatten)]
    options: CloudOptions,
}

#[derive(Debug, Args)]
struct RevokeCommand {
    #[arg(value_name = OrganizationArg::VALUE_NAME, help = OrganizationArg::HELP)]
    organization: OrganizationArg,

    #[arg(value_name = "KEY", value_parser = parse_join_key_id, help = "Runner join key ID")]
    join_key: String,

    #[command(flatten)]
    confirmation: super::super::ConfirmationArgs,

    #[command(flatten)]
    options: CloudOptions,
}

impl Command {
    pub(super) fn execute(self) -> super::super::CommandResult {
        let Some(command) = self.command else {
            return super::super::print_help(COMMAND_PATH);
        };
        super::execute_cloud(command, |command, deployment| match command {
            JoinKeyCommand::Issue(command) => command.execute(deployment),
            JoinKeyCommand::List(command) => command.execute(deployment),
            JoinKeyCommand::Revoke(command) => command.execute(deployment),
        })
    }
}

impl IssueCommand {
    fn execute(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        // The file exists before the key does, so a refused path costs no key.
        let destination = prepare_destination(self.join_key_file.as_deref(), self.options.json)?;
        let key = generate_idempotency_key().context("generate join key request identity")?;
        let result = cloud::with_api(
            deployment,
            self.options.http.transport_policy(),
            &self.options.authentication,
            |api| {
                let pool_id = match &self.pool {
                    Some(pool) => pool.resolve_id(api, &self.organization)?,
                    None => api.get_personal_pool(&self.organization)?.id,
                };
                api.issue_join_key(
                    &self.organization,
                    &pool_id,
                    &key,
                    self.expires_in,
                    self.remove_after,
                )
            },
        )?;
        let issuance = match completed_cloud_result(
            deployment,
            result,
            self.options.authentication.kind(),
            self.options.json,
        )? {
            Ok(issuance) => issuance,
            Err(exit_code) => return Ok(exit_code),
        };
        let artifact = issuance.artifact.as_deref().ok_or_else(|| {
            anyhow!(
                "join key issuance replay omitted its secret; issue a replacement join key with a new command"
            )
        })?;
        let join_key_file = destination
            .file_path()
            .map(|path| path.display().to_string());
        write_artifact(destination, artifact)?;
        let join_key = &*issuance.join_key;
        let Some(join_key_file) = join_key_file else {
            // Standard output carries the key alone, so the status goes to stderr.
            writeln!(
                io::stderr().lock(),
                "✓ Runner join key {} issued for pool {}.",
                join_key.id,
                join_key.runner_pool_id
            )?;
            return Ok(ExitCode::Success);
        };
        if self.options.json {
            cloud::write_json(&JoinKeyResult {
                schema_version: 1,
                deployment: deployment.fingerprint().api_url(),
                outcome: "issued",
                join_key,
                join_key_file: Some(&join_key_file),
            })?;
        } else {
            let stdout = io::stdout();
            let mut stdout = stdout.lock();
            writeln!(stdout, "✓ Runner join key issued.\n")?;
            writeln!(stdout, "  Join key:      {}", join_key.id)?;
            writeln!(stdout, "  Pool:          {}", join_key.runner_pool_id)?;
            writeln!(stdout, "  Expires:       {}", join_key.expires_at)?;
            writeln!(
                stdout,
                "  Remove after:  {}",
                duration_text(join_key.offline_removal_seconds)
            )?;
            writeln!(stdout, "  Join key file: {join_key_file}")?;
        }
        Ok(ExitCode::Success)
    }
}

impl ListCommand {
    fn execute(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let result = cloud::with_api(
            deployment,
            self.options.http.transport_policy(),
            &self.options.authentication,
            |api| {
                let pool_id = self
                    .pool
                    .as_ref()
                    .map(|pool| pool.resolve_id(api, &self.organization))
                    .transpose()?;
                api.list_join_keys(
                    &self.organization,
                    pool_id.as_deref(),
                    self.pagination.limit,
                    self.pagination.cursor.as_deref(),
                )
            },
        )?;
        let page = match result {
            Ok(page) => page,
            Err(failure) => return self.options.write_failure(deployment, &failure),
        };
        let api_url = deployment.fingerprint().api_url();
        if self.options.json {
            cloud::write_json(&JoinKeyListResult {
                schema_version: 1,
                deployment: api_url,
                outcome: "listed",
                items: &page.items,
                next_cursor: page.next_cursor.as_deref(),
            })?;
            return Ok(ExitCode::Success);
        }
        let stdout = io::stdout();
        let mut stdout = stdout.lock();
        writeln!(stdout, "✓ Runner join keys listed.\n")?;
        for join_key in &page.items {
            writeln!(
                stdout,
                "  Join key: {}  Pool: {}  Issuer: {}  State: {}  Expires: {}  Remove after: {}",
                join_key.id,
                join_key.runner_pool_id,
                join_key.issued_by_principal_id,
                cloud::enum_text(&join_key.state)?,
                join_key.expires_at,
                duration_text(join_key.offline_removal_seconds)
            )?;
        }
        if !page.items.is_empty() {
            writeln!(stdout)?;
        }
        if let Some(cursor) = &page.next_cursor {
            writeln!(stdout, "  Next cursor: {cursor}")?;
        }
        writeln!(stdout, "  Deployment: {api_url}")?;
        Ok(ExitCode::Success)
    }
}

impl RevokeCommand {
    fn execute(self, deployment: &Deployment) -> anyhow::Result<ExitCode> {
        let key = generate_idempotency_key().context("generate join key revocation identity")?;
        let result = cloud::with_api(
            deployment,
            self.options.http.transport_policy(),
            &self.options.authentication,
            |api| api.revoke_join_key(&self.organization, &self.join_key, &key),
        )?;
        let join_key = match result {
            Ok(join_key) => join_key,
            Err(failure) => return self.options.write_failure(deployment, &failure),
        };
        let api_url = deployment.fingerprint().api_url();
        if self.options.json {
            cloud::write_json(&JoinKeyResult {
                schema_version: 1,
                deployment: api_url,
                outcome: "revoked",
                join_key: &join_key,
                join_key_file: None,
            })?;
        } else {
            writeln!(
                io::stdout().lock(),
                "✓ Runner join key revoked.\n\n  Join key:   {}\n  Deployment: {api_url}",
                join_key.id
            )?;
        }
        Ok(ExitCode::Success)
    }
}

/// Prepares where an issued join key goes (REQ-RUNNERENROLL-93): no
/// `--join-key-file`, like `-`, means standard output, which structured
/// output never shares with a token; a path is a new file only its owner can
/// read.
fn prepare_destination(
    destination: Option<&str>,
    json: bool,
) -> anyhow::Result<ArtifactDestination> {
    match destination {
        None if json => Err(anyhow!("--json requires --join-key-file PATH")),
        Some("-") if json => Err(anyhow!("--json cannot be combined with --join-key-file -")),
        None => Ok(ArtifactDestination::Stdout),
        Some(path) => ArtifactDestination::prepare(path)
            .with_context(|| format!("create join key file {path}")),
    }
}

/// Writes the artifact as one line of JSON, so the same text can also be
/// held in an environment variable.
fn write_artifact(
    destination: ArtifactDestination,
    artifact: &impl Serialize,
) -> anyhow::Result<()> {
    let mut line = Zeroizing::new(serde_json::to_vec(artifact).context("serialize join key")?);
    line.push(b'\n');
    let path = destination
        .file_path()
        .map(|path| path.display().to_string());
    destination.write(&line).with_context(|| match path {
        Some(path) => format!("write join key file {path}"),
        None => "write join key to stdout".to_owned(),
    })
}

fn parse_expiry(value: &str) -> Result<i32, String> {
    parse_duration_within(value, 60..=31_536_000, "from 1m to 365d")
}

fn parse_removal_time(value: &str) -> Result<i32, String> {
    parse_duration_within(value, 300..=2_592_000, "from 5m to 30d")
}

/// Reads a whole number of minutes, hours or days, such as `30m`, `12h` or
/// `7d`, as seconds within `bounds`.
fn parse_duration_within(
    value: &str,
    bounds: std::ops::RangeInclusive<u32>,
    range: &str,
) -> Result<i32, String> {
    let invalid = || {
        format!("must be a whole number of minutes, hours or days, {range}, such as 30m, 12h or 7d")
    };
    let (quantity, unit_seconds) =
        super::super::split_unit_suffix(value, &DURATION_UNITS).ok_or_else(invalid)?;
    if quantity.is_empty() || !quantity.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(invalid());
    }
    quantity
        .parse::<u32>()
        .ok()
        .and_then(|quantity| quantity.checked_mul(unit_seconds))
        .filter(|seconds| bounds.contains(seconds))
        .and_then(|seconds| i32::try_from(seconds).ok())
        .ok_or_else(invalid)
}

fn duration_text(seconds: i32) -> String {
    let Ok(unsigned) = u32::try_from(seconds) else {
        return format!("{seconds}s");
    };
    DURATION_UNITS
        .into_iter()
        .find(|(_, unit_seconds)| unsigned % unit_seconds == 0)
        .map_or_else(
            || format!("{seconds}s"),
            |(suffix, unit_seconds)| format!("{}{suffix}", unsigned / unit_seconds),
        )
}

fn parse_join_key_id(value: &str) -> Result<String, String> {
    if um_support::valid_typed_id(value, "rjk_") {
        Ok(value.to_owned())
    } else {
        Err("must be an exact runner join key ID".to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // REQ-RUNNERENROLL-93
    #[test]
    fn join_key_durations_read_minutes_hours_and_days_within_their_bounds() {
        assert_eq!(parse_expiry("1m"), Ok(60));
        assert_eq!(parse_expiry("7d"), Ok(604_800));
        assert_eq!(parse_expiry("365d"), Ok(31_536_000));
        assert_eq!(parse_removal_time("5m"), Ok(300));
        assert_eq!(parse_removal_time("30d"), Ok(2_592_000));
        for refused in [
            "",
            "d",
            "0m",
            "366d",
            "60",
            "60s",
            "+5m",
            "1.5h",
            "4294967296m",
            "7dé",
        ] {
            assert!(parse_expiry(refused).is_err(), "{refused:?}");
        }
        assert!(parse_removal_time("4m").is_err());
        assert!(parse_removal_time("31d").is_err());
        assert_eq!(duration_text(3_600), "1h");
        assert_eq!(duration_text(604_800), "7d");
        assert_eq!(duration_text(5_400), "90m");
        assert_eq!(duration_text(301), "301s");
    }
}
