//! Command plumbing shared by the project resources managed through the public API: the
//! deployment entry point, the authenticated API session, the closed result envelope, the
//! single-key mutation with its lost-response replay, the shared update and delete
//! arguments, configuration reading and configuration refusals.

use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, anyhow};
use clap::Args;
use serde::Serialize;
use serde_json::Value;
use um_api::{HttpClient, HttpTransportPolicy, ResourceFailure};
use um_human_auth::Deployment;

use super::super::{self as cli, ObservationControl};
use super::Options;
use crate::exit_code::ExitCode;

/// The words a resource's commands report with, held as a `const` by each resource module.
pub(super) struct Vocabulary {
    /// The command path, whose last segment names the resource in diagnostics.
    pub(super) path: &'static [&'static str],
    /// The deployment command's error context.
    pub(super) purpose: &'static str,
    /// The resource's name in plain reports.
    pub(super) noun: &'static str,
    /// The plain-report name of each JSON field that does not hold the resource itself.
    pub(super) nouns: &'static [(&'static str, &'static str)],
    /// The signal runtime's context for a mutation.
    pub(super) mutation: &'static str,
    /// The context of a networking setup failure.
    pub(super) networking: &'static str,
}

impl Vocabulary {
    fn subject(&self) -> &'static str {
        self.path.last().copied().unwrap_or(self.noun)
    }

    fn name(&self, field: &str) -> &'static str {
        self.nouns
            .iter()
            .find(|(candidate, _)| *candidate == field)
            .map_or(self.noun, |(_, name)| name)
    }
}

pub(super) trait Run<M> {
    fn run(self, d: Deployment, action: M) -> cli::CommandResult;
}

/// `delete`: the resource's target, confirmed by `--yes`.
#[derive(Debug, Args)]
pub(super) struct Delete<T: Args> {
    #[command(flatten)]
    target: T,
    #[command(flatten)]
    confirmation: cli::ConfirmationArgs,
}

impl<M, T: Args + Run<M>> Run<M> for Delete<T> {
    fn run(self, d: Deployment, action: M) -> cli::CommandResult {
        let _ = self.confirmation;
        self.target.run(d, action)
    }
}

/// `update`: the resource's target, the replacement fields and the version they replace.
#[derive(Debug, Args)]
pub(super) struct Update<T: Args> {
    #[command(flatten)]
    pub(super) target: T,
    #[arg(
        long,
        value_name = "PATH",
        help = "Read replacement configuration fields from a JSON file or - for stdin"
    )]
    pub(super) config_file: PathBuf,
    #[arg(long, value_name = "VERSION", value_parser = clap::value_parser!(i32).range(1..), help = "Required current configuration version")]
    pub(super) expected_version: i32,
}

/// The most a `--config-file` may hold.
const CONFIGURATION_LIMIT: usize = 1024 * 1024;

/// Reads `--config-file PATH|-` up to 1 MiB, refusing `-` when authentication also
/// reads standard input.
pub(super) fn read_configuration(path: &Path, stdin_claimed: bool) -> anyhow::Result<Vec<u8>> {
    if path == Path::new("-") && stdin_claimed {
        return Err(anyhow!(
            "configuration and authentication cannot both read stdin"
        ));
    }
    let mut bytes = Vec::new();
    let limit = CONFIGURATION_LIMIT as u64 + 1;
    if path == Path::new("-") {
        io::stdin().take(limit).read_to_end(&mut bytes)?;
    } else {
        std::fs::File::open(path)?
            .take(limit)
            .read_to_end(&mut bytes)?;
    }
    if bytes.len() > CONFIGURATION_LIMIT {
        return Err(anyhow!("configuration exceeds 1 MiB"));
    }
    Ok(bytes)
}

/// Parses a configuration and checks it with the resource's grammar, told whether it is
/// an update; an update, which carries its `expected` version, gains `expectedVersion`.
pub(super) fn parse_configuration(
    bytes: &[u8],
    expected: Option<i32>,
    validate: impl FnOnce(&Value, bool) -> anyhow::Result<()>,
) -> anyhow::Result<Value> {
    let mut value: Value =
        serde_json::from_slice(bytes).map_err(|_| anyhow!("invalid configuration JSON"))?;
    validate(&value, expected.is_some())?;
    if let Some(version) = expected {
        if version < 1 {
            return Err(anyhow!("expected version must be positive"));
        }
        value
            .as_object_mut()
            .ok_or_else(|| anyhow!("invalid configuration"))?
            .insert("expectedVersion".into(), version.into());
    }
    Ok(value)
}

pub(super) fn execute<M, T: Run<M>>(
    vocabulary: &'static Vocabulary,
    cmd: T,
    action: M,
) -> cli::CommandResult {
    cli::execute_deployment_command(
        Some(cmd),
        vocabulary.path,
        vocabulary.purpose,
        |cmd, deployment| cmd.run(deployment.clone(), action),
    )
}

pub(super) fn with_api<A, T>(
    vocabulary: &Vocabulary,
    d: &Deployment,
    options: &Options,
    new: impl Fn(&str, &str, HttpTransportPolicy) -> anyhow::Result<A>,
    mut operation: impl FnMut(&A) -> Result<T, ResourceFailure>,
) -> anyhow::Result<Result<T, ResourceFailure>> {
    let policy = options.http.transport_policy();
    let client = HttpClient::new(policy)
        .map_err(|error| anyhow!(error))
        .context(vocabulary.networking)?;
    cli::execute_selected_api_operation(
        cli::principal_api_context(&client, d, &options.authentication, "acquire human session"),
        |token| Ok(operation(&new(d.fingerprint().api_url(), token, policy)?)),
        ResourceFailure::credential_rejected,
        || ResourceFailure::Unauthenticated,
        |category| ResourceFailure::Unreachable { category },
    )
}

#[expect(
    clippy::too_many_arguments,
    reason = "a report names its resource, deployment, organization, label, value, cursor, failure and format"
)]
pub(super) fn output(
    vocabulary: &Vocabulary,
    d: &Deployment,
    org: &str,
    label: (&str, &str),
    resource: Option<&impl Serialize>,
    cursor: Option<&str>,
    failure: Option<&ResourceFailure>,
    json: bool,
) -> cli::CommandResult {
    let (action, field) = label;
    let (outcome, exit) = match failure {
        None => (action, ExitCode::Success),
        Some(ResourceFailure::Unauthenticated) => {
            ("authentication_required", ExitCode::AuthenticationRequired)
        }
        Some(ResourceFailure::Forbidden) => ("forbidden", ExitCode::GeneralFailure),
        Some(ResourceFailure::NotFound) => ("not_found", ExitCode::GeneralFailure),
        Some(ResourceFailure::Conflict) => ("conflict", ExitCode::GeneralFailure),
        Some(ResourceFailure::InvalidInput { .. }) => ("invalid_input", ExitCode::UsageError),
        Some(ResourceFailure::RateLimited { .. } | ResourceFailure::Unreachable { .. }) => {
            ("unavailable", ExitCode::Unavailable)
        }
        Some(ResourceFailure::InvalidResponse { .. }) => {
            ("invalid_response", ExitCode::GeneralFailure)
        }
    };
    let reason = match failure {
        Some(ResourceFailure::InvalidInput {
            problem: Some(problem),
        }) => Some(problem.as_str()),
        _ => None,
    };
    let write = || -> anyhow::Result<()> {
        if json {
            let mut out = io::stdout().lock();
            let mut document = serde_json::json!({"schemaVersion":1, "deployment":d.fingerprint().api_url(), "organizationRef":org, "outcome":outcome});
            if let Some(cursor) = cursor {
                document["nextCursor"] = cursor.into();
            }
            if let Some(reason) = reason {
                document["reason"] = reason.into();
            }
            document[field] = serde_json::to_value(resource)?;
            serde_json::to_writer(&mut out, &document)?;
            writeln!(out)?;
        } else if failure.is_some() {
            let mut out = io::stderr().lock();
            writeln!(out, "error: {} {outcome}", vocabulary.noun)?;
            if let Some(reason) = reason {
                writeln!(out, "reason: {reason}")?;
            }
            writeln!(
                out,
                "\nInspect the {} and your access before trying again.",
                vocabulary.subject(),
            )?;
        } else {
            let mut out = io::stdout().lock();
            writeln!(out, "{}: {outcome}", vocabulary.name(field))?;
            if let Some(value) = resource {
                serde_json::to_writer_pretty(&mut out, value)?;
                writeln!(out)?;
            }
            if let Some(cursor) = cursor {
                writeln!(out, "next cursor: {cursor}")?;
            }
        }
        Ok(())
    };
    write().map_err(cli::CommandFailure::from)?;
    Ok(exit)
}

#[expect(
    clippy::too_many_arguments,
    reason = "a result report adds its cursor selector to the report's parameters"
)]
pub(super) fn result<T: Serialize>(
    vocabulary: &Vocabulary,
    d: &Deployment,
    org: &str,
    action: &str,
    field: &str,
    value: Result<T, ResourceFailure>,
    cursor: impl Fn(&T) -> Option<&str>,
    json: bool,
) -> cli::CommandResult {
    match value {
        Ok(value) => output(
            vocabulary,
            d,
            org,
            (action, field),
            Some(&value),
            cursor(&value),
            None,
            json,
        ),
        Err(failure) => output(
            vocabulary,
            d,
            org,
            (action, field),
            None::<&T>,
            None,
            Some(&failure),
            json,
        ),
    }
}

pub(super) fn uncertain(
    vocabulary: &Vocabulary,
    d: &Deployment,
    org: &str,
    key: &str,
    json: bool,
    exit: ExitCode,
    replay_failure: Option<&ResourceFailure>,
) -> cli::CommandResult {
    let replay_outcome = replay_failure.map(|failure| match failure {
        ResourceFailure::Unauthenticated => "authentication_required",
        ResourceFailure::Forbidden => "forbidden",
        ResourceFailure::RateLimited { .. } => "rate_limited",
        ResourceFailure::Unreachable { .. } => "unavailable",
        ResourceFailure::InvalidResponse { .. } => "invalid_response",
        ResourceFailure::Conflict => "conflict",
        ResourceFailure::NotFound => "not_found",
        ResourceFailure::InvalidInput { .. } => "invalid_input",
    });
    let write = || -> anyhow::Result<()> {
        if json {
            let mut out = io::stdout().lock();
            let mut document = serde_json::json!({"schemaVersion":1,"deployment":d.fingerprint().api_url(),"organizationRef":org,"outcome":"commitment_unknown","idempotencyKey":key});
            if let Some(reason) = replay_outcome {
                document["replayOutcome"] = reason.into();
            }
            serde_json::to_writer(&mut out, &document)?;
            writeln!(out)?;
        } else {
            let mut out = io::stderr().lock();
            writeln!(
                out,
                "error: {} commitment unknown\nrequest key: {key}",
                vocabulary.noun
            )?;
            if let Some(reason) = replay_outcome {
                writeln!(out, "replay outcome: {reason}")?;
            }
            writeln!(
                out,
                "\nInspect the {} and your access before repeating this operation.",
                vocabulary.subject()
            )?;
        }
        Ok(())
    };
    write().map_err(cli::CommandFailure::from)?;
    Ok(exit)
}

// Mutations reuse a single key for a lost response. Never continue with another key after
// an uncertain result or a signal; a read is required before any deliberate new write.
#[expect(
    clippy::too_many_arguments,
    reason = "a mutation carries its report label, API constructor and operation alongside its session"
)]
pub(super) fn mutate<A, T: Serialize + Send + 'static>(
    vocabulary: &'static Vocabulary,
    d: Deployment,
    org: String,
    options: Options,
    action: &'static str,
    field: &'static str,
    new: impl Fn(&str, &str, HttpTransportPolicy) -> anyhow::Result<A> + Send + 'static,
    mut call: impl FnMut(&A, &str) -> Result<T, ResourceFailure> + Send + 'static,
) -> cli::CommandResult {
    let key = um_support::generate_idempotency_key()
        .with_context(|| format!("generate {} request identity", vocabulary.subject()))?;
    let signal_deployment = d.clone();
    let signal_org = org.clone();
    let signal_key = key.clone();
    let json = options.json;
    cli::execute_mutation_with_signals(
        vocabulary.mutation,
        (),
        move |control| {
            if !control.begin_dispatch() {
                return Ok(ExitCode::Interrupted);
            }
            let mut response_lost = false;
            let mut replay_failure = None;
            let value = with_api(vocabulary, &d, &options, new, |api| match call(api, &key) {
                Err(ResourceFailure::Unreachable { .. }) if !control.is_stopped() => {
                    response_lost = true;
                    let replay = call(api, &key);
                    replay_failure = replay.as_ref().err().cloned();
                    replay
                }
                value => value,
            });
            // A credential refresh can fail after a rejected replay. It does not
            // settle the first dispatch; retain the key even without a final API result.
            let value = match value {
                Ok(value) => value,
                Err(_) if response_lost => {
                    return cli::complete_operation(control, || {
                        uncertain(
                            vocabulary,
                            &d,
                            &org,
                            &key,
                            json,
                            ExitCode::Unavailable,
                            replay_failure.as_ref(),
                        )
                    });
                }
                Err(error) => return Err(error.into()),
            };
            cli::complete_operation(control, || {
                if matches!(
                    value,
                    Err(ResourceFailure::Unreachable { .. }
                        | ResourceFailure::InvalidResponse { .. })
                ) || (response_lost && value.is_err())
                {
                    uncertain(
                        vocabulary,
                        &d,
                        &org,
                        &key,
                        json,
                        ExitCode::Unavailable,
                        if response_lost {
                            value.as_ref().err()
                        } else {
                            None
                        },
                    )
                } else {
                    result(vocabulary, &d, &org, action, field, value, |_| None, json)
                }
            })
        },
        move |exit, snapshot| {
            if snapshot.dispatched {
                uncertain(
                    vocabulary,
                    &signal_deployment,
                    &signal_org,
                    &signal_key,
                    json,
                    exit,
                    None,
                )
            } else {
                Ok(exit)
            }
        },
    )
}

pub(super) fn configuration_error(
    vocabulary: &Vocabulary,
    error: anyhow::Error,
    json: bool,
) -> cli::CommandResult {
    let subject = vocabulary.subject();
    if json {
        let mut out = io::stdout().lock();
        serde_json::to_writer(
            &mut out,
            &serde_json::json!({"schemaVersion":1,"outcome":"invalid_configuration"}),
        )
        .map_err(|e| cli::CommandFailure::from(anyhow!(e).context("write configuration error")))?;
        writeln!(out).map_err(|e| {
            cli::CommandFailure::from(anyhow!(e).context("write configuration error"))
        })?;
        writeln!(io::stderr().lock(), "error: {subject} configuration rejected: {error}\n\nCheck the configuration and try again.")
            .map_err(|e| cli::CommandFailure::from(anyhow!(e).context("write configuration diagnostic")))?;
        Ok(ExitCode::UsageError)
    } else {
        Err(cli::CommandFailure::with_exit_code(
            error.context(format!(
                "Check the {subject} configuration file and try again"
            )),
            ExitCode::UsageError,
        ))
    }
}

pub(super) fn list_output<T: Serialize>(
    vocabulary: &Vocabulary,
    d: &Deployment,
    org: &str,
    page: Result<(Vec<T>, Option<String>), ResourceFailure>,
    json: bool,
) -> cli::CommandResult {
    match page {
        Ok((items, cursor)) => output(
            vocabulary,
            d,
            org,
            ("listed", "items"),
            Some(&items),
            cursor.as_deref(),
            None,
            json,
        ),
        Err(failure) => output(
            vocabulary,
            d,
            org,
            ("listed", "items"),
            None::<&Vec<T>>,
            None,
            Some(&failure),
            json,
        ),
    }
}
