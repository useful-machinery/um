use std::time::Duration;

use anyhow::Context;
use clap::{Args, ValueEnum};

use crate::exit_code::ExitCode;
use um_api::{GrantFailure, GrantProvider};
use um_human_auth::Deployment;

use super::super::super::{ceremony_poll_interval, retry_delay, wait_for_poll};
use super::output::{self, ConsentProvider, SessionView};
use super::with_api;

pub(super) const ABOUT: &str = "Create a provider grant through the provider's consent";

// The consent streams its start and its result, so its JSON is newline-delimited events.
type Options = super::super::super::CommonArgs<
    super::super::super::StreamingJson,
    super::super::super::NoAuthenticationArgs,
>;
type OperationControl = super::super::super::OperationControl<Recovery>;

const CONSENT_TIMEOUT: Duration = Duration::from_secs(900);

#[derive(Debug, Args)]
pub(super) struct Command {
    #[arg(
        value_name = "PROVIDER",
        hide_possible_values = true,
        help = "Provider to ask for a grant: github, gitlab.com or linear"
    )]
    provider: GrantProviderArg,

    #[arg(
        long,
        value_name = "IDENTITY",
        value_parser = parse_identity_id,
        help = "Linked identity ID to bind the grant to; omit to let the consent find or create the link"
    )]
    identity_id: Option<String>,

    #[command(flatten)]
    wait: super::super::super::WaitTimeoutArgs,

    #[arg(
        long,
        conflicts_with = "timeout",
        help = "Return after starting consent instead of waiting for the callback"
    )]
    no_wait: bool,

    #[command(flatten)]
    options: Options,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum GrantProviderArg {
    Github,
    #[value(name = "gitlab.com")]
    GitlabCom,
    Linear,
}

impl GrantProviderArg {
    const fn provider(self) -> GrantProvider {
        match self {
            Self::Github => GrantProvider::CreateGrantSessionRequestProviderGithub,
            Self::GitlabCom => GrantProvider::CreateGrantSessionRequestProviderGitlabCom,
            Self::Linear => GrantProvider::CreateGrantSessionRequestProviderLinear,
        }
    }

    const fn consent(self) -> ConsentProvider {
        match self {
            Self::Github => ConsentProvider::GITHUB,
            Self::GitlabCom => ConsentProvider::GITLAB_COM,
            Self::Linear => ConsentProvider::LINEAR,
        }
    }
}

fn parse_identity_id(value: &str) -> Result<String, String> {
    if um_support::valid_typed_id(value, "idn_") {
        Ok(value.to_owned())
    } else {
        Err("must be an exact linked identity ID".to_owned())
    }
}

/// The consent this invocation started, once its session is known.
#[derive(Clone)]
struct Recovery {
    session: Option<String>,
}

impl Command {
    pub(super) fn run(self, deployment: &Deployment) -> super::super::super::CommandResult {
        let key = um_support::generate_idempotency_key()
            .context("generate provider grant consent request key")?;
        let timeout = (!self.no_wait).then(|| self.wait.timeout.unwrap_or(CONSENT_TIMEOUT));
        let deployment = deployment.clone();
        let provider = self.provider.consent();
        let json = self.options.json;
        let signal_deployment = deployment.fingerprint().api_url().to_owned();
        let timeout_deployment = signal_deployment.clone();
        super::super::super::execute_mutation_with_signals_and_deferred_timeout(
            "provider grant consent",
            Recovery { session: None },
            timeout,
            move |control, timeout_start| {
                if !control.begin_dispatch() {
                    return Ok(ExitCode::Interrupted);
                }
                let result = with_api(&deployment, &self.options, |api| {
                    // A lost response may have started the consent. Replay only this key, once.
                    let start = || match &self.identity_id {
                        Some(identity_id) => api.start_for_identity(identity_id, &key),
                        None => api.start(self.provider.provider(), &key),
                    };
                    match start() {
                        Err(GrantFailure::Unreachable { .. }) => start(),
                        result => result,
                    }
                })?;
                let api_url = deployment.fingerprint().api_url();
                let session = match result {
                    Ok(session) => session,
                    Err(failure) => {
                        return finish(control, || {
                            output::write_consent_failure(api_url, provider, None, &failure, json)
                        });
                    }
                };
                if !control.update_recovery(Recovery {
                    session: Some(session.id.clone()),
                }) {
                    return Ok(ExitCode::Interrupted);
                }
                let view = SessionView::new(&session);
                if self.no_wait || !view.is_pending() {
                    return finish(control, || view.write_result(api_url, false, json));
                }
                if control.is_cancelled() {
                    return Ok(ExitCode::Interrupted);
                }
                view.write_pending(api_url, json)?;
                timeout_start.start();
                observe(&deployment, provider, &session.id, &self.options, control)
            },
            move |signal, snapshot| {
                if !snapshot.dispatched {
                    return Ok(signal);
                }
                stopped(&signal_deployment, &snapshot.recovery, signal, json)
            },
            move |snapshot| {
                stopped(
                    &timeout_deployment,
                    &snapshot.recovery,
                    ExitCode::GeneralFailure,
                    json,
                )
            },
        )
    }
}

/// Polls the consent until it settles; a pending answer keeps polling at the consent cadence
/// and a rate-limited one waits out the server's `Retry-After`.
fn observe(
    deployment: &Deployment,
    provider: ConsentProvider,
    id: &str,
    options: &Options,
    control: &OperationControl,
) -> super::super::super::CommandResult {
    let api_url = deployment.fingerprint().api_url();
    let mut next = ceremony_poll_interval();
    loop {
        if !wait_for_poll(control, next, &super::super::super::SystemObservationClock) {
            return Ok(ExitCode::Interrupted);
        }
        match with_api(deployment, options, |api| api.session(id))? {
            Ok(session) if SessionView::new(&session).is_pending() => {
                next = ceremony_poll_interval();
            }
            Ok(session) => {
                return finish(control, || {
                    SessionView::new(&session).write_result(api_url, true, options.json)
                });
            }
            Err(GrantFailure::RateLimited { retry_after }) => next = retry_delay(retry_after),
            Err(failure) => {
                return finish(control, || {
                    output::write_consent_failure(
                        api_url,
                        provider,
                        Some(id),
                        &failure,
                        options.json,
                    )
                });
            }
        }
    }
}

fn finish(
    control: &OperationControl,
    output: impl FnOnce() -> anyhow::Result<ExitCode>,
) -> super::super::super::CommandResult {
    super::super::super::complete_operation(control, || output().map_err(Into::into))
}

fn stopped(
    deployment: &str,
    recovery: &Recovery,
    signal: ExitCode,
    json: bool,
) -> super::super::super::CommandResult {
    let outcome = if signal == ExitCode::GeneralFailure {
        "timed_out"
    } else {
        "observation_stopped"
    };
    output::write_consent_stopped(deployment, recovery.session.as_deref(), outcome, json)?;
    Ok(signal)
}
