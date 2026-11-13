//! The provider's consent that makes an OAuth grant (REQ-GRANT-1, REQ-GRANT-69), which both
//! `um grant create` and its earlier spelling run: it starts the session with one key,
//! replayed once on a lost response, prints where to approve it, opens that URL when asked to,
//! and polls the session until it settles.

mod output;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::Context;

use crate::exit_code::ExitCode;
use um_api::GrantFailure;
use um_human_auth::Deployment;

use super::super::{ceremony_poll_interval, retry_delay, wait_for_poll};
use output::SessionView;

pub(in crate::cli) use output::{ConsentProvider, GrantView, report, write_refusal};

// The consent streams its start and its result, so its JSON is newline-delimited events.
pub(in crate::cli) type Options =
    super::super::CommonArgs<super::super::StreamingJson, super::super::NoAuthenticationArgs>;
type OperationControl = super::super::OperationControl<Recovery>;

const CONSENT_TIMEOUT: Duration = Duration::from_secs(900);

/// The command a member ran the consent with, as its remedies name it: its input, and the
/// commands it is to run next, which are `um grant`'s from either (REQ-GRANT-77).
#[derive(Clone, Copy)]
pub(in crate::cli) struct Front {
    create: &'static str,
    list: &'static str,
    check_input: &'static str,
}

impl Front {
    pub(in crate::cli) const AUTH: Self = Self {
        check_input: "Check the provider and --identity-id values.",
        ..Self::GRANT
    };
    pub(in crate::cli) const GRANT: Self = Self {
        create: "um grant create",
        list: "um grant list",
        check_input: "Check the provider.",
    };
}

/// One consent to run.
pub(in crate::cli) struct Consent {
    pub(in crate::cli) front: Front,
    pub(in crate::cli) provider: ConsentProvider,
    /// The linked identity to bind the grant to; without one the consent finds or creates the
    /// link.
    pub(in crate::cli) identity_id: Option<String>,
    /// Whether to open the consent URL in this machine's browser.
    pub(in crate::cli) open: bool,
    pub(in crate::cli) no_wait: bool,
    pub(in crate::cli) timeout: Option<Duration>,
    pub(in crate::cli) options: Options,
}

/// The consent this invocation started, once its session is known.
#[derive(Clone)]
struct Recovery {
    session: Option<String>,
}

/// How a consent ended: its exit status, and whether the deployment answered that it does not
/// serve the provider, which a command that runs several consents passes over.
pub(super) struct Ended {
    pub(super) code: ExitCode,
    pub(super) unavailable: bool,
}

impl Consent {
    pub(in crate::cli) fn run(self, deployment: &Deployment) -> super::super::CommandResult {
        self.run_to_end(deployment).map(|ended| ended.code)
    }

    pub(super) fn run_to_end(
        self,
        deployment: &Deployment,
    ) -> Result<Ended, super::super::CommandFailure> {
        let unavailable = Arc::new(AtomicBool::new(false));
        let answered_unavailable = Arc::clone(&unavailable);
        let key = um_support::generate_idempotency_key()
            .context("generate provider grant consent request key")?;
        let timeout = (!self.no_wait).then(|| self.timeout.unwrap_or(CONSENT_TIMEOUT));
        let deployment = deployment.clone();
        let front = self.front;
        let json = self.options.json;
        let signal_deployment = deployment.fingerprint().api_url().to_owned();
        let timeout_deployment = signal_deployment.clone();
        let code = super::super::execute_mutation_with_signals_and_deferred_timeout(
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
                        None => api.start(self.provider.request, &key),
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
                        if matches!(failure, GrantFailure::ProviderUnavailable) {
                            answered_unavailable.store(true, Ordering::Relaxed);
                        }
                        return finish(control, || {
                            output::write_consent_failure(
                                api_url,
                                front,
                                self.provider,
                                None,
                                &failure,
                                json,
                            )
                        });
                    }
                };
                if !control.update_recovery(Recovery {
                    session: Some(session.id.clone()),
                }) {
                    return Ok(ExitCode::Interrupted);
                }
                let view = SessionView::new(&session, front);
                if !view.is_pending() {
                    return finish(control, || view.write_result(api_url, false, json));
                }
                // The URL is printed first, so a browser that does not open loses nothing, and
                // the browser is only started, so the wait below does not wait on it either.
                let show = || {
                    let code = view.write_pending(api_url, json)?;
                    if let Some(url) = view.authorization_url().filter(|_| self.open) {
                        super::browser::open(url);
                    }
                    Ok(code)
                };
                if self.no_wait {
                    return finish(control, show);
                }
                if control.is_cancelled() {
                    return Ok(ExitCode::Interrupted);
                }
                show()?;
                timeout_start.start();
                observe(&deployment, &self, &session.id, control)
            },
            move |signal, snapshot| {
                if !snapshot.dispatched {
                    return Ok(signal);
                }
                stopped(&signal_deployment, front, &snapshot.recovery, signal, json)
            },
            move |snapshot| {
                stopped(
                    &timeout_deployment,
                    front,
                    &snapshot.recovery,
                    ExitCode::GeneralFailure,
                    json,
                )
            },
        )?;
        Ok(Ended {
            code,
            unavailable: unavailable.load(Ordering::Relaxed),
        })
    }
}

/// Runs `f` against the grant API with the signed-in human session, refreshing a rejected
/// session once.
fn with_api<T>(
    deployment: &Deployment,
    options: &Options,
    f: impl FnMut(&um_api::GrantApi) -> Result<T, GrantFailure>,
) -> anyhow::Result<Result<T, GrantFailure>> {
    super::super::with_human_session_api(deployment, options.http.transport_policy(), f)
}

/// Polls the consent until it settles; a pending answer keeps polling at the consent cadence
/// and a rate-limited one waits out the server's `Retry-After`.
fn observe(
    deployment: &Deployment,
    consent: &Consent,
    id: &str,
    control: &OperationControl,
) -> super::super::CommandResult {
    let api_url = deployment.fingerprint().api_url();
    let json = consent.options.json;
    let mut next = ceremony_poll_interval();
    loop {
        if !wait_for_poll(control, next, &super::super::SystemObservationClock) {
            return Ok(ExitCode::Interrupted);
        }
        match with_api(deployment, &consent.options, |api| api.session(id))? {
            Ok(session) if SessionView::new(&session, consent.front).is_pending() => {
                next = ceremony_poll_interval();
            }
            Ok(session) => {
                return finish(control, || {
                    SessionView::new(&session, consent.front).write_result(api_url, true, json)
                });
            }
            Err(GrantFailure::RateLimited { retry_after }) => next = retry_delay(retry_after),
            Err(failure) => {
                return finish(control, || {
                    output::write_consent_failure(
                        api_url,
                        consent.front,
                        consent.provider,
                        Some(id),
                        &failure,
                        json,
                    )
                });
            }
        }
    }
}

fn finish(
    control: &OperationControl,
    output: impl FnOnce() -> anyhow::Result<ExitCode>,
) -> super::super::CommandResult {
    super::super::complete_operation(control, || output().map_err(Into::into))
}

fn stopped(
    deployment: &str,
    front: Front,
    recovery: &Recovery,
    signal: ExitCode,
    json: bool,
) -> super::super::CommandResult {
    let outcome = if signal == ExitCode::GeneralFailure {
        "timed_out"
    } else {
        "observation_stopped"
    };
    output::write_consent_stopped(
        deployment,
        front,
        recovery.session.as_deref(),
        outcome,
        json,
    )?;
    Ok(signal)
}
