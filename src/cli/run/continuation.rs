use anyhow::Context;
use clap::Args;
use serde::Serialize;
use std::collections::HashSet;
use std::io::{self, Write};
use um_api::{
    Run, RunContinuationDefinition, RunContinuationEnvelope, RunContinuationRequest, RunFailure,
};
use um_human_auth::Deployment;

use crate::exit_code::ExitCode;

use super::{
    RunOptions, RunReference, RunWaitArgs, failure_code, finish_operation, start_cloud_observation,
};

#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(flatten)]
    run: RunReference,
    #[arg(long = "from", required = true, value_name = "STEP", value_parser = parse_step,
        help = "Reexecute from this step (repeatable; order is preserved)")]
    from_steps: Vec<String>,
    #[arg(long, value_name = "OID", requires = "workflow_path", value_parser = parse_oid,
        help = "Use a replacement workflow commit")]
    workflow_commit: Option<String>,
    #[arg(long, value_name = "PATH", requires = "workflow_commit", value_parser = parse_workflow_path,
        help = "Use a replacement canonical workflow path")]
    workflow_path: Option<String>,
    #[arg(long, value_name = "VERSION", value_parser = clap::value_parser!(i64).range(1..),
        help = "Require this positive run version at admission")]
    expected_version: Option<i64>,
    #[command(flatten)]
    wait: RunWaitArgs,
    #[command(flatten)]
    options: RunOptions,
}

fn parse_step(value: &str) -> Result<String, String> {
    let mut bytes = value.bytes();
    if value.len() <= 64
        && bytes.next().is_some_and(|byte| byte.is_ascii_lowercase())
        && bytes.all(|byte| byte.is_ascii_alphanumeric())
    {
        Ok(value.to_owned())
    } else {
        Err("step must begin with a lowercase letter and contain at most 64 ASCII letters or digits".to_owned())
    }
}

fn parse_workflow_path(value: &str) -> Result<String, String> {
    if !value.is_empty()
        && value.len() <= 4096
        && !value.starts_with('/')
        && value
            .split('/')
            .all(|part| !part.is_empty() && part != "." && part != ".." && !part.contains('\0'))
    {
        Ok(value.to_owned())
    } else {
        Err("workflow path must be canonical and repository-relative".to_owned())
    }
}

fn parse_oid(value: &str) -> Result<String, String> {
    if value.len() == 40
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        Ok(value.to_owned())
    } else {
        Err("commit OID must be 40 lowercase hexadecimal characters".to_owned())
    }
}

#[derive(Clone)]
struct Recovery {
    key: String,
    envelope: Option<RunContinuationEnvelope>,
    diagnostic: Option<Vec<um_api::ContinuationAdmissionViolation>>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ResultDocument<'a> {
    schema_version: u8,
    operation: &'static str,
    deployment: &'a str,
    organization_ref: &'a str,
    run_id: &'a str,
    outcome: &'a str,
    idempotency_key: Option<&'a str>,
    request: Option<&'a um_api::RunContinuationReceipt>,
    run: Option<&'a Run>,
    code: Option<&'a str>,
    diagnostic: Option<&'a [um_api::ContinuationAdmissionViolation]>,
}

impl Command {
    pub(super) fn execute(self, deployment: Deployment) -> super::super::CommandResult {
        let key = um_support::generate_idempotency_key()
            .context("generate Cloud run continuation identity")?;
        let signal_deployment = deployment.clone();
        let timeout_deployment = deployment.clone();
        let signal_org = self.run.organization.clone();
        let timeout_org = signal_org.clone();
        let signal_run = self.run.run_id.clone();
        let timeout_run = signal_run.clone();
        let json = self.options.json;
        let timeout = self.wait.timeout.filter(|_| self.wait.wait);
        super::super::execute_mutation_with_signals_and_deferred_timeout(
            "Cloud run continuation",
            Recovery {
                key,
                envelope: None,
                diagnostic: None,
            },
            timeout,
            move |control, timer| self.execute_blocking(&deployment, control, timer),
            move |signal, snapshot| {
                let recovery = snapshot.recovery;
                render(
                    (&signal_deployment, &signal_org, &signal_run, json),
                    &recovery,
                    if recovery.envelope.is_some() {
                        "observation_stopped"
                    } else {
                        "acceptance_unknown"
                    },
                    None,
                    signal,
                )
                .map_err(Into::into)
            },
            move |snapshot| {
                render(
                    (&timeout_deployment, &timeout_org, &timeout_run, json),
                    &snapshot.recovery,
                    "timed_out",
                    Some("wait_timed_out"),
                    ExitCode::GeneralFailure,
                )
                .map_err(Into::into)
            },
        )
    }

    fn execute_blocking(
        self,
        deployment: &Deployment,
        control: &super::super::OperationControl<Recovery>,
        timer: &super::super::DeferredObservationTimeoutStart,
    ) -> super::super::CommandResult {
        let mut recovery = control.recovery();
        let mut seen = HashSet::new();
        if !self.from_steps.iter().all(|step| seen.insert(step)) {
            return self.finish(
                deployment,
                control,
                &recovery,
                "error",
                Some("invalid_input"),
                ExitCode::GeneralFailure,
            );
        }
        let definition = match (self.workflow_commit.clone(), self.workflow_path.clone()) {
            (Some(commit), Some(path)) => {
                RunContinuationDefinition::RunContinuationRequestDefinitionOneOf(Box::new(
                    um_api::RunContinuationReplacement::new(
                        um_api::RunContinuationReplacementSource::new(commit, path),
                    ),
                ))
            }
            _ => RunContinuationDefinition::String("inherited".to_owned()),
        };
        let mut request = RunContinuationRequest::new(self.from_steps.clone(), definition);
        request.expected_run_version = self.expected_version;
        let result = pinned_submission(
            deployment,
            &self.run,
            &self.options,
            control,
            &recovery.key,
            &request,
        );
        let envelope = match result {
            Ok(Ok(envelope)) => envelope,
            Ok(Err(failure)) => {
                if let RunFailure::ContinuationAdmissionInvalid(violations) = &failure {
                    recovery.diagnostic = Some(violations.clone());
                }
                let ambiguous = matches!(
                    failure,
                    RunFailure::ContinuationAcceptanceUnknown
                        | RunFailure::Unreachable(_)
                        | RunFailure::Protocol { .. }
                ) && control.dispatched();
                let (code, exit) = failure_code(&failure, self.options.authentication.kind());
                return finish_operation(control, || {
                    render(
                        (
                            deployment,
                            &self.run.organization,
                            &self.run.run_id,
                            self.options.json,
                        ),
                        &recovery,
                        if ambiguous {
                            "acceptance_unknown"
                        } else {
                            "error"
                        },
                        Some(if ambiguous {
                            "acceptance_unknown"
                        } else {
                            code
                        }),
                        if ambiguous {
                            ExitCode::Unavailable
                        } else {
                            exit
                        },
                    )
                });
            }
            Err(_) => {
                return finish_operation(control, || {
                    render(
                        (
                            deployment,
                            &self.run.organization,
                            &self.run.run_id,
                            self.options.json,
                        ),
                        &recovery,
                        if control.dispatched() {
                            "acceptance_unknown"
                        } else {
                            "error"
                        },
                        Some(if control.dispatched() {
                            "acceptance_unknown"
                        } else {
                            "submission_failed"
                        }),
                        if control.dispatched() {
                            ExitCode::Unavailable
                        } else {
                            ExitCode::GeneralFailure
                        },
                    )
                });
            }
        };
        recovery.envelope = Some(envelope);
        if !control.update_recovery(recovery.clone()) {
            return Ok(ExitCode::GeneralFailure);
        }
        if !self.wait.wait {
            return self.finish(
                deployment,
                control,
                &recovery,
                "accepted",
                None,
                ExitCode::Success,
            );
        }
        let (clock, started) = start_cloud_observation(timer);
        let deadline = self
            .wait
            .timeout
            .map(|duration| started.checked_add(duration).unwrap_or(started));
        let request_id = recovery
            .envelope
            .as_ref()
            .map(|value| value.request.request_id.clone())
            .unwrap_or_default();
        let mut setup_failure = None;
        let result = super::super::wait_for_terminal_observation_bounded(
            |_| {
                let response = super::with_api_until(
                    deployment,
                    self.options.http.transport_policy(),
                    &self.options.authentication,
                    deadline,
                    |api, budget| {
                        api.get_continuation(
                            &self.run.organization,
                            &self.run.run_id,
                            &request_id,
                            budget,
                        )
                    },
                )
                .map_err(|error| {
                    match classify_observation_setup(&error) {
                        Some(failure) => failure,
                        None => {
                            // Keep an unclassified local cause out of the API-protocol
                            // category; the sentinel is resolved at the command boundary.
                            setup_failure = Some(error);
                            RunFailure::Interrupted
                        }
                    }
                })?;
                if let Ok(envelope) = &response {
                    if recovery
                        .envelope
                        .as_ref()
                        .is_some_and(|previous| previous.request != envelope.request)
                    {
                        return Err(RunFailure::Protocol {
                            credential_rejected: false,
                        });
                    }
                    recovery.envelope = Some(envelope.clone());
                    control.update_recovery(recovery.clone());
                }
                response
            },
            |envelope| super::terminal_run_state(envelope.run.state),
            RunFailure::retryable_observation,
            self.wait.timeout,
            started,
            control,
            &clock,
        );
        match result {
            Ok(super::super::TerminalObservation::Terminal { resource, .. }) => {
                recovery.envelope = Some(*resource);
                self.finish(
                    deployment,
                    control,
                    &recovery,
                    "observed",
                    None,
                    ExitCode::Success,
                )
            }
            Ok(super::super::TerminalObservation::TimedOut) => self.finish(
                deployment,
                control,
                &recovery,
                "timed_out",
                Some("wait_timed_out"),
                ExitCode::GeneralFailure,
            ),
            Ok(super::super::TerminalObservation::Stopped) => Ok(ExitCode::GeneralFailure),
            Err(failure) => {
                let (code, exit) = if setup_failure.is_some() {
                    ("observation_failed", ExitCode::GeneralFailure)
                } else {
                    failure_code(&failure, self.options.authentication.kind())
                };
                self.finish(deployment, control, &recovery, "error", Some(code), exit)
            }
        }
    }

    fn finish(
        &self,
        deployment: &Deployment,
        control: &super::super::OperationControl<Recovery>,
        recovery: &Recovery,
        outcome: &str,
        code: Option<&str>,
        exit: ExitCode,
    ) -> super::super::CommandResult {
        finish_operation(control, || {
            render(
                (
                    deployment,
                    &self.run.organization,
                    &self.run.run_id,
                    self.options.json,
                ),
                recovery,
                outcome,
                code,
                exit,
            )
        })
    }
}

fn classify_observation_setup(error: &anyhow::Error) -> Option<RunFailure> {
    // A failed HTTP client construction precedes the GET and may recover on
    // the one observation retry. No POST is retried by this path.
    if error
        .chain()
        .any(|cause| cause.is::<um_api::HttpClientError>())
    {
        Some(RunFailure::Unreachable(
            um_api::UnreachableCategory::Connection,
        ))
    } else if error.chain().any(|cause| {
        cause
            .downcast_ref::<um_human_auth::SessionError>()
            .is_some_and(um_human_auth::SessionError::observation_lock_timeout)
    }) {
        Some(RunFailure::Unreachable(
            um_api::UnreachableCategory::Timeout,
        ))
    } else if error.chain().any(|cause| {
        cause.is::<um_human_auth::SessionError>()
            || cause.is::<crate::service_auth::ServiceApiKeyError>()
    }) {
        Some(RunFailure::Unauthenticated)
    } else {
        None
    }
}

fn pinned_submission(
    deployment: &Deployment,
    run: &RunReference,
    options: &RunOptions,
    control: &super::super::OperationControl<Recovery>,
    key: &str,
    request: &RunContinuationRequest,
) -> anyhow::Result<Result<RunContinuationEnvelope, RunFailure>> {
    super::with_pinned_run_mutation(
        deployment,
        options,
        "acquire human session for Cloud run continuation",
        |api| {
            api.request_continuation(&run.organization, &run.run_id, key, request, || {
                control.begin_dispatch()
            })
        },
    )
}

fn render(
    (deployment, org, run_id, json): (&Deployment, &str, &str, bool),
    recovery: &Recovery,
    outcome: &str,
    code: Option<&str>,
    exit: ExitCode,
) -> anyhow::Result<ExitCode> {
    let envelope = recovery.envelope.as_ref();
    if json {
        let document = ResultDocument {
            schema_version: 1,
            operation: "continue",
            deployment: deployment.fingerprint().api_url(),
            organization_ref: org,
            run_id,
            outcome,
            idempotency_key: (envelope.is_none()).then_some(recovery.key.as_str()),
            request: envelope.map(|value| value.request.as_ref()),
            run: envelope.map(|value| value.run.as_ref()),
            code,
            diagnostic: recovery.diagnostic.as_deref(),
        };
        let mut stdout = io::stdout().lock();
        serde_json::to_writer(&mut stdout, &document)
            .context("serialize Cloud run continuation")?;
        writeln!(stdout).context("write Cloud run continuation")?;
    } else if let Some(envelope) = envelope {
        super::write_run_human(
            deployment.fingerprint().api_url(),
            if outcome == "accepted" {
                "✓ Continuation accepted (execution not yet confirmed)."
            } else if code.is_some() {
                "Continuation accepted; observation incomplete."
            } else {
                "✓ Continuation observed."
            },
            &envelope.run,
        )?;
        let mut stdout = io::stdout().lock();
        writeln!(
            stdout,
            "\ncontinuation request: {}\naccepted attempt: {} (number {})\npartition: reexecuted [{}]; inherited [{}]",
            envelope.request.request_id,
            envelope.request.attempt_id,
            envelope.request.attempt_number,
            envelope.request.reexecuted_steps.join(", "),
            envelope
                .request
                .inherited_steps
                .iter()
                .map(|step| step.id.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        )?;
        if let Some(code) = code {
            writeln!(
                io::stderr().lock(),
                "error: continuation observation {outcome} ({code})\n\nShow this run before requesting another continuation."
            )?;
        }
    } else {
        let mut stderr = io::stderr().lock();
        writeln!(
            stderr,
            "error: continuation {outcome}\n\nrun: {run_id}\norganization: {org}\ncode: {}\nidempotency key: {}",
            code.unwrap_or(outcome),
            recovery.key
        )?;
        if let Some(violations) = &recovery.diagnostic {
            for violation in violations {
                let code = serde_json::to_value(violation.code)?
                    .as_str()
                    .unwrap_or("invalid")
                    .to_owned();
                write!(stderr, "  {code}")?;
                for (label, value) in [
                    ("node", &violation.node),
                    ("producer", &violation.producer),
                    ("reference", &violation.reference),
                    ("prior state", &violation.prior_state),
                ] {
                    if let Some(value) = value {
                        write!(stderr, " {label}: {}", um_execution::visible_text(value))?;
                    }
                }
                writeln!(stderr)?;
            }
        }
        writeln!(
            stderr,
            "\nInspect the run before requesting another continuation with a new key."
        )?;
    }
    Ok(exit)
}

pub(super) fn write_run_continuation(out: &mut impl Write, run: &Run) -> anyhow::Result<()> {
    writeln!(
        out,
        "\nportable result: {}",
        super::enum_text(&run.portable_result)?
    )?;
    let Some(continuation) = run.continuation.as_deref() else {
        return Ok(());
    };
    writeln!(out, "\ncontinuation:")?;
    writeln!(
        out,
        "  selected from: {}",
        continuation.from_steps.join(", ")
    )?;
    writeln!(
        out,
        "  reexecution partition: {}",
        continuation.reexecuted_steps.join(", ")
    )?;
    for inherited in &continuation.inherited_steps {
        writeln!(
            out,
            "  inherited: {} (prior: {}, definition changed: {})",
            inherited.id,
            super::enum_text(&inherited.prior_state)?,
            inherited.definition_changed
        )?;
    }
    writeln!(
        out,
        "  effective definition: {} (digest {}:{}, prior {}:{})",
        super::enum_text(&continuation.definition_source.kind)?,
        super::enum_text(&continuation.definition_source.manifest_digest.algorithm)?,
        continuation.definition_source.manifest_digest.value,
        super::enum_text(
            &continuation
                .definition_source
                .prior_manifest_digest
                .algorithm
        )?,
        continuation.definition_source.prior_manifest_digest.value
    )?;
    if let RunContinuationDefinition::RunContinuationRequestDefinitionOneOf(replacement) =
        continuation.request.definition.as_ref()
    {
        writeln!(
            out,
            "  effective commit: {}",
            replacement.replaced.commit_oid
        )?;
        writeln!(
            out,
            "  effective workflow: {}",
            um_execution::visible_text(&replacement.replaced.workflow_path)
        )?;
    }
    let workspace = &continuation.workspace;
    writeln!(
        out,
        "  preparation: {}",
        super::enum_text(&workspace.preparation)?
    )?;
    if matches!(
        workspace.preparation,
        um_api::RunContinuationPreparation::Unavailable
    ) && run
        .interruption
        .as_deref()
        .is_some_and(|interruption| interruption.phase == um_api::RunInterruptionPhase::Accepted)
    {
        writeln!(out, "  engine result: not received")?;
    }
    writeln!(
        out,
        "  execution root: {}",
        um_execution::visible_text(&workspace.execution_root)
    )?;
    writeln!(
        out,
        "  prior execution root: {}",
        um_execution::visible_text(&workspace.prior_execution_root)
    )?;
    let modified = match workspace.modified.as_ref() {
        um_api::RunContinuationWorkspaceModified::Boolean(value) => value.to_string(),
        um_api::RunContinuationWorkspaceModified::String(value) => value.clone(),
    };
    writeln!(out, "  modified: {modified}")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workflow_path_matches_the_server_canonical_path_boundary() {
        assert_eq!(
            parse_workflow_path("workflows/build.yaml"),
            Ok("workflows/build.yaml".to_owned())
        );
        for path in [
            "/workflows/build.yaml",
            "workflows/../build.yaml",
            "workflows/",
            "workflows/a\0b",
        ] {
            assert!(parse_workflow_path(path).is_err(), "accepted {path:?}");
        }
    }

    #[test]
    fn receipt_observation_setup_distinguishes_auth_network_and_local_failures() {
        let network = anyhow::Error::new(um_api::HttpClientError::BuildRuntime(io::Error::other(
            "fixture runtime unavailable",
        )));
        assert_eq!(
            classify_observation_setup(&network),
            Some(RunFailure::Unreachable(
                um_api::UnreachableCategory::Connection
            ))
        );
        let auth = anyhow::Error::new(um_human_auth::SessionError::RefreshProtocol {
            reason: "fixture refresh invalid",
        });
        assert_eq!(
            classify_observation_setup(&auth),
            Some(RunFailure::Unauthenticated)
        );
        let contention = anyhow::Error::new(um_human_auth::SessionError::CredentialStore(
            um_human_auth::CredentialError::LockTimeout,
        ));
        assert_eq!(
            classify_observation_setup(&contention),
            Some(RunFailure::Unreachable(
                um_api::UnreachableCategory::Timeout
            ))
        );
        let key = anyhow::Error::new(crate::service_auth::ServiceApiKeyError::Invalid {
            source: crate::service_auth::SecretSource::Stdin,
            reason: "fixture invalid key",
        });
        assert_eq!(
            classify_observation_setup(&key),
            Some(RunFailure::Unauthenticated)
        );
        let local = anyhow::anyhow!("fixture invalid deployment configuration");
        assert_eq!(classify_observation_setup(&local), None);
    }
}
