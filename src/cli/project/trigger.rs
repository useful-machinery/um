mod config;
mod management;

use std::cell::Cell;
use std::io::{self, Write};
use std::time::Duration;

use anyhow::{Context, anyhow};
use clap::{Args, Subcommand};
use serde::Serialize;
use um_api::{
    HttpClient, HttpTransportPolicy, LinearApi, LinearEvaluation, LinearEvaluationState,
    LinearFailure,
};
use um_human_auth::{BoundRequiredOperation, Deployment, SessionBinding};

use crate::exit_code::ExitCode;

use super::super::{ObservationClock, ObservationControl, OrganizationArg, ProjectArg};

type Options =
    super::super::CommonArgs<super::super::ProjectJson, super::super::PrincipalAuthenticationArgs>;

// Clap's explicit nested groups mirror the webhook grammar; each leaf owns its own flags.
#[derive(Debug, Args)]
pub(super) struct Command {
    #[command(subcommand)]
    command: Option<Leaf>,
}
#[derive(Debug, Subcommand)]
enum Leaf {
    #[command(about = "Create a Linear trigger")]
    Create(management::Create),
    #[command(about = "Delete a Linear trigger")]
    Delete(management::Delete),
    #[command(about = "Disable a Linear trigger")]
    Disable(management::Target),
    #[command(about = "Enable a Linear trigger")]
    Enable(management::Target),
    #[command(about = "Manage Linear trigger evaluations")]
    Evaluation(EvaluationCommand),
    #[command(about = "List Linear triggers")]
    List(management::List),
    #[command(about = "Show a Linear trigger")]
    Show(management::Target),
    #[command(about = "Update a Linear trigger")]
    Update(management::Update),
}
#[derive(Debug, Args)]
struct EvaluationCommand {
    #[command(subcommand)]
    command: Option<EvaluationLeaf>,
}
#[derive(Debug, Subcommand)]
enum EvaluationLeaf {
    #[command(about = "List retained evaluations")]
    List(management::EvaluationList),
    #[command(about = "Retry a failed evaluation")]
    Retry(Retry),
    #[command(about = "Show an evaluation")]
    Show(management::EvaluationTarget),
}
#[derive(Debug, Args)]
struct Retry {
    #[arg(value_name = OrganizationArg::VALUE_NAME, help = OrganizationArg::HELP)]
    organization: OrganizationArg,
    #[arg(value_name = ProjectArg::VALUE_NAME, help = ProjectArg::HELP)]
    project: ProjectArg,
    #[arg(value_name = "TRIGGER", help = "Linear trigger ID")]
    trigger: String,
    #[arg(value_name = "EVALUATION", help = "Failed evaluation ID")]
    evaluation: String,
    #[arg(
        long,
        value_name = "KEY", value_parser = super::super::publication::parse_idempotency_key,
        help = "Reuse a previous request key after an interrupted submission"
    )]
    idempotency_key: Option<String>,
    #[arg(long, value_name = "DURATION", value_parser = super::super::parse_wait_timeout,
        help = "Stop waiting after a positive duration (units: ms, s, m, or h)")]
    timeout: Option<Duration>,
    #[command(flatten)]
    options: Options,
}

struct RetryAuthentication<'a> {
    deployment: &'a Deployment,
    client: &'a HttpClient,
    policy: HttpTransportPolicy,
    api_key: Option<&'a str>,
    binding: Option<SessionBinding>,
}

#[derive(Clone)]
struct Recovery {
    key: String,
    accepted_cycle: Option<i32>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ResultDocument<'a> {
    schema_version: u8,
    operation: &'static str,
    organization_ref: &'a str,
    project_id: &'a str,
    trigger_id: &'a str,
    evaluation_id: &'a str,
    outcome: &'a str,
    idempotency_key: &'a str,
    accepted_cycle: Option<i32>,
    evaluation: Option<&'a LinearEvaluation>,
    code: Option<&'a str>,
}

impl Command {
    pub(super) fn execute(self) -> super::super::CommandResult {
        match self.command {
            None => super::super::print_help(&["project", "trigger"]),
            Some(Leaf::Create(cmd)) => management::execute(cmd, management::Management::Create),
            Some(Leaf::List(cmd)) => management::execute(cmd, management::Management::List),
            Some(Leaf::Show(cmd)) => management::execute(cmd, management::Management::Show),
            Some(Leaf::Update(cmd)) => management::execute(cmd, management::Management::Update),
            Some(Leaf::Enable(cmd)) => management::execute(cmd, management::Management::Enable),
            Some(Leaf::Disable(cmd)) => management::execute(cmd, management::Management::Disable),
            Some(Leaf::Delete(cmd)) => management::execute(cmd, management::Management::Delete),
            Some(Leaf::Evaluation(cmd)) => match cmd.command {
                None => super::super::print_help(&["project", "trigger", "evaluation"]),
                Some(EvaluationLeaf::List(cmd)) => {
                    management::execute(cmd, management::Management::EvaluationList)
                }
                Some(EvaluationLeaf::Show(cmd)) => {
                    management::execute(cmd, management::Management::EvaluationShow)
                }
                Some(EvaluationLeaf::Retry(cmd)) => super::super::execute_deployment_command(
                    Some(cmd),
                    &["project", "trigger", "evaluation", "retry"],
                    "retry a Linear evaluation",
                    |cmd, deployment| cmd.execute(deployment.clone()),
                ),
            },
        }
    }
}

impl Retry {
    fn execute(self, deployment: Deployment) -> super::super::CommandResult {
        let key = match &self.idempotency_key {
            Some(key) => key.clone(),
            None => um_support::generate_idempotency_key()
                .context("generate Linear evaluation retry key")?,
        };
        let recovery = Recovery {
            key,
            accepted_cycle: None,
        };
        let signal = (
            deployment.clone(),
            self.organization.to_string(),
            self.project.to_string(),
            self.trigger.clone(),
            self.evaluation.clone(),
            self.options.json,
        );
        let timed_out = signal.clone();
        super::super::execute_mutation_with_signals_and_deferred_timeout(
            "Linear evaluation retry",
            recovery,
            self.timeout,
            move |control, timer| self.execute_blocking(&deployment, control, timer),
            move |exit, snapshot| {
                if !snapshot.dispatched {
                    return Ok(exit);
                }
                render(
                    (&signal.1, &signal.2, &signal.3, &signal.4, signal.5),
                    &snapshot.recovery,
                    None,
                    if snapshot.recovery.accepted_cycle.is_some() {
                        "observation_stopped"
                    } else {
                        "acceptance_unknown"
                    },
                    None,
                    exit,
                )
                .map_err(Into::into)
            },
            // Keep timeout reporting and pinned auth near the evaluation's cycle recovery;
            // run cancellation has a different recovery contract despite similar plumbing.
            move |snapshot| {
                render(
                    (
                        &timed_out.1,
                        &timed_out.2,
                        &timed_out.3,
                        &timed_out.4,
                        timed_out.5,
                    ),
                    &snapshot.recovery,
                    None,
                    "timed_out",
                    None,
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
        let policy = self.options.http.transport_policy();
        let client = HttpClient::new(policy)
            .map_err(|error| anyhow!(error))
            .context("prepare Linear evaluation networking")?;
        let api_key = self.options.authentication.service_api_key()?;
        let mut authentication = RetryAuthentication {
            deployment,
            client: &client,
            policy,
            api_key: api_key.as_ref().map(|key| key.expose()),
            binding: None,
        };
        let result = self.poll(&mut authentication, control, timer)?;
        let (code, exit) = match result {
            Ok(exit) => return Ok(exit),
            Err(failure) => failure_code(
                &failure,
                control.dispatched() && control.recovery().accepted_cycle.is_none(),
            ),
        };
        self.finish(
            control,
            None,
            if control.recovery().accepted_cycle.is_some() {
                "observation_stopped"
            } else if code == "acceptance_unknown" {
                "acceptance_unknown"
            } else {
                "error"
            },
            Some(code),
            exit,
        )
    }

    // Pin the submitting session, then refresh only one bounded HTTP request at
    // a time. Rotated bindings remain tied to that session across observations.
    fn request<T>(
        &self,
        authentication: &mut RetryAuthentication<'_>,
        mut operation: impl FnMut(&LinearApi) -> Result<T, LinearFailure>,
    ) -> anyhow::Result<Result<T, LinearFailure>> {
        let mut send = |token: &str| -> anyhow::Result<Result<T, LinearFailure>> {
            let api = LinearApi::new(
                authentication.deployment.fingerprint().api_url(),
                token,
                authentication.policy,
            )
            .map_err(|error| anyhow!(error))
            .context("prepare Linear evaluation API")?;
            Ok(operation(&api))
        };
        if let Some(key) = authentication.api_key {
            return send(key);
        }
        let rejected = |result: &anyhow::Result<Result<T, LinearFailure>>| {
            result.as_ref().is_ok_and(|value| {
                value
                    .as_ref()
                    .is_err_and(LinearFailure::credential_rejected)
            })
        };
        let outcome = if let Some(current) = authentication.binding.as_ref() {
            um_human_auth::execute_bound_required(
                authentication.client,
                authentication.deployment,
                current,
                |token| send(token.expose()),
                rejected,
            )
        } else {
            um_human_auth::execute_pinned_required(
                authentication.client,
                authentication.deployment,
                |token| send(token.expose()),
                rejected,
            )
        };
        match outcome {
            Ok(BoundRequiredOperation::Completed {
                result,
                binding: current,
                ..
            }) => {
                authentication.binding = Some(current);
                result
            }
            Ok(
                BoundRequiredOperation::Unauthenticated { .. }
                | BoundRequiredOperation::ActingSessionChanged,
            ) => Ok(Err(LinearFailure::Unauthenticated)),
            Err(error) => {
                Err(anyhow!(error).context("acquire session for Linear evaluation retry"))
            }
        }
    }

    fn poll(
        &self,
        authentication: &mut RetryAuthentication<'_>,
        control: &super::super::OperationControl<Recovery>,
        timer: &super::super::DeferredObservationTimeoutStart,
    ) -> Result<Result<ExitCode, LinearFailure>, super::super::CommandFailure> {
        if !control.admit_read() {
            return Ok(Ok(ExitCode::Interrupted));
        }
        let mut recovery = control.recovery();
        let stopped = Cell::new(false);
        let mut submit = || {
            self.request(authentication, |api| {
                if !control.begin_dispatch() {
                    stopped.set(true);
                    return Err(LinearFailure::Unauthenticated);
                }
                api.retry_evaluation(
                    &self.organization,
                    &self.project,
                    &self.trigger,
                    &self.evaluation,
                    &recovery.key,
                )
            })
        };
        let first = match submit() {
            Ok(result) => result,
            Err(_) if control.dispatched() => {
                // Session refresh can fail after the POST was sent. Even without
                // a replay, retain the request key for explicit recovery.
                return self
                    .finish(
                        control,
                        None,
                        "acceptance_unknown",
                        Some("unavailable"),
                        ExitCode::Unavailable,
                    )
                    .map(Ok);
            }
            Err(error) => return Err(error.into()),
        };
        if stopped.get() {
            return Ok(Ok(ExitCode::Interrupted));
        }
        // A failed transport or unconfirmed response cannot settle a dispatched
        // retry. Reuse the key once; even an authorization rejection on replay
        // cannot prove whether the first request was accepted.
        let uncertain = matches!(
            &first,
            Err(LinearFailure::Unreachable { .. } | LinearFailure::InvalidResponse { .. })
        );
        let accepted = if uncertain && !control.is_stopped() {
            match submit() {
                Ok(result) => result,
                // The first POST may have committed even if session refresh (or
                // preparing the replay) fails. Preserve its request identity.
                Err(_) => {
                    return self
                        .finish(
                            control,
                            None,
                            "acceptance_unknown",
                            Some("unavailable"),
                            ExitCode::Unavailable,
                        )
                        .map(Ok);
                }
            }
        } else {
            first
        };
        if stopped.get() {
            return Ok(Ok(ExitCode::Interrupted));
        }
        let accepted = match accepted {
            Ok(value) => value,
            Err(failure) if uncertain => {
                let (code, _) = failure_code(&failure, false);
                return self
                    .finish(
                        control,
                        None,
                        "acceptance_unknown",
                        Some(code),
                        ExitCode::Unavailable,
                    )
                    .map(Ok);
            }
            Err(failure) => return Ok(Err(failure)),
        };
        recovery.accepted_cycle = Some(accepted.cycle_number);
        if !control.update_recovery(recovery.clone()) {
            return Ok(Ok(ExitCode::Interrupted));
        }
        timer.start();
        let clock = super::super::SystemObservationClock;
        let mut next = Duration::ZERO;
        loop {
            if !next.is_zero() {
                let started = clock.now();
                while !control.is_stopped() && clock.now().saturating_duration_since(started) < next
                {
                    let remaining =
                        next.saturating_sub(clock.now().saturating_duration_since(started));
                    clock.sleep(remaining.min(Duration::from_millis(100)));
                }
            }
            if !control.admit_read() {
                return Ok(Ok(ExitCode::Interrupted));
            }
            let observation = match self.request(authentication, |api| {
                api.evaluation(
                    &self.organization,
                    &self.project,
                    &self.trigger,
                    &self.evaluation,
                )
            }) {
                // An observation failure does not undo a confirmed retry cycle.
                Err(_) => {
                    return self
                        .finish(
                            control,
                            None,
                            "observation_stopped",
                            Some("unavailable"),
                            ExitCode::Unavailable,
                        )
                        .map(Ok);
                }
                Ok(observation) => match observation {
                    Err(LinearFailure::RateLimited { retry_after }) => {
                        next = Duration::from_secs(retry_after.unwrap_or(2).max(2));
                        continue;
                    }
                    Err(failure) => return Ok(Err(failure)),
                    Ok(observation) => observation,
                },
            };
            if observation.cycle_number != accepted.cycle_number {
                return self
                    .finish(
                        control,
                        Some(&observation),
                        "observation_stopped",
                        Some("cycle_changed"),
                        ExitCode::GeneralFailure,
                    )
                    .map(Ok);
            }
            let terminal = match observation.state {
                LinearEvaluationState::RunCreated => Some(("run_created", ExitCode::Success)),
                LinearEvaluationState::NotMatched => {
                    Some(("not_matched", ExitCode::GeneralFailure))
                }
                LinearEvaluationState::SkippedActive => {
                    Some(("skipped_active", ExitCode::GeneralFailure))
                }
                LinearEvaluationState::Stopped => Some(("stopped", ExitCode::GeneralFailure)),
                LinearEvaluationState::Failed => Some(("failed", ExitCode::GeneralFailure)),
                _ => None,
            };
            if let Some((outcome, exit)) = terminal {
                return self
                    .finish(
                        control,
                        Some(&observation),
                        outcome,
                        observation.reason_code.as_deref(),
                        exit,
                    )
                    .map(Ok);
            }
            next = Duration::from_secs(2);
        }
    }

    fn finish(
        &self,
        control: &super::super::OperationControl<Recovery>,
        observation: Option<&LinearEvaluation>,
        outcome: &str,
        code: Option<&str>,
        exit: ExitCode,
    ) -> super::super::CommandResult {
        super::super::complete_operation(control, || {
            render(
                (
                    &self.organization,
                    &self.project,
                    &self.trigger,
                    &self.evaluation,
                    self.options.json,
                ),
                &control.recovery(),
                observation,
                outcome,
                code,
                exit,
            )
            .map_err(Into::into)
        })
    }
}

fn failure_code(failure: &LinearFailure, acceptance_unknown: bool) -> (&'static str, ExitCode) {
    match failure {
        LinearFailure::Unauthenticated => {
            ("authentication_required", ExitCode::AuthenticationRequired)
        }
        LinearFailure::Forbidden => ("forbidden", ExitCode::GeneralFailure),
        LinearFailure::NotFound => ("not_found", ExitCode::GeneralFailure),
        LinearFailure::Conflict => ("ineligible", ExitCode::GeneralFailure),
        LinearFailure::InvalidInput => ("invalid_input", ExitCode::UsageError),
        LinearFailure::RateLimited { .. } => ("rate_limited", ExitCode::Unavailable),
        LinearFailure::Unreachable { .. } => (
            if acceptance_unknown {
                "acceptance_unknown"
            } else {
                "unavailable"
            },
            ExitCode::Unavailable,
        ),
        LinearFailure::InvalidResponse { .. } => ("invalid_response", ExitCode::GeneralFailure),
    }
}

fn render(
    (org, project, trigger, id, json): (&str, &str, &str, &str, bool),
    recovery: &Recovery,
    evaluation: Option<&LinearEvaluation>,
    outcome: &str,
    code: Option<&str>,
    exit: ExitCode,
) -> anyhow::Result<ExitCode> {
    if json {
        let mut stdout = io::stdout().lock();
        serde_json::to_writer(
            &mut stdout,
            &ResultDocument {
                schema_version: 1,
                operation: "retry",
                organization_ref: org,
                project_id: project,
                trigger_id: trigger,
                evaluation_id: id,
                outcome,
                idempotency_key: &recovery.key,
                accepted_cycle: recovery.accepted_cycle,
                evaluation,
                code,
            },
        )
        .context("serialize Linear evaluation retry result")?;
        writeln!(stdout).context("write Linear evaluation retry result")?;
    } else if exit == ExitCode::Success {
        writeln!(
            io::stdout().lock(),
            "✓ Linear evaluation created a run.\n\nevaluation: {id}\nrun: {}",
            evaluation
                .and_then(|value| value.run_id.as_deref())
                .unwrap_or("not available")
        )?;
    } else {
        let mut stderr = io::stderr().lock();
        writeln!(
            stderr,
            "error: Linear evaluation retry {outcome}\n\norganization: {org}\nevaluation: {id}\nkey: {}\ncode: {}",
            recovery.key,
            code.unwrap_or(outcome)
        )?;
        writeln!(
            stderr,
            "\nShow the evaluation before requesting another retry."
        )?;
    }
    Ok(exit)
}
