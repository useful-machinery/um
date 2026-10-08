use std::sync::Arc;

use serde_json::Value;
use tokio::sync::{mpsc, oneshot};

use super::{
    AgentCompatibilityProfile, AgentFailureCause, AgentInvocation, AgentInvocationIdentity,
    AgentObservation, AgentObservationEmissionError, AgentOutcome, AgentStartCallback,
    AgentStartReportError, AgentValueKind, AgentValueMode, BoundedAgentResponse, CapturedJson,
    CompletedAgentInvocation, failed_agent_outcome,
};
use crate::workflow::canonical_json;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ScriptedAgentValue {
    Response(Arc<str>),
    Result(Arc<Value>),
}

impl ScriptedAgentValue {
    fn kind(&self) -> AgentValueKind {
        match self {
            Self::Response(_) => AgentValueKind::Response,
            Self::Result(_) => AgentValueKind::Result,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ScriptedInvocationStarted {
    identity: AgentInvocationIdentity,
    profile: AgentCompatibilityProfile,
    system_prompt: Arc<str>,
    message: Arc<str>,
    working_directory: std::path::PathBuf,
    result_endpoint_directory: std::path::PathBuf,
    diagnostic_directory: std::path::PathBuf,
    environment: crate::workflow::admission::EnvironmentSnapshot,
    attachments: Arc<[super::StagedAgentAttachment]>,
    value_kind: AgentValueKind,
    control: ScriptedInvocationControl,
}

impl ScriptedInvocationStarted {
    pub(crate) fn identity(&self) -> &AgentInvocationIdentity {
        &self.identity
    }

    pub(crate) fn profile(&self) -> AgentCompatibilityProfile {
        self.profile
    }

    // The scripted adapter exposes prompt fields for protocol-order assertions; these accessors
    // intentionally mirror the immutable production prompt without sharing its authority type.
    pub(crate) fn system_prompt(&self) -> &str {
        &self.system_prompt
    }

    pub(crate) fn message(&self) -> &str {
        &self.message
    }

    pub(crate) fn working_directory(&self) -> &std::path::Path {
        &self.working_directory
    }

    pub(crate) fn result_endpoint_directory(&self) -> &std::path::Path {
        &self.result_endpoint_directory
    }

    pub(crate) fn diagnostic_directory(&self) -> &std::path::Path {
        &self.diagnostic_directory
    }

    pub(crate) fn environment(&self) -> &crate::workflow::admission::EnvironmentSnapshot {
        &self.environment
    }

    pub(crate) fn attachments(&self) -> &[super::StagedAgentAttachment] {
        &self.attachments
    }

    pub(crate) fn value_kind(&self) -> AgentValueKind {
        self.value_kind
    }

    pub(crate) fn control(&self) -> &ScriptedInvocationControl {
        &self.control
    }
}

#[derive(Clone)]
pub(crate) struct ScriptedAgentDispatcher {
    started: mpsc::UnboundedSender<ScriptedInvocationStarted>,
}

pub(crate) struct ScriptedAgentControl {
    current: Option<ScriptedInvocationControl>,
    started: mpsc::UnboundedReceiver<ScriptedInvocationStarted>,
}

#[derive(Clone, Debug)]
pub(crate) struct ScriptedInvocationControl {
    commands: mpsc::UnboundedSender<ScriptedCommand>,
}

type CommandAcknowledgement = oneshot::Sender<Result<(), ScriptedAgentError>>;

enum ScriptedCommand {
    Start {
        acknowledged: CommandAcknowledgement,
    },
    Barrier {
        reached: oneshot::Sender<()>,
        release: oneshot::Receiver<()>,
    },
    Observe {
        observation: AgentObservation,
        acknowledged: CommandAcknowledgement,
    },
    Propose {
        value: ScriptedAgentValue,
        acknowledged: CommandAcknowledgement,
    },
    Complete {
        acknowledged: CommandAcknowledgement,
    },
    Fail {
        cause: AgentFailureCause,
        acknowledged: CommandAcknowledgement,
    },
}

pub(crate) struct ScriptedBarrier {
    reached: oneshot::Receiver<()>,
    release: Option<oneshot::Sender<()>>,
}

impl ScriptedBarrier {
    pub(crate) async fn wait_until_blocked(&mut self) -> Result<(), ScriptedAgentError> {
        (&mut self.reached)
            .await
            .map_err(|_| ScriptedAgentError::AdapterStopped)
    }

    pub(crate) fn release(mut self) -> Result<(), ScriptedAgentError> {
        self.release
            .take()
            .ok_or(ScriptedAgentError::BarrierAlreadyReleased)?
            .send(())
            .map_err(|_| ScriptedAgentError::AdapterStopped)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ScriptedAgentError {
    AdapterStopped,
    BarrierAlreadyReleased,
    InvocationAlreadyStarted,
    InvocationCancelled,
    InvocationNotStarted,
    ObservationSequenceExhausted,
    ValueAlreadyProposed,
    ValueTooLarge,
    WrongValueMode,
}

pub(crate) fn scripted_agent_dispatcher() -> (ScriptedAgentDispatcher, ScriptedAgentControl) {
    let (started_sender, started) = mpsc::unbounded_channel();
    (
        ScriptedAgentDispatcher {
            started: started_sender,
        },
        ScriptedAgentControl {
            current: None,
            started,
        },
    )
}

impl ScriptedAgentControl {
    pub(crate) async fn wait_until_started(
        &mut self,
    ) -> Result<ScriptedInvocationStarted, ScriptedAgentError> {
        let started = self
            .started
            .recv()
            .await
            .ok_or(ScriptedAgentError::AdapterStopped)?;
        self.current = Some(started.control.clone());
        Ok(started)
    }

    fn current(&self) -> Result<&ScriptedInvocationControl, ScriptedAgentError> {
        self.current
            .as_ref()
            .ok_or(ScriptedAgentError::AdapterStopped)
    }

    pub(crate) async fn observe(
        &self,
        observation: AgentObservation,
    ) -> Result<(), ScriptedAgentError> {
        self.current()?.observe(observation).await
    }

    pub(crate) async fn propose(
        &self,
        value: ScriptedAgentValue,
    ) -> Result<(), ScriptedAgentError> {
        self.current()?.propose(value).await
    }

    pub(crate) async fn complete(&self) -> Result<(), ScriptedAgentError> {
        self.current()?.complete().await
    }
}

impl ScriptedInvocationControl {
    pub(crate) async fn start(&self) -> Result<(), ScriptedAgentError> {
        let (acknowledged, acknowledgement) = oneshot::channel();
        self.commands
            .send(ScriptedCommand::Start { acknowledged })
            .map_err(|_| ScriptedAgentError::AdapterStopped)?;
        receive_acknowledgement(acknowledgement).await
    }

    pub(crate) fn block(&self) -> Result<ScriptedBarrier, ScriptedAgentError> {
        let (reached, wait_for_reached) = oneshot::channel();
        let (release, wait_for_release) = oneshot::channel();
        self.commands
            .send(ScriptedCommand::Barrier {
                reached,
                release: wait_for_release,
            })
            .map_err(|_| ScriptedAgentError::AdapterStopped)?;
        Ok(ScriptedBarrier {
            reached: wait_for_reached,
            release: Some(release),
        })
    }

    pub(crate) async fn observe(
        &self,
        observation: AgentObservation,
    ) -> Result<(), ScriptedAgentError> {
        let (acknowledged, acknowledgement) = oneshot::channel();
        self.commands
            .send(ScriptedCommand::Observe {
                observation,
                acknowledged,
            })
            .map_err(|_| ScriptedAgentError::AdapterStopped)?;
        receive_acknowledgement(acknowledgement).await
    }

    pub(crate) async fn propose(
        &self,
        value: ScriptedAgentValue,
    ) -> Result<(), ScriptedAgentError> {
        let (acknowledged, acknowledgement) = oneshot::channel();
        self.commands
            .send(ScriptedCommand::Propose {
                value,
                acknowledged,
            })
            .map_err(|_| ScriptedAgentError::AdapterStopped)?;
        receive_acknowledgement(acknowledgement).await
    }

    pub(crate) async fn complete(&self) -> Result<(), ScriptedAgentError> {
        let (acknowledged, acknowledgement) = oneshot::channel();
        self.commands
            .send(ScriptedCommand::Complete { acknowledged })
            .map_err(|_| ScriptedAgentError::AdapterStopped)?;
        receive_acknowledgement(acknowledgement).await
    }

    pub(crate) async fn fail(&self, cause: AgentFailureCause) -> Result<(), ScriptedAgentError> {
        let (acknowledged, acknowledgement) = oneshot::channel();
        self.commands
            .send(ScriptedCommand::Fail {
                cause,
                acknowledged,
            })
            .map_err(|_| ScriptedAgentError::AdapterStopped)?;
        receive_acknowledgement(acknowledgement).await
    }
}

async fn receive_acknowledgement(
    acknowledgement: oneshot::Receiver<Result<(), ScriptedAgentError>>,
) -> Result<(), ScriptedAgentError> {
    acknowledgement
        .await
        .map_err(|_| ScriptedAgentError::AdapterStopped)?
}

impl super::AgentAdapter for ScriptedAgentDispatcher {
    async fn invoke(
        &self,
        invocation: AgentInvocation,
        started: AgentStartCallback,
    ) -> AgentOutcome {
        let (command_sender, mut commands) = mpsc::unbounded_channel();
        let mut cancellation = invocation.cancellation().subscribe();
        if let Some(reason) = *cancellation.borrow_and_update() {
            return AgentOutcome::Cancelled { reason };
        }
        if self
            .started
            .send(ScriptedInvocationStarted {
                identity: invocation.identity().clone(),
                profile: invocation.adapter().profile(),
                system_prompt: Arc::from(invocation.prompt().system_prompt()),
                message: Arc::from(invocation.prompt().message()),
                working_directory: invocation.process().cwd().to_owned(),
                result_endpoint_directory: invocation
                    .staging()
                    .result_endpoint_directory()
                    .to_owned(),
                diagnostic_directory: invocation.diagnostic_session().directory().to_owned(),
                environment: invocation.process().environment().clone(),
                attachments: Arc::from(invocation.attachments()),
                value_kind: invocation.value_mode().kind(),
                control: ScriptedInvocationControl {
                    commands: command_sender,
                },
            })
            .is_err()
        {
            return failed_agent_outcome(AgentFailureCause::HarnessProtocolFailed);
        }

        let mut lifecycle_started = false;
        let mut provisional = None;
        loop {
            if let Some(reason) = invocation.cancellation().cancellation_reason() {
                drop(provisional.take());
                return AgentOutcome::Cancelled { reason };
            }

            tokio::select! {
                biased;
                changed = cancellation.changed() => {
                    if changed.is_err() {
                        return failed_agent_outcome(AgentFailureCause::HarnessProtocolFailed);
                    }
                    if let Some(reason) = *cancellation.borrow_and_update() {
                        drop(provisional.take());
                        return AgentOutcome::Cancelled { reason };
                    }
                }
                command = commands.recv() => {
                    let Some(command) = command else {
                        break;
                    };
                    match command {
                        ScriptedCommand::Start { acknowledged } => {
                            let result = if lifecycle_started {
                                Err(ScriptedAgentError::InvocationAlreadyStarted)
                            } else {
                                started.report().map_err(ScriptedAgentError::from)
                            };
                            if result.is_ok() {
                                lifecycle_started = true;
                            }
                            let _ = acknowledged.send(result);
                        }
                        ScriptedCommand::Barrier { reached, release } => {
                            let _ = reached.send(());
                            let _ = release.await;
                        }
                        ScriptedCommand::Observe {
                            observation,
                            acknowledged,
                        } => {
                            let result = if lifecycle_started {
                                invocation
                                    .observations()
                                    .emit(observation)
                                    .await
                                    .map_err(ScriptedAgentError::from)
                            } else {
                                Err(ScriptedAgentError::InvocationNotStarted)
                            };
                            let _ = acknowledged.send(result);
                        }
                        ScriptedCommand::Propose {
                            value,
                            acknowledged,
                        } => {
                            let result = if lifecycle_started {
                                propose_value(&invocation, &mut provisional, value)
                            } else {
                                Err(ScriptedAgentError::InvocationNotStarted)
                            };
                            let _ = acknowledged.send(result);
                        }
                        ScriptedCommand::Complete { acknowledged } => {
                            if !lifecycle_started {
                                let _ = acknowledged.send(Err(
                                    ScriptedAgentError::InvocationNotStarted,
                                ));
                                continue;
                            }
                            let outcome = cancellation_outcome(&invocation).unwrap_or_else(|| {
                                completed_outcome(&invocation, provisional.take())
                            });
                            let _ = acknowledged.send(Ok(()));
                            return outcome;
                        }
                        ScriptedCommand::Fail {
                            cause,
                            acknowledged,
                        } => {
                            let outcome = cancellation_outcome(&invocation)
                                .unwrap_or_else(|| failed_agent_outcome(cause));
                            let _ = acknowledged.send(Ok(()));
                            return outcome;
                        }
                    }
                }
            }
        }

        cancellation_outcome(&invocation)
            .unwrap_or_else(|| failed_agent_outcome(AgentFailureCause::HarnessProtocolFailed))
    }
}

fn propose_value(
    invocation: &AgentInvocation,
    provisional: &mut Option<CompletedAgentInvocation>,
    value: ScriptedAgentValue,
) -> Result<(), ScriptedAgentError> {
    if invocation.cancellation().is_cancelled() {
        return Err(ScriptedAgentError::InvocationCancelled);
    }
    if provisional.is_some() {
        return Err(ScriptedAgentError::ValueAlreadyProposed);
    }
    if invocation.value_mode().kind() != value.kind() {
        return Err(ScriptedAgentError::WrongValueMode);
    }

    let completed = match value {
        ScriptedAgentValue::Response(value) => {
            if u64::try_from(value.len()).map_or(true, |bytes| {
                bytes > invocation.limits().maximum_response_bytes().get()
            }) {
                return Err(ScriptedAgentError::ValueTooLarge);
            }
            CompletedAgentInvocation::Response(BoundedAgentResponse::from_bounded(value))
        }
        ScriptedAgentValue::Result(value) => {
            CompletedAgentInvocation::Result(captured_json(invocation, value)?)
        }
    };
    *provisional = Some(completed);
    Ok(())
}

fn captured_json(
    invocation: &AgentInvocation,
    value: Arc<Value>,
) -> Result<CapturedJson, ScriptedAgentError> {
    let AgentValueMode::Result { schema, .. } = invocation.value_mode() else {
        return Err(ScriptedAgentError::WrongValueMode);
    };
    let carrier =
        canonical_json::to_bounded_bytes(&value, invocation.limits().maximum_result_bytes().get())
            .map_err(|failure| match failure {
                canonical_json::CanonicalJsonError::SizeLimitExceeded => {
                    ScriptedAgentError::ValueTooLarge
                }
                canonical_json::CanonicalJsonError::SerializationFailed => {
                    ScriptedAgentError::WrongValueMode
                }
            })?;
    Ok(CapturedJson::from_validated(value, carrier, schema.clone()))
}

fn completed_outcome(
    invocation: &AgentInvocation,
    provisional: Option<CompletedAgentInvocation>,
) -> AgentOutcome {
    match (invocation.value_mode().kind(), provisional) {
        (AgentValueKind::None, None) => AgentOutcome::Completed(CompletedAgentInvocation::NoValue),
        (AgentValueKind::Response, Some(completed @ CompletedAgentInvocation::Response(_)))
        | (AgentValueKind::Result, Some(completed @ CompletedAgentInvocation::Result(_))) => {
            AgentOutcome::Completed(completed)
        }
        (AgentValueKind::Response, None) => {
            failed_agent_outcome(AgentFailureCause::MissingResponse)
        }
        (AgentValueKind::Result, None) => failed_agent_outcome(AgentFailureCause::MissingResult),
        (AgentValueKind::None, Some(_))
        | (AgentValueKind::Response, Some(_))
        | (AgentValueKind::Result, Some(_)) => {
            failed_agent_outcome(AgentFailureCause::HarnessProtocolFailed)
        }
    }
}

fn cancellation_outcome(invocation: &AgentInvocation) -> Option<AgentOutcome> {
    invocation
        .cancellation()
        .cancellation_reason()
        .map(|reason| AgentOutcome::Cancelled { reason })
}

impl From<AgentObservationEmissionError> for ScriptedAgentError {
    fn from(value: AgentObservationEmissionError) -> Self {
        match value {
            AgentObservationEmissionError::SequenceExhausted => Self::ObservationSequenceExhausted,
        }
    }
}

impl From<AgentStartReportError> for ScriptedAgentError {
    fn from(value: AgentStartReportError) -> Self {
        match value {
            AgentStartReportError::AlreadyReported => Self::InvocationAlreadyStarted,
            AgentStartReportError::ReceiverClosed => Self::AdapterStopped,
        }
    }
}
