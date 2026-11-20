//! The adapter's end of the Claude Code result tool (INV-HARNESS-23): the private socket the
//! relay reaches, the MCP configuration file that tells Claude Code to start the relay, the
//! hello record, and the judgement of each submission.

use std::io;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use serde_json::{Value, json};

use super::result_tool::{
    MAXIMUM_REQUEST_FRAME_BYTES, ROLE_ENVIRONMENT, ROLE_VALUE, ResultChannelAnswer,
    ResultChannelFailure, ResultChannelRequest, SERVER_NAME, SOCKET_ENVIRONMENT,
};
use crate::workflow::admission::{CancellationReason, CancellationSource};
use crate::workflow::agent::{
    AgentFailureCause, AgentObservation, AgentValueKind, CapturedJson, PositiveDuration,
};
use crate::workflow::agent_result_socket::{
    self, bind_result_socket, remove_materialized_file, remove_result_socket,
    validate_result_endpoint_directory, write_private_file,
};
use crate::workflow::coordinator::CoordinatorClock;
use crate::workflow::result_validation::{
    AuthoritativeResultValidator, ResultValidationDecision, ResultValidationOutcome,
    ResultValidationWorker,
};
use crate::workflow::socket_alias::{SOCKET_ALIAS_ROOT, random_socket_alias_directory};

const SOCKET_FILE_NAME: &str = "result-tool.sock";
const CONFIGURATION_FILE_NAME: &str = "result-tool-mcp.json";
const SOCKET_ALIAS_PREFIX: &str = ".szc-";
const MISSING_RESULT_FEEDBACK: &str = "Submission rejected: give the value as \
    `submission.result`, as in {\"submission\": {\"result\": <value>}}.\n";

type ResultChannelServer =
    agent_result_socket::ResultSocketServer<ResultChannelRequest, ResultChannelAnswer>;
pub(super) type ResultChannelEvent =
    agent_result_socket::ResultSocketEvent<ResultChannelRequest, ResultChannelAnswer>;

/// Whether the relay has said hello. It is recorded when the adapter receives the hello, before
/// the hello can be answered, so code that reads it needs no await: Claude Code writes
/// `system/init` only after the relay has answered `initialize`, which follows the answer.
#[derive(Clone, Debug, Default)]
pub(super) struct ResultChannelHello(Arc<AtomicBool>);

impl ResultChannelHello {
    pub(super) fn recorded(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }

    fn record(&self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[derive(Debug)]
pub(super) struct PreparedResultChannel {
    configuration_path: PathBuf,
    socket_path: PathBuf,
    socket_alias_directory: PathBuf,
    server: ResultChannelServer,
    hello: ResultChannelHello,
}

impl PreparedResultChannel {
    /// Binds the socket (0600) in the private `directory` through a short alias and writes the
    /// MCP configuration file (0400) that starts the internal worker as the relay.
    pub(super) fn prepare<Clock: CoordinatorClock>(
        directory: &Path,
        receive_deadline: PositiveDuration,
        clock: Clock,
    ) -> io::Result<Self> {
        validate_result_endpoint_directory(directory)?;
        let executable = crate::process::internal_worker_executable()?;
        let executable = executable
            .to_str()
            .ok_or_else(|| invalid_channel("internal worker path is not UTF-8"))?;
        let socket = bind_result_socket(directory, SOCKET_FILE_NAME, || {
            random_socket_alias_directory(Path::new(SOCKET_ALIAS_ROOT), SOCKET_ALIAS_PREFIX)
        })?;
        let configuration_path = directory.join(CONFIGURATION_FILE_NAME);
        let written = socket
            .address
            .to_str()
            .ok_or_else(|| invalid_channel("socket path is not UTF-8"))
            .and_then(|address| {
                serde_json::to_vec(&mcp_configuration(executable, address)).map_err(io::Error::from)
            })
            .and_then(|bytes| {
                write_private_file(&configuration_path, &bytes).map_err(|error| {
                    io::Error::new(error.kind(), format!("MCP configuration write: {error}"))
                })
            });
        if let Err(error) = written {
            drop(socket.listener);
            let _ = remove_result_socket(&socket.socket_path, &socket.alias_directory);
            return Err(error);
        }
        let server = ResultChannelServer::start(
            socket.listener,
            MAXIMUM_REQUEST_FRAME_BYTES,
            receive_deadline,
            clock,
        );
        Ok(Self {
            configuration_path,
            socket_path: socket.socket_path,
            socket_alias_directory: socket.alias_directory,
            server,
            hello: ResultChannelHello::default(),
        })
    }

    /// The file `--mcp-config` names.
    pub(super) fn configuration_path(&self) -> &Path {
        &self.configuration_path
    }

    pub(super) fn hello(&self) -> ResultChannelHello {
        self.hello.clone()
    }

    /// The next request, a hello already recorded. Cancel-safe: nothing is awaited after the
    /// request is taken.
    pub(super) async fn receive(&mut self) -> ResultChannelEvent {
        let event = self.server.receive().await;
        if let ResultChannelEvent::Request(incoming) = &event
            && *incoming.request() == ResultChannelRequest::Hello
        {
            self.hello.record();
        }
        event
    }

    /// Stops serving and removes the socket, its alias and the configuration file.
    pub(super) async fn shutdown(self) -> io::Result<()> {
        let server_result = self.server.shutdown().await;
        let configuration_result = remove_materialized_file(&self.configuration_path);
        let socket_result = remove_result_socket(&self.socket_path, &self.socket_alias_directory);
        server_result.and(configuration_result).and(socket_result)
    }
}

/// The MCP configuration Claude Code reads: the one server, started with the role and the
/// socket's alias address in its environment, and loaded at once rather than deferred behind
/// tool search.
fn mcp_configuration(executable: &str, socket_address: &str) -> Value {
    json!({
        "mcpServers": {
            SERVER_NAME: {
                "type": "stdio",
                "command": executable,
                "args": [],
                "env": {
                    ROLE_ENVIRONMENT: ROLE_VALUE,
                    SOCKET_ENVIRONMENT: socket_address,
                },
                "alwaysLoad": true,
            },
        },
    })
}

fn invalid_channel(error: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}

/// What to do with one request: the answer for the relay and what to observe, the adapter's
/// own failure (answer, then stop the harness with the cause), or the invocation's
/// cancellation.
#[derive(Debug)]
pub(super) enum SubmissionJudgement {
    Answer {
        answer: ResultChannelAnswer,
        observation: Option<AgentObservation>,
    },
    Fatal {
        answer: ResultChannelAnswer,
        cause: AgentFailureCause,
    },
    Cancelled(CancellationReason),
}

/// The submissions of one invocation: the last one accepted is the step's value
/// (REQ-AGENT-85).
pub(super) struct ResultSubmissions {
    accepted: Option<CapturedJson>,
    maximum_feedback_bytes: NonZeroU64,
}

impl ResultSubmissions {
    pub(super) fn new(maximum_feedback_bytes: NonZeroU64) -> Self {
        Self {
            accepted: None,
            maximum_feedback_bytes,
        }
    }

    pub(super) fn accepted(&self) -> Option<&CapturedJson> {
        self.accepted.as_ref()
    }

    /// Judges one request. A submission's `submission.result` goes to the validator as
    /// received; accepted, it replaces the value held, and rejected, the value held stands
    /// (REQ-AGENT-84, REQ-AGENT-85).
    pub(super) async fn judge<Clock, Worker>(
        &mut self,
        request: &ResultChannelRequest,
        validator: &AuthoritativeResultValidator<Clock, Worker>,
        cancellation: &CancellationSource,
    ) -> SubmissionJudgement
    where
        Clock: CoordinatorClock,
        Worker: ResultValidationWorker,
    {
        let arguments = match request {
            ResultChannelRequest::Hello => {
                return SubmissionJudgement::Answer {
                    answer: ResultChannelAnswer::Ready,
                    observation: None,
                };
            }
            ResultChannelRequest::Submission { arguments } => arguments,
        };
        let Some(candidate) = submitted_result(arguments) else {
            let maximum_bytes =
                usize::try_from(self.maximum_feedback_bytes.get()).unwrap_or(usize::MAX);
            let feedback = &MISSING_RESULT_FEEDBACK
                [..MISSING_RESULT_FEEDBACK.floor_char_boundary(maximum_bytes)];
            return rejected(Arc::from(feedback));
        };
        match validator
            .validate(Arc::new(candidate.clone()), cancellation)
            .await
        {
            ResultValidationOutcome::Cancelled { reason } => SubmissionJudgement::Cancelled(reason),
            ResultValidationOutcome::Decided(ResultValidationDecision::Valid(value)) => {
                self.accepted = Some(value);
                SubmissionJudgement::Answer {
                    answer: ResultChannelAnswer::Accepted,
                    observation: None,
                }
            }
            ResultValidationOutcome::Decided(ResultValidationDecision::Rejected { feedback }) => {
                rejected(feedback)
            }
            ResultValidationOutcome::Decided(ResultValidationDecision::Fatal(fatal)) => {
                SubmissionJudgement::Fatal {
                    answer: ResultChannelAnswer::Failed {
                        cause: ResultChannelFailure::Validation,
                    },
                    cause: AgentFailureCause::from(fatal),
                }
            }
        }
    }
}

fn submitted_result(arguments: &Value) -> Option<&Value> {
    arguments.get("submission")?.as_object()?.get("result")
}

fn rejected(feedback: Arc<str>) -> SubmissionJudgement {
    SubmissionJudgement::Answer {
        answer: ResultChannelAnswer::Rejected {
            feedback: feedback.to_string(),
        },
        observation: Some(AgentObservation::ValueRejected {
            kind: AgentValueKind::Result,
            feedback,
        }),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::workflow::agent::RetainedJsonSchema;
    use crate::workflow::test_support::claude_validation::{
        FailingValidationWorker, PendingValidationWorker,
    };
    use crate::workflow::test_support::{ControlledClock, InlineValidationWorker, PendingClock};

    const FEEDBACK_BYTES: NonZeroU64 = NonZeroU64::new(8 * 1024).unwrap();
    const RESULT_BYTES: NonZeroU64 = NonZeroU64::new(8 * 1024 * 1024).unwrap();

    fn schema(mut document: Value) -> RetainedJsonSchema {
        document["$schema"] = json!("https://json-schema.org/draft/2020-12/schema");
        let bytes = Arc::<[u8]>::from(serde_json::to_vec(&document).unwrap());
        RetainedJsonSchema::compile(bytes, Arc::new(document)).unwrap()
    }

    fn deadline() -> PositiveDuration {
        PositiveDuration::new(Duration::from_secs(60)).unwrap()
    }

    fn validator<Clock: CoordinatorClock, Worker: ResultValidationWorker>(
        document: Value,
        maximum_result_bytes: NonZeroU64,
        clock: Clock,
        worker: Worker,
    ) -> AuthoritativeResultValidator<Clock, Worker> {
        AuthoritativeResultValidator::new(
            schema(document),
            maximum_result_bytes,
            FEEDBACK_BYTES,
            deadline(),
            clock,
            worker,
        )
    }

    fn positive_integers() -> AuthoritativeResultValidator<PendingClock, InlineValidationWorker> {
        validator(
            json!({"type": "integer", "minimum": 1}),
            RESULT_BYTES,
            PendingClock,
            InlineValidationWorker,
        )
    }

    fn submission(result: Value) -> ResultChannelRequest {
        ResultChannelRequest::Submission {
            arguments: json!({"submission": {"result": result}}),
        }
    }

    /// Asserts a rejection answered and observed with the same feedback, and returns it.
    fn rejection_feedback(judgement: SubmissionJudgement) -> Arc<str> {
        let SubmissionJudgement::Answer {
            answer: ResultChannelAnswer::Rejected { feedback: answered },
            observation:
                Some(AgentObservation::ValueRejected {
                    kind: AgentValueKind::Result,
                    feedback,
                }),
        } = judgement
        else {
            panic!("expected a rejection, got {judgement:?}");
        };
        assert_eq!(answered, feedback.as_ref());
        assert!(!feedback.is_empty());
        feedback
    }

    fn assert_accepted(judgement: &SubmissionJudgement) {
        assert!(
            matches!(
                judgement,
                SubmissionJudgement::Answer {
                    answer: ResultChannelAnswer::Accepted,
                    observation: None,
                }
            ),
            "expected an acceptance, got {judgement:?}"
        );
    }

    // REQ-AGENT-85
    #[tokio::test]
    async fn the_last_accepted_submission_is_held_and_a_later_rejection_leaves_it() {
        let validator = positive_integers();
        let cancellation = CancellationSource::new();
        let mut submissions = ResultSubmissions::new(FEEDBACK_BYTES);
        assert!(submissions.accepted().is_none());

        for value in [3, 5] {
            let judgement = submissions
                .judge(&submission(json!(value)), &validator, &cancellation)
                .await;
            assert_accepted(&judgement);
        }
        assert_eq!(submissions.accepted().unwrap().value(), &json!(5));

        let judgement = submissions
            .judge(&submission(json!(0)), &validator, &cancellation)
            .await;
        rejection_feedback(judgement);
        assert_eq!(submissions.accepted().unwrap().value(), &json!(5));
    }

    // REQ-AGENT-84, REQ-AGENT-32
    #[tokio::test]
    async fn a_string_and_null_are_judged_and_held_as_written() {
        let cancellation = CancellationSource::new();
        for (document, value) in [
            (json!({"type": "string"}), json!("7")),
            (json!({"type": "null"}), Value::Null),
        ] {
            let validator = validator(document, RESULT_BYTES, PendingClock, InlineValidationWorker);
            let mut submissions = ResultSubmissions::new(FEEDBACK_BYTES);
            let judgement = submissions
                .judge(&submission(value.clone()), &validator, &cancellation)
                .await;
            assert_accepted(&judgement);
            assert_eq!(submissions.accepted().unwrap().value(), &value);
        }
    }

    // REQ-AGENT-84, REQ-AGENT-29
    #[tokio::test]
    async fn arguments_without_submission_result_are_rejected_before_the_validator() {
        // A validator reached would answer Fatal: this worker cannot start.
        let validator = validator(
            json!({}),
            RESULT_BYTES,
            PendingClock,
            FailingValidationWorker,
        );
        let cancellation = CancellationSource::new();
        let mut submissions = ResultSubmissions::new(FEEDBACK_BYTES);
        for arguments in [
            json!({"result": 7}),
            json!({"submission": 7}),
            json!({"submission": "{\"result\": 7}"}),
            json!({"submission": {}}),
            json!({}),
            Value::Null,
        ] {
            let judgement = submissions
                .judge(
                    &ResultChannelRequest::Submission { arguments },
                    &validator,
                    &cancellation,
                )
                .await;
            rejection_feedback(judgement);
        }
        assert!(submissions.accepted().is_none());
    }

    // REQ-AGENT-30
    #[tokio::test]
    async fn a_value_above_the_result_limit_is_rejected_with_feedback() {
        let validator = validator(
            json!({"type": "string"}),
            NonZeroU64::new(16).unwrap(),
            PendingClock,
            InlineValidationWorker,
        );
        let cancellation = CancellationSource::new();
        let mut submissions = ResultSubmissions::new(FEEDBACK_BYTES);
        let judgement = submissions
            .judge(
                &submission(json!("x".repeat(64))),
                &validator,
                &cancellation,
            )
            .await;
        rejection_feedback(judgement);
        assert!(submissions.accepted().is_none());
    }

    // REQ-AGENT-46
    #[tokio::test]
    async fn a_cancelled_invocation_judges_nothing() {
        let validator = positive_integers();
        let cancellation = CancellationSource::new();
        assert!(cancellation.request_cancellation(CancellationReason::UserRequest));
        let mut submissions = ResultSubmissions::new(FEEDBACK_BYTES);
        let judgement = submissions
            .judge(&submission(json!(7)), &validator, &cancellation)
            .await;
        let SubmissionJudgement::Cancelled(reason) = judgement else {
            panic!("expected the cancellation, got {judgement:?}");
        };
        assert_eq!(reason, CancellationReason::UserRequest);
        assert!(submissions.accepted().is_none());
    }

    // REQ-AGENT-34
    #[tokio::test]
    async fn a_validator_deadline_is_the_fatal_cause() {
        let stopped = Arc::new(AtomicBool::new(false));
        let quiesced = Arc::new(AtomicBool::new(false));
        let (clock, mut control) = ControlledClock::new();
        let validator = validator(
            json!({"type": "integer"}),
            RESULT_BYTES,
            clock,
            PendingValidationWorker {
                stopped: Arc::clone(&stopped),
                quiesced: Arc::clone(&quiesced),
            },
        );
        let cancellation = CancellationSource::new();
        let mut submissions = ResultSubmissions::new(FEEDBACK_BYTES);
        let request = submission(json!(7));
        let (judgement, ()) = tokio::join!(
            submissions.judge(&request, &validator, &cancellation),
            async {
                assert_eq!(control.deadlines.recv().await, Some(deadline().get()));
                control.expired.send_replace(true);
            }
        );
        let SubmissionJudgement::Fatal { answer, cause } = judgement else {
            panic!("expected the fatal cause, got {judgement:?}");
        };
        assert_eq!(
            answer,
            ResultChannelAnswer::Failed {
                cause: ResultChannelFailure::Validation
            }
        );
        assert_eq!(
            cause,
            AgentFailureCause::ResultValidationLimitExceeded {
                deadline: deadline()
            }
        );
        assert!(stopped.load(Ordering::SeqCst));
        assert!(quiesced.load(Ordering::SeqCst));
        assert!(submissions.accepted().is_none());
    }
}
