//! The Claude Code result tool: the `useful` executable in this worker role is the stdio MCP
//! server `useful_workflow_result` that Claude Code starts from the invocation's
//! `--mcp-config` file. It serves the one tool `submit` and relays each call to the
//! invocation's result channel, where the adapter judges it (INV-HARNESS-23). It holds no state
//! of its own, so a Claude Code that starts it again loses nothing.

use std::io::{self, BufRead, BufReader, Read, Write};
use std::num::NonZeroU64;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Sender};
use std::thread;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::ExecutionOutcome;
use crate::workflow::agent_result_socket::ResultSocketResponse;

pub(super) const ROLE_ENVIRONMENT: &str = "USEFUL_INTERNAL_RESULT_TOOL_WORKER";
pub(super) const ROLE_VALUE: &str = "claude-code-result-tool";
pub(super) const SOCKET_ENVIRONMENT: &str = "USEFUL_INTERNAL_RESULT_TOOL_SOCKET";
pub(super) const SERVER_NAME: &str = "useful_workflow_result";
pub(super) const TOOL_NAME: &str = "submit";
/// The largest request frame the result channel reads: twice the 8 MiB result limit, so the
/// validator, not the channel, rejects a value above that limit (REQ-AGENT-30).
pub(super) const MAXIMUM_REQUEST_FRAME_BYTES: NonZeroU64 = match NonZeroU64::new(16 * 1024 * 1024) {
    Some(bytes) => bytes,
    None => NonZeroU64::MIN,
};

const MAXIMUM_ANSWER_FRAME_BYTES: u32 = 1024 * 1024;
const MAXIMUM_REQUEST_LINE_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_PROTOCOL_VERSION: &str = "2025-06-18";
const SERVER_VERSION: &str = "1";
const TOOL_DESCRIPTION: &str = "Submit this step's result here, as `submission.result`. A \
    rejected submission comes back with the reasons; correct it and submit again. The last \
    accepted submission is the step's result. A step that ends without an accepted submission \
    fails.";
const ACCEPTED_TEXT: &str = "Submission accepted. It is this step's result unless a later \
    submission is accepted.";
const CHANNEL_FAILED_TEXT: &str = "The workflow result channel failed, so this submission was \
    not recorded.";
const VALIDATION_FAILED_TEXT: &str = "The workflow could not validate this submission.";
const TOO_LARGE_TEXT: &str = "Submission rejected: it is larger than the 16 MiB the workflow \
    result channel carries. Submit a smaller result.";
const CHANNEL_UNAVAILABLE_MESSAGE: &str = "The workflow result channel is unavailable.";

const PARSE_ERROR: i64 = -32700;
const INVALID_REQUEST: i64 = -32600;
const METHOD_NOT_FOUND: i64 = -32601;
const INVALID_PARAMS: i64 = -32602;
const INTERNAL_ERROR: i64 = -32603;

/// What the relay asks the result channel: a hello when Claude Code initializes it, or a
/// submission carrying a `tools/call`'s arguments verbatim.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(super) enum ResultChannelRequest {
    Hello,
    Submission { arguments: Value },
}

/// The result channel's answer: the hello is recorded, the submission is accepted, rejected
/// with feedback, or the channel failed with a closed cause.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(super) enum ResultChannelAnswer {
    Ready,
    Accepted,
    Rejected { feedback: String },
    Failed { cause: ResultChannelFailure },
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum ResultChannelFailure {
    /// The channel could not read the request.
    Channel,
    /// The adapter could not judge the submission.
    Validation,
}

impl ResultSocketResponse for ResultChannelAnswer {
    fn channel_failure() -> Self {
        Self::Failed {
            cause: ResultChannelFailure::Channel,
        }
    }
}

/// The tool the server lists, exactly as Claude Code is given it.
pub(super) fn tool_definition() -> Value {
    json!({
        "name": TOOL_NAME,
        "description": TOOL_DESCRIPTION,
        "inputSchema": {
            "type": "object",
            "properties": {
                "submission": {
                    "type": "object",
                    "properties": {"result": {}},
                    "required": ["result"],
                },
            },
            "required": ["submission"],
        },
    })
}

/// Whether this process was launched as the Claude Code result tool. Asking also records that
/// the executable dispatches the worker roles (INV-TESTING-20), as the other roles' do.
pub fn internal_worker_requested() -> bool {
    crate::process::record_internal_worker_dispatch();
    std::env::var(ROLE_ENVIRONMENT).as_deref() == Ok(ROLE_VALUE)
}

pub fn run_internal_worker() -> ExecutionOutcome {
    let socket = std::env::var_os(SOCKET_ENVIRONMENT).map(PathBuf::from);
    match serve(io::stdin(), &mut io::stdout().lock(), socket) {
        Ok(()) => ExecutionOutcome::Succeeded,
        Err(failure) => {
            let _ = writeln!(io::stderr(), "useful result tool: {failure}");
            ExecutionOutcome::Failed
        }
    }
}

#[derive(Debug)]
enum RelayFailure {
    ReadInput(io::Error),
    WriteOutput(io::Error),
}

impl std::fmt::Display for RelayFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ReadInput(error) => write!(formatter, "reading stdin: {error}"),
            Self::WriteOutput(error) => write!(formatter, "writing stdout: {error}"),
        }
    }
}

enum RelayEvent {
    Line(Vec<u8>),
    OverlongLine,
    InputEnded,
    InputFailed(io::Error),
    Answer(Value),
}

/// Reads requests on one thread and answers them on this one, so writes to `output` are
/// serialized; each exchange with the channel runs on a thread of its own, so `ping` and
/// unknown methods are answered while a validation is pending. EOF on the input ends it.
fn serve(
    input: impl Read + Send + 'static,
    output: &mut impl Write,
    socket: Option<PathBuf>,
) -> Result<(), RelayFailure> {
    let (events, received) = mpsc::channel();
    let reader_events = events.clone();
    thread::Builder::new()
        .name("result-tool-input".to_owned())
        .spawn(move || read_requests(input, &reader_events))
        .map_err(RelayFailure::ReadInput)?;
    for event in received {
        let answer = match event {
            RelayEvent::Line(line) => match dispatch(&line) {
                Dispatch::Answer(answer) => answer,
                Dispatch::Ignore => continue,
                Dispatch::Exchange(exchange) => {
                    match start_exchange(exchange, socket.clone(), events.clone()) {
                        Ok(()) => continue,
                        Err(answer) => answer,
                    }
                }
            },
            RelayEvent::OverlongLine => error_answer(Value::Null, PARSE_ERROR, "Parse error"),
            RelayEvent::InputEnded => return Ok(()),
            RelayEvent::InputFailed(error) => return Err(RelayFailure::ReadInput(error)),
            RelayEvent::Answer(answer) => answer,
        };
        write_message(output, &answer).map_err(RelayFailure::WriteOutput)?;
    }
    Ok(())
}

fn read_requests(input: impl Read, events: &Sender<RelayEvent>) {
    let mut input = BufReader::new(input);
    let mut line = Vec::new();
    loop {
        let event = match read_bounded_line(&mut input, &mut line) {
            Ok(LineRead::Line) => RelayEvent::Line(std::mem::take(&mut line)),
            Ok(LineRead::Overlong) => RelayEvent::OverlongLine,
            Ok(LineRead::Ended) => RelayEvent::InputEnded,
            Err(error) => RelayEvent::InputFailed(error),
        };
        let ended = matches!(event, RelayEvent::InputEnded | RelayEvent::InputFailed(_));
        if events.send(event).is_err() || ended {
            return;
        }
    }
}

enum LineRead {
    Line,
    Overlong,
    Ended,
}

fn read_bounded_line(input: &mut impl BufRead, line: &mut Vec<u8>) -> io::Result<LineRead> {
    line.clear();
    let mut overlong = false;
    loop {
        let available = match input.fill_buf() {
            Ok(available) => available,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        };
        if available.is_empty() {
            return Ok(match (overlong, line.is_empty()) {
                (true, _) => LineRead::Overlong,
                (false, true) => LineRead::Ended,
                (false, false) => LineRead::Line,
            });
        }
        let (taken, complete) = match available.iter().position(|byte| *byte == b'\n') {
            Some(end) => (end, true),
            None => (available.len(), false),
        };
        if line.len().saturating_add(taken) > MAXIMUM_REQUEST_LINE_BYTES {
            overlong = true;
            line.clear();
        } else if !overlong {
            line.extend_from_slice(&available[..taken]);
        }
        input.consume(if complete { taken + 1 } else { taken });
        if complete {
            return Ok(if overlong {
                LineRead::Overlong
            } else {
                LineRead::Line
            });
        }
    }
}

enum Dispatch {
    Answer(Value),
    Ignore,
    Exchange(Exchange),
}

enum Exchange {
    Initialize { id: Value, protocol_version: Value },
    Call { id: Value, arguments: Value },
}

fn dispatch(line: &[u8]) -> Dispatch {
    if line.iter().all(u8::is_ascii_whitespace) {
        return Dispatch::Ignore;
    }
    let Ok(message) = serde_json::from_slice::<Value>(line) else {
        return Dispatch::Answer(error_answer(Value::Null, PARSE_ERROR, "Parse error"));
    };
    let Some(object) = message.as_object() else {
        return Dispatch::Answer(error_answer(
            Value::Null,
            INVALID_REQUEST,
            "Invalid Request",
        ));
    };
    let id = object.get("id").cloned();
    let Some(method) = object.get("method").and_then(Value::as_str) else {
        // A response: the relay sends no requests, so it has nothing to match it with.
        return if object.contains_key("result") || object.contains_key("error") {
            Dispatch::Ignore
        } else {
            Dispatch::Answer(error_answer(
                id.unwrap_or(Value::Null),
                INVALID_REQUEST,
                "Invalid Request",
            ))
        };
    };
    // A notification asks for no answer, `notifications/initialized` and cancellation among them.
    let Some(id) = id else {
        return Dispatch::Ignore;
    };
    let params = object.get("params");
    match method {
        "initialize" => Dispatch::Exchange(Exchange::Initialize {
            id,
            protocol_version: params
                .and_then(|params| params.get("protocolVersion"))
                .filter(|version| version.is_string())
                .cloned()
                .unwrap_or_else(|| Value::from(DEFAULT_PROTOCOL_VERSION)),
        }),
        "ping" => Dispatch::Answer(result_answer(id, json!({}))),
        "tools/list" => Dispatch::Answer(result_answer(id, json!({"tools": [tool_definition()]}))),
        "tools/call" => {
            if params
                .and_then(|params| params.get("name"))
                .and_then(Value::as_str)
                != Some(TOOL_NAME)
            {
                return Dispatch::Answer(error_answer(id, INVALID_PARAMS, "Unknown tool"));
            }
            Dispatch::Exchange(Exchange::Call {
                id,
                arguments: params
                    .and_then(|params| params.get("arguments"))
                    .cloned()
                    .unwrap_or(Value::Null),
            })
        }
        _ => Dispatch::Answer(error_answer(id, METHOD_NOT_FOUND, "Method not found")),
    }
}

/// Runs `exchange` on a thread of its own, which sends its answer to the writing thread; the
/// channel-failure answer at once when no thread could be started.
fn start_exchange(
    exchange: Exchange,
    socket: Option<PathBuf>,
    events: Sender<RelayEvent>,
) -> Result<(), Value> {
    let unavailable = exchange.unavailable();
    thread::Builder::new()
        .name("result-tool-exchange".to_owned())
        .spawn(move || {
            let _ = events.send(RelayEvent::Answer(exchange.run(socket.as_deref())));
        })
        .map(drop)
        .map_err(|error| {
            write_diagnostic("starting an exchange", &error);
            unavailable
        })
}

impl Exchange {
    fn run(self, socket: Option<&Path>) -> Value {
        let request = match &self {
            Self::Initialize { .. } => ResultChannelRequest::Hello,
            Self::Call { arguments, .. } => ResultChannelRequest::Submission {
                arguments: arguments.clone(),
            },
        };
        let answer = match socket {
            Some(socket) => exchange_with_channel(socket, &request),
            None => Err(ChannelExchangeFailure::new(
                ChannelExchangeStage::Address,
                io::Error::new(
                    io::ErrorKind::NotFound,
                    "no socket address in the environment",
                ),
            )),
        };
        match (self, answer) {
            (
                Self::Initialize {
                    id,
                    protocol_version,
                },
                Ok(ResultChannelAnswer::Ready),
            ) => result_answer(
                id,
                json!({
                    "protocolVersion": protocol_version,
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": SERVER_NAME, "version": SERVER_VERSION},
                }),
            ),
            (Self::Call { id, .. }, Ok(ResultChannelAnswer::Accepted)) => {
                tool_answer(id, false, ACCEPTED_TEXT)
            }
            (Self::Call { id, .. }, Ok(ResultChannelAnswer::Rejected { feedback })) => {
                tool_answer(id, true, &feedback)
            }
            (
                Self::Call { id, .. },
                Ok(ResultChannelAnswer::Failed {
                    cause: ResultChannelFailure::Validation,
                }),
            ) => tool_answer(id, true, VALIDATION_FAILED_TEXT),
            (Self::Call { id, .. }, Err(failure)) if failure.too_large => {
                tool_answer(id, true, TOO_LARGE_TEXT)
            }
            (exchange, answer) => {
                match answer {
                    Ok(answer) => {
                        write_diagnostic("result channel answer", &format!("{answer:?}"));
                    }
                    Err(failure) => write_diagnostic("result channel", &failure),
                }
                exchange.unavailable()
            }
        }
    }

    /// The answer when the result channel could not be reached or failed: `initialize` fails, so
    /// Claude Code marks the server failed, and a call is an error with a closed text.
    fn unavailable(&self) -> Value {
        match self {
            Self::Initialize { id, .. } => {
                error_answer(id.clone(), INTERNAL_ERROR, CHANNEL_UNAVAILABLE_MESSAGE)
            }
            Self::Call { id, .. } => tool_answer(id.clone(), true, CHANNEL_FAILED_TEXT),
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum ChannelExchangeStage {
    Address,
    Encode,
    Connect,
    SendRequest,
    ReceiveAnswer,
    Decode,
}

#[derive(Debug)]
struct ChannelExchangeFailure {
    stage: ChannelExchangeStage,
    error: io::Error,
    too_large: bool,
}

impl ChannelExchangeFailure {
    fn new(stage: ChannelExchangeStage, error: io::Error) -> Self {
        Self {
            stage,
            error,
            too_large: false,
        }
    }
}

impl std::fmt::Display for ChannelExchangeFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{:?}: {}", self.stage, self.error)
    }
}

/// One request frame to the channel and its one answer frame: a 4-byte big-endian length and
/// a JSON payload each way, as Pi's bridge frames them.
fn exchange_with_channel(
    socket: &Path,
    request: &ResultChannelRequest,
) -> Result<ResultChannelAnswer, ChannelExchangeFailure> {
    use ChannelExchangeStage::{Connect, Decode, Encode, ReceiveAnswer, SendRequest};
    let failure = ChannelExchangeFailure::new;
    let payload = serde_json::to_vec(request).map_err(|error| failure(Encode, error.into()))?;
    let length = u32::try_from(payload.len())
        .ok()
        .filter(|length| u64::from(*length) <= MAXIMUM_REQUEST_FRAME_BYTES.get())
        .ok_or_else(|| ChannelExchangeFailure {
            stage: Encode,
            error: io::Error::new(
                io::ErrorKind::InvalidInput,
                "request exceeds the frame limit",
            ),
            too_large: true,
        })?;
    let mut stream = UnixStream::connect(socket).map_err(|error| failure(Connect, error))?;
    stream
        .write_all(&length.to_be_bytes())
        .and_then(|()| stream.write_all(&payload))
        .and_then(|()| stream.flush())
        .map_err(|error| failure(SendRequest, error))?;
    let mut length = [0_u8; 4];
    stream
        .read_exact(&mut length)
        .map_err(|error| failure(ReceiveAnswer, error))?;
    let length = u32::from_be_bytes(length);
    if length > MAXIMUM_ANSWER_FRAME_BYTES {
        return Err(failure(
            ReceiveAnswer,
            io::Error::new(io::ErrorKind::InvalidData, "answer exceeds the frame limit"),
        ));
    }
    let mut answer = Vec::new();
    stream
        .take(u64::from(length))
        .read_to_end(&mut answer)
        .map_err(|error| failure(ReceiveAnswer, error))?;
    if answer.len() != usize::try_from(length).unwrap_or(usize::MAX) {
        return Err(failure(
            ReceiveAnswer,
            io::Error::new(io::ErrorKind::UnexpectedEof, "answer frame truncated"),
        ));
    }
    serde_json::from_slice(&answer).map_err(|error| failure(Decode, error.into()))
}

fn result_answer(id: Value, result: Value) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "result": result})
}

fn error_answer(id: Value, code: i64, message: &str) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message}})
}

fn tool_answer(id: Value, is_error: bool, text: &str) -> Value {
    result_answer(
        id,
        json!({"content": [{"type": "text", "text": text}], "isError": is_error}),
    )
}

fn write_message(output: &mut impl Write, message: &Value) -> io::Result<()> {
    let mut line = serde_json::to_vec(message).map_err(io::Error::from)?;
    line.push(b'\n');
    output.write_all(&line)?;
    output.flush()
}

/// One line on stderr, which Claude Code keeps in its MCP server log.
fn write_diagnostic(stage: &str, cause: &dyn std::fmt::Display) {
    let _ = writeln!(io::stderr(), "useful result tool: {stage}: {cause}");
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::num::NonZeroU64;
    use std::os::unix::fs::PermissionsExt as _;
    use std::sync::Arc;
    use std::time::Duration;

    use serde_json::{Value, json};
    use useful_test_support::mcp::{MCP_PROTOCOL_VERSION, McpServerProcess, tool_answer};

    use super::super::result_channel::{
        PreparedResultChannel, ResultChannelEvent, ResultSubmissions, SubmissionJudgement,
    };
    use super::{ResultChannelAnswer, ResultChannelRequest, TOOL_NAME, tool_definition};
    use crate::workflow::admission::CancellationSource;
    use crate::workflow::agent::{PositiveDuration, RetainedJsonSchema};
    use crate::workflow::result_validation::AuthoritativeResultValidator;
    use crate::workflow::test_support::{InlineValidationWorker, PendingClock};

    const FEEDBACK_BYTES: NonZeroU64 = NonZeroU64::new(8 * 1024).unwrap();

    type Validator = AuthoritativeResultValidator<PendingClock, InlineValidationWorker>;

    /// A result channel in a private temporary directory, and the adapter's side of it: the
    /// validator of positive integers and the submissions judged so far.
    struct Channel {
        _directory: tempfile::TempDir,
        channel: PreparedResultChannel,
        validator: Validator,
        submissions: ResultSubmissions,
        cancellation: CancellationSource,
    }

    impl Channel {
        fn prepare() -> Self {
            let directory = tempfile::tempdir().unwrap();
            fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700)).unwrap();
            let deadline = PositiveDuration::new(Duration::from_secs(60)).unwrap();
            let channel =
                PreparedResultChannel::prepare(directory.path(), deadline, PendingClock).unwrap();
            let document = json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "integer",
                "minimum": 1,
            });
            let bytes = Arc::<[u8]>::from(serde_json::to_vec(&document).unwrap());
            let validator = AuthoritativeResultValidator::new(
                RetainedJsonSchema::compile(bytes, Arc::new(document)).unwrap(),
                NonZeroU64::new(8 * 1024 * 1024).unwrap(),
                FEEDBACK_BYTES,
                deadline,
                PendingClock,
                InlineValidationWorker,
            );
            Self {
                _directory: directory,
                channel,
                validator,
                submissions: ResultSubmissions::new(FEEDBACK_BYTES),
                cancellation: CancellationSource::new(),
            }
        }

        /// Starts the relay as Claude Code does, from the MCP configuration file.
        fn start_relay(&self) -> McpServerProcess {
            McpServerProcess::start(self.channel.configuration_path()).unwrap()
        }

        /// Receives the next request, judges it and delivers the answer; the request and the
        /// judgement's answer.
        async fn answer_next(&mut self) -> (ResultChannelRequest, ResultChannelAnswer) {
            let ResultChannelEvent::Request(incoming) = self.channel.receive().await else {
                panic!("the relay's request must reach the channel");
            };
            let judgement = self
                .submissions
                .judge(incoming.request(), &self.validator, &self.cancellation)
                .await;
            let SubmissionJudgement::Answer { answer, .. } = judgement else {
                panic!("expected an answer, got {judgement:?}");
            };
            let request = incoming.request().clone();
            incoming.respond(answer.clone()).await.unwrap();
            (request, answer)
        }

        async fn initialize(&mut self, relay: &mut McpServerProcess) -> Value {
            relay.send(&initialize_request(json!(0))).unwrap();
            let (request, answer) = self.answer_next().await;
            assert_eq!(request, ResultChannelRequest::Hello);
            assert_eq!(answer, ResultChannelAnswer::Ready);
            let response = relay.receive().unwrap();
            relay
                .send(&json!({"jsonrpc": "2.0", "method": "notifications/initialized"}))
                .unwrap();
            response
        }

        /// One `submit` call answered through the channel: what the channel received and the
        /// tool answer the relay wrote.
        async fn submit(
            &mut self,
            relay: &mut McpServerProcess,
            id: i64,
            arguments: Value,
        ) -> (
            ResultChannelRequest,
            useful_test_support::mcp::McpToolAnswer,
        ) {
            relay.send(&call_request(json!(id), arguments)).unwrap();
            let (request, _) = self.answer_next().await;
            let response = relay.receive().unwrap();
            assert_eq!(response["id"], json!(id));
            (request, tool_answer(&response["result"]).unwrap())
        }
    }

    fn initialize_request(id: Value) -> Value {
        json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "initialize",
            "params": {
                "protocolVersion": MCP_PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": {"name": "claude-code", "version": "2.1.321"},
            },
        })
    }

    fn call_request(id: Value, arguments: Value) -> Value {
        json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "tools/call",
            "params": {
                "name": TOOL_NAME,
                "arguments": arguments,
                "_meta": {"claudecode/toolUseId": "toolu_1", "progressToken": 1},
            },
        })
    }

    fn request(id: Value, method: &str) -> Value {
        json!({"jsonrpc": "2.0", "id": id, "method": method, "params": {}})
    }

    fn assert_method_not_found(response: &Value, id: &Value) {
        assert_eq!(&response["id"], id);
        assert_eq!(response["error"]["code"], json!(-32601), "{response}");
        assert!(response.get("result").is_none());
    }

    // REQ-AGENT-84, INV-HARNESS-23
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_relay_serves_the_tool_and_relays_each_call_to_the_channel() {
        let mut channel = Channel::prepare();
        let hello = channel.channel.hello();
        let mut relay = channel.start_relay();

        // `initialize` waits on the channel's hello while `ping` is answered at once.
        relay.send(&initialize_request(json!("init"))).unwrap();
        relay.send(&request(json!("ping-1"), "ping")).unwrap();
        let ping = relay.receive().unwrap();
        assert_eq!(ping["id"], json!("ping-1"));
        assert_eq!(ping["result"], json!({}));
        assert!(!hello.recorded());
        let (hello_request, _) = channel.answer_next().await;
        assert_eq!(hello_request, ResultChannelRequest::Hello);
        assert!(hello.recorded());
        let initialized = relay.receive().unwrap();
        assert_eq!(initialized["id"], json!("init"));
        assert_eq!(
            initialized["result"]["protocolVersion"],
            json!(MCP_PROTOCOL_VERSION)
        );
        assert!(initialized["result"]["capabilities"]["tools"].is_object());
        assert_eq!(
            initialized["result"]["serverInfo"]["name"],
            json!("useful_workflow_result")
        );
        relay
            .send(&json!({"jsonrpc": "2.0", "method": "notifications/initialized"}))
            .unwrap();

        // The one tool `submit`, whose only top-level property `submission` is an object.
        let listed = relay.request("tools/list", json!({})).unwrap();
        let tools = listed["result"]["tools"].as_array().unwrap();
        assert_eq!(tools, &vec![tool_definition()]);
        assert_eq!(tools[0]["name"], json!("submit"));
        let properties = tools[0]["inputSchema"]["properties"].as_object().unwrap();
        assert_eq!(properties.keys().collect::<Vec<_>>(), ["submission"]);
        assert_eq!(properties["submission"]["type"], json!("object"));

        // While a call is pending, discovery and unknown methods are refused and ping answered.
        let rejected = json!({"submission": {"result": 0}});
        relay
            .send(&call_request(json!(10), rejected.clone()))
            .unwrap();
        let discover = json!("server-discover-probe-1");
        relay
            .send(&request(discover.clone(), "server/discover"))
            .unwrap();
        relay.send(&request(json!(11), "resources/list")).unwrap();
        relay.send(&request(json!(12), "ping")).unwrap();
        assert_method_not_found(&relay.receive().unwrap(), &discover);
        assert_method_not_found(&relay.receive().unwrap(), &json!(11));
        let ping = relay.receive().unwrap();
        assert_eq!(ping["id"], json!(12));
        assert_eq!(ping["result"], json!({}));

        // The pending call's arguments reach the channel verbatim; its rejection is the answer.
        let (submitted, answer) = channel.answer_next().await;
        assert_eq!(
            submitted,
            ResultChannelRequest::Submission {
                arguments: rejected
            }
        );
        let ResultChannelAnswer::Rejected { feedback } = answer else {
            panic!("0 is not a positive integer: {answer:?}");
        };
        let response = relay.receive().unwrap();
        assert_eq!(response["id"], json!(10));
        let rejection = tool_answer(&response["result"]).unwrap();
        assert!(rejection.is_error);
        assert_eq!(rejection.text, feedback);

        // A call for another tool is an error, and the process goes on.
        let other = relay
            .request(
                "tools/call",
                json!({"name": "other", "arguments": {"submission": {"result": 7}}}),
            )
            .unwrap();
        assert_eq!(other["error"]["code"], json!(-32602));

        let (_, accepted) = channel
            .submit(&mut relay, 13, json!({"submission": {"result": 7}}))
            .await;
        assert!(!accepted.is_error);
        assert!(!accepted.text.is_empty());
        assert_eq!(channel.submissions.accepted().unwrap().value(), &json!(7));

        // A relay started again on the same channel needs nothing the first one held.
        drop(relay);
        let mut relay = channel.start_relay();
        let initialized = channel.initialize(&mut relay).await;
        assert_eq!(initialized["id"], json!(0));
        assert!(initialized.get("error").is_none());
        let (_, accepted) = channel
            .submit(&mut relay, 1, json!({"submission": {"result": 8}}))
            .await;
        assert!(!accepted.is_error);
        assert_eq!(channel.submissions.accepted().unwrap().value(), &json!(8));

        drop(relay);
        channel.channel.shutdown().await.unwrap();
    }

    // INV-HARNESS-23
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn without_its_socket_the_relay_fails_initialize_and_every_call() {
        let channel = Channel::prepare();
        let configuration = channel.channel.configuration_path().to_owned();
        assert_eq!(
            fs::metadata(&configuration).unwrap().permissions().mode() & 0o777,
            0o400
        );
        let document: Value = serde_json::from_slice(&fs::read(&configuration).unwrap()).unwrap();
        let entry = &document["mcpServers"]["useful_workflow_result"];
        let command = entry["command"].as_str().unwrap();
        let socket = entry["env"]["USEFUL_INTERNAL_RESULT_TOOL_SOCKET"]
            .as_str()
            .unwrap();
        assert!(std::path::Path::new(command).is_absolute());
        assert!(socket.len() < 100);
        assert_eq!(
            document,
            json!({"mcpServers": {"useful_workflow_result": {
                "type": "stdio",
                "command": command,
                "args": [],
                "env": {
                    "USEFUL_INTERNAL_RESULT_TOOL_WORKER": "claude-code-result-tool",
                    "USEFUL_INTERNAL_RESULT_TOOL_SOCKET": socket,
                },
                "alwaysLoad": true,
            }}})
        );
        let mut relay = channel.start_relay();
        channel.channel.shutdown().await.unwrap();
        assert!(!configuration.exists());
        assert!(!std::path::Path::new(socket).exists());

        let initialized = relay
            .request("initialize", initialize_request(json!(0))["params"].clone())
            .unwrap();
        assert_eq!(initialized["error"]["code"], json!(-32603), "{initialized}");
        assert!(initialized.get("result").is_none());

        let response = relay
            .request(
                "tools/call",
                call_request(json!(0), json!({"submission": {"result": 7}}))["params"].clone(),
            )
            .unwrap();
        let answer = tool_answer(&response["result"]).unwrap();
        assert!(answer.is_error);
        assert!(!answer.text.is_empty());
    }
}
