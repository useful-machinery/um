use std::collections::BTreeMap;
use std::fmt;
use std::io::{self, Write};
use std::num::NonZeroU8;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{SyncSender, TrySendError, sync_channel};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::Instant;

use opentelemetry::propagation::{Injector, TextMapPropagator};
use opentelemetry::trace::{SpanKind, Status, TraceContextExt, Tracer, TracerProvider};
use opentelemetry::{Context, KeyValue, Value as AttributeValue};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::{SdkTracer, SdkTracerProvider};
use serde_json::{Map, Number, Value};
use time::format_description::well_known::Iso8601;
use time::format_description::well_known::iso8601::{
    Config as Iso8601Config, EncodedConfig, TimePrecision,
};
use time::{OffsetDateTime, UtcOffset};

mod log_file;
mod otlp;
pub(crate) mod attribute {
    /// A `runner.agent_login_setup` record names each provider whose
    /// readiness changed in the attribute's key, so one record can carry
    /// every provider a pass changed (REQ-TELEMETRY-45).
    pub(crate) const AGENT_LOGIN_CODEX_STATE: &str = "um.agent_login.codex.state";
    pub(crate) const AGENT_LOGIN_CODEX_GENERATION: &str = "um.agent_login.codex.generation";
    pub(crate) const AGENT_LOGIN_CLAUDE_CODE_STATE: &str = "um.agent_login.claude_code.state";
    pub(crate) const AGENT_LOGIN_CLAUDE_CODE_GENERATION: &str =
        "um.agent_login.claude_code.generation";
    /// Beside a provider's state when a step of its setup failed: the stage,
    /// the closed cause, and Cloud's HTTP status when the cause is one.
    pub(crate) const AGENT_LOGIN_CODEX_FAILURE_STAGE: &str = "um.agent_login.codex.failure_stage";
    pub(crate) const AGENT_LOGIN_CODEX_FAILURE_CAUSE: &str = "um.agent_login.codex.failure_cause";
    pub(crate) const AGENT_LOGIN_CODEX_HTTP_STATUS: &str = "um.agent_login.codex.http_status";
    pub(crate) const AGENT_LOGIN_CLAUDE_CODE_FAILURE_STAGE: &str =
        "um.agent_login.claude_code.failure_stage";
    pub(crate) const AGENT_LOGIN_CLAUDE_CODE_FAILURE_CAUSE: &str =
        "um.agent_login.claude_code.failure_cause";
    pub(crate) const AGENT_LOGIN_CLAUDE_CODE_HTTP_STATUS: &str =
        "um.agent_login.claude_code.http_status";
    /// A `runner.grant_token` record names one OAuth provider, the phase
    /// the runner asked in (`admission` or `refresh`), the provider's new
    /// state and the generation of the token it keeps, and for a failure
    /// its stage, closed cause and Cloud's HTTP status when the cause is one
    /// (REQ-TELEMETRY-46).
    pub(crate) const GRANT_TOKEN_PROVIDER: &str = "um.grant_token.provider";
    pub(crate) const GRANT_TOKEN_PHASE: &str = "um.grant_token.phase";
    pub(crate) const GRANT_TOKEN_STATE: &str = "um.grant_token.state";
    pub(crate) const GRANT_TOKEN_GENERATION: &str = "um.grant_token.generation";
    pub(crate) const GRANT_TOKEN_FAILURE_STAGE: &str = "um.grant_token.failure_stage";
    pub(crate) const GRANT_TOKEN_FAILURE_CAUSE: &str = "um.grant_token.failure_cause";
    pub(crate) const GRANT_TOKEN_HTTP_STATUS: &str = "um.grant_token.http_status";
    /// A `runner.credential_sources` record names the assignment whose
    /// credential-source report Cloud did not take, the closed cause, and
    /// Cloud's HTTP status when the cause is one (REQ-TELEMETRY-48).
    pub(crate) const CREDENTIAL_SOURCES_FAILURE_CAUSE: &str = "um.credential_sources.failure_cause";
    pub(crate) const CREDENTIAL_SOURCES_HTTP_STATUS: &str = "um.credential_sources.http_status";
    pub(crate) const BACKOFF_MS: &str = "um.connection.backoff_ms";
    pub(crate) const CONNECTION_ATTEMPT: &str = "um.connection.attempt";
    pub(crate) const EFFECT_ACKNOWLEDGEMENTS_CONFIRMED: &str =
        "um.runner.effect_acknowledgements_confirmed";
    pub(crate) const EFFECT_ID: &str = "um.effect.id";
    pub(crate) const LEASE_DISPOSITION: &str = "um.lease.disposition";
    pub(crate) const LEASE_DECISION_DELAY_MS: &str = "um.lease.decision_delay_ms";
    pub(crate) const LEASE_CANCELLATION_HEADROOM_MS: &str = "um.lease.cancellation_headroom_ms";
    pub(crate) const LEASE_REQUEST_AGE_MS: &str = "um.lease.request_age_ms";
    pub(crate) const EFFECTS_RECEIVED: &str = "um.runner.effects_received";
    pub(crate) const ERROR_TYPE: &str = "error.type";
    pub(crate) const FAILURE_KIND: &str = "um.connection.failure_kind";
    pub(crate) const HARNESS_PROFILE: &str = "um.harness.profile";
    pub(crate) const HARNESS_QUALIFICATION_VERSION: &str = "um.harness.qualification_version";
    pub(crate) const HARNESS_VERSION: &str = "um.harness.version";
    pub(crate) const HANDSHAKE_COMPLETED: &str = "um.runner.handshake_completed";
    pub(crate) const OPENING_ACKNOWLEDGED: &str = "um.runner.opening_acknowledged";
    pub(crate) const RUN_ID: &str = "um.run.id";
    pub(crate) const RUN_RESULT: &str = "um.run.result";
    pub(crate) const STEP_REPORT_REFUSAL: &str = "um.run.step_report_refusal";
    pub(crate) const MIRROR_READ_FAILURE: &str = "um.run.mirror_read_failure";
    pub(crate) const EXPORT_READ_FAILURE: &str = "um.run.export_read_failure";
    pub(crate) const FAILURE_PHASE: &str = "um.failure.phase";
    pub(crate) const FAILURE_CODE: &str = "um.failure.code";
    pub(crate) const FAILURE_CAUSE_TYPE: &str = "um.failure.cause_type";
    pub(crate) const INTERRUPTION_CAUSE: &str = "um.interruption.cause";
    pub(crate) const EXECUTOR_FAULT_REASON: &str = "um.executor_fault.reason";
    pub(crate) const DIAGNOSTIC_STAGE: &str = "um.diagnostic.stage";
    pub(crate) const RUNNER_BOOT_ID: &str = "um.runner.boot_id";
    pub(crate) const RUNNER_ID: &str = "um.runner.id";
    pub(crate) const RUNNER_SEQUENCE: &str = "um.runner.sequence";
    pub(crate) const RUNNER_TEXT_FRAMES_SENT: &str = "um.runner.text_frames_sent";
    pub(crate) const RUNNER_VERSION: &str = "um.runner.version";
    pub(crate) const CLOUD_TEXT_FRAMES_RECEIVED: &str = "um.cloud.text_frames_received";
    pub(crate) const SERVER_ADDRESS: &str = "server.address";
    pub(crate) const SERVER_PORT: &str = "server.port";
    pub(crate) const ASSIGNMENT_ID: &str = "um.assignment.id";
    pub(crate) const ATTEMPT_ID: &str = "um.attempt.id";
    pub(crate) const ARTIFACT_DELIVERY_ID: &str = "um.artifact.delivery_id";
    pub(crate) const ARTIFACT_OPERATION: &str = "um.artifact.operation";
    pub(crate) const ARTIFACT_MEMBER: &str = "um.artifact.member";
    pub(crate) const ARTIFACT_FAILURE_ORIGIN: &str = "um.artifact.failure_origin";
    pub(crate) const ARTIFACT_FAILURE_CODE: &str = "um.artifact.failure_code";
    pub(crate) const ARTIFACT_PREPARATION_STAGE: &str = "um.artifact.preparation_stage";
    pub(crate) const ARTIFACT_PUBLICATION_PHASE: &str = "um.artifact.publication_phase";
    pub(crate) const ARTIFACT_PUBLICATION_KIND: &str = "um.artifact.publication_kind";
    pub(crate) const ARTIFACT_RESULT_INVARIANT: &str = "um.artifact.result_invariant";
    pub(crate) const ARTIFACT_MEMBER_INDEX: &str = "um.artifact.member_index";
    pub(crate) const ARTIFACT_NODE_ID: &str = "um.artifact.node_id";
    pub(crate) const PROTOCOL_REQUEST_MESSAGE_ID: &str = "um.protocol.request_message_id";
    pub(crate) const ASSIGNMENT_PREPARATION_PHASE: &str = "um.assignment.preparation_phase";
    pub(crate) const OWNED_PROCESSES_STOPPED: &str = "um.teardown.owned_processes_stopped";
    pub(crate) const RETENTION_REASON: &str = "um.workspace.retention_reason";
    pub(crate) const RETENTION_RECORDED: &str = "um.workspace.retention_recorded";
    pub(crate) const RUNNER_CREDENTIALS_REMOVED: &str = "um.teardown.runner_credentials_removed";
    pub(crate) const RUNNER_CREDENTIALS_REVOKED: &str = "um.teardown.runner_credentials_revoked";
    pub(crate) const WORKSPACE_PATH: &str = "um.workspace.path";
    pub(crate) const PROTOCOL_ACKNOWLEDGED_MESSAGE_ID: &str = "um.protocol.acknowledged_message_id";
    pub(crate) const PROTOCOL_ACKNOWLEDGED_SEQUENCE: &str = "um.protocol.acknowledged_sequence";
    pub(crate) const PROTOCOL_CLOSE_CODE: &str = "um.protocol.close_code";
    pub(crate) const PROTOCOL_CLOSE_INITIATOR: &str = "um.protocol.close_initiator";
    /// A `runner.run` that ends `rejected` names the cause of a diagnosed
    /// decline and, for a refused workflow tree, the validation code and the
    /// location's kind, never the path or the step (REQ-TELEMETRY-47).
    pub(crate) const PROTOCOL_DECLINE_CAUSE: &str = "um.protocol.decline_cause";
    pub(crate) const WORKFLOW_VALIDATION_CODE: &str = "um.workflow.validation_code";
    pub(crate) const WORKFLOW_VALIDATION_LOCATION: &str = "um.workflow.validation_location";
    pub(crate) const PROTOCOL_DECLINE_REASON: &str = "um.protocol.decline_reason";
    pub(crate) const PROTOCOL_DECLINE_TYPE: &str = "um.protocol.decline_type";
    pub(crate) const PROTOCOL_DIRECTION: &str = "um.protocol.direction";
    pub(crate) const PROTOCOL_EVENT: &str = "um.protocol.event";
    pub(crate) const PROTOCOL_FRAME_KIND: &str = "um.protocol.frame_kind";
    pub(crate) const PROTOCOL_FRAME_TYPE: &str = "um.protocol.frame_type";
    pub(crate) const PROTOCOL_LEASE_SEQUENCE: &str = "um.protocol.lease_sequence";
    pub(crate) const PROTOCOL_MESSAGE_ID: &str = "um.protocol.message_id";
    pub(crate) const PROTOCOL_ORDER: &str = "um.protocol.order";
    pub(crate) const PROTOCOL_PAYLOAD_VERSION: &str = "um.protocol.payload_version";
    pub(crate) const PROTOCOL_PING_INTERVAL_SECONDS: &str = "um.protocol.ping_interval_seconds";
    pub(crate) const PROTOCOL_PONG_TIMEOUT_SECONDS: &str = "um.protocol.pong_timeout_seconds";
    pub(crate) const PROTOCOL_SENT_AT: &str = "um.protocol.sent_at";
    pub(crate) const PROTOCOL_TIMER: &str = "um.protocol.timer";
    pub(crate) const PROTOCOL_VERSION: &str = "um.protocol.version";
    pub(crate) const RUNNER_SESSION_ID: &str = "um.runner.session_id";
    pub(crate) const STEP_ID: &str = "um.step.id";
    pub(crate) const STEP_ROLE: &str = "um.step.role";
    pub(crate) const STEP_EXECUTION_NUMBER: &str = "um.step.execution_number";
    pub(crate) const STEP_RECOVERY_ROUND: &str = "um.step.recovery_round";
    pub(crate) const STEP_RECOVERY_FAILURE_KIND: &str = "um.step.recovery_failure_kind";
}

const EVENT_NAME: &str = "event.name";
const MAIN: &str = "um.main";
const SCHEMA_VERSION: &str = "um.event.schema_version";
const OUTCOME: &str = "um.outcome";
const SERVICE_NAME: &str = "service.name";
const SERVICE_VERSION: &str = "service.version";
const SERVICE_INSTANCE_ID: &str = "service.instance.id";
const DROPPED_COUNT: &str = "um.telemetry.dropped_count";
const DURATION_MS: &str = "duration_ms";
const TRACE_ID: &str = "trace_id";
const SPAN_ID: &str = "span_id";
const PARENT_SPAN_ID: &str = "parent_span_id";
const TIME: &str = "time";
// A record's `time` is UTC with exactly three subsecond digits and `Z`, for
// example `2026-11-19T04:54:24.993Z`; well-known RFC 3339 would print a
// varying number of digits.
const RECORD_TIME_FORMAT: EncodedConfig = Iso8601Config::DEFAULT
    .set_time_precision(TimePrecision::Second {
        decimal_digits: NonZeroU8::new(3),
    })
    .encode();
const EVENT_QUEUE_CAPACITY: usize = 128;
const EVENT_QUEUE_DRAIN_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(50);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Outcome {
    Success,
    Failure,
    Cancelled,
    Timeout,
    Disconnected,
    // An offer the runner took and then rejected: not an error, as the
    // gateway treats a 4xx response (REQ-TELEMETRY-6).
    Rejected,
}

impl Outcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Failure => "failure",
            Self::Cancelled => "cancelled",
            Self::Timeout => "timeout",
            Self::Disconnected => "disconnected",
            Self::Rejected => "rejected",
        }
    }

    const fn is_error(self) -> bool {
        matches!(self, Self::Failure | Self::Timeout)
    }
}

/// The wall clock a local record's `time` is read from when the record is
/// serialized (REQ-TELEMETRY-51). Production passes the service's
/// `Sleeper::utc_now`.
pub(crate) type RecordClock = Arc<dyn Fn() -> OffsetDateTime + Send + Sync>;

pub(crate) trait EventWriter: Send + Sync {
    fn write(&self, event: &Map<String, Value>) -> io::Result<()>;
}

/// Told of each runner record the telemetry writer drops (REQ-TELEMETRY-14). It is told
/// outside every lock this module holds and must write no telemetry.
pub(crate) trait DroppedRecordObserver: Send + Sync {
    /// One more record was dropped.
    fn record_dropped(&self);
}

// Drops is what the event source and the output threads share of the records
// they drop: the count the next completed main record reports, and the
// observer the serving loop may set once.
#[derive(Default)]
struct Drops {
    pending: AtomicU64,
    observer: OnceLock<Arc<dyn DroppedRecordObserver>>,
}

impl Drops {
    // dropped counts one dropped record and puts back `carried`, the count the
    // lost record was reporting, which is not dropped again: the observer is
    // told of the one record only.
    fn dropped(&self, carried: u64) {
        self.pending
            .fetch_add(carried.saturating_add(1), Ordering::AcqRel);
        if let Some(observer) = self.observer.get() {
            observer.record_dropped();
        }
    }

    // take answers the pending count and leaves none.
    fn take(&self) -> u64 {
        self.pending.swap(0, Ordering::AcqRel)
    }
}

/// One serialized record as a local output's queue holds it. Every output
/// the record is offered to holds the same bytes and the same tally of the
/// outputs that may still take it (REQ-TELEMETRY-14).
#[derive(Clone)]
struct QueuedRecord {
    bytes: Arc<[u8]>,
    represented_dropped_count: u64,
    holders: Arc<AtomicUsize>,
}

impl QueuedRecord {
    // serialize frames one local record as a JSON line stamped with `time`,
    // the UTC time it was written (REQ-TELEMETRY-51), to be offered to
    // `outputs` outputs. The stamp is added here, once per record, so every
    // output carries the same bytes and the stamp never reaches an exported
    // span.
    fn serialize(
        event: &Map<String, Value>,
        written_at: OffsetDateTime,
        outputs: usize,
    ) -> io::Result<Self> {
        let time = written_at
            .to_offset(UtcOffset::UTC)
            .format(&Iso8601::<RECORD_TIME_FORMAT>)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        let mut record = event.clone();
        record.insert(TIME.to_owned(), Value::String(time));
        let mut bytes = serde_json::to_vec(&record)?;
        bytes.push(b'\n');
        Ok(Self {
            bytes: Arc::from(bytes),
            represented_dropped_count: event
                .get(DROPPED_COUNT)
                .and_then(Value::as_u64)
                .unwrap_or(0),
            holders: Arc::new(AtomicUsize::new(outputs)),
        })
    }

    // release records that one output will not take the record, its queue
    // having refused it or its write having failed, and answers whether
    // that output was the last that could: the record then reached none and
    // its caller counts it dropped, once.
    fn release(&self) -> bool {
        self.holders.fetch_sub(1, Ordering::AcqRel) == 1
    }
}

enum QueueMessage {
    Record(QueuedRecord),
    Flush(SyncSender<io::Result<()>>),
    Drain(SyncSender<()>),
}

/// What a local output's thread writes each record to: standard error or
/// the log file.
trait RecordSink {
    fn write_record(&mut self, record: &[u8]) -> io::Result<()>;
    fn flush(&mut self) -> io::Result<()>;
}

/// One local output: a queue of `EVENT_QUEUE_CAPACITY` records and the
/// thread that writes them to its sink, so an output that is slow, blocked
/// or failing delays neither the runner nor another output.
struct QueuedWriter {
    sender: Option<SyncSender<QueueMessage>>,
}

impl QueuedWriter {
    // start spawns the output's thread `name`, which makes its sink with
    // `open` and then writes each queued record to it, and answers once the
    // thread has made the sink: an output whose sink could not be made is
    // no output, and its thread has ended.
    fn start<S: RecordSink, E: Send + 'static>(
        name: &str,
        open: impl FnOnce() -> Result<S, E> + Send + 'static,
        drops: Arc<Drops>,
    ) -> Result<Self, OutputStartError<E>> {
        let (sender, receiver) = sync_channel::<QueueMessage>(EVENT_QUEUE_CAPACITY);
        let (opened, open_answer) = sync_channel::<Result<(), E>>(1);
        thread::Builder::new()
            .name(name.to_owned())
            .spawn(move || {
                let mut sink = match open() {
                    Ok(sink) => sink,
                    Err(error) => {
                        let _ = opened.send(Err(error));
                        return;
                    }
                };
                let _ = opened.send(Ok(()));
                while let Ok(message) = receiver.recv() {
                    match message {
                        QueueMessage::Record(record) => {
                            if sink.write_record(&record.bytes).is_err() && record.release() {
                                drops.dropped(record.represented_dropped_count);
                            }
                        }
                        QueueMessage::Flush(completed) => {
                            let _ = completed.send(sink.flush());
                        }
                        QueueMessage::Drain(completed) => {
                            let _ = completed.send(());
                            break;
                        }
                    }
                }
            })
            .map_err(OutputStartError::Thread)?;
        open_answer
            .recv()
            .map_err(|_| {
                OutputStartError::Thread(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "runner event writer stopped",
                ))
            })?
            .map_err(OutputStartError::Open)?;
        Ok(Self {
            sender: Some(sender),
        })
    }

    #[cfg(test)]
    fn from_sender(sender: SyncSender<QueueMessage>) -> Self {
        Self {
            sender: Some(sender),
        }
    }

    // offer queues `record` without waiting, refusing it when the queue is
    // full or its thread has stopped.
    fn offer(&self, record: QueuedRecord) -> io::Result<()> {
        let Some(sender) = &self.sender else {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "runner event writer unavailable",
            ));
        };
        sender
            .try_send(QueueMessage::Record(record))
            .map_err(|error| match error {
                TrySendError::Full(_) => {
                    io::Error::new(io::ErrorKind::WouldBlock, "runner event queue is full")
                }
                TrySendError::Disconnected(_) => {
                    io::Error::new(io::ErrorKind::BrokenPipe, "runner event writer stopped")
                }
            })
    }

    fn flush(&self) -> io::Result<()> {
        let Some(sender) = &self.sender else {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "runner event writer unavailable",
            ));
        };
        let (completed, completion) = sync_channel(0);
        sender.send(QueueMessage::Flush(completed)).map_err(|_| {
            io::Error::new(io::ErrorKind::BrokenPipe, "runner event writer stopped")
        })?;
        completion
            .recv()
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "runner event writer stopped"))?
    }
}

/// Why a local output did not start: its thread did not start or stopped
/// before answering, or its sink could not be made.
enum OutputStartError<E> {
    Thread(io::Error),
    Open(E),
}

impl Drop for QueuedWriter {
    #[expect(
        clippy::disallowed_methods,
        reason = "QueuedWriter owns the production deadline for draining telemetry"
    )]
    fn drop(&mut self) {
        let Some(sender) = self.sender.take() else {
            return;
        };
        let (completed, completion) = sync_channel(0);
        if sender.try_send(QueueMessage::Drain(completed)).is_ok() {
            let _ = completion.recv_timeout(EVENT_QUEUE_DRAIN_TIMEOUT);
        }
    }
}

/// The runner's local outputs behind its one `EventWriter`: standard error
/// and, when it opened, the log file (REQ-TELEMETRY-14, INV-TELEMETRY-17).
/// A record is serialized and stamped once and the same bytes are offered
/// to each output without waiting on any.
struct LocalOutputs {
    outputs: Vec<QueuedWriter>,
    clock: RecordClock,
}

impl LocalOutputs {
    fn new(outputs: Vec<QueuedWriter>, clock: RecordClock) -> Self {
        Self { outputs, clock }
    }

    fn flush(&self) -> io::Result<()> {
        self.outputs.iter().try_for_each(QueuedWriter::flush)
    }
}

impl EventWriter for LocalOutputs {
    // write answers `Err` when the record reached no output, so its caller
    // counts it dropped; a record some output may still write is `Ok`, and
    // an output's thread counts it if every holder then fails to write it.
    fn write(&self, event: &Map<String, Value>) -> io::Result<()> {
        if self.outputs.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "runner event writer unavailable",
            ));
        }
        let record = QueuedRecord::serialize(event, (self.clock)(), self.outputs.len())?;
        for output in &self.outputs {
            if let Err(error) = output.offer(record.clone())
                && record.release()
            {
                return Err(error);
            }
        }
        Ok(())
    }
}

struct FramedWriter<W> {
    output: W,
    separator_required: bool,
}

impl<W: Write> FramedWriter<W> {
    fn new(output: W) -> Self {
        Self::resuming(output, false)
    }

    // resuming frames records after output whose last line is unfinished
    // when `unterminated`, ending that line before the first record.
    fn resuming(output: W, unterminated: bool) -> Self {
        Self {
            output,
            separator_required: unterminated,
        }
    }
}

impl<W> FramedWriter<W> {
    // framed_len is the number of bytes writing `record` adds to the output.
    fn framed_len(&self, record: &[u8]) -> usize {
        record
            .len()
            .saturating_add(usize::from(self.separator_required))
    }
}

impl<W: Write> RecordSink for FramedWriter<W> {
    fn write_record(&mut self, record: &[u8]) -> io::Result<()> {
        if self.separator_required {
            self.output.write_all(b"\n")?;
            self.separator_required = false;
        }
        if let Err(error) = self.output.write_all(record) {
            self.separator_required = true;
            return Err(error);
        }
        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.output.flush()
    }
}

pub(crate) struct Recorder {
    provider: SdkTracerProvider,
    source: EventSource,
    log_path: Option<PathBuf>,
}

// EventSource is what starting an event needs: the tracer, the sink and the
// service identity. The recorder holds one and every event keeps a clone, so a
// child event starts from its parent without a recorder handle.
#[derive(Clone)]
struct EventSource {
    tracer: SdkTracer,
    writer: Arc<dyn EventWriter>,
    drops: Arc<Drops>,
    service_version: Arc<str>,
    service_instance_id: Arc<str>,
}

impl fmt::Debug for Recorder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Recorder { .. }")
    }
}

impl Recorder {
    /// The serving runner's recorder, whose local outputs are standard error
    /// and, given `log_directory`, the log file `runner.log` in it
    /// (REQ-TELEMETRY-52). It waits for the log's thread to answer whether
    /// the log opened; a log that did not open leaves standard error the
    /// only local output, is reported as `open_failed` and never fails the
    /// recorder (REQ-TELEMETRY-54).
    pub(crate) fn open(
        service_instance_id: &str,
        service_version: &str,
        clock: RecordClock,
        log_directory: Option<PathBuf>,
    ) -> Arc<Self> {
        Self::open_with(
            service_instance_id,
            service_version,
            clock,
            || Ok(FramedWriter::new(io::stderr())),
            log_directory.map(|directory| {
                (
                    log_file::path_in(&directory),
                    move |clock: RecordClock, reporter: log_file::LogReporter| {
                        log_file::LogFile::open(
                            &directory,
                            log_file::RUNNER_LOG_LIMITS,
                            clock,
                            reporter,
                        )
                    },
                )
            }),
        )
    }

    // open_with is `open` with standard error's sink made by `stderr` and
    // the log's, when there is one, by the opener `log` gives with the path
    // of the file it opens, from the clock and the reporter of the log's
    // failures, on the outputs' own threads. The recorder keeps the path only
    // when the log opened.
    fn open_with<E, L, O>(
        service_instance_id: &str,
        service_version: &str,
        clock: RecordClock,
        stderr: impl FnOnce() -> io::Result<E> + Send + 'static,
        log: Option<(PathBuf, O)>,
    ) -> Arc<Self>
    where
        E: RecordSink,
        L: RecordSink,
        O: FnOnce(RecordClock, log_file::LogReporter) -> Result<L, log_file::OpenError>
            + Send
            + 'static,
    {
        let drops = Arc::new(Drops::default());
        let stderr = QueuedWriter::start("useful-runner-events", stderr, Arc::clone(&drops));
        let reporter = log_file::LogReporter::default();
        let mut log_path = None;
        let log = log.and_then(|(path, open)| {
            let clock = Arc::clone(&clock);
            let log_reporter = reporter.clone();
            let started = QueuedWriter::start(
                "useful-runner-log",
                move || {
                    open(clock, log_reporter.clone())
                        .map(|sink| log_file::ReportedWrites::new(sink, log_reporter))
                },
                Arc::clone(&drops),
            );
            match started {
                Ok(log) => {
                    log_path = Some(path);
                    Some(log)
                }
                Err(OutputStartError::Thread(error)) => {
                    reporter.report(log_file::LogFailure::Open, error.kind());
                    None
                }
                Err(OutputStartError::Open(error)) => {
                    reporter.report(log_file::LogFailure::Open, error.kind());
                    None
                }
            }
        });
        let outputs = Arc::new(LocalOutputs::new(
            stderr.into_iter().chain(log).collect(),
            clock,
        ));
        let writer: Arc<dyn EventWriter> = outputs.clone();
        let mut provider = SdkTracerProvider::builder();
        if let Some(processor) = otlp::configured_processor(Arc::clone(&writer)) {
            provider = provider.with_span_processor(processor);
        }
        let resource = runner_resource(service_version, service_instance_id);
        let provider = provider.with_resource(resource).build();
        let mut recorder = Self::with_drops(
            provider,
            writer,
            service_version,
            service_instance_id,
            drops,
        );
        recorder.log_path = log_path;
        let recorder = Arc::new(recorder);
        // The log's failures until now, its open's among them, are written
        // through the recorder from here.
        reporter.attach(&recorder);
        // Configuration and log diagnostics are startup output. Establish that every
        // accepted record has reached its outputs before Runner Serve can terminate on a
        // later error.
        let _ = outputs.flush();
        recorder
    }

    /// Sets the observer told of each record dropped from now on. Only the first observer
    /// set is kept; a later one is ignored.
    pub(crate) fn set_dropped_record_observer(&self, observer: Arc<dyn DroppedRecordObserver>) {
        let _ = self.source.drops.observer.set(observer);
    }

    pub(crate) fn service_version(&self) -> &str {
        &self.source.service_version
    }

    pub(crate) fn boot_id(&self) -> &str {
        &self.source.service_instance_id
    }

    /// The path of the log file the recorder writes, when it opened one
    /// (REQ-TELEMETRY-52); none when it was given no log directory or the
    /// log did not open (REQ-TELEMETRY-54).
    pub(crate) fn log_path(&self) -> Option<&Path> {
        self.log_path.as_deref()
    }

    #[cfg(test)]
    fn new(
        provider: SdkTracerProvider,
        writer: Arc<dyn EventWriter>,
        service_version: &str,
        service_instance_id: &str,
    ) -> Self {
        Self::with_drops(
            provider,
            writer,
            service_version,
            service_instance_id,
            Arc::new(Drops::default()),
        )
    }

    fn with_drops(
        provider: SdkTracerProvider,
        writer: Arc<dyn EventWriter>,
        service_version: &str,
        service_instance_id: &str,
        drops: Arc<Drops>,
    ) -> Self {
        let tracer = provider.tracer("um-runner");
        Self {
            provider,
            source: EventSource {
                tracer,
                writer,
                drops,
                service_version: Arc::from(service_version),
                service_instance_id: Arc::from(service_instance_id),
            },
            log_path: None,
        }
    }

    pub(crate) fn start(
        &self,
        name: &'static str,
        attributes: impl IntoIterator<Item = KeyValue>,
    ) -> Event {
        self.source.start(&Context::new(), name, attributes)
    }

    // record writes one instantaneous, reviewed diagnostic without creating a
    // main event or span. Protocol call sites pass decoded fields only; this
    // interface must never receive credentials, raw frames, or peer reasons.
    pub(crate) fn record(
        &self,
        name: &'static str,
        attributes: impl IntoIterator<Item = KeyValue>,
    ) {
        let mut fields = Map::new();
        for attribute in self
            .source
            .common_attributes(false)
            .into_iter()
            .chain(attributes)
        {
            if let Some(value) = json_value(&attribute.value) {
                fields.insert(attribute.key.as_str().to_owned(), value);
            }
        }
        fields.insert(EVENT_NAME.to_owned(), Value::String(name.to_owned()));
        if self.source.writer.write(&fields).is_err() {
            self.source.drops.dropped(0);
        }
    }
}

impl EventSource {
    fn common_attributes(&self, main: bool) -> [KeyValue; 5] {
        [
            KeyValue::new(MAIN, main),
            KeyValue::new(SCHEMA_VERSION, 1_i64),
            KeyValue::new(SERVICE_NAME, "um-runner"),
            KeyValue::new(SERVICE_VERSION, Arc::clone(&self.service_version)),
            KeyValue::new(SERVICE_INSTANCE_ID, Arc::clone(&self.service_instance_id)),
        ]
    }

    // start opens one main event whose span is a child of the span in
    // `parent`, or a root when `parent` carries no valid span. A child's local
    // record names its parent as `parent_span_id` from the start, so the two
    // sinks link the same way; `trace_id` and `span_id` are written at finish.
    fn start(
        &self,
        parent: &Context,
        name: &'static str,
        attributes: impl IntoIterator<Item = KeyValue>,
    ) -> Event {
        let mut fields = BTreeMap::new();
        let mut span_attributes = Vec::new();
        for attribute in self.common_attributes(true).into_iter().chain(attributes) {
            if let Some(value) = json_value(&attribute.value) {
                fields.insert(attribute.key.as_str().to_owned(), value);
                span_attributes.push(attribute);
            }
        }
        fields.insert(EVENT_NAME.to_owned(), Value::String(name.to_owned()));
        let parent_span_context = parent.span().span_context().clone();
        if parent_span_context.is_valid() {
            fields.insert(
                PARENT_SPAN_ID.to_owned(),
                Value::String(parent_span_context.span_id().to_string()),
            );
        }

        let span = self
            .tracer
            .span_builder(name)
            .with_kind(SpanKind::Internal)
            .with_attributes(span_attributes)
            .start_with_context(&self.tracer, parent);
        Event {
            inner: Arc::new(EventInner {
                context: Context::new().with_span(span),
                source: self.clone(),
                started_at: useful_support::monotonic_now(),
                state: Mutex::new(EventState {
                    fields,
                    completed: false,
                }),
                name,
            }),
        }
    }
}

fn runner_resource(service_version: &str, service_instance_id: &str) -> Resource {
    Resource::builder_empty()
        .with_attributes([
            KeyValue::new(SERVICE_NAME, "um-runner"),
            KeyValue::new(SERVICE_VERSION, service_version.to_owned()),
            KeyValue::new(SERVICE_INSTANCE_ID, service_instance_id.to_owned()),
        ])
        .build()
}

#[derive(Clone)]
pub(crate) struct Event {
    inner: Arc<EventInner>,
}

struct EventInner {
    context: Context,
    source: EventSource,
    started_at: Instant,
    state: Mutex<EventState>,
    name: &'static str,
}

struct EventState {
    fields: BTreeMap<String, Value>,
    completed: bool,
}

impl fmt::Debug for Event {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Event")
            .field("name", &self.inner.name)
            .finish_non_exhaustive()
    }
}

impl Event {
    // child starts a main event whose span is a child of this event's span,
    // with the same attribute shape as `Recorder::start`.
    pub(crate) fn child(
        &self,
        name: &'static str,
        attributes: impl IntoIterator<Item = KeyValue>,
    ) -> Event {
        self.inner
            .source
            .start(&self.inner.context, name, attributes)
    }

    pub(crate) fn inject_trace_context(&self, injector: &mut dyn Injector) {
        TraceContextPropagator::new().inject_context(&self.inner.context, injector);
    }

    pub(crate) fn set(&self, attribute: KeyValue) {
        let Some(value) = json_value(&attribute.value) else {
            return;
        };
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.completed {
            return;
        }
        state
            .fields
            .insert(attribute.key.as_str().to_owned(), value);
        self.inner.context.span().set_attribute(attribute);
    }

    pub(crate) fn elapsed_milliseconds(&self) -> i64 {
        integer_u128(useful_support::elapsed(self.inner.started_at).as_millis())
    }

    pub(crate) fn finish(&self, outcome: Outcome) {
        let (event, prior_dropped_count) = {
            let mut state = self
                .inner
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.completed {
                return;
            }
            state.completed = true;

            let outcome_attribute = KeyValue::new(OUTCOME, outcome.as_str());
            state.fields.insert(
                OUTCOME.to_owned(),
                Value::String(outcome.as_str().to_owned()),
            );
            let duration_ms = self.elapsed_milliseconds();
            state
                .fields
                .insert(DURATION_MS.to_owned(), Value::Number(duration_ms.into()));
            let span = self.inner.context.span();
            let prior_dropped_count = self.inner.source.drops.take();
            if prior_dropped_count > 0 {
                let count = integer(prior_dropped_count);
                state
                    .fields
                    .insert(DROPPED_COUNT.to_owned(), Value::Number(count.into()));
                span.set_attribute(KeyValue::new(DROPPED_COUNT, count));
            }
            span.set_attribute(outcome_attribute);
            if outcome.is_error() {
                span.set_status(Status::error(outcome.as_str()));
            }
            let span_context = span.span_context();
            if span_context.is_valid() {
                state.fields.insert(
                    TRACE_ID.to_owned(),
                    Value::String(span_context.trace_id().to_string()),
                );
                state.fields.insert(
                    SPAN_ID.to_owned(),
                    Value::String(span_context.span_id().to_string()),
                );
            }
            span.end();

            let event = state
                .fields
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect::<Map<_, _>>();
            (event, prior_dropped_count)
        };

        if self.inner.source.writer.write(&event).is_err() {
            self.inner.source.drops.dropped(prior_dropped_count);
        }
    }
}

impl Drop for Recorder {
    fn drop(&mut self) {
        let _ = self
            .provider
            .shutdown_with_timeout(otlp::MAX_SHUTDOWN_TIMEOUT);
    }
}

fn json_value(value: &AttributeValue) -> Option<Value> {
    match value {
        AttributeValue::Bool(value) => Some(Value::Bool(*value)),
        AttributeValue::I64(value) => Some(Value::Number((*value).into())),
        AttributeValue::F64(value) => Number::from_f64(*value).map(Value::Number),
        AttributeValue::String(value) => Some(Value::String(value.as_str().to_owned())),
        AttributeValue::Array(_) => None,
        _ => None,
    }
}

pub(crate) fn integer(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

pub(crate) fn integer_u128(value: u128) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

/// The closed class a record carries for an I/O failure, never its message:
/// the kinds a runner host commonly meets, else `io_error`.
pub(crate) fn io_error_class(kind: io::ErrorKind) -> &'static str {
    match kind {
        io::ErrorKind::NotFound => "not_found",
        io::ErrorKind::PermissionDenied => "permission_denied",
        io::ErrorKind::NotADirectory => "not_a_directory",
        io::ErrorKind::IsADirectory => "is_a_directory",
        io::ErrorKind::StorageFull => "storage_full",
        io::ErrorKind::Unsupported => "unsupported",
        io::ErrorKind::InvalidData => "invalid_data",
        _ => "io_error",
    }
}

#[cfg(test)]
#[derive(Clone, Debug, Default)]
pub(crate) struct TestCapture {
    events: Arc<Mutex<Vec<Map<String, Value>>>>,
    spans: Arc<Mutex<Vec<opentelemetry_sdk::trace::SpanData>>>,
    event_notification: Arc<tokio::sync::Notify>,
}

#[cfg(test)]
impl TestCapture {
    pub(crate) fn records(&self) -> Vec<Map<String, Value>> {
        self.events
            .lock()
            .expect("event capture mutex poisoned")
            .clone()
    }

    pub(crate) fn events(&self) -> Vec<Map<String, Value>> {
        self.records()
            .into_iter()
            .filter(|event| event.get(MAIN).and_then(Value::as_bool) == Some(true))
            .collect()
    }

    pub(crate) fn spans(&self) -> Vec<opentelemetry_sdk::trace::SpanData> {
        self.spans
            .lock()
            .expect("span capture mutex poisoned")
            .clone()
    }

    pub(crate) fn event(&self, name: &str) -> Map<String, Value> {
        self.events()
            .into_iter()
            .find(|event| event[EVENT_NAME] == name)
            .expect("captured event should exist")
    }

    pub(crate) async fn wait_for_event(&self, name: &str) {
        loop {
            let notified = self.event_notification.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.events().iter().any(|event| event[EVENT_NAME] == name) {
                return;
            }
            notified.await;
        }
    }

    pub(crate) fn span_count(&self, name: &str) -> usize {
        self.spans
            .lock()
            .expect("span capture mutex poisoned")
            .iter()
            .filter(|span| &*span.name == name)
            .count()
    }
}

#[cfg(test)]
impl EventWriter for TestCapture {
    fn write(&self, event: &Map<String, Value>) -> io::Result<()> {
        self.events
            .lock()
            .expect("event capture mutex poisoned")
            .push(event.clone());
        self.event_notification.notify_waiters();
        Ok(())
    }
}

#[cfg(test)]
impl opentelemetry_sdk::trace::SpanExporter for TestCapture {
    async fn export(
        &self,
        spans: Vec<opentelemetry_sdk::trace::SpanData>,
    ) -> opentelemetry_sdk::error::OTelSdkResult {
        self.spans
            .lock()
            .expect("span capture mutex poisoned")
            .extend(spans);
        Ok(())
    }
}

#[cfg(test)]
pub(crate) const TEST_SERVICE_VERSION: &str = "0.0.0-test";

#[cfg(test)]
pub(crate) fn test_recorder(service_instance_id: &str) -> (Arc<Recorder>, TestCapture) {
    let capture = TestCapture::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(capture.clone())
        .build();
    let recorder = Recorder::new(
        provider,
        Arc::new(capture.clone()),
        TEST_SERVICE_VERSION,
        service_instance_id,
    );
    (Arc::new(recorder), capture)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::{self, Write};
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::mpsc::{self, sync_channel};
    use std::thread;

    use opentelemetry::KeyValue;
    use opentelemetry::trace::SpanId;
    use serde_json::Value;

    use super::log_file::{self, LogReporter};
    use super::{
        DROPPED_COUNT, DroppedRecordObserver, EventWriter, FramedWriter, LocalOutputs, Outcome,
        QueueMessage, QueuedWriter, RecordClock, RecordSink as _, Recorder, TEST_SERVICE_VERSION,
        TestCapture, test_recorder,
    };

    fn epoch_clock() -> RecordClock {
        Arc::new(|| time::OffsetDateTime::UNIX_EPOCH)
    }

    /// The record `receiver` holds next, as the line its output would write.
    fn queued_line(
        receiver: &std::sync::mpsc::Receiver<QueueMessage>,
    ) -> Option<(Vec<u8>, serde_json::Map<String, Value>)> {
        let Ok(QueueMessage::Record(record)) = receiver.try_recv() else {
            return None;
        };
        let line = serde_json::from_slice(&record.bytes).expect("record is one JSON object");
        Some((record.bytes.to_vec(), line))
    }

    // REQ-TELEMETRY-2
    #[test]
    fn projects_scalar_attributes_to_one_json_event_and_span() {
        let (recorder, capture) = test_recorder("rbt_fixture");
        let event = recorder.start(
            "runner.fixture",
            [
                KeyValue::new("fixture.bool", true),
                KeyValue::new("fixture.integer", 42_i64),
                KeyValue::new("fixture.float", 1.5_f64),
                KeyValue::new("fixture.string", "line one\nline two"),
            ],
        );
        event.finish(Outcome::Success);
        event.finish(Outcome::Failure);
        drop(event);

        let events = capture.events();
        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert_eq!(event["event.name"], "runner.fixture");
        assert_eq!(event["fixture.bool"], true);
        assert_eq!(event["fixture.integer"], 42);
        assert_eq!(event["fixture.float"], 1.5);
        assert_eq!(event["fixture.string"], "line one\nline two");
        assert_eq!(event["um.outcome"], "success");
        assert!(event["duration_ms"].is_i64());
        assert!(event["trace_id"].is_string());
        assert!(event["span_id"].is_string());
        let encoded = serde_json::to_string(event).expect("encode captured event");
        assert!(!encoded.contains('\n'));

        let spans = capture.spans();
        assert_eq!(spans.len(), 1);
        assert_eq!(&*spans[0].name, "runner.fixture");
        assert_eq!(spans[0].status, opentelemetry::trace::Status::Unset);
        let span_attribute = |name: &str| {
            spans[0]
                .attributes
                .iter()
                .find(|attribute| attribute.key.as_str() == name)
                .map(|attribute| &attribute.value)
        };
        assert_eq!(
            span_attribute("um.main"),
            Some(&opentelemetry::Value::Bool(true))
        );
        assert_eq!(
            span_attribute("service.name"),
            Some(&opentelemetry::Value::String("um-runner".into()))
        );
        assert_eq!(
            span_attribute("fixture.integer"),
            Some(&opentelemetry::Value::I64(42))
        );
        assert_eq!(
            span_attribute("um.outcome"),
            Some(&opentelemetry::Value::String("success".into()))
        );
    }

    // REQ-TELEMETRY-24
    #[test]
    fn a_child_event_is_linked_to_its_parent_on_both_sinks() {
        let (recorder, capture) = test_recorder("rbt_fixture");
        let parent = recorder.start("runner.run", [KeyValue::new("fixture.run", "run_1")]);
        let child = parent.child(
            "runner.run.checkout",
            [KeyValue::new("fixture.phase", "checkout")],
        );
        child.finish(Outcome::Rejected);
        parent.finish(Outcome::Success);

        let child_event = capture.event("runner.run.checkout");
        let parent_event = capture.event("runner.run");
        assert_eq!(child_event["trace_id"], parent_event["trace_id"]);
        assert_eq!(child_event["parent_span_id"], parent_event["span_id"]);
        assert!(child_event["span_id"].is_string());
        assert_ne!(child_event["span_id"], parent_event["span_id"]);
        assert!(parent_event.get("parent_span_id").is_none());
        assert_eq!(child_event["um.main"], true);
        assert_eq!(child_event["um.event.schema_version"], 1);
        assert_eq!(child_event["service.name"], "um-runner");
        assert_eq!(child_event["service.version"], TEST_SERVICE_VERSION);
        assert_eq!(child_event["service.instance.id"], "rbt_fixture");
        assert_eq!(child_event["fixture.phase"], "checkout");
        assert_eq!(child_event["um.outcome"], "rejected");

        let spans = capture.spans();
        assert_eq!(spans.len(), 2);
        let span = |name: &str| {
            spans
                .iter()
                .find(|span| &*span.name == name)
                .unwrap_or_else(|| panic!("span {name} exported"))
        };
        let parent_span = span("runner.run");
        let child_span = span("runner.run.checkout");
        assert_eq!(
            child_span.span_context.trace_id(),
            parent_span.span_context.trace_id()
        );
        assert_eq!(
            child_span.parent_span_id,
            parent_span.span_context.span_id()
        );
        assert_eq!(parent_span.parent_span_id, SpanId::INVALID);
        assert_eq!(
            child_event["span_id"],
            child_span.span_context.span_id().to_string()
        );
        assert_eq!(child_span.status, opentelemetry::trace::Status::Unset);
        let child_attribute = |name: &str| {
            child_span
                .attributes
                .iter()
                .find(|attribute| attribute.key.as_str() == name)
                .map(|attribute| &attribute.value)
        };
        assert_eq!(
            child_attribute("um.main"),
            Some(&opentelemetry::Value::Bool(true))
        );
        assert_eq!(
            child_attribute("um.outcome"),
            Some(&opentelemetry::Value::String("rejected".into()))
        );
    }

    #[test]
    fn concurrent_enrichment_is_complete_and_duplicate_finish_is_ignored() {
        let (recorder, capture) = test_recorder("rbt_fixture");
        let event = recorder.start("runner.concurrent", []);
        let mut workers = Vec::new();
        for index in 0..20 {
            let event = event.clone();
            workers.push(thread::spawn(move || {
                event.set(KeyValue::new(
                    format!("fixture.field_{index}"),
                    index as i64,
                ));
            }));
        }
        for worker in workers {
            worker.join().expect("join event worker");
        }
        event.finish(Outcome::Failure);
        event.finish(Outcome::Success);

        let events = capture.events();
        assert_eq!(events.len(), 1);
        for index in 0..20 {
            assert_eq!(events[0][&format!("fixture.field_{index}")], index as i64);
        }
        assert_eq!(events[0]["um.outcome"], "failure");
        let spans = capture.spans();
        assert_eq!(spans.len(), 1);
        assert!(matches!(
            spans[0].status,
            opentelemetry::trace::Status::Error { .. }
        ));
    }

    // REQ-TELEMETRY-14
    #[test]
    fn writer_failure_is_nonfatal_and_reported_by_the_next_event() {
        #[derive(Debug)]
        struct FailOnceWriter {
            failed: AtomicBool,
            captured: TestCapture,
        }

        impl EventWriter for FailOnceWriter {
            fn write(&self, event: &serde_json::Map<String, Value>) -> std::io::Result<()> {
                if !self.failed.swap(true, Ordering::AcqRel) {
                    return Err(std::io::Error::other("synthetic writer failure"));
                }
                self.captured.write(event)
            }
        }

        let captured = TestCapture::default();
        let writer = Arc::new(FailOnceWriter {
            failed: AtomicBool::new(false),
            captured: captured.clone(),
        });
        let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder().build();
        let recorder = Recorder::new(provider, writer, "0.2.0-test", "rbt_fixture");
        let dropped = recorder.start("runner.dropped", []);
        let recovered = recorder.start("runner.recovered", []);
        dropped.finish(Outcome::Failure);
        recovered.finish(Outcome::Success);

        let events = captured.events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0][DROPPED_COUNT], 1);
    }

    // REQ-TELEMETRY-14, REQ-RUNNERENROLL-138: the observer is told of each record the
    // writer refuses, once, never of the count a refused record carried; the pending count
    // and the record that reports it are what they are without one.
    #[test]
    fn each_refused_record_tells_the_observer_once_and_the_count_is_unchanged() {
        #[derive(Debug, Default)]
        struct RefusingWriter {
            refusing: AtomicBool,
            captured: TestCapture,
        }

        impl EventWriter for RefusingWriter {
            fn write(&self, event: &serde_json::Map<String, Value>) -> std::io::Result<()> {
                if self.refusing.load(Ordering::Acquire) {
                    return Err(std::io::Error::other("synthetic writer refusal"));
                }
                self.captured.write(event)
            }
        }

        #[derive(Debug, Default)]
        struct CountingObserver {
            told: AtomicU64,
        }

        impl DroppedRecordObserver for CountingObserver {
            fn record_dropped(&self) {
                self.told.fetch_add(1, Ordering::AcqRel);
            }
        }

        // Three refused records, one taken, two refused, one refused that carries their
        // count, one taken: the dropped counts the taken records carry.
        let reported = |recorder: &Recorder, writer: &RefusingWriter| {
            writer.refusing.store(true, Ordering::Release);
            recorder.record("runner.dropped_diagnostic", []);
            recorder
                .start("runner.dropped", [])
                .finish(Outcome::Failure);
            recorder
                .start("runner.dropped", [])
                .finish(Outcome::Success);
            writer.refusing.store(false, Ordering::Release);
            recorder
                .start("runner.recovered", [])
                .finish(Outcome::Success);
            writer.refusing.store(true, Ordering::Release);
            recorder.record("runner.dropped_diagnostic", []);
            recorder.record("runner.dropped_diagnostic", []);
            recorder
                .start("runner.dropped_carrying", [])
                .finish(Outcome::Success);
            writer.refusing.store(false, Ordering::Release);
            recorder
                .start("runner.recovered", [])
                .finish(Outcome::Success);
            writer
                .captured
                .events()
                .iter()
                .map(|event| event[DROPPED_COUNT].clone())
                .collect::<Vec<_>>()
        };
        let recorder_with = |writer: &Arc<RefusingWriter>| {
            let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder().build();
            Recorder::new(provider, writer.clone(), "0.2.0-test", "rbt_fixture")
        };

        let observer = Arc::new(CountingObserver::default());
        let observed_writer = Arc::new(RefusingWriter::default());
        let observed = recorder_with(&observed_writer);
        observed.set_dropped_record_observer(observer.clone());
        assert_eq!(reported(&observed, &observed_writer), [3, 3]);
        assert_eq!(observer.told.load(Ordering::Acquire), 6);

        let unobserved_writer = Arc::new(RefusingWriter::default());
        let unobserved = recorder_with(&unobserved_writer);
        assert_eq!(reported(&unobserved, &unobserved_writer), [3, 3]);
    }

    // REQ-TELEMETRY-14
    #[test]
    fn a_full_output_queue_drops_without_waiting() {
        let (sender, _receiver) = sync_channel(1);
        let writer = LocalOutputs::new(vec![QueuedWriter::from_sender(sender)], epoch_clock());
        let event = serde_json::Map::new();

        writer.write(&event).expect("fill event queue");
        let error = writer
            .write(&event)
            .expect_err("full queue should drop event");

        assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
    }

    // REQ-TELEMETRY-51
    #[test]
    fn every_local_record_carries_the_utc_time_it_was_written() {
        // The clock reads the instant 2026-11-19T04:54:24.993Z with a finer
        // fraction and in another offset: the record carries it in UTC to
        // the millisecond.
        let written_at = time::OffsetDateTime::parse(
            "2026-11-19T06:54:24.993456789+02:00",
            &time::format_description::well_known::Rfc3339,
        )
        .expect("parse fixed clock");
        let clock: RecordClock = Arc::new(move || written_at);
        let (sender, receiver) = sync_channel(4);
        let writer = Arc::new(LocalOutputs::new(
            vec![QueuedWriter::from_sender(sender)],
            clock,
        ));
        let spans = TestCapture::default();
        let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
            .with_simple_exporter(spans.clone())
            .build();
        let recorder = Recorder::new(provider, writer, TEST_SERVICE_VERSION, "rbt_fixture");

        recorder
            .start("runner.finished", [])
            .finish(Outcome::Success);
        recorder.record("runner.recorded", []);

        for name in ["runner.finished", "runner.recorded"] {
            let Ok(QueueMessage::Record(record)) = receiver.try_recv() else {
                panic!("{name} should be queued as a record");
            };
            let line: serde_json::Map<String, Value> =
                serde_json::from_slice(&record.bytes).expect("record is one JSON object");
            assert_eq!(line["event.name"], name);
            assert_eq!(line["time"], "2026-11-19T04:54:24.993Z");
        }
        let finished = spans
            .spans()
            .into_iter()
            .find(|span| span.name == "runner.finished")
            .expect("finished event exports a span");
        assert!(
            finished
                .attributes
                .iter()
                .all(|attribute| attribute.key.as_str() != "time")
        );
    }

    // REQ-TELEMETRY-14, INV-TELEMETRY-17
    #[test]
    fn a_record_is_dropped_only_when_no_output_takes_it() {
        let (first_sender, first) = sync_channel(1);
        let (second_sender, second) = sync_channel(3);
        let writer = Arc::new(LocalOutputs::new(
            vec![
                QueuedWriter::from_sender(first_sender),
                QueuedWriter::from_sender(second_sender),
            ],
            epoch_clock(),
        ));
        let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder().build();
        let recorder = Recorder::new(provider, writer, TEST_SERVICE_VERSION, "rbt_fixture");
        let finish = |name: &'static str| recorder.start(name, []).finish(Outcome::Success);

        finish("runner.both");
        let (first_bytes, _) = queued_line(&first).expect("the first output takes the record");
        let (second_bytes, line) = queued_line(&second).expect("the second output takes it");
        assert_eq!(first_bytes, second_bytes);
        assert_eq!(line["event.name"], "runner.both");

        // With the first queue full, the second output alone takes a record,
        // which is not counted dropped.
        finish("runner.fills_first");
        finish("runner.first_full");
        finish("runner.after_first_full");
        // With both queues full, the record reaches no output and is counted.
        finish("runner.dropped");
        for name in [
            "runner.fills_first",
            "runner.first_full",
            "runner.after_first_full",
        ] {
            let (_, line) = queued_line(&second).expect("the second output takes the record");
            assert_eq!(line["event.name"], name);
            assert!(line.get(DROPPED_COUNT).is_none(), "{line:?}");
        }
        assert!(queued_line(&second).is_none());
        finish("runner.after_drop");

        let (_, line) = queued_line(&second).expect("the second output takes the record");
        assert_eq!(line["event.name"], "runner.after_drop");
        assert_eq!(line[DROPPED_COUNT], 1);
        let (_, line) = queued_line(&first).expect("the first output held one record");
        assert_eq!(line["event.name"], "runner.fills_first");
        assert!(queued_line(&first).is_none());
    }

    #[test]
    fn a_partial_write_is_terminated_before_the_next_record() {
        #[derive(Default)]
        struct PartialThenRecover {
            calls: usize,
            output: Vec<u8>,
        }

        impl Write for PartialThenRecover {
            fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
                self.calls += 1;
                match self.calls {
                    1 => {
                        let written = buffer.len().min(4);
                        self.output.extend_from_slice(&buffer[..written]);
                        Ok(written)
                    }
                    2 => Err(io::Error::other("synthetic partial write failure")),
                    _ => {
                        self.output.extend_from_slice(buffer);
                        Ok(buffer.len())
                    }
                }
            }

            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }

        let mut writer = FramedWriter::new(PartialThenRecover::default());
        writer
            .write_record(b"{\"broken\":true}\n")
            .expect_err("first record should fail after a partial write");
        writer
            .write_record(b"{\"event.name\":\"runner.recovered\"}\n")
            .expect("write recovery record");

        let lines: Vec<_> = writer.output.output.split(|byte| *byte == b'\n').collect();
        assert_eq!(lines.len(), 3);
        assert!(serde_json::from_slice::<Value>(lines[0]).is_err());
        let recovered: Value = serde_json::from_slice(lines[1]).expect("decode recovery record");
        assert_eq!(recovered["event.name"], "runner.recovered");
    }

    // Standard error's output as a channel of the lines written to it, one
    // message for each record.
    struct ChannelOutput(mpsc::Sender<Vec<u8>>);

    impl Write for ChannelOutput {
        fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
            let _ = self.0.send(buffer.to_vec());
            Ok(buffer.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn channel_stderr() -> (
        impl FnOnce() -> io::Result<FramedWriter<ChannelOutput>> + Send + 'static,
        mpsc::Receiver<Vec<u8>>,
    ) {
        let (sender, lines) = mpsc::channel();
        (move || Ok(FramedWriter::new(ChannelOutput(sender))), lines)
    }

    fn line(bytes: &[u8]) -> serde_json::Map<String, Value> {
        serde_json::from_slice(bytes).expect("a record is one JSON object")
    }

    fn log_reports(
        lines: &[serde_json::Map<String, Value>],
    ) -> Vec<serde_json::Map<String, Value>> {
        lines
            .iter()
            .filter(|line| line["event.name"] == "runner.telemetry_log")
            .cloned()
            .collect()
    }

    // REQ-TELEMETRY-54, REQ-TELEMETRY-13
    #[test]
    fn a_log_that_cannot_open_is_reported_once_and_the_recorder_serves() {
        let root = tempfile::tempdir().expect("temporary directory");
        let logs = root.path().join("logs");
        fs::write(&logs, b"not a directory\n").expect("write logs as a file");
        let (stderr, lines) = channel_stderr();
        let directory = logs.clone();

        let recorder = Recorder::open_with(
            "rbt_fixture",
            TEST_SERVICE_VERSION,
            epoch_clock(),
            stderr,
            Some((
                log_file::path_in(&logs),
                move |clock: RecordClock, reporter: LogReporter| {
                    log_file::LogFile::open(
                        &directory,
                        log_file::RUNNER_LOG_LIMITS,
                        clock,
                        reporter,
                    )
                },
            )),
        );
        assert_eq!(recorder.log_path(), None);

        let startup = lines
            .try_iter()
            .map(|bytes| line(&bytes))
            .collect::<Vec<_>>();
        let reports = log_reports(&startup);
        assert_eq!(reports.len(), 1, "{startup:?}");
        let report = &reports[0];
        assert_eq!(report["diagnostic.classification"], "open_failed");
        assert_eq!(
            report["error.type"],
            super::io_error_class(io::ErrorKind::NotADirectory)
        );
        assert_eq!(report["um.main"], false);
        let mut keys = report.keys().map(String::as_str).collect::<Vec<_>>();
        keys.sort_unstable();
        assert_eq!(
            keys,
            [
                "diagnostic.classification",
                "error.type",
                "event.name",
                "service.instance.id",
                "service.name",
                "service.version",
                "time",
                "um.event.schema_version",
                "um.main",
            ]
        );

        recorder.record("runner.serving", []);
        let serving = line(&lines.recv().expect("standard error takes the record"));
        assert_eq!(serving["event.name"], "runner.serving");
        assert_eq!(fs::read(&logs).expect("read logs"), b"not a directory\n");
        let entries = fs::read_dir(root.path())
            .expect("list the state directory")
            .map(|entry| entry.expect("an entry").file_name())
            .collect::<Vec<_>>();
        assert_eq!(entries, ["logs"]);
    }

    // REQ-TELEMETRY-54, REQ-TELEMETRY-14
    #[test]
    fn a_log_whose_writes_fail_is_reported_once_and_drops_nothing() {
        // The log's output: every write fails, and tells the test it did.
        struct FailingOutput(mpsc::Sender<()>);

        impl Write for FailingOutput {
            fn write(&mut self, _buffer: &[u8]) -> io::Result<usize> {
                let _ = self.0.send(());
                Err(io::ErrorKind::StorageFull.into())
            }

            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }

        let (stderr, lines) = channel_stderr();
        let (failed, failures) = mpsc::channel();
        let recorder = Recorder::open_with(
            "rbt_fixture",
            TEST_SERVICE_VERSION,
            epoch_clock(),
            stderr,
            Some((
                PathBuf::from("/failing/runner.log"),
                move |_clock: RecordClock, _reporter: LogReporter| {
                    Ok(FramedWriter::new(FailingOutput(failed)))
                },
            )),
        );
        assert_eq!(recorder.log_path(), Some(Path::new("/failing/runner.log")));

        let names = ["runner.first", "runner.second", "runner.third"];
        for name in names {
            recorder.record(name, []);
        }
        // The three records and the one report of their failure each fail
        // in the log.
        for _ in 0..4 {
            failures.recv().expect("the log tries the record");
        }
        recorder
            .start("runner.finished", [])
            .finish(Outcome::Success);

        let mut received = Vec::new();
        loop {
            let line = line(&lines.recv().expect("standard error takes the record"));
            let finished = line["event.name"] == "runner.finished";
            received.push(line);
            if finished {
                break;
            }
        }
        let reports = log_reports(&received);
        assert_eq!(reports.len(), 1, "{received:?}");
        assert_eq!(reports[0]["diagnostic.classification"], "write_failed");
        assert_eq!(
            reports[0]["error.type"],
            super::io_error_class(io::ErrorKind::StorageFull)
        );
        for name in names {
            assert!(
                received.iter().any(|line| line["event.name"] == name),
                "{name} reaches standard error"
            );
        }
        let finished = received.last().expect("the finished event");
        assert!(finished.get(DROPPED_COUNT).is_none(), "{finished:?}");
    }

    #[test]
    fn debug_output_does_not_include_service_or_attribute_values() {
        let (recorder, _capture) = test_recorder("INSTANCE-MUST-NOT-APPEAR");
        let event = recorder.start(
            "runner.redacted",
            [KeyValue::new("fixture.secret", "SECRET-MUST-NOT-APPEAR")],
        );

        let recorder_debug = format!("{recorder:?}");
        let event_debug = format!("{event:?}");
        assert!(!recorder_debug.contains("INSTANCE-MUST-NOT-APPEAR"));
        assert!(!event_debug.contains("SECRET-MUST-NOT-APPEAR"));
        assert_eq!(recorder_debug, "Recorder { .. }");
        assert!(event_debug.contains("runner.redacted"));
    }
}
