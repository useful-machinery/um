use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::future::{Future as _, poll_fn};
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use serde_json::Value;
use tokio::sync::watch;

use super::agent::{AgentCompatibilityProfile, AgentInvocationLimits, PositiveDuration};
use super::artifact::CaptureCancellation;
use super::cancellation::{MAXIMUM_CANCELLATION_GRACE, MINIMUM_CANCELLATION_GRACE};
use super::capacity::WorkflowCapacity;
use super::claude_code::ClaudeCodeConfig;
use super::claude_code::ClaudeCodeStreamJsonV1ProtocolLimits;
use super::codex::CodexAppServerV1ProtocolLimits;
use super::codex::CodexConfig;
use super::document::WaitRule;
use super::execution_root::{AdmittedExecutionRoot, ExecutionRootAdmissionFailure};
use super::git_capture::{
    CloudGitCaptureProjection, GitCaptureContext, GitWorkspaceAdmissionFailure, LocalGitBaseline,
};
use super::pi::PiConfig;
use super::pi::PiJsonV1ProtocolLimits;
use super::resolution::{ResolvedWorkflow, WorkflowContentDigest};
use super::tree::{TreeFile, ValidatedTree};
use super::validated::{
    ValidatedEdgeTarget, ValidatedHarness, ValidatedRecoveryHandler, ValidatedStep,
};
use crate::claude_code::ValidatedClaudeCodeInstallation;
use crate::codex::ValidatedCodexInstallation;
use crate::harness_installation::UntestedHarnessAdvisory;
use crate::harness_installation::is_executable_file;
use crate::pi::ValidatedPiInstallation;

const MAXIMUM_CAPTURED_FILES: usize = 1024;
const MAXIMUM_CAPTURED_FILE_BYTES: u64 = 64 * 1024 * 1024;
const MAXIMUM_TOTAL_CAPTURED_BYTES: u64 = 256 * 1024 * 1024;
const MAXIMUM_CAPTURED_GIT_CARRIERS: usize = 1024;
const MAXIMUM_CAPTURED_GIT_CARRIER_BYTES: u64 = 64 * 1024 * 1024;
const MAXIMUM_TOTAL_CAPTURED_GIT_CARRIER_BYTES: u64 = 256 * 1024 * 1024;
const MAXIMUM_INPUT_VALUES: usize = 1024;
const MAXIMUM_INPUT_VALUE_BYTES: u64 = 64 * 1024 * 1024;
const MAXIMUM_TOTAL_INPUT_BYTES: u64 = 256 * 1024 * 1024;
const MAXIMUM_LIVE_INPUT_BYTES: u64 = 256 * 1024 * 1024;
pub(crate) const MAXIMUM_AGENT_PROMPT_BYTES: u64 = 1024 * 1024;
const MAXIMUM_AGENT_ATTACHMENTS: usize = 256;
const MAXIMUM_AGENT_ATTACHMENT_BYTES: u64 = 256 * 1024 * 1024;
pub(crate) const MAXIMUM_AGENT_RESPONSE_BYTES: u64 = 8 * 1024 * 1024;
pub(crate) const MAXIMUM_AGENT_RESULT_BYTES: u64 = 8 * 1024 * 1024;
const MAXIMUM_AGENT_RESULT_REJECTION_FEEDBACK_BYTES: u64 = 8 * 1024;
const AGENT_RESULT_VALIDATION_DEADLINE: Duration = Duration::from_secs(60);
const AGENT_RESULT_SETTLEMENT_GRACE: Duration = Duration::from_secs(30);

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CancellationReason {
    UserRequest,
    TerminationRequest,
    CallerOutputFailure,
    RunnerShutdown,
    ExecutionLeaseExpired,
    ForceAbort,
}

impl CancellationReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::UserRequest => "user_request",
            Self::TerminationRequest => "termination_request",
            Self::CallerOutputFailure => "caller_output_failure",
            Self::RunnerShutdown => "runner_shutdown",
            Self::ExecutionLeaseExpired => "execution_lease_expired",
            Self::ForceAbort => "force_abort",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct CancellationOperationId(u64);

impl CancellationOperationId {
    pub(crate) const fn get(self) -> u64 {
        self.0
    }

    #[cfg(test)]
    pub(crate) const fn fixture(value: u64) -> Self {
        Self(value)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CancellationOperation {
    Graceful {
        id: CancellationOperationId,
        reason: CancellationReason,
    },
    OrdinaryOnly {
        id: CancellationOperationId,
        reason: CancellationReason,
    },
    /// A graceful stop that settles the stopped ordinary steps `failed` with `interrupted`
    /// instead of `cancelled` (REQ-WFEXEC-69).
    Interruption {
        id: CancellationOperationId,
        reason: CancellationReason,
    },
    ForceAbort {
        id: CancellationOperationId,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OrdinaryCancellationRequestResult {
    Applied,
    AlreadyRequested,
    FinalizersPreserved,
}

#[derive(Debug)]
struct CancellationOperationState {
    operations: Vec<CancellationOperation>,
    next_id: u64,
    finalization_arming: bool,
    finalization_armed: bool,
    pending_finalization_reason: Option<CancellationReason>,
    pending_force_abort: bool,
    phase_reason: Option<CancellationReason>,
    force_abort_requested: bool,
    /// The ordinary-phase cancellation was requested as a withdrawal (REQ-WFEXEC-73).
    withdrawal: bool,
    interruption_requested: bool,
}

impl Default for CancellationOperationState {
    fn default() -> Self {
        Self {
            operations: Vec::with_capacity(3),
            next_id: 1,
            finalization_arming: false,
            finalization_armed: false,
            pending_finalization_reason: None,
            pending_force_abort: false,
            phase_reason: None,
            force_abort_requested: false,
            withdrawal: false,
            interruption_requested: false,
        }
    }
}

type PendingPollObserver = Arc<dyn Fn() + Send + Sync>;

#[derive(Clone)]
pub struct CancellationSource {
    reason: watch::Sender<Option<CancellationReason>>,
    operation_version: watch::Sender<u64>,
    operations: Arc<Mutex<CancellationOperationState>>,
    pending_poll_observer: Option<PendingPollObserver>,
}

impl fmt::Debug for CancellationSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CancellationSource")
            .finish_non_exhaustive()
    }
}

impl Default for CancellationSource {
    fn default() -> Self {
        Self::new()
    }
}

impl CancellationSource {
    pub fn new() -> Self {
        let (reason, _) = watch::channel(None);
        let (operation_version, _) = watch::channel(0);
        Self {
            reason,
            operation_version,
            operations: Arc::new(Mutex::new(CancellationOperationState::default())),
            pending_poll_observer: None,
        }
    }

    #[cfg(test)]
    pub(super) fn with_pending_poll_observer(observer: impl Fn() + Send + Sync + 'static) -> Self {
        let mut source = Self::new();
        source.pending_poll_observer = Some(Arc::new(observer));
        source
    }

    pub fn request_cancellation(&self, reason: CancellationReason) -> bool {
        self.request_graceful(reason, false)
    }

    /// Requests the graceful `user_request` cancellation a withdrawal honored at a `workflow`
    /// step boundary ends the attempt with (REQ-WFEXEC-73), and marks it a withdrawal when it
    /// is the one the ordinary phase accepts; false when another cancellation came first or
    /// the ordinary phase has ended.
    pub fn request_withdrawal(&self) -> bool {
        self.request_graceful(CancellationReason::UserRequest, true)
    }

    /// Whether the ordinary-phase cancellation was requested by `request_withdrawal`.
    pub fn withdrawal_requested(&self) -> bool {
        lock_cancellation_operations(&self.operations).withdrawal
    }

    fn request_graceful(&self, reason: CancellationReason, withdrawal: bool) -> bool {
        if reason == CancellationReason::ForceAbort {
            return false;
        }
        let version = {
            let mut state = lock_cancellation_operations(&self.operations);
            if state.finalization_arming || (withdrawal && state.finalization_armed) {
                if withdrawal
                    || state.pending_finalization_reason.is_some()
                    || state.pending_force_abort
                {
                    return false;
                }
                state.pending_finalization_reason = Some(reason);
                None
            } else {
                if state.phase_reason.is_some() || state.force_abort_requested {
                    return false;
                }
                let id = CancellationOperationId(state.next_id);
                state.next_id = state.next_id.saturating_add(1);
                state.phase_reason = Some(reason);
                state.withdrawal = withdrawal;
                state
                    .operations
                    .push(CancellationOperation::Graceful { id, reason });
                Some(u64::try_from(state.operations.len()).unwrap_or(u64::MAX))
            }
        };
        if let Some(version) = version {
            self.reason.send_replace(Some(reason));
            self.operation_version.send_replace(version);
        }
        true
    }

    /// Requests an interruption: the local CLI's stop for a signal or the terminal
    /// interface (REQ-WFEXEC-69). Only `UserRequest` and `TerminationRequest` are accepted.
    /// While finalization is armed or being armed, or once a stop was requested, it is
    /// exactly `request_cancellation(reason)` (REQ-WFFINALIZE-15). The runner never makes it.
    pub fn request_interruption(&self, reason: CancellationReason) -> bool {
        if !matches!(
            reason,
            CancellationReason::UserRequest | CancellationReason::TerminationRequest
        ) {
            return false;
        }
        let version = {
            let mut state = lock_cancellation_operations(&self.operations);
            if state.finalization_arming
                || state.finalization_armed
                || state.phase_reason.is_some()
                || state.force_abort_requested
            {
                None
            } else {
                let id = CancellationOperationId(state.next_id);
                state.next_id = state.next_id.saturating_add(1);
                state.phase_reason = Some(reason);
                state.interruption_requested = true;
                state
                    .operations
                    .push(CancellationOperation::Interruption { id, reason });
                Some(u64::try_from(state.operations.len()).unwrap_or(u64::MAX))
            }
        };
        let Some(version) = version else {
            return self.request_cancellation(reason);
        };
        self.reason.send_replace(Some(reason));
        self.operation_version.send_replace(version);
        true
    }

    /// Whether an interruption, rather than a cancellation, stopped ordinary execution.
    pub fn interruption_requested(&self) -> bool {
        lock_cancellation_operations(&self.operations).interruption_requested
    }

    pub fn request_ordinary_cancellation(
        &self,
        reason: CancellationReason,
    ) -> OrdinaryCancellationRequestResult {
        if reason == CancellationReason::ForceAbort {
            return OrdinaryCancellationRequestResult::AlreadyRequested;
        }
        let version = {
            let mut state = lock_cancellation_operations(&self.operations);
            if state.finalization_arming || state.finalization_armed {
                return OrdinaryCancellationRequestResult::FinalizersPreserved;
            }
            if state.phase_reason.is_some() || state.force_abort_requested {
                return OrdinaryCancellationRequestResult::AlreadyRequested;
            }
            let id = CancellationOperationId(state.next_id);
            state.next_id = state.next_id.saturating_add(1);
            state.phase_reason = Some(reason);
            state
                .operations
                .push(CancellationOperation::OrdinaryOnly { id, reason });
            u64::try_from(state.operations.len()).unwrap_or(u64::MAX)
        };
        self.reason.send_replace(Some(reason));
        self.operation_version.send_replace(version);
        OrdinaryCancellationRequestResult::Applied
    }

    pub fn request_force_abort(&self) -> bool {
        let admission = {
            let mut state = lock_cancellation_operations(&self.operations);
            if state.finalization_arming {
                if state.force_abort_requested || state.pending_force_abort {
                    return false;
                }
                state.pending_force_abort = true;
                None
            } else {
                if state.force_abort_requested {
                    return false;
                }
                let id = CancellationOperationId(state.next_id);
                state.next_id = state.next_id.saturating_add(1);
                state.force_abort_requested = true;
                let closed_open_gate = state.phase_reason.is_none();
                if closed_open_gate {
                    state.phase_reason = Some(CancellationReason::ForceAbort);
                }
                state
                    .operations
                    .push(CancellationOperation::ForceAbort { id });
                Some((
                    u64::try_from(state.operations.len()).unwrap_or(u64::MAX),
                    closed_open_gate,
                ))
            }
        };
        if let Some((version, closed_open_gate)) = admission {
            if closed_open_gate {
                self.reason
                    .send_replace(Some(CancellationReason::ForceAbort));
            }
            self.operation_version.send_replace(version);
        }
        true
    }

    pub fn cancellation_reason(&self) -> Option<CancellationReason> {
        *self.reason.borrow()
    }

    pub fn finalization_cancellation_requested(&self) -> bool {
        let state = lock_cancellation_operations(&self.operations);
        state.finalization_armed && state.phase_reason.is_some()
            || state.finalization_arming && state.pending_finalization_reason.is_some()
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancellation_reason().is_some()
    }

    pub async fn wait_for_cancellation(&self) -> CancellationReason {
        let mut subscription = self.subscribe();
        loop {
            if let Some(reason) = *subscription.borrow_and_update() {
                return reason;
            }
            let _ = subscription.changed().await;
        }
    }

    /// Resolves once a force abort has been requested, even when a graceful cancellation
    /// preceded it and so remains the source's reason.
    pub async fn wait_for_force_abort(&self) {
        let mut operations = self.subscribe_operations();
        loop {
            while let Some(operation) = operations.next_operation() {
                if matches!(operation, CancellationOperation::ForceAbort { .. }) {
                    return;
                }
            }
            let _ = operations.changed().await;
        }
    }

    pub(super) fn subscribe(&self) -> CancellationSubscription {
        CancellationSubscription {
            receiver: self.reason.subscribe(),
            pending_poll_observer: self.pending_poll_observer.clone(),
        }
    }

    pub(super) fn subscribe_operations(&self) -> CancellationOperationSubscription {
        CancellationOperationSubscription {
            source: self.clone(),
            receiver: self.operation_version.subscribe(),
            next_index: 0,
            pending_poll_observer: self.pending_poll_observer.clone(),
        }
    }

    pub(super) fn begin_finalization_arm(&self) -> bool {
        let mut state = lock_cancellation_operations(&self.operations);
        if state.finalization_arming || state.finalization_armed {
            return false;
        }
        state.finalization_arming = true;
        true
    }

    pub fn fixture_begin_finalization_arm(&self) -> bool {
        self.begin_finalization_arm()
    }

    pub(super) fn complete_finalization_arm(&self) -> bool {
        let (reason, version) = {
            let mut state = lock_cancellation_operations(&self.operations);
            if !state.finalization_arming || state.finalization_armed {
                return false;
            }
            state.finalization_arming = false;
            state.finalization_armed = true;
            state.phase_reason = None;
            let previous_operations = state.operations.len();
            let reason = state.pending_finalization_reason.take();
            if let Some(reason) = reason {
                let id = CancellationOperationId(state.next_id);
                state.next_id = state.next_id.saturating_add(1);
                state.phase_reason = Some(reason);
                state
                    .operations
                    .push(CancellationOperation::Graceful { id, reason });
            }
            if state.pending_force_abort && !state.force_abort_requested {
                state.pending_force_abort = false;
                let id = CancellationOperationId(state.next_id);
                state.next_id = state.next_id.saturating_add(1);
                state.force_abort_requested = true;
                if state.phase_reason.is_none() {
                    state.phase_reason = Some(CancellationReason::ForceAbort);
                }
                state
                    .operations
                    .push(CancellationOperation::ForceAbort { id });
            }
            let version = (state.operations.len() > previous_operations)
                .then(|| u64::try_from(state.operations.len()).unwrap_or(u64::MAX));
            (state.phase_reason, version)
        };
        self.reason.send_replace(reason);
        if let Some(version) = version {
            self.operation_version.send_replace(version);
        }
        true
    }

    pub fn fixture_complete_finalization_arm(&self) -> bool {
        self.complete_finalization_arm()
    }

    pub(super) fn abort_finalization_arm(&self) -> bool {
        let mut state = lock_cancellation_operations(&self.operations);
        if !state.finalization_arming || state.finalization_armed {
            return false;
        }
        state.finalization_arming = false;
        state.pending_finalization_reason = None;
        state.pending_force_abort = false;
        true
    }
}

fn lock_cancellation_operations(
    operations: &Mutex<CancellationOperationState>,
) -> MutexGuard<'_, CancellationOperationState> {
    operations
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

pub(super) struct CancellationSubscription {
    receiver: watch::Receiver<Option<CancellationReason>>,
    pending_poll_observer: Option<PendingPollObserver>,
}

impl CancellationSubscription {
    pub(super) async fn changed(&mut self) -> Result<(), watch::error::RecvError> {
        wait_for_watch_change(&mut self.receiver, &mut self.pending_poll_observer).await
    }

    pub(super) fn borrow_and_update(&mut self) -> watch::Ref<'_, Option<CancellationReason>> {
        self.receiver.borrow_and_update()
    }

    #[cfg(test)]
    pub(super) fn has_changed(&self) -> Result<bool, watch::error::RecvError> {
        self.receiver.has_changed()
    }
}

pub(super) struct CancellationOperationSubscription {
    source: CancellationSource,
    receiver: watch::Receiver<u64>,
    next_index: usize,
    pending_poll_observer: Option<PendingPollObserver>,
}

impl CancellationOperationSubscription {
    pub(super) fn next_operation(&mut self) -> Option<CancellationOperation> {
        let operation = lock_cancellation_operations(&self.source.operations)
            .operations
            .get(self.next_index)
            .copied();
        if operation.is_some() {
            self.next_index = self.next_index.saturating_add(1);
            self.receiver.borrow_and_update();
        }
        operation
    }

    pub(super) async fn changed(&mut self) -> Result<(), watch::error::RecvError> {
        if self.next_operation_available() {
            return Ok(());
        }
        wait_for_watch_change(&mut self.receiver, &mut self.pending_poll_observer).await
    }

    fn next_operation_available(&self) -> bool {
        lock_cancellation_operations(&self.source.operations)
            .operations
            .len()
            > self.next_index
    }
}

async fn wait_for_watch_change<T: Clone>(
    receiver: &mut watch::Receiver<T>,
    pending_poll_observer: &mut Option<PendingPollObserver>,
) -> Result<(), watch::error::RecvError> {
    let mut observer = pending_poll_observer.take();
    let changed = receiver.changed();
    tokio::pin!(changed);
    poll_fn(|context| {
        let result = changed.as_mut().poll(context);
        if result.is_pending()
            && let Some(observer) = observer.take()
        {
            observer();
        }
        result
    })
    .await
}

#[derive(Clone, Eq, PartialEq)]
pub struct ResolvedFile {
    media_type: Arc<str>,
    bytes: Arc<[u8]>,
}

impl std::fmt::Debug for ResolvedFile {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ResolvedFile(<redacted>)")
    }
}

impl ResolvedFile {
    // A singular File deliberately remains a distinct value type from attachment members.
    pub fn new(media_type: Arc<str>, bytes: Arc<[u8]>) -> Self {
        Self { media_type, bytes }
    }

    pub fn media_type(&self) -> &str {
        &self.media_type
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct ResolvedAttachment {
    media_type: Arc<str>,
    bytes: Arc<[u8]>,
    diagnostic_source_name: Option<Arc<str>>,
}

impl std::fmt::Debug for ResolvedAttachment {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ResolvedAttachment(<redacted>)")
    }
}

impl ResolvedAttachment {
    pub fn new(media_type: Arc<str>, bytes: Arc<[u8]>) -> Self {
        Self {
            media_type,
            bytes,
            diagnostic_source_name: None,
        }
    }

    pub fn with_diagnostic_source_name(mut self, name: Arc<str>) -> Self {
        self.diagnostic_source_name = Some(name);
        self
    }

    pub fn media_type(&self) -> &str {
        &self.media_type
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn diagnostic_source_name(&self) -> Option<&str> {
        self.diagnostic_source_name.as_deref()
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct ResolvedJsonInput {
    source: Arc<[u8]>,
    value: Arc<Value>,
    canonical: Arc<[u8]>,
}

#[derive(Debug)]
pub enum ResolvedJsonInputError {
    InvalidSource(serde_json::Error),
    Canonicalization(super::canonical_json::CanonicalJsonError),
}

impl fmt::Display for ResolvedJsonInputError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidSource(_) => "named JSON source is invalid",
            Self::Canonicalization(error) => match error {
                super::canonical_json::CanonicalJsonError::SizeLimitExceeded => {
                    "named JSON canonicalization exceeded its limit"
                }
                super::canonical_json::CanonicalJsonError::SerializationFailed => {
                    "named JSON canonicalization failed"
                }
            },
        })
    }
}

impl std::error::Error for ResolvedJsonInputError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidSource(error) => Some(error),
            Self::Canonicalization(_) => None,
        }
    }
}

impl ResolvedJsonInput {
    pub fn from_source(source: Arc<[u8]>) -> Result<Self, ResolvedJsonInputError> {
        let value = Arc::new(
            um_support::strict_json_from_slice(&source)
                .map_err(ResolvedJsonInputError::InvalidSource)?,
        );
        let canonical = super::canonical_json::to_bounded_bytes(&value, u64::MAX)
            .map_err(ResolvedJsonInputError::Canonicalization)?;
        Ok(Self {
            source,
            value,
            canonical,
        })
    }

    /// JSON `null`, the value an omitted optional JSON input receives.
    fn null() -> Self {
        Self {
            source: Arc::from(&b"null"[..]),
            value: Arc::new(Value::Null),
            canonical: Arc::from(&b"null"[..]),
        }
    }

    pub fn source(&self) -> &[u8] {
        &self.source
    }

    pub fn value(&self) -> &Value {
        &self.value
    }

    pub(crate) fn value_arc(&self) -> Arc<Value> {
        Arc::clone(&self.value)
    }

    pub fn canonical(&self) -> &[u8] {
        &self.canonical
    }
}

impl std::fmt::Debug for ResolvedJsonInput {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ResolvedJsonInput(<redacted>)")
    }
}

#[derive(Clone, Eq, PartialEq)]
pub enum ResolvedInput {
    Text(Arc<str>),
    Json(ResolvedJsonInput),
    File(ResolvedFile),
    Attachments(Arc<[ResolvedAttachment]>),
}

impl std::fmt::Debug for ResolvedInput {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ResolvedInput(<redacted>)")
    }
}

#[derive(Clone, Default, Eq, PartialEq)]
pub struct ResolvedInputs {
    values: BTreeMap<String, ResolvedInput>,
}

impl std::fmt::Debug for ResolvedInputs {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ResolvedInputs(<redacted>)")
    }
}

impl ResolvedInputs {
    pub fn new(values: BTreeMap<String, ResolvedInput>) -> Self {
        Self { values }
    }

    /// The inputs by name; a Cloud tree run hands each to `um workflow run create`.
    pub fn values(&self) -> &BTreeMap<String, ResolvedInput> {
        &self.values
    }

    pub fn get(&self, name: &str) -> Option<&ResolvedInput> {
        self.values.get(name)
    }

    /// Sets the input `name` to `value`, replacing any value it had: a successor's
    /// `--input-text` on top of the projected inputs (REQ-LOCALRUN-194).
    pub fn insert(&mut self, name: String, value: ResolvedInput) {
        self.values.insert(name, value);
    }
}

#[derive(Clone, Debug)]
pub struct CancellationPolicy {
    source: CancellationSource,
    grace: Duration,
}

impl CancellationPolicy {
    pub fn new(source: CancellationSource, grace: Duration) -> Self {
        Self { source, grace }
    }

    pub fn source(&self) -> &CancellationSource {
        &self.source
    }

    pub fn grace(&self) -> Duration {
        self.grace
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct EnvironmentSnapshot {
    variables: Arc<BTreeMap<OsString, OsString>>,
}

impl EnvironmentSnapshot {
    pub fn new<I, Name, Value>(variables: I) -> Self
    where
        I: IntoIterator<Item = (Name, Value)>,
        Name: Into<OsString>,
        Value: Into<OsString>,
    {
        Self {
            variables: Arc::new(
                variables
                    .into_iter()
                    .map(|(name, value)| (name.into(), value.into()))
                    .collect(),
            ),
        }
    }

    pub fn variables(&self) -> &BTreeMap<OsString, OsString> {
        &self.variables
    }

    pub fn variable(&self, name: &OsStr) -> Option<&OsStr> {
        self.variables.get(name).map(OsString::as_os_str)
    }

    pub(super) fn with_variable(&self, name: OsString, value: OsString) -> Self {
        let mut variables = self.variables.as_ref().clone();
        variables.insert(name, value);
        Self {
            variables: Arc::new(variables),
        }
    }

    fn for_workflow_children(&self, passthrough: &BTreeSet<String>) -> Self {
        self.without_variables_matching(|name| {
            is_managed_runner_private_environment_name(name)
                || !(is_base_workflow_environment_name(name)
                    || name.to_str().is_some_and(|name| passthrough.contains(name)))
        })
    }

    /// Removes the root-declared names a node's effective `environmentPassthrough` omits
    /// (INV-ENGINE-31). A fixed inherited name stays even when the root lists it, and an
    /// engine variable cannot be a passthrough name, so neither is ever removed.
    fn narrowed_for_node(&self, root: &BTreeSet<String>, kept: &BTreeSet<String>) -> Self {
        self.without_variables_matching(|name| {
            !is_base_workflow_environment_name(name)
                && name
                    .to_str()
                    .is_some_and(|name| root.contains(name) && !kept.contains(name))
        })
    }

    pub fn without_managed_runner_credentials_and_helpers(&self) -> Self {
        self.without_variables_matching(is_managed_runner_private_environment_name)
    }

    fn without_variables_matching(&self, excluded: impl Fn(&OsStr) -> bool) -> Self {
        Self {
            variables: Arc::new(
                self.variables
                    .iter()
                    .filter(|(name, _)| !excluded(name))
                    .map(|(name, value)| (name.clone(), value.clone()))
                    .collect(),
            ),
        }
    }
}

fn is_base_workflow_environment_name(name: &OsStr) -> bool {
    matches!(
        name.as_encoded_bytes(),
        b"PATH"
            | b"HOME"
            | b"USER"
            | b"LOGNAME"
            | b"LANG"
            | b"LC_ALL"
            | b"LC_CTYPE"
            | b"TERM"
            | b"TMPDIR"
    )
}

/// The grant token names (REQ-WFFORMAT-32): a workflow may declare them in
/// `environmentPassthrough` although `GH_TOKEN` is a GitHub credential name and the
/// Linear names start with `UM_`. On a runner they are set only from a grant, never
/// from the service's own environment (REQ-WORKSPACE-18).
pub fn grant_token_environment_names() -> [&'static str; 3] {
    [
        "GH_TOKEN",
        "UM_LINEAR_ACCESS_TOKEN",
        "UM_LINEAR_ACCESS_TOKEN_FILE",
    ]
}

fn is_grant_token_environment_name(name: &OsStr) -> bool {
    grant_token_environment_names()
        .iter()
        .any(|grant| name.as_encoded_bytes() == grant.as_bytes())
}

fn is_engine_reserved_environment_name(name: &OsStr) -> bool {
    (name.as_encoded_bytes().starts_with(b"UM_") && !is_grant_token_environment_name(name))
        || name.as_encoded_bytes() == b"SCHERZO_CONTINUATION_CONTEXT"
}

pub(super) fn is_managed_runner_private_environment_name(name: &OsStr) -> bool {
    if is_engine_reserved_environment_name(name) {
        return true;
    }
    let name = name.as_encoded_bytes();
    matches!(
        name,
        b"GIT_ASKPASS"
            | b"GIT_ASKPASS_REQUIRE"
            | b"GIT_TERMINAL_PROMPT"
            | b"GIT_CURL_VERBOSE"
            | b"GIT_CONFIG"
            | b"GIT_CONFIG_COUNT"
            | b"GIT_CONFIG_PARAMETERS"
            | b"GIT_CONFIG_GLOBAL"
            | b"GIT_CONFIG_NOSYSTEM"
            | b"GIT_CONFIG_SYSTEM"
            | b"GIT_SSH"
            | b"GIT_SSH_COMMAND"
            | b"SSH_ASKPASS"
            | b"SSH_ASKPASS_REQUIRE"
            | b"SSH_AUTH_SOCK"
            | b"SSH_AGENT_PID"
            | b"GITHUB_TOKEN"
    ) || name.starts_with(b"GIT_CONFIG_KEY_")
        || name.starts_with(b"GIT_CONFIG_VALUE_")
        || name.starts_with(b"GIT_TRACE")
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CaptureLimits {
    maximum_files: usize,
    maximum_file_bytes: u64,
    maximum_total_bytes: u64,
    maximum_git_carriers: usize,
    maximum_git_carrier_bytes: u64,
    maximum_total_git_carrier_bytes: u64,
}

impl CaptureLimits {
    pub(crate) fn new(
        maximum_files: usize,
        maximum_file_bytes: u64,
        maximum_total_bytes: u64,
    ) -> Self {
        Self {
            maximum_files,
            maximum_file_bytes,
            maximum_total_bytes,
            maximum_git_carriers: MAXIMUM_CAPTURED_GIT_CARRIERS,
            maximum_git_carrier_bytes: MAXIMUM_CAPTURED_GIT_CARRIER_BYTES,
            maximum_total_git_carrier_bytes: MAXIMUM_TOTAL_CAPTURED_GIT_CARRIER_BYTES,
        }
    }

    #[cfg(test)]
    pub(crate) fn with_git_carrier_limits(
        mut self,
        maximum_git_carriers: usize,
        maximum_git_carrier_bytes: u64,
        maximum_total_git_carrier_bytes: u64,
    ) -> Self {
        self.maximum_git_carriers = maximum_git_carriers;
        self.maximum_git_carrier_bytes = maximum_git_carrier_bytes;
        self.maximum_total_git_carrier_bytes = maximum_total_git_carrier_bytes;
        self
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct InputLimits {
    maximum_values: usize,
    maximum_value_bytes: u64,
    maximum_total_bytes: u64,
    maximum_live_bytes: u64,
}

impl InputLimits {
    pub(crate) fn new(
        maximum_values: usize,
        maximum_value_bytes: u64,
        maximum_total_bytes: u64,
        maximum_live_bytes: u64,
    ) -> Self {
        Self {
            maximum_values,
            maximum_value_bytes,
            maximum_total_bytes,
            maximum_live_bytes,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExecutionPolicyLimits {
    maximum_parallel_steps: usize,
    capture: CaptureLimits,
    input: InputLimits,
    maximum_step_log_bytes: u64,
}

impl ExecutionPolicyLimits {
    pub(crate) fn new(
        maximum_parallel_steps: usize,
        capture: CaptureLimits,
        input: InputLimits,
        maximum_step_log_bytes: u64,
    ) -> Self {
        Self {
            maximum_parallel_steps,
            capture,
            input,
            maximum_step_log_bytes,
        }
    }
}

pub fn default_execution_policy_limits(maximum_parallel_steps: usize) -> ExecutionPolicyLimits {
    ExecutionPolicyLimits::new(
        maximum_parallel_steps,
        CaptureLimits::new(
            MAXIMUM_CAPTURED_FILES,
            MAXIMUM_CAPTURED_FILE_BYTES,
            MAXIMUM_TOTAL_CAPTURED_BYTES,
        ),
        InputLimits::new(
            MAXIMUM_INPUT_VALUES,
            MAXIMUM_INPUT_VALUE_BYTES,
            MAXIMUM_TOTAL_INPUT_BYTES,
            MAXIMUM_LIVE_INPUT_BYTES,
        ),
        super::MAXIMUM_RETAINED_BYTES_PER_STREAM,
    )
}

#[derive(Clone, Debug)]
enum GitCaptureAdmission {
    None,
    Local,
    LocalBaseline(LocalGitBaseline),
    LocalBaselineUnavailable,
    Cloud(CloudGitCaptureProjection),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkflowCapacityBudget {
    pub maximum_invocations: u64,
    pub diagnostic_retention_bytes: u64,
    pub native_session_retention_bytes: u64,
    pub aggregate_retention_bytes: u64,
    pub encoded_outbox_bytes: u64,
}

impl WorkflowCapacityBudget {
    pub(crate) const fn supported_maximum() -> Self {
        Self {
            maximum_invocations: 488,
            diagnostic_retention_bytes: 134_217_728,
            native_session_retention_bytes: 67_108_864,
            aggregate_retention_bytes: 201_326_592,
            encoded_outbox_bytes: 1_024_720_896,
        }
    }

    #[cfg(test)]
    pub(crate) fn exact(capacity: &WorkflowCapacity) -> Self {
        let requirements = capacity.requirements;
        Self {
            maximum_invocations: requirements.maximum_invocations,
            diagnostic_retention_bytes: requirements.diagnostic_retention_bytes,
            native_session_retention_bytes: requirements.native_session_retention_bytes,
            aggregate_retention_bytes: requirements.aggregate_retention_bytes,
            encoded_outbox_bytes: requirements.encoded_outbox_bytes,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkflowExecutionContract {
    General,
    WorkflowV1CloudInputsArtifactsV1,
}

impl WorkflowExecutionContract {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::General => "workflow_v1_general@1",
            Self::WorkflowV1CloudInputsArtifactsV1 => "workflow_v1_cloud_inputs_artifacts@1",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdmittedWorkflowCapacity {
    pub resolved: WorkflowCapacity,
    pub execution_contract: WorkflowExecutionContract,
    pub maximum_transitions: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceRevisionProvenance {
    branch: Option<Arc<str>>,
    commit_oid: Arc<str>,
    definition: Option<(Arc<str>, Arc<str>)>,
}

impl SourceRevisionProvenance {
    pub fn new(branch: impl Into<Arc<str>>, commit_oid: impl Into<Arc<str>>) -> Self {
        Self {
            branch: Some(branch.into()),
            commit_oid: commit_oid.into(),
            definition: None,
        }
    }

    pub fn with_definition(
        mut self,
        branch: impl Into<Arc<str>>,
        commit_oid: impl Into<Arc<str>>,
    ) -> Self {
        self.definition = Some((branch.into(), commit_oid.into()));
        self
    }

    pub fn without_workspace_branch(mut self) -> Self {
        self.branch = None;
        self
    }
}

/// The variable that carries a Codex login to its harness: the runner offers a member's
/// login through it and the Codex adapter reads its home from it.
pub const CODEX_LOGIN_VARIABLE: &str = "CODEX_HOME";

/// The variable that carries a Claude Code login to its harness.
pub const CLAUDE_CODE_LOGIN_VARIABLE: &str = "CLAUDE_CODE_OAUTH_TOKEN";

/// How the harness of one agent invocation that received its login variable answered it
/// (INV-ENGINE-36).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HarnessLoginOutcome {
    /// The invocation succeeded.
    Accepted,
    /// The invocation failed with the harness error `unauthorized`.
    Refused,
}

/// Told, by the step runtime and never by an adapter, how a harness that received its
/// login variable fared at one agent invocation (INV-ENGINE-36). It receives the
/// variable's name only, never its value, and is called on the engine's executor, so it
/// must return without blocking.
pub trait HarnessLoginObserver: Send + Sync {
    fn observe(&self, variable: &str, outcome: HarnessLoginOutcome);
}

/// The execution context's shared harness-login observer. Its `Debug` names the role and
/// prints nothing of what the observer holds.
#[derive(Clone)]
struct SharedHarnessLoginObserver(Arc<dyn HarnessLoginObserver>);

impl fmt::Debug for SharedHarnessLoginObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("HarnessLoginObserver")
    }
}

#[derive(Clone, Debug)]
pub struct ExecutionContext {
    root: PathBuf,
    limits: ExecutionPolicyLimits,
    environment: EnvironmentSnapshot,
    withheld_agent_logins: BTreeSet<String>,
    harness_login_observer: Option<SharedHarnessLoginObserver>,
    cancellation: CancellationPolicy,
    source_revision: Option<SourceRevisionProvenance>,
    pi_installation: Option<ValidatedPiInstallation>,
    claude_code_installation: Option<ValidatedClaudeCodeInstallation>,
    codex_installation: Option<ValidatedCodexInstallation>,
    git_capture: GitCaptureAdmission,
    continuation_reexecuted: Option<BTreeSet<String>>,
    capacity_budget: WorkflowCapacityBudget,
    tree: Option<ValidatedTree>,
    conditions: BTreeSet<(String, String)>,
}

impl ExecutionContext {
    pub fn new(
        root: PathBuf,
        limits: ExecutionPolicyLimits,
        environment: EnvironmentSnapshot,
        cancellation: CancellationPolicy,
    ) -> Self {
        Self {
            root,
            limits,
            environment,
            withheld_agent_logins: BTreeSet::new(),
            harness_login_observer: None,
            cancellation,
            source_revision: None,
            pi_installation: None,
            claude_code_installation: None,
            codex_installation: None,
            git_capture: GitCaptureAdmission::None,
            continuation_reexecuted: None,
            capacity_budget: WorkflowCapacityBudget::supported_maximum(),
            tree: None,
            conditions: BTreeSet::new(),
        }
    }

    /// The login variables the admitting runner withheld beside `environment`: a coding
    /// agent's login it was selected to offer and could not, which fails an agent step that
    /// would have received it (INV-ENGINE-35). None unless given; a local run gives none.
    pub fn with_withheld_agent_logins<Name: Into<String>>(
        mut self,
        variables: impl IntoIterator<Item = Name>,
    ) -> Self {
        self.withheld_agent_logins = variables.into_iter().map(Into::into).collect();
        self
    }

    /// The observer the step runtime tells how a harness that received its login variable
    /// fared at each agent invocation (INV-ENGINE-36). None unless given; a local run gives
    /// none.
    pub fn with_harness_login_observer(mut self, observer: Arc<dyn HarnessLoginObserver>) -> Self {
        self.harness_login_observer = Some(SharedHarnessLoginObserver(observer));
        self
    }

    /// The `(source, event)` pairs the caller's condition sources evaluate, against which
    /// admission checks each `external` wait's rule (REQ-WFEXEC-59, REQ-WFEXEC-65); none
    /// unless given.
    pub fn with_conditions<'a>(
        mut self,
        supported: impl IntoIterator<Item = (&'a str, &'a str)>,
    ) -> Self {
        self.conditions = supported
            .into_iter()
            .map(|(source, event)| (source.to_owned(), event.to_owned()))
            .collect();
        self
    }

    /// Admission from a tree: probe its `requires.tools` and record it for the run copy
    /// (REQ-WFEXEC-53, REQ-WFEXEC-55).
    pub fn with_tree(mut self, tree: ValidatedTree) -> Self {
        self.tree = Some(tree);
        self
    }

    pub fn with_source_revision(mut self, source_revision: SourceRevisionProvenance) -> Self {
        self.source_revision = Some(source_revision);
        self
    }

    pub fn with_local_git_capture(mut self) -> Self {
        self.git_capture = GitCaptureAdmission::Local;
        self
    }

    pub fn with_local_git_baseline(mut self, baseline: Option<LocalGitBaseline>) -> Self {
        self.git_capture = baseline.map_or(
            GitCaptureAdmission::LocalBaselineUnavailable,
            GitCaptureAdmission::LocalBaseline,
        );
        self
    }

    pub fn with_cloud_git_capture(mut self, projection: CloudGitCaptureProjection) -> Self {
        self.git_capture = GitCaptureAdmission::Cloud(projection);
        self
    }

    /// Continuation alone can avoid re-admitting Git outputs that are inherited.
    /// The locked claim independently verifies the exact reexecution partition.
    pub fn with_continuation_reexecuted_steps(mut self, steps: &[String]) -> Self {
        self.continuation_reexecuted = Some(steps.iter().cloned().collect());
        self
    }

    pub fn with_capacity_budget(mut self, budget: WorkflowCapacityBudget) -> Self {
        self.capacity_budget = budget;
        self
    }

    pub fn with_pi_installation(mut self, installation: ValidatedPiInstallation) -> Self {
        self.pi_installation = Some(installation);
        self
    }

    pub fn with_claude_code_installation(
        mut self,
        installation: ValidatedClaudeCodeInstallation,
    ) -> Self {
        self.claude_code_installation = Some(installation);
        self
    }

    pub fn with_codex_installation(mut self, installation: ValidatedCodexInstallation) -> Self {
        self.codex_installation = Some(installation);
        self
    }

    /// One advisory per validated installation admitted as untested, in Pi, Claude Code,
    /// Codex order. The context holds one installation per selected harness.
    pub fn untested_harness_advisories(&self) -> Vec<UntestedHarnessAdvisory> {
        untested_harness_advisories(
            self.pi_installation.as_ref(),
            self.claude_code_installation.as_ref(),
            self.codex_installation.as_ref(),
        )
    }
}

/// The advisories of the installations a local run's context or a runner's configuration
/// keeps, in Pi, Claude Code, Codex order; an installation in its qualified range yields
/// none.
pub fn untested_harness_advisories(
    pi: Option<&ValidatedPiInstallation>,
    claude_code: Option<&ValidatedClaudeCodeInstallation>,
    codex: Option<&ValidatedCodexInstallation>,
) -> Vec<UntestedHarnessAdvisory> {
    [
        pi.and_then(ValidatedPiInstallation::untested_advisory),
        claude_code.and_then(ValidatedClaudeCodeInstallation::untested_advisory),
        codex.and_then(ValidatedCodexInstallation::untested_advisory),
    ]
    .into_iter()
    .flatten()
    .collect()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExecutionLimits {
    maximum_parallel_steps: NonZeroUsize,
    maximum_captured_files: NonZeroUsize,
    maximum_captured_file_bytes: NonZeroU64,
    maximum_total_captured_bytes: NonZeroU64,
    maximum_captured_git_carriers: NonZeroUsize,
    maximum_captured_git_carrier_bytes: NonZeroU64,
    maximum_total_captured_git_carrier_bytes: NonZeroU64,
    maximum_input_values: NonZeroUsize,
    maximum_input_value_bytes: NonZeroU64,
    maximum_total_input_bytes: NonZeroU64,
    maximum_live_input_bytes: NonZeroU64,
    maximum_step_log_bytes: NonZeroU64,
}

impl ExecutionLimits {
    pub fn maximum_parallel_steps(self) -> NonZeroUsize {
        self.maximum_parallel_steps
    }

    pub(crate) fn maximum_captured_files(self) -> NonZeroUsize {
        self.maximum_captured_files
    }

    pub(crate) fn maximum_captured_file_bytes(self) -> NonZeroU64 {
        self.maximum_captured_file_bytes
    }

    pub(crate) fn maximum_total_captured_bytes(self) -> NonZeroU64 {
        self.maximum_total_captured_bytes
    }

    pub(crate) fn maximum_captured_git_carriers(self) -> NonZeroUsize {
        self.maximum_captured_git_carriers
    }

    pub(crate) fn maximum_captured_git_carrier_bytes(self) -> NonZeroU64 {
        self.maximum_captured_git_carrier_bytes
    }

    pub(crate) fn maximum_total_captured_git_carrier_bytes(self) -> NonZeroU64 {
        self.maximum_total_captured_git_carrier_bytes
    }

    pub(crate) fn maximum_input_values(self) -> NonZeroUsize {
        self.maximum_input_values
    }

    pub(crate) fn maximum_input_value_bytes(self) -> NonZeroU64 {
        self.maximum_input_value_bytes
    }

    pub(crate) fn maximum_total_input_bytes(self) -> NonZeroU64 {
        self.maximum_total_input_bytes
    }

    pub(crate) fn maximum_live_input_bytes(self) -> NonZeroU64 {
        self.maximum_live_input_bytes
    }

    pub fn maximum_step_log_bytes(self) -> NonZeroU64 {
        self.maximum_step_log_bytes
    }
}

#[derive(Clone, Debug)]
pub struct AdmittedExecutionContext {
    root: AdmittedExecutionRoot,
    limits: ExecutionLimits,
    environment: EnvironmentSnapshot,
    withheld_agent_logins: BTreeSet<String>,
    harness_login_observer: Option<SharedHarnessLoginObserver>,
    cancellation: CancellationPolicy,
}

impl AdmittedExecutionContext {
    pub fn root(&self) -> &Path {
        self.root.provenance_path()
    }

    pub(super) fn root_identity(&self) -> &AdmittedExecutionRoot {
        &self.root
    }

    pub fn limits(&self) -> ExecutionLimits {
        self.limits
    }

    pub fn environment(&self) -> &EnvironmentSnapshot {
        &self.environment
    }

    pub fn cancellation(&self) -> &CancellationPolicy {
        &self.cancellation
    }

    pub(crate) fn harness_login_observer(&self) -> Option<&dyn HarnessLoginObserver> {
        self.harness_login_observer
            .as_ref()
            .map(|observer| observer.0.as_ref())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PiJsonV1Admission {
    installation: Arc<ValidatedPiInstallation>,
    configuration: PiConfig,
    limits: AgentInvocationLimits<PiJsonV1ProtocolLimits>,
}

impl PiJsonV1Admission {
    pub(crate) fn installation(&self) -> &ValidatedPiInstallation {
        &self.installation
    }

    pub(crate) fn configuration(&self) -> &PiConfig {
        &self.configuration
    }

    pub(crate) fn limits(&self) -> &AgentInvocationLimits<PiJsonV1ProtocolLimits> {
        &self.limits
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClaudeCodeStreamJsonV1Admission {
    installation: Arc<ValidatedClaudeCodeInstallation>,
    configuration: ClaudeCodeConfig,
    limits: AgentInvocationLimits<ClaudeCodeStreamJsonV1ProtocolLimits>,
}

impl ClaudeCodeStreamJsonV1Admission {
    pub(crate) fn installation(&self) -> &ValidatedClaudeCodeInstallation {
        &self.installation
    }

    pub(crate) fn configuration(&self) -> &ClaudeCodeConfig {
        &self.configuration
    }

    pub(crate) fn limits(&self) -> &AgentInvocationLimits<ClaudeCodeStreamJsonV1ProtocolLimits> {
        &self.limits
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CodexAppServerV1Admission {
    installation: Arc<ValidatedCodexInstallation>,
    configuration: CodexConfig,
    limits: AgentInvocationLimits<CodexAppServerV1ProtocolLimits>,
}

impl CodexAppServerV1Admission {
    pub(crate) fn installation(&self) -> &ValidatedCodexInstallation {
        &self.installation
    }

    pub(crate) fn configuration(&self) -> &CodexConfig {
        &self.configuration
    }

    pub(crate) fn limits(&self) -> &AgentInvocationLimits<CodexAppServerV1ProtocolLimits> {
        &self.limits
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdmittedHarness {
    Pi(PiJsonV1Admission),
    ClaudeCode(ClaudeCodeStreamJsonV1Admission),
    Codex(CodexAppServerV1Admission),
}

impl AdmittedHarness {
    pub(crate) const fn profile(&self) -> AgentCompatibilityProfile {
        match self {
            Self::Pi(_) => AgentCompatibilityProfile::PiJsonV1,
            Self::ClaudeCode(_) => AgentCompatibilityProfile::ClaudeCodeStreamJsonV1,
            Self::Codex(_) => AgentCompatibilityProfile::CodexAppServerV1,
        }
    }

    /// The variable that carries this harness's coding-agent login; Pi takes none.
    pub(crate) const fn login_variable(&self) -> Option<&'static str> {
        match self {
            Self::Pi(_) => None,
            Self::ClaudeCode(_) => Some(CLAUDE_CODE_LOGIN_VARIABLE),
            Self::Codex(_) => Some(CODEX_LOGIN_VARIABLE),
        }
    }

    pub(crate) fn maximum_attachments(&self) -> NonZeroUsize {
        match self {
            Self::Pi(admission) => admission.limits().maximum_attachments(),
            Self::ClaudeCode(admission) => admission.limits().maximum_attachments(),
            Self::Codex(admission) => admission.limits().maximum_attachments(),
        }
    }

    pub(crate) fn maximum_attachment_bytes(&self) -> NonZeroU64 {
        match self {
            Self::Pi(admission) => admission.limits().maximum_attachment_bytes(),
            Self::ClaudeCode(admission) => admission.limits().maximum_attachment_bytes(),
            Self::Codex(admission) => admission.limits().maximum_attachment_bytes(),
        }
    }
}

/// The tree a run was admitted from: what its run copy is written from and what
/// `AdmittedWorkflow::with_tree_copy` binds (REQ-WFEXEC-53).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdmittedTree {
    root: PathBuf,
    digest: WorkflowContentDigest,
    programs: Vec<String>,
    required_tools: Vec<String>,
    files: BTreeMap<String, TreeFile>,
}

impl AdmittedTree {
    fn new(tree: ValidatedTree) -> Self {
        Self {
            root: tree.root,
            digest: tree.digest,
            programs: tree.programs,
            required_tools: tree.required_tools,
            files: tree.files,
        }
    }

    /// The canonical source tree root admission validated and probed.
    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn digest(&self) -> &WorkflowContentDigest {
        &self.digest
    }

    /// The root's `programs` directories, tree-relative, in declared order.
    pub fn programs(&self) -> &[String] {
        &self.programs
    }

    pub fn required_tools(&self) -> &[String] {
        &self.required_tools
    }

    /// Every kept file by tree-relative `/` path, `workflow.yaml` included.
    pub fn files(&self) -> &BTreeMap<String, TreeFile> {
        &self.files
    }

    /// Each `programs` directory under `root`, in declared order.
    fn program_directories<'a>(&'a self, root: &'a Path) -> impl Iterator<Item = PathBuf> + 'a {
        self.programs
            .iter()
            .map(move |program| root.join(program.trim_end_matches('/')))
    }

    /// REQ-WFEXEC-55: each required tool is an executable regular file in a `programs`
    /// directory of the source tree or on the inherited `PATH`, looked up as a step looks
    /// up a bare program (REQ-WFEXEC-20); a relative `PATH` entry is taken from the
    /// execution root, a step's default working directory.
    fn probe_required_tools(
        &self,
        environment: &EnvironmentSnapshot,
        execution_root: &Path,
    ) -> Result<(), AdmissionFailure> {
        let directories = self
            .program_directories(&self.root)
            .chain(
                environment
                    .variable(OsStr::new("PATH"))
                    .into_iter()
                    .flat_map(env::split_paths),
            )
            .map(|directory| execution_root.join(directory))
            .collect::<Vec<_>>();
        match self.required_tools.iter().find(|tool| {
            !directories
                .iter()
                .any(|directory| is_executable_file(&directory.join(tool)))
        }) {
            Some(tool) => Err(AdmissionFailure::new(
                AdmissionFailureKind::RequiredToolUnavailable,
                AdmissionLocation::RequiredTool { name: tool.clone() },
            )),
            None => Ok(()),
        }
    }
}

#[derive(Clone, Debug)]
pub struct AdmittedWorkflow {
    workflow: Arc<ResolvedWorkflow>,
    inputs: ResolvedInputs,
    execution: AdmittedExecutionContext,
    agent_steps: Arc<BTreeMap<String, AdmittedHarness>>,
    recovery_handlers: Arc<BTreeMap<String, AdmittedHarness>>,
    capacity: AdmittedWorkflowCapacity,
    git_capture: Option<Arc<GitCaptureContext>>,
    tree: Option<Arc<AdmittedTree>>,
}

impl AdmittedWorkflow {
    pub fn workflow(&self) -> &ResolvedWorkflow {
        &self.workflow
    }

    pub fn inputs(&self) -> &ResolvedInputs {
        &self.inputs
    }

    pub fn execution(&self) -> &AdmittedExecutionContext {
        &self.execution
    }

    /// The admitted environment a step or finalizer process starts from, narrowed by the
    /// node's own `environmentPassthrough` when it declares one (REQ-WFEXEC-71).
    pub(crate) fn node_environment(&self, node: &str) -> EnvironmentSnapshot {
        self.environment_narrowed_to(self.node_passthrough(node))
    }

    /// Whether the admission withheld the login variable of `step`'s harness and the step's
    /// effective `environmentPassthrough` keeps it: the root names it and the step's own
    /// list, when it declares one, keeps it (INV-ENGINE-31). Such a step would have received
    /// the withheld login, so it must not start (INV-ENGINE-35). Only names are compared; no
    /// credential is read.
    pub(crate) fn withholds_agent_login(&self, step: &str) -> bool {
        self.withholds_login_kept_by(self.agent_step(step), self.node_passthrough(step))
    }

    /// The same check for the agent recovery handler of `step`, whose effective list is the
    /// one `recovery_handler_environment` narrows by (REQ-WFRECOVERY-64, INV-ENGINE-35).
    pub(crate) fn withholds_recovery_handler_login(&self, step: &str) -> bool {
        self.withholds_login_kept_by(
            self.recovery_handler(step),
            self.recovery_handler_passthrough(step),
        )
    }

    /// Whether `harness`'s login variable is withheld and an environment narrowed to `kept`
    /// would carry it, as `environment_narrowed_to` decides.
    fn withholds_login_kept_by(
        &self,
        harness: Option<&AdmittedHarness>,
        kept: Option<&BTreeSet<String>>,
    ) -> bool {
        let Some(variable) = harness.and_then(AdmittedHarness::login_variable) else {
            return false;
        };
        self.execution.withheld_agent_logins.contains(variable)
            && self
                .workflow
                .definition
                .environment_passthrough
                .contains(variable)
            && kept.is_none_or(|kept| kept.contains(variable))
    }

    /// The admitted environment the recovery handler of `step` starts from: narrowed by
    /// the handler's own `environmentPassthrough`, or its step's when it declares none
    /// (REQ-WFRECOVERY-64).
    pub(crate) fn recovery_handler_environment(&self, step: &str) -> EnvironmentSnapshot {
        self.environment_narrowed_to(self.recovery_handler_passthrough(step))
    }

    /// The `environmentPassthrough` a node declares, if any.
    fn node_passthrough(&self, node: &str) -> Option<&BTreeSet<String>> {
        self.workflow
            .definition
            .node(node)
            .and_then(|body| body.common().environment_passthrough.as_ref())
    }

    /// The effective `environmentPassthrough` of `step`'s recovery handler: its own list,
    /// or its step's when it declares none.
    fn recovery_handler_passthrough(&self, step: &str) -> Option<&BTreeSet<String>> {
        self.workflow
            .definition
            .recoveries
            .get(step)
            .and_then(Option::as_ref)
            .and_then(|recovery| recovery.handler.as_ref())
            .and_then(|handler| match handler {
                ValidatedRecoveryHandler::Command {
                    environment_passthrough,
                    ..
                }
                | ValidatedRecoveryHandler::Agent {
                    environment_passthrough,
                    ..
                } => environment_passthrough.as_ref(),
            })
            .or_else(|| self.node_passthrough(step))
    }

    fn environment_narrowed_to(&self, kept: Option<&BTreeSet<String>>) -> EnvironmentSnapshot {
        match kept {
            Some(kept) => self
                .execution
                .environment
                .narrowed_for_node(&self.workflow.definition.environment_passthrough, kept),
            None => self.execution.environment.clone(),
        }
    }

    /// Engine-only binding after the continuation claim, never from caller-supplied
    /// environment (which admission strips as an engine-reserved variable).
    pub(crate) fn with_continuation_context(mut self, path: &Path) -> Self {
        self.execution.environment = self
            .execution
            .environment
            .with_variable(
                "UM_CONTINUATION_CONTEXT".into(),
                path.as_os_str().to_owned(),
            )
            .with_variable(
                "SCHERZO_CONTINUATION_CONTEXT".into(),
                path.as_os_str().to_owned(),
            );
        self
    }

    /// The same admission observing `source` for cancellation and force abort: a child
    /// continuation admitted before its parent's attempt begins observes the cancellation of
    /// the step that dispatches it (REQ-WFEXEC-59, INV-ENGINE-27).
    pub fn with_cancellation_source(mut self, source: CancellationSource) -> Self {
        self.execution.cancellation =
            CancellationPolicy::new(source, self.execution.cancellation.grace());
        self
    }

    /// The tree this run was admitted from, when it was (REQ-LOCALRUN-169).
    pub fn tree(&self) -> Option<&AdmittedTree> {
        self.tree.as_deref()
    }

    /// Binds the run's copy of the admitted tree once it exists (REQ-WFEXEC-53): `PATH`
    /// is led by `<copy_root>/<program>` for each `programs` directory in declared order,
    /// ahead of the inherited value, and `UM_WORKFLOW_ROOT` names `copy_root`. Every
    /// command, recovery handler and agent child reads this environment. A run admitted
    /// without a tree is returned unchanged. A copy root that cannot be a `PATH` entry,
    /// one containing `:`, fails with the cause.
    pub fn with_tree_copy(mut self, copy_root: &Path) -> Result<Self, env::JoinPathsError> {
        let Some(tree) = self.tree.clone() else {
            return Ok(self);
        };
        let mut environment = self
            .execution
            .environment
            .with_variable("UM_WORKFLOW_ROOT".into(), copy_root.as_os_str().to_owned());
        if !tree.programs.is_empty() {
            let mut path = env::join_paths(tree.program_directories(copy_root))?;
            if let Some(inherited) = self.execution.environment.variable(OsStr::new("PATH")) {
                path.push(":");
                path.push(inherited);
            }
            environment = environment.with_variable("PATH".into(), path);
        }
        self.execution.environment = environment;
        Ok(self)
    }

    pub(crate) fn agent_step(&self, step: &str) -> Option<&AdmittedHarness> {
        self.agent_steps.get(step)
    }

    pub fn agent_steps(&self) -> &BTreeMap<String, AdmittedHarness> {
        &self.agent_steps
    }

    /// Whether execution needs the agent runtime: admission admitted a harness for an agent
    /// step, an agent finalizer or an agent recovery handler (INV-ENGINE-30). Hosts prepare
    /// agent staging, and the engine refuses `AgentExecution::Disabled`, by this predicate.
    pub fn requires_agent_runtime(&self) -> bool {
        !self.agent_steps().is_empty() || !self.recovery_handlers().is_empty()
    }

    pub(crate) fn recovery_handler(&self, step: &str) -> Option<&AdmittedHarness> {
        self.recovery_handlers.get(step)
    }

    pub(crate) fn recovery_handlers(&self) -> &BTreeMap<String, AdmittedHarness> {
        &self.recovery_handlers
    }

    pub fn capacity(&self) -> &AdmittedWorkflowCapacity {
        &self.capacity
    }

    #[cfg(test)]
    pub(crate) fn set_transition_ceiling(&mut self, maximum_transitions: u64) {
        self.capacity.maximum_transitions = maximum_transitions;
    }

    pub fn git_capture(&self) -> Option<&GitCaptureContext> {
        self.git_capture.as_deref()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdmissionFailureKind {
    MissingRequiredInput,
    UnexpectedInput,
    InputKindMismatch,
    InputSchemaMismatch,
    InvalidFileMediaType,
    InvalidAttachmentMediaType,
    InputMediaTypeMismatch,
    AgentStepRuntimeUnsupported,
    ExecutionRootUnavailable,
    ExecutionRootNotDirectory,
    GitContextRequired,
    GitContextUnavailable,
    GitContextNotRepository,
    GitContextExecutionRootMismatch,
    GitObjectFormatUnsupported,
    GitBaselineUnavailable,
    GitInitialWorkspaceDirty,
    GitWorkflowDigestMismatch,
    NonPositiveParallelism,
    NonPositiveCapturedFiles,
    NonPositiveCapturedFileBytes,
    NonPositiveTotalCapturedBytes,
    NonPositiveCapturedGitCarriers,
    NonPositiveCapturedGitCarrierBytes,
    NonPositiveTotalCapturedGitCarrierBytes,
    NonPositiveInputValues,
    NonPositiveInputValueBytes,
    NonPositiveTotalInputBytes,
    NonPositiveLiveInputBytes,
    NonPositiveStepLogBytes,
    CancellationGraceTooShort,
    CancellationGraceTooLong,
    CapacitySourceBindingMismatch,
    InvocationCapacityUnavailable,
    DiagnosticRetentionCapacityUnavailable,
    NativeSessionRetentionCapacityUnavailable,
    AggregateRetentionCapacityUnavailable,
    EncodedOutboxCapacityUnavailable,
    RequiredToolUnavailable,
    /// An edge of a `workflow` step targets a wait whose `then` is an exit, which this
    /// release does not run.
    WaitTargetUnsupported,
    /// An edge of a `workflow` step targets an `external` wait whose rule names a `source`
    /// and `event` pair no condition source of the caller evaluates (REQ-WFEXEC-65).
    WaitConditionUnsupported,
    /// A workflow with `workflow` steps was not admitted from a tree.
    WorkflowStepRequiresTree,
    /// Cloud runs no `workflow` step.
    WorkflowStepUnsupported,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum AdmissionLocation {
    Input { name: String },
    AttachmentInput { name: String, index: usize },
    Step { step: String },
    RecoveryHandler { step: String },
    ExecutionRoot,
    GitContext,
    MaximumParallelSteps,
    MaximumCapturedFiles,
    MaximumCapturedFileBytes,
    MaximumTotalCapturedBytes,
    MaximumCapturedGitCarriers,
    MaximumCapturedGitCarrierBytes,
    MaximumTotalCapturedGitCarrierBytes,
    MaximumInputValues,
    MaximumInputValueBytes,
    MaximumTotalInputBytes,
    MaximumLiveInputBytes,
    MaximumStepLogBytes,
    CancellationPolicy,
    CapacitySourceBinding,
    MaximumInvocations,
    DiagnosticRetention,
    NativeSessionRetention,
    AggregateRetention,
    EncodedOutbox,
    RequiredTool { name: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdmissionFailure {
    kind: AdmissionFailureKind,
    location: AdmissionLocation,
}

impl AdmissionFailureKind {
    pub const fn is_execution_root_failure(self) -> bool {
        matches!(
            self,
            Self::ExecutionRootUnavailable | Self::ExecutionRootNotDirectory
        )
    }

    pub const fn is_projected_execution_limit_failure(self) -> bool {
        matches!(
            self,
            Self::NonPositiveParallelism
                | Self::CancellationGraceTooShort
                | Self::CancellationGraceTooLong
        )
    }
}

impl AdmissionFailure {
    pub fn kind(&self) -> AdmissionFailureKind {
        self.kind
    }

    pub(crate) fn location(&self) -> &AdmissionLocation {
        &self.location
    }

    fn new(kind: AdmissionFailureKind, location: AdmissionLocation) -> Self {
        Self { kind, location }
    }
}

impl fmt::Display for AdmissionFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "workflow admission failure at {:?}: {:?}",
            self.location, self.kind
        )
    }
}

impl std::error::Error for AdmissionFailure {}

/// Admits a local run (REQ-WFEXEC-59): a workflow with `workflow` steps runs only from a
/// tree, since its children read the run's tree copy.
pub fn admit_local_workflow(
    workflow: ResolvedWorkflow,
    inputs: ResolvedInputs,
    context: ExecutionContext,
) -> Result<AdmittedWorkflow, AdmissionFailure> {
    if context.tree.is_none()
        && let Some(step) = first_workflow_step(&workflow)
    {
        return Err(AdmissionFailure::new(
            AdmissionFailureKind::WorkflowStepRequiresTree,
            AdmissionLocation::Step {
                step: step.to_owned(),
            },
        ));
    }
    admit_local_workflow_for(workflow, inputs, context)
}

fn first_workflow_step(workflow: &ResolvedWorkflow) -> Option<&str> {
    workflow
        .definition
        .source_order
        .iter()
        .find(|step| {
            matches!(
                workflow.definition.steps.get(step.as_str()),
                Some(ValidatedStep::Workflow(_))
            )
        })
        .map(String::as_str)
}

/// The refusals every admission applies to `workflow` steps (REQ-WFEXEC-59): Cloud runs
/// none, and no admission runs an edge whose target is a wait whose `then` is an exit, or
/// an `external` wait whose rule names a `source` and `event` pair
/// outside `conditions` (REQ-WFEXEC-65), at any depth of a wait's `then` or a limited
/// edge's `else`.
fn admit_workflow_steps(
    workflow: &ResolvedWorkflow,
    execution_contract: WorkflowExecutionContract,
    conditions: &BTreeSet<(String, String)>,
) -> Result<(), AdmissionFailure> {
    fn waits(target: &ValidatedEdgeTarget) -> bool {
        match target {
            // A local wait continues at a step: its published record's `then` is a step.
            ValidatedEdgeTarget::Wait { then, .. }
                if matches!(then.as_ref(), ValidatedEdgeTarget::Exit(_)) =>
            {
                true
            }
            ValidatedEdgeTarget::Wait { then, .. } => waits(then),
            ValidatedEdgeTarget::Limited { else_, .. } => waits(else_),
            ValidatedEdgeTarget::Step(_) | ValidatedEdgeTarget::Exit(_) => false,
        }
    }
    fn rules<'a>(target: &'a ValidatedEdgeTarget, found: &mut Vec<&'a WaitRule>) {
        match target {
            ValidatedEdgeTarget::Wait { then, rule, .. } => {
                found.extend(rule);
                rules(then, found);
            }
            ValidatedEdgeTarget::Limited { else_, .. } => rules(else_, found),
            ValidatedEdgeTarget::Step(_) | ValidatedEdgeTarget::Exit(_) => {}
        }
    }
    let unsupported = |target: &ValidatedEdgeTarget| {
        let mut found = Vec::new();
        rules(target, &mut found);
        found.iter().any(|rule| {
            rule.event
                .iter()
                .any(|event| !conditions.contains(&(rule.source.clone(), event.clone())))
        })
    };
    for step in &workflow.definition.source_order {
        let Some(ValidatedStep::Workflow(child)) = workflow.definition.steps.get(step.as_str())
        else {
            continue;
        };
        let location = || AdmissionLocation::Step { step: step.clone() };
        if execution_contract == WorkflowExecutionContract::WorkflowV1CloudInputsArtifactsV1 {
            return Err(AdmissionFailure::new(
                AdmissionFailureKind::WorkflowStepUnsupported,
                location(),
            ));
        }
        if child.edges.values().any(waits) {
            return Err(AdmissionFailure::new(
                AdmissionFailureKind::WaitTargetUnsupported,
                location(),
            ));
        }
        if child.edges.values().any(unsupported) {
            return Err(AdmissionFailure::new(
                AdmissionFailureKind::WaitConditionUnsupported,
                location(),
            ));
        }
    }
    Ok(())
}

/// Continuation discovery already reports missing harness installations separately.
/// Admit the independently checkable execution-root and Git context first so those
/// diagnostics are not hidden by an unavailable harness.
pub fn local_continuation_input_failures(
    workflow: &ResolvedWorkflow,
    inputs: &ResolvedInputs,
) -> Vec<AdmissionFailure> {
    collect_input_admission_failures(workflow, inputs)
}

/// Whether `supplied`, a child's mapped inputs, are the inputs its run retained, once
/// admission completes each omitted optional or defaulted input of `workflow` as it did for
/// the retained ones (REQ-WFFORMAT-84, REQ-CONTINUE-82): the same names, the same text and the
/// same canonical JSON.
pub fn completed_inputs_match(
    workflow: &ResolvedWorkflow,
    supplied: ResolvedInputs,
    retained: &ResolvedInputs,
) -> bool {
    let completed = complete_inputs(workflow, supplied);
    completed.values.len() == retained.values.len()
        && completed
            .values
            .iter()
            .all(|(name, value)| match (value, retained.get(name)) {
                (ResolvedInput::Text(supplied), Some(ResolvedInput::Text(retained))) => {
                    supplied == retained
                }
                (ResolvedInput::Json(supplied), Some(ResolvedInput::Json(retained))) => {
                    supplied.canonical() == retained.canonical()
                }
                _ => false,
            })
}

pub fn admit_local_continuation_workflow(
    workflow: ResolvedWorkflow,
    inputs: ResolvedInputs,
    context: ExecutionContext,
) -> Result<AdmittedWorkflow, Vec<AdmissionFailure>> {
    let mut failures = collect_input_admission_failures(&workflow, &inputs);
    let admission = admit_workflow_for(
        workflow,
        inputs,
        context,
        WorkflowExecutionContract::General,
        false,
        false,
    );
    match (admission, failures.is_empty()) {
        (Ok(admitted), true) => Ok(admitted),
        (Ok(_), false) => Err(failures),
        (Err(failure), _) => {
            failures.push(failure);
            Err(failures)
        }
    }
}

fn admit_local_workflow_for(
    workflow: ResolvedWorkflow,
    inputs: ResolvedInputs,
    context: ExecutionContext,
) -> Result<AdmittedWorkflow, AdmissionFailure> {
    admit_workflow_for(
        workflow,
        inputs,
        context,
        WorkflowExecutionContract::General,
        true,
        true,
    )
}

pub fn admit_runner_workflow(
    workflow: ResolvedWorkflow,
    inputs: ResolvedInputs,
    context: ExecutionContext,
) -> Result<AdmittedWorkflow, AdmissionFailure> {
    admit_workflow_for(
        workflow,
        inputs,
        context,
        WorkflowExecutionContract::WorkflowV1CloudInputsArtifactsV1,
        true,
        true,
    )
}

/// Admits a workflow as a local run does, without the local requirement that a workflow
/// with `workflow` steps comes from a tree.
pub fn admit_workflow(
    workflow: ResolvedWorkflow,
    inputs: ResolvedInputs,
    context: ExecutionContext,
) -> Result<AdmittedWorkflow, AdmissionFailure> {
    admit_local_workflow_for(workflow, inputs, context)
}

fn collect_input_admission_failures(
    workflow: &ResolvedWorkflow,
    inputs: &ResolvedInputs,
) -> Vec<AdmissionFailure> {
    let declared = workflow.declared_inputs();
    let mut failures = Vec::new();
    for name in declared
        .keys()
        .chain(inputs.values().keys())
        .collect::<BTreeSet<_>>()
    {
        let declaration = declared.get(name.as_str());
        let supplied = inputs.get(name);
        match (declaration, supplied) {
            (Some(_), None) if workflow.definition.input_may_be_omitted(name) => {}
            (Some(_), None) => failures.push(AdmissionFailure::new(
                AdmissionFailureKind::MissingRequiredInput,
                AdmissionLocation::Input { name: name.clone() },
            )),
            (None, Some(_)) => failures.push(AdmissionFailure::new(
                AdmissionFailureKind::UnexpectedInput,
                AdmissionLocation::Input { name: name.clone() },
            )),
            (Some(super::validated::WorkflowValueType::Text), Some(ResolvedInput::Text(_))) => {}
            (Some(super::validated::WorkflowValueType::Json), Some(ResolvedInput::Json(json))) => {
                if workflow
                    .input_json_schema(name)
                    .is_some_and(|schema| !schema.is_valid(json.value()))
                {
                    failures.push(AdmissionFailure::new(
                        AdmissionFailureKind::InputSchemaMismatch,
                        AdmissionLocation::Input { name: name.clone() },
                    ));
                }
            }
            (Some(super::validated::WorkflowValueType::File), Some(ResolvedInput::File(file))) => {
                if !super::is_valid_media_type(file.media_type()) {
                    failures.push(AdmissionFailure::new(
                        AdmissionFailureKind::InvalidFileMediaType,
                        AdmissionLocation::Input { name: name.clone() },
                    ));
                }
                if workflow
                    .definition
                    .input_file_media_types
                    .get(name)
                    .is_some_and(|required| required != file.media_type())
                {
                    failures.push(AdmissionFailure::new(
                        AdmissionFailureKind::InputMediaTypeMismatch,
                        AdmissionLocation::Input { name: name.clone() },
                    ));
                }
            }
            (
                Some(super::validated::WorkflowValueType::AttachmentCollection),
                Some(ResolvedInput::Attachments(attachments)),
            ) => {
                failures.extend(
                    attachments
                        .iter()
                        .enumerate()
                        .filter(|(_, attachment)| {
                            !super::is_valid_media_type(attachment.media_type())
                        })
                        .map(|(index, _)| {
                            AdmissionFailure::new(
                                AdmissionFailureKind::InvalidAttachmentMediaType,
                                AdmissionLocation::AttachmentInput {
                                    name: name.clone(),
                                    index,
                                },
                            )
                        }),
                );
            }
            (Some(_), Some(_)) => failures.push(AdmissionFailure::new(
                AdmissionFailureKind::InputKindMismatch,
                AdmissionLocation::Input { name: name.clone() },
            )),
            (None, None) => {}
        }
    }
    failures
}

/// Completes the supplied inputs with every omitted optional or defaulted one, after the
/// input check, so no schema applies to an engine-supplied empty value (REQ-WFFORMAT-84).
fn complete_inputs(workflow: &ResolvedWorkflow, inputs: ResolvedInputs) -> ResolvedInputs {
    let mut values = inputs.values;
    for (name, kind) in workflow.declared_inputs() {
        if values.contains_key(name) || !workflow.definition.input_may_be_omitted(name) {
            continue;
        }
        let value = if let Some(default) = workflow.json_input_default(name) {
            ResolvedInput::Json(default.clone())
        } else if let Some(super::validated::ValidatedInputDefault::Text(text)) =
            workflow.definition.input_defaults.get(name)
        {
            ResolvedInput::Text(Arc::from(text.as_str()))
        } else {
            match kind {
                super::validated::WorkflowValueType::Text => ResolvedInput::Text(Arc::from("")),
                super::validated::WorkflowValueType::Json => {
                    ResolvedInput::Json(ResolvedJsonInput::null())
                }
                super::validated::WorkflowValueType::AttachmentCollection => {
                    ResolvedInput::Attachments(Arc::from(Vec::new()))
                }
                // The format admits neither `optional` nor `default` on these kinds.
                super::validated::WorkflowValueType::File
                | super::validated::WorkflowValueType::GitBranch => continue,
            }
        };
        values.insert(name.clone(), value);
    }
    ResolvedInputs::new(values)
}

fn admit_workflow_for(
    workflow: ResolvedWorkflow,
    inputs: ResolvedInputs,
    context: ExecutionContext,
    execution_contract: WorkflowExecutionContract,
    admit_harnesses_first: bool,
    validate_inputs: bool,
) -> Result<AdmittedWorkflow, AdmissionFailure> {
    admit_workflow_steps(&workflow, execution_contract, &context.conditions)?;
    let capacity = admit_capacity(&workflow, context.capacity_budget, execution_contract)?;
    if validate_inputs
        && let Some(failure) = collect_input_admission_failures(&workflow, &inputs)
            .into_iter()
            .next()
    {
        return Err(failure);
    }
    let inputs = complete_inputs(&workflow, inputs);
    let admitted_harnesses = if admit_harnesses_first {
        let available_harnesses = AvailableHarnesses::new(
            context.pi_installation.as_ref(),
            context.claude_code_installation.as_ref(),
            context.codex_installation.as_ref(),
        );
        Some((
            admit_agent_steps(&workflow, &available_harnesses)?,
            admit_recovery_handlers(&workflow, &available_harnesses)?,
        ))
    } else {
        None
    };

    let maximum_parallel_steps = NonZeroUsize::new(context.limits.maximum_parallel_steps)
        .ok_or_else(|| {
            AdmissionFailure::new(
                AdmissionFailureKind::NonPositiveParallelism,
                AdmissionLocation::MaximumParallelSteps,
            )
        })?;
    let maximum_captured_files = NonZeroUsize::new(context.limits.capture.maximum_files)
        .ok_or_else(|| {
            AdmissionFailure::new(
                AdmissionFailureKind::NonPositiveCapturedFiles,
                AdmissionLocation::MaximumCapturedFiles,
            )
        })?;
    let maximum_captured_file_bytes = NonZeroU64::new(context.limits.capture.maximum_file_bytes)
        .ok_or_else(|| {
            AdmissionFailure::new(
                AdmissionFailureKind::NonPositiveCapturedFileBytes,
                AdmissionLocation::MaximumCapturedFileBytes,
            )
        })?;
    let maximum_total_captured_bytes = NonZeroU64::new(context.limits.capture.maximum_total_bytes)
        .ok_or_else(|| {
            AdmissionFailure::new(
                AdmissionFailureKind::NonPositiveTotalCapturedBytes,
                AdmissionLocation::MaximumTotalCapturedBytes,
            )
        })?;
    let maximum_captured_git_carriers =
        NonZeroUsize::new(context.limits.capture.maximum_git_carriers).ok_or_else(|| {
            AdmissionFailure::new(
                AdmissionFailureKind::NonPositiveCapturedGitCarriers,
                AdmissionLocation::MaximumCapturedGitCarriers,
            )
        })?;
    let maximum_captured_git_carrier_bytes =
        NonZeroU64::new(context.limits.capture.maximum_git_carrier_bytes).ok_or_else(|| {
            AdmissionFailure::new(
                AdmissionFailureKind::NonPositiveCapturedGitCarrierBytes,
                AdmissionLocation::MaximumCapturedGitCarrierBytes,
            )
        })?;
    let maximum_total_captured_git_carrier_bytes = NonZeroU64::new(
        context.limits.capture.maximum_total_git_carrier_bytes,
    )
    .ok_or_else(|| {
        AdmissionFailure::new(
            AdmissionFailureKind::NonPositiveTotalCapturedGitCarrierBytes,
            AdmissionLocation::MaximumTotalCapturedGitCarrierBytes,
        )
    })?;
    let maximum_input_values =
        NonZeroUsize::new(context.limits.input.maximum_values).ok_or_else(|| {
            AdmissionFailure::new(
                AdmissionFailureKind::NonPositiveInputValues,
                AdmissionLocation::MaximumInputValues,
            )
        })?;
    let maximum_input_value_bytes = NonZeroU64::new(context.limits.input.maximum_value_bytes)
        .ok_or_else(|| {
            AdmissionFailure::new(
                AdmissionFailureKind::NonPositiveInputValueBytes,
                AdmissionLocation::MaximumInputValueBytes,
            )
        })?;
    let maximum_total_input_bytes = NonZeroU64::new(context.limits.input.maximum_total_bytes)
        .ok_or_else(|| {
            AdmissionFailure::new(
                AdmissionFailureKind::NonPositiveTotalInputBytes,
                AdmissionLocation::MaximumTotalInputBytes,
            )
        })?;
    let maximum_live_input_bytes = NonZeroU64::new(context.limits.input.maximum_live_bytes)
        .ok_or_else(|| {
            AdmissionFailure::new(
                AdmissionFailureKind::NonPositiveLiveInputBytes,
                AdmissionLocation::MaximumLiveInputBytes,
            )
        })?;
    let configured_maximum_step_log_bytes = NonZeroU64::new(context.limits.maximum_step_log_bytes)
        .ok_or_else(|| {
            AdmissionFailure::new(
                AdmissionFailureKind::NonPositiveStepLogBytes,
                AdmissionLocation::MaximumStepLogBytes,
            )
        })?;
    let maximum_step_log_bytes = NonZeroU64::new(
        configured_maximum_step_log_bytes.get().min(
            capacity
                .resolved
                .requirements
                .maximum_retained_bytes_per_invocation,
        ),
    )
    .ok_or_else(|| {
        AdmissionFailure::new(
            AdmissionFailureKind::NonPositiveStepLogBytes,
            AdmissionLocation::MaximumStepLogBytes,
        )
    })?;
    if context.cancellation.grace() < MINIMUM_CANCELLATION_GRACE {
        return Err(AdmissionFailure::new(
            AdmissionFailureKind::CancellationGraceTooShort,
            AdmissionLocation::CancellationPolicy,
        ));
    }
    if context.cancellation.grace() > MAXIMUM_CANCELLATION_GRACE {
        return Err(AdmissionFailure::new(
            AdmissionFailureKind::CancellationGraceTooLong,
            AdmissionLocation::CancellationPolicy,
        ));
    }

    let root = canonical_execution_root(&context.root)?;
    let mut environment = context
        .environment
        .for_workflow_children(&workflow.definition.environment_passthrough);
    if let Some(source_revision) = context.source_revision {
        environment = environment.with_variable(
            OsString::from("UM_SOURCE_COMMIT_OID"),
            OsString::from(source_revision.commit_oid.as_ref()),
        );
        if let Some(branch) = source_revision.branch {
            environment = environment.with_variable(
                OsString::from("UM_SOURCE_BRANCH"),
                OsString::from(branch.as_ref()),
            );
        }
        if let Some((branch, commit_oid)) = source_revision.definition {
            environment = environment
                .with_variable(
                    OsString::from("UM_WORKFLOW_SOURCE_BRANCH"),
                    OsString::from(branch.as_ref()),
                )
                .with_variable(
                    OsString::from("UM_WORKFLOW_SOURCE_COMMIT_OID"),
                    OsString::from(commit_oid.as_ref()),
                );
        }
    }
    let tree = context.tree.map(AdmittedTree::new).map(Arc::new);
    if let Some(tree) = &tree {
        tree.probe_required_tools(&environment, root.provenance_path())?;
    }
    let execution = AdmittedExecutionContext {
        root,
        limits: ExecutionLimits {
            maximum_parallel_steps,
            maximum_captured_files,
            maximum_captured_file_bytes,
            maximum_total_captured_bytes,
            maximum_captured_git_carriers,
            maximum_captured_git_carrier_bytes,
            maximum_total_captured_git_carrier_bytes,
            maximum_input_values,
            maximum_input_value_bytes,
            maximum_total_input_bytes,
            maximum_live_input_bytes,
            maximum_step_log_bytes,
        },
        environment,
        withheld_agent_logins: context.withheld_agent_logins,
        harness_login_observer: context.harness_login_observer,
        cancellation: context.cancellation,
    };
    let needs_git_capture = context.continuation_reexecuted.as_ref().map_or_else(
        || workflow.requires_git_capture(),
        |steps| {
            workflow
                .definition
                .finalizers
                .values()
                .any(|node| has_git_output(&node.body))
                || steps.iter().any(|id| {
                    workflow
                        .definition
                        .steps
                        .get(id)
                        .is_some_and(has_git_output)
                })
        },
    );
    let git_capture = if needs_git_capture {
        if let GitCaptureAdmission::Cloud(projection) = &context.git_capture
            && projection.workflow_digest() != workflow.content_digest.value
        {
            return Err(AdmissionFailure::new(
                AdmissionFailureKind::GitWorkflowDigestMismatch,
                AdmissionLocation::GitContext,
            ));
        }
        let capture = match &context.git_capture {
            GitCaptureAdmission::None => {
                return Err(AdmissionFailure::new(
                    AdmissionFailureKind::GitContextRequired,
                    AdmissionLocation::GitContext,
                ));
            }
            GitCaptureAdmission::Local => {
                GitCaptureContext::admit_local(&execution, &CaptureCancellation::default())
            }
            GitCaptureAdmission::LocalBaseline(baseline) => {
                GitCaptureContext::admit_local_with_baseline(
                    &execution,
                    baseline,
                    &CaptureCancellation::default(),
                )
            }
            GitCaptureAdmission::LocalBaselineUnavailable => {
                return Err(AdmissionFailure::new(
                    AdmissionFailureKind::GitBaselineUnavailable,
                    AdmissionLocation::GitContext,
                ));
            }
            GitCaptureAdmission::Cloud(projection) => {
                GitCaptureContext::admit_cloud(&execution, projection)
            }
        }
        .map_err(git_admission_failure)?;
        Some(Arc::new(capture))
    } else {
        None
    };
    let (agent_steps, recovery_handlers) = match admitted_harnesses {
        Some(admitted) => admitted,
        None => {
            let available_harnesses = AvailableHarnesses::new(
                context.pi_installation.as_ref(),
                context.claude_code_installation.as_ref(),
                context.codex_installation.as_ref(),
            );
            (
                admit_agent_steps(&workflow, &available_harnesses)?,
                admit_recovery_handlers(&workflow, &available_harnesses)?,
            )
        }
    };
    Ok(AdmittedWorkflow {
        workflow: Arc::new(workflow),
        inputs,
        execution,
        agent_steps: Arc::new(agent_steps),
        recovery_handlers: Arc::new(recovery_handlers),
        capacity,
        git_capture,
        tree,
    })
}

fn admit_capacity(
    workflow: &ResolvedWorkflow,
    budget: WorkflowCapacityBudget,
    execution_contract: WorkflowExecutionContract,
) -> Result<AdmittedWorkflowCapacity, AdmissionFailure> {
    if !workflow.capacity_is_bound_to_source_closure() {
        return Err(AdmissionFailure::new(
            AdmissionFailureKind::CapacitySourceBindingMismatch,
            AdmissionLocation::CapacitySourceBinding,
        ));
    }
    let requirements = workflow.capacity.requirements;
    for (available, required, kind, location) in [
        (
            budget.maximum_invocations,
            requirements.maximum_invocations,
            AdmissionFailureKind::InvocationCapacityUnavailable,
            AdmissionLocation::MaximumInvocations,
        ),
        (
            budget.diagnostic_retention_bytes,
            requirements.diagnostic_retention_bytes,
            AdmissionFailureKind::DiagnosticRetentionCapacityUnavailable,
            AdmissionLocation::DiagnosticRetention,
        ),
        (
            budget.native_session_retention_bytes,
            requirements.native_session_retention_bytes,
            AdmissionFailureKind::NativeSessionRetentionCapacityUnavailable,
            AdmissionLocation::NativeSessionRetention,
        ),
        (
            budget.aggregate_retention_bytes,
            requirements.aggregate_retention_bytes,
            AdmissionFailureKind::AggregateRetentionCapacityUnavailable,
            AdmissionLocation::AggregateRetention,
        ),
    ] {
        if available < required {
            return Err(AdmissionFailure::new(kind, location));
        }
    }
    if execution_contract == WorkflowExecutionContract::WorkflowV1CloudInputsArtifactsV1
        && budget.encoded_outbox_bytes < requirements.encoded_outbox_bytes
    {
        return Err(AdmissionFailure::new(
            AdmissionFailureKind::EncodedOutboxCapacityUnavailable,
            AdmissionLocation::EncodedOutbox,
        ));
    }
    let maximum_transitions = match execution_contract {
        WorkflowExecutionContract::General => requirements.general_maximum_transitions,
        WorkflowExecutionContract::WorkflowV1CloudInputsArtifactsV1 => {
            requirements.cloud_maximum_transitions
        }
    };
    Ok(AdmittedWorkflowCapacity {
        resolved: workflow.capacity.clone(),
        execution_contract,
        maximum_transitions,
    })
}

pub(crate) fn has_git_output(step: &ValidatedStep) -> bool {
    step.common().outputs.values().any(|output| {
        matches!(
            output.definition,
            super::document::Output::GitBranchWorkspace
        )
    })
}

fn git_admission_failure(failure: GitWorkspaceAdmissionFailure) -> AdmissionFailure {
    let kind = match failure {
        GitWorkspaceAdmissionFailure::Cancelled
        | GitWorkspaceAdmissionFailure::GitUnavailable
        | GitWorkspaceAdmissionFailure::GitTimedOut
        | GitWorkspaceAdmissionFailure::GitOutputLimitExceeded => {
            AdmissionFailureKind::GitContextUnavailable
        }
        GitWorkspaceAdmissionFailure::NotWorkTree => AdmissionFailureKind::GitContextNotRepository,
        GitWorkspaceAdmissionFailure::ExecutionRootRebound
        | GitWorkspaceAdmissionFailure::ExecutionRootNotWorkTreeRoot => {
            AdmissionFailureKind::GitContextExecutionRootMismatch
        }
        GitWorkspaceAdmissionFailure::UnsupportedObjectFormat => {
            AdmissionFailureKind::GitObjectFormatUnsupported
        }
        GitWorkspaceAdmissionFailure::BaselineUnavailable => {
            AdmissionFailureKind::GitBaselineUnavailable
        }
        GitWorkspaceAdmissionFailure::InitialWorkspaceDirty => {
            AdmissionFailureKind::GitInitialWorkspaceDirty
        }
    };
    AdmissionFailure::new(kind, AdmissionLocation::GitContext)
}

struct AvailableHarnesses {
    pi: Option<Arc<ValidatedPiInstallation>>,
    claude_code: Option<Arc<ValidatedClaudeCodeInstallation>>,
    codex: Option<Arc<ValidatedCodexInstallation>>,
}

impl AvailableHarnesses {
    fn new(
        pi: Option<&ValidatedPiInstallation>,
        claude_code: Option<&ValidatedClaudeCodeInstallation>,
        codex: Option<&ValidatedCodexInstallation>,
    ) -> Self {
        Self {
            pi: pi.cloned().map(Arc::new),
            claude_code: claude_code.cloned().map(Arc::new),
            codex: codex.cloned().map(Arc::new),
        }
    }
}

fn admit_agent_steps(
    workflow: &ResolvedWorkflow,
    available: &AvailableHarnesses,
) -> Result<BTreeMap<String, AdmittedHarness>, AdmissionFailure> {
    let requests = workflow
        .definition
        .steps
        .iter()
        .chain(
            workflow
                .definition
                .finalizers
                .iter()
                .map(|(name, finalizer)| (name, &finalizer.body)),
        )
        .filter_map(|(step_name, step)| {
            let ValidatedStep::Agent(step) = step else {
                return None;
            };
            Some((
                step_name.clone(),
                &step.agent.harness,
                AdmissionLocation::Step {
                    step: step_name.clone(),
                },
            ))
        });
    admit_harness_requests(requests, available)
}

fn admit_recovery_handlers(
    workflow: &ResolvedWorkflow,
    available: &AvailableHarnesses,
) -> Result<BTreeMap<String, AdmittedHarness>, AdmissionFailure> {
    let requests = workflow
        .definition
        .recoveries
        .iter()
        .filter_map(|(step_name, recovery)| {
            let Some(super::validated::ValidatedStepRecovery {
                handler: Some(ValidatedRecoveryHandler::Agent { harness, .. }),
                ..
            }) = recovery
            else {
                return None;
            };
            Some((
                step_name.clone(),
                harness,
                AdmissionLocation::RecoveryHandler {
                    step: step_name.clone(),
                },
            ))
        });
    admit_harness_requests(requests, available)
}

fn admit_harness_requests<'a>(
    requests: impl IntoIterator<Item = (String, &'a ValidatedHarness, AdmissionLocation)>,
    available: &AvailableHarnesses,
) -> Result<BTreeMap<String, AdmittedHarness>, AdmissionFailure> {
    requests
        .into_iter()
        .map(|(name, harness, location)| {
            admit_harness(harness, available, location).map(|admitted| (name, admitted))
        })
        .collect()
}

fn admit_harness(
    harness: &ValidatedHarness,
    available: &AvailableHarnesses,
    location: AdmissionLocation,
) -> Result<AdmittedHarness, AdmissionFailure> {
    let missing_installation = || {
        AdmissionFailure::new(
            AdmissionFailureKind::AgentStepRuntimeUnsupported,
            location.clone(),
        )
    };
    match harness {
        ValidatedHarness::Pi(configuration) => {
            let installation = available.pi.as_ref().ok_or_else(missing_installation)?;
            Ok(AdmittedHarness::Pi(PiJsonV1Admission {
                installation: Arc::clone(installation),
                configuration: configuration.clone(),
                limits: pi_json_v1_limits(),
            }))
        }
        ValidatedHarness::ClaudeCode(configuration) => {
            let installation = available
                .claude_code
                .as_ref()
                .ok_or_else(missing_installation)?;
            Ok(AdmittedHarness::ClaudeCode(
                ClaudeCodeStreamJsonV1Admission {
                    installation: Arc::clone(installation),
                    configuration: configuration.clone(),
                    limits: claude_code_stream_json_v1_limits(),
                },
            ))
        }
        ValidatedHarness::Codex(configuration) => {
            let installation = available.codex.as_ref().ok_or_else(missing_installation)?;
            Ok(AdmittedHarness::Codex(CodexAppServerV1Admission {
                installation: Arc::clone(installation),
                configuration: configuration.clone(),
                limits: codex_app_server_v1_limits(),
            }))
        }
    }
}

fn pi_json_v1_limits() -> AgentInvocationLimits<PiJsonV1ProtocolLimits> {
    agent_invocation_limits(PiJsonV1ProtocolLimits::profile())
}

fn claude_code_stream_json_v1_limits() -> AgentInvocationLimits<ClaudeCodeStreamJsonV1ProtocolLimits>
{
    agent_invocation_limits(ClaudeCodeStreamJsonV1ProtocolLimits::profile())
}

fn codex_app_server_v1_limits() -> AgentInvocationLimits<CodexAppServerV1ProtocolLimits> {
    agent_invocation_limits(CodexAppServerV1ProtocolLimits::profile())
}

fn agent_invocation_limits<ProtocolLimits>(
    protocol: ProtocolLimits,
) -> AgentInvocationLimits<ProtocolLimits> {
    AgentInvocationLimits::new(
        positive_u64(MAXIMUM_AGENT_PROMPT_BYTES),
        positive_u64(MAXIMUM_AGENT_PROMPT_BYTES),
        positive_usize(MAXIMUM_AGENT_ATTACHMENTS),
        positive_u64(MAXIMUM_AGENT_ATTACHMENT_BYTES),
        positive_u64(MAXIMUM_AGENT_RESPONSE_BYTES),
        positive_u64(MAXIMUM_AGENT_RESULT_BYTES),
        positive_u64(MAXIMUM_AGENT_RESULT_REJECTION_FEEDBACK_BYTES),
        positive_duration(AGENT_RESULT_VALIDATION_DEADLINE),
        positive_duration(AGENT_RESULT_SETTLEMENT_GRACE),
        protocol,
    )
}

fn positive_u64(value: u64) -> NonZeroU64 {
    NonZeroU64::new(value).unwrap_or(NonZeroU64::MIN)
}

fn positive_usize(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or(NonZeroUsize::MIN)
}

fn positive_duration(value: Duration) -> PositiveDuration {
    PositiveDuration::new(value).unwrap_or(PositiveDuration::MIN)
}

fn canonical_execution_root(root: &Path) -> Result<AdmittedExecutionRoot, AdmissionFailure> {
    AdmittedExecutionRoot::admit(root).map_err(|failure| {
        let kind = match failure {
            ExecutionRootAdmissionFailure::Unavailable => {
                AdmissionFailureKind::ExecutionRootUnavailable
            }
            ExecutionRootAdmissionFailure::NotDirectory => {
                AdmissionFailureKind::ExecutionRootNotDirectory
            }
        };
        AdmissionFailure::new(kind, AdmissionLocation::ExecutionRoot)
    })
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod untested_advisory_tests {
    use std::path::PathBuf;
    use std::time::Duration;

    use super::{
        CancellationPolicy, CancellationSource, EnvironmentSnapshot, ExecutionContext,
        default_execution_policy_limits,
    };
    use crate::claude_code::ValidatedClaudeCodeInstallation;
    use crate::codex::ValidatedCodexInstallation;
    use crate::pi::ValidatedPiInstallation;

    fn context() -> ExecutionContext {
        ExecutionContext::new(
            PathBuf::from("/execution-root"),
            default_execution_policy_limits(1),
            EnvironmentSnapshot::new(std::iter::empty::<(String, String)>()),
            CancellationPolicy::new(CancellationSource::new(), Duration::from_secs(1)),
        )
    }

    // REQ-AGENT-66
    #[test]
    fn untested_harness_advisories_name_each_untested_installation_once() {
        let anchors = context()
            .with_pi_installation(ValidatedPiInstallation::fixture(PathBuf::from("/bin/pi")))
            .with_claude_code_installation(ValidatedClaudeCodeInstallation::fixture(PathBuf::from(
                "/bin/claude",
            )))
            .with_codex_installation(ValidatedCodexInstallation::fixture(PathBuf::from(
                "/bin/codex",
            )));
        assert_eq!(anchors.untested_harness_advisories(), []);

        let untested = context()
            .with_codex_installation(ValidatedCodexInstallation::fixture_with_version(
                PathBuf::from("/bin/codex"),
                (0, 164, 0),
                "0.164.0",
            ))
            .with_claude_code_installation(ValidatedClaudeCodeInstallation::fixture(PathBuf::from(
                "/bin/claude",
            )))
            .with_pi_installation(ValidatedPiInstallation::fixture_with_version(
                PathBuf::from("/bin/pi"),
                (1, 4, 0),
                "1.4.0",
            ));
        let advisories = untested.untested_harness_advisories();
        let named = advisories
            .iter()
            .map(|advisory| {
                (
                    advisory.harness(),
                    advisory.version(),
                    advisory.qualification_version(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            named,
            [("Pi", "1.4.0", "1.3.0"), ("Codex", "0.164.0", "0.163.0")]
        );
    }
}
