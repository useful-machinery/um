//! The wait record `wait.json` and the control events under `controls/` (REQ-WFDEF-5,
//! REQ-WFDEF-6, REQ-LOCALRUN-178): the shapes published as
//! `contracts/jsonschema/workflow-run-wait-v1.schema.json` and
//! `workflow-run-control-v1.schema.json`, the rule each local wait kind derives
//! (`derive_rule`, the only place a kind is named), the substitution of an `external` wait's
//! run-value operands, the matcher that resolves a wait through its rule alone over any
//! `Event` (`evaluate`), the `ConditionSource` an `external` wait's events come from, the
//! stop-in-force rule, and the numbered record directory's writer and reader, which the
//! controls directory and the mirror's `mirror/` share. `local_run` writes the wait record
//! into the run directory.

use std::collections::BTreeMap;
use std::fmt;
use std::os::fd::OwnedFd;
use std::path::{Path, PathBuf};

use rustix::fs::{
    AtFlags, FileType, FlockOperation, RenameFlags, flock, renameat_with, statat, unlinkat,
};
use rustix::io::Errno;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use super::admission::{ResolvedInput, ResolvedInputs};
pub use super::document::{WaitKind, WaitRule};
use super::local_run::{
    CONTROLS_DIRECTORY, LocalRunDirectoryError, PRIVATE_DIRECTORY, create_or_open_directory,
    directory_entries, encode_json, file_error, open_directory_at, open_run_root,
    read_regular_file, sync_directory, write_new_immutable_file,
};
use super::runtime::{EnteredWait, StepState, step_outputs};
use super::schema_common::{is_identifier, is_prefixed_ulid, parse_canonical_utc_timestamp};
use super::tree_copy::file_locator;
use super::value::CapturedValue;

/// The source every derived rule and every control event names.
pub(crate) const SOURCE: &str = "um";
const RUN_ID_PREFIX: &str = "run_";
const CONTROL_PREFIX: &str = "control.";
/// The control that answers an `answer` wait's questions (REQ-LOCALRUN-182).
pub const CONTROL_ANSWER: &str = "control.answer";
/// The control that approves an `approval` wait (REQ-LOCALRUN-183).
pub const CONTROL_APPROVE: &str = "control.approve";
/// The control that hands an `approval` wait back to its `onHandBack` step (REQ-LOCALRUN-183).
pub const CONTROL_HAND_BACK: &str = "control.hand_back";
/// The control that retries a `repair` wait (REQ-LOCALRUN-184).
pub const CONTROL_RETRY: &str = "control.retry";
/// The control that stops a run before a step (REQ-WFEXEC-62, REQ-LOCALRUN-185).
pub const CONTROL_STOP: &str = "control.stop";
/// The control that lifts the stop in force before a step (REQ-LOCALRUN-185).
pub const CONTROL_LIFT: &str = "control.lift";
/// The control that ends a waiting run instead of continuing it (REQ-CONTINUE-75).
pub const CONTROL_WITHDRAW: &str = "control.withdraw";
/// The control that cancels a run (REQ-LOCALRUN-186).
pub const CONTROL_CANCEL: &str = "control.cancel";
/// The control that releases an exited run (REQ-LOCALRUN-187).
pub const CONTROL_RELEASE: &str = "control.release";
/// The events of the published control schema (REQ-WFDEF-6).
const CONTROL_EVENTS: [&str; 9] = [
    CONTROL_ANSWER,
    CONTROL_APPROVE,
    CONTROL_HAND_BACK,
    CONTROL_RETRY,
    CONTROL_STOP,
    CONTROL_LIFT,
    CONTROL_WITHDRAW,
    CONTROL_CANCEL,
    CONTROL_RELEASE,
];
/// A derived rule's correlation: the control names this run and this wait's identity.
const WHERE_RUN_AND_WAIT: &str = "event.run == run.id and event.wait == wait.id";
/// An answer's correlation: the run, the wait, and an answer set with no question open
/// (REQ-WFEXEC-61, REQ-LOCALRUN-182).
const WHERE_RUN_WAIT_AND_COMPLETE: &str =
    "event.run == run.id and event.wait == wait.id and event.payload.complete == true";
/// A stop's correlation: the stop control exists before the wait does, so the lift names
/// the step the wait holds and the stop it lifts by that stop's `at` (REQ-WFEXEC-62).
const WHERE_RUN_STEP_AND_STOP: &str = "event.run == run.id and event.payload.before == wait.then \
     and event.payload.stop == wait.payload.at";
/// The highest ordinal a numbered record directory holds: `<NNNN>` has four digits.
const NUMBERED_ORDINAL_LIMIT: u64 = 9999;
const CONTROL_TEMPORARY_FILE: &str = "control.json.tmp";

/// One `wait.json` (REQ-WFDEF-5): what the run waits for, since when, at which step, where
/// it continues and the rule whose matching control resolves it.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WaitRecord {
    pub kind: WaitKind,
    /// A canonical UTC RFC 3339 timestamp; with `kind` it is the wait's identity.
    pub since: String,
    /// The waiting step's position: `[<step>]`, or `[<step>, {"item": n}]` for a fan-out
    /// waiting at its one-based element n.
    pub step: Vec<Value>,
    pub then: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_hand_back: Option<String>,
    pub rule: WaitRule,
    pub payload: Map<String, Value>,
}

/// One `controls/<NNNN>-<event>.json` (REQ-WFDEF-6).
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ControlEvent {
    pub source: String,
    pub event: String,
    pub run: String,
    pub by: String,
    pub at: String,
    /// The wait the control acts on as `<kind>@<since>`, present when the run waits on it
    /// whether or not the wait's rule resolves on this control.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wait: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<Map<String, Value>>,
}

/// An event a condition source observed for an `external` wait (REQ-WFEXEC-65): its
/// provider `source`, its `event` name, when it was observed and the facts it carries, which
/// a rule's `event.<path>` reads beside `source`, `event` and `at`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExternalEvent {
    pub source: String,
    pub event: String,
    pub at: String,
    pub payload: Map<String, Value>,
}

/// What a wait's rule matches (REQ-CONTINUE-74, INV-ENGINE-21): a control on source `um` or
/// an event a condition source observed, each read as JSON by `event.<path>`.
pub trait Event {
    fn source(&self) -> &str;
    fn event(&self) -> &str;
    /// The document `event.<path>` reads.
    fn as_json(&self) -> Value;
}

impl Event for ControlEvent {
    fn source(&self) -> &str {
        &self.source
    }

    fn event(&self) -> &str {
        &self.event
    }

    /// The control as published: `event.payload.<path>` reads its payload.
    fn as_json(&self) -> Value {
        serde_json::to_value(self).unwrap_or(Value::Null)
    }
}

impl Event for ExternalEvent {
    fn source(&self) -> &str {
        &self.source
    }

    fn event(&self) -> &str {
        &self.event
    }

    /// The payload's fields beside `source`, `event` and `at`, which win a clash, so
    /// `event.pullRequest` reads the payload's `pullRequest`.
    fn as_json(&self) -> Value {
        let mut document = self.payload.clone();
        document.insert("source".to_owned(), Value::String(self.source.clone()));
        document.insert("event".to_owned(), Value::String(self.event.clone()));
        document.insert("at".to_owned(), Value::String(self.at.clone()));
        Value::Object(document)
    }
}

/// A condition the CLI could not read for an `external` wait (REQ-CONTINUE-74): `code` is
/// `wait_condition_unavailable`, or `wait_condition_unsupported` for a `source` and `event`
/// no condition evaluates, and `message` names the cause.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConditionError {
    pub code: &'static str,
    pub message: String,
}

impl ConditionError {
    pub const UNAVAILABLE: &'static str = "wait_condition_unavailable";
    pub const UNSUPPORTED: &'static str = "wait_condition_unsupported";

    pub fn unavailable(message: impl Into<String>) -> Self {
        Self {
            code: Self::UNAVAILABLE,
            message: message.into(),
        }
    }
}

impl fmt::Display for ConditionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for ConditionError {}

/// Where an `external` wait's events come from (REQ-WFEXEC-65): the execution crate declares
/// it and the CLI implements it. `supports` answers whether a `source` and `event` pair is
/// evaluated, which admission asks before a run starts (REQ-WFEXEC-59); `observe` reads the
/// condition the wait's rule names now and answers the events it saw, which `evaluate`
/// matches against the rule as it matches controls.
pub trait ConditionSource {
    fn supports(&self, source: &str, event: &str) -> bool;
    fn observe(&self, wait: &WaitRecord) -> Result<Vec<ExternalEvent>, ConditionError>;
}

/// The condition source that evaluates nothing: it supports no pair and observes no event,
/// so an `external` wait holds. The control commands, which never resolve a wait, use it.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoConditions;

impl ConditionSource for NoConditions {
    fn supports(&self, _source: &str, _event: &str) -> bool {
        false
    }

    fn observe(&self, _wait: &WaitRecord) -> Result<Vec<ExternalEvent>, ConditionError> {
        Ok(Vec::new())
    }
}

/// A rule whose `where` is not a conjunction of the supported equalities; the CLI reports it
/// as `wait_rule_invalid` (REQ-CONTINUE-74).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuleError {
    clause: String,
}

impl RuleError {
    pub const CODE: &'static str = "wait_rule_invalid";

    /// The `where` clause that did not parse.
    pub fn clause(&self) -> &str {
        &self.clause
    }
}

impl fmt::Display for RuleError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}: `where` {:?} is not a conjunction of equalities over event.<path>, run.id, \
             wait.<field>, wait.payload.<path>, quoted literals, true and false",
            Self::CODE,
            self.clause
        )
    }
}

impl std::error::Error for RuleError {}

/// The rule a local wait kind derives (REQ-WFEXEC-61, REQ-WFEXEC-62): the controls that
/// resolve it on source `um`, correlated by the run and the wait's identity (for an answer
/// only once the answer set is complete), or for a stop by the step it holds and the stop's
/// `at`. An approval without a hand-back step has no `control.hand_back`.
/// `external` and `children` waits carry an authored rule and derive none.
pub(crate) fn derive_rule(kind: WaitKind, has_hand_back: bool) -> Option<WaitRule> {
    let (events, where_): (&[&str], _) = match kind {
        WaitKind::Answer => (&[CONTROL_ANSWER], WHERE_RUN_WAIT_AND_COMPLETE),
        WaitKind::Approval if has_hand_back => {
            (&[CONTROL_APPROVE, CONTROL_HAND_BACK], WHERE_RUN_AND_WAIT)
        }
        WaitKind::Approval => (&[CONTROL_APPROVE], WHERE_RUN_AND_WAIT),
        WaitKind::Repair => (&[CONTROL_RETRY, CONTROL_WITHDRAW], WHERE_RUN_AND_WAIT),
        WaitKind::Stop => (&[CONTROL_LIFT], WHERE_RUN_STEP_AND_STOP),
        WaitKind::External | WaitKind::Children => return None,
    };
    Some(WaitRule {
        source: SOURCE.to_owned(),
        event: events.iter().map(|event| (*event).to_owned()).collect(),
        where_: where_.to_owned(),
    })
}

impl WaitRecord {
    /// The record of the wait an attempt ended on (REQ-WFEXEC-61, REQ-WFEXEC-62,
    /// REQ-WFEXEC-65): the waiting step with `item`, the one-based fan-out element it waits
    /// at, its continuation targets, the rule, `payload` as given and `since` in canonical UTC. A local kind derives its rule; an `external` wait
    /// carries its authored rule with each `inputs.<name>` and `outputs.<step>.<output>`
    /// operand replaced by the quoted literal of its text value in `inputs` or `steps`. A
    /// kind without a rule, an operand whose value the run does not hold or that no quoted
    /// literal can spell, or a wait whose `then` is not a step (an exit) has no published
    /// record and is `WaitRecordInvalid`.
    pub fn entered(
        wait: &EnteredWait,
        item: Option<u64>,
        payload: Map<String, Value>,
        since: OffsetDateTime,
        inputs: &ResolvedInputs,
        steps: &BTreeMap<String, StepState<CapturedValue>>,
    ) -> Result<Self, LocalRunDirectoryError> {
        let rule = match (wait.kind, &wait.rule) {
            (WaitKind::External, Some(rule)) => WaitRule {
                where_: substitute_run_values(&rule.where_, inputs, steps)
                    .ok_or(LocalRunDirectoryError::WaitRecordInvalid)?,
                ..rule.clone()
            },
            (WaitKind::External, None) => return Err(LocalRunDirectoryError::WaitRecordInvalid),
            (kind, _) => derive_rule(kind, wait.on_hand_back.is_some())
                .ok_or(LocalRunDirectoryError::WaitRecordInvalid)?,
        };
        let since = super::schema_common::utc_timestamp(since)
            .map_err(|_| LocalRunDirectoryError::WaitRecordInvalid)?;
        Some(Self {
            kind: wait.kind,
            since,
            step: std::iter::once(Value::String(wait.step.clone()))
                .chain(item.map(super::run_record::item_position_element))
                .collect(),
            then: wait.then.clone(),
            on_hand_back: wait.on_hand_back.clone(),
            rule,
            payload,
        })
        .filter(Self::is_valid)
        .ok_or(LocalRunDirectoryError::WaitRecordInvalid)
    }

    /// The wait's identity `<kind>@<since>`, which a control names in its `wait`.
    pub fn id(&self) -> String {
        format!("{}@{}", self.kind.name(), self.since)
    }

    /// The kind's name as the record spells it.
    pub fn kind_name(&self) -> &'static str {
        self.kind.name()
    }

    /// The waiting step's position, written as `position_path` writes it.
    pub fn step_path(&self) -> String {
        position_path(&self.step)
    }

    /// The facts this crate writes and relies on: a canonical `since`, a step position that
    /// starts with a step identifier and holds identifiers and fan-out elements of at least
    /// 1, step identifiers for `then` and `onHandBack`, and a rule with a source, distinct
    /// event names and a `where`.
    pub fn is_valid(&self) -> bool {
        parse_canonical_utc_timestamp(&self.since).is_some()
            && self
                .step
                .first()
                .and_then(Value::as_str)
                .is_some_and(is_identifier)
            && self.step.iter().all(super::run_record::is_position_element)
            && is_identifier(&self.then)
            && self.on_hand_back.as_deref().is_none_or(is_identifier)
            && !self.rule.source.is_empty()
            && !self.rule.event.is_empty()
            && self.rule.event.iter().all(|event| is_event_name(event))
            && self
                .rule
                .event
                .iter()
                .enumerate()
                .all(|(index, event)| !self.rule.event[..index].contains(event))
            && !self.rule.where_.is_empty()
    }
}

/// `^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+$`, the schema's event name.
fn is_event_name(event: &str) -> bool {
    let mut segments = 0;
    event.split('.').all(|segment| {
        segments += 1;
        segment.starts_with(|first: char| first.is_ascii_lowercase())
            && segment
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    }) && segments >= 2
}

impl ControlEvent {
    /// A control `event` on source `um` for `run` by `by`, stamped `at` the current UTC time,
    /// naming `wait` and carrying `payload` when given. An event outside the published schema
    /// is `ControlEventInvalid`.
    pub fn new(
        event: &str,
        run: &str,
        by: &str,
        wait: Option<String>,
        payload: Option<Map<String, Value>>,
    ) -> Result<Self, LocalRunDirectoryError> {
        let at = super::schema_common::utc_timestamp(um_support::utc_now())
            .map_err(|_| LocalRunDirectoryError::ControlEventInvalid)?;
        Some(Self {
            source: SOURCE.to_owned(),
            event: event.to_owned(),
            run: run.to_owned(),
            by: by.to_owned(),
            at,
            wait,
            payload,
        })
        .filter(Self::is_valid)
        .ok_or(LocalRunDirectoryError::ControlEventInvalid)
    }

    /// The facts of the published schema: source `um`, a named control event, a `run_` id,
    /// a non-empty `by`, an RFC 3339 `at`, and a `wait` of `<kind>@<RFC 3339 timestamp>`.
    pub fn is_valid(&self) -> bool {
        self.source == SOURCE
            && CONTROL_EVENTS.contains(&self.event.as_str())
            && is_prefixed_ulid(&self.run, RUN_ID_PREFIX)
            && !self.by.is_empty()
            && OffsetDateTime::parse(&self.at, &Rfc3339).is_ok()
            && self.wait.as_deref().is_none_or(|wait| {
                wait.split_once('@').is_some_and(|(kind, since)| {
                    serde_json::from_value::<WaitKind>(Value::String(kind.to_owned())).is_ok()
                        && OffsetDateTime::parse(since, &Rfc3339).is_ok()
                })
            })
    }

    /// The event name after `control.`, the suffix of the control's file name.
    fn name(&self) -> Option<&str> {
        self.event.strip_prefix(CONTROL_PREFIX)
    }

    fn before(&self) -> Option<&str> {
        self.payload.as_ref()?.get("before")?.as_str()
    }
}

/// One side of a `where` equality.
#[derive(Debug)]
pub(crate) enum Operand {
    /// `event.<dotted path>`, read from the event's serialized JSON.
    Event(Vec<String>),
    RunId,
    Wait(WaitField),
    Literal(String),
    /// The bare `true` or `false`.
    Bool(bool),
    /// `inputs.<name>`, a `text` input of the workflow an authored rule names
    /// (REQ-WFFORMAT-97); the matcher never resolves it.
    Inputs(String),
    /// `outputs.<step>.<output>`, a `text` export of an earlier `workflow` step an authored
    /// rule names (REQ-WFFORMAT-97); the matcher never resolves it.
    Outputs {
        step: String,
        output: String,
    },
}

#[derive(Debug)]
pub(crate) enum WaitField {
    Id,
    Kind,
    Since,
    Then,
    Step,
    /// `wait.payload.<dotted path>`, read from the record's payload.
    Payload(Vec<String>),
}

/// A dotted path of non-empty ASCII alphanumeric or `_` segments.
fn parse_path(path: &str) -> Option<Vec<String>> {
    path.split('.')
        .map(|segment| {
            (!segment.is_empty()
                && segment
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_'))
            .then(|| segment.to_owned())
        })
        .collect()
}

fn parse_operand(text: &str) -> Option<Operand> {
    if let Some(literal) = text
        .strip_prefix('"')
        .and_then(|rest| rest.strip_suffix('"'))
    {
        return (!literal.contains(['"', '\\'])).then(|| Operand::Literal(literal.to_owned()));
    }
    match text {
        "true" => return Some(Operand::Bool(true)),
        "false" => return Some(Operand::Bool(false)),
        "run.id" => return Some(Operand::RunId),
        _ => {}
    }
    if let Some(name) = text.strip_prefix("inputs.") {
        return match parse_path(name)?.as_slice() {
            [name] => Some(Operand::Inputs(name.clone())),
            _ => None,
        };
    }
    if let Some(path) = text.strip_prefix("outputs.") {
        return match parse_path(path)?.as_slice() {
            [step, output] => Some(Operand::Outputs {
                step: step.clone(),
                output: output.clone(),
            }),
            _ => None,
        };
    }
    if let Some(path) = text.strip_prefix("wait.payload.") {
        return parse_path(path).map(|path| Operand::Wait(WaitField::Payload(path)));
    }
    if let Some(field) = text.strip_prefix("wait.") {
        return Some(Operand::Wait(match field {
            "id" => WaitField::Id,
            "kind" => WaitField::Kind,
            "since" => WaitField::Since,
            "then" => WaitField::Then,
            "step" => WaitField::Step,
            _ => return None,
        }));
    }
    parse_path(text.strip_prefix("event.")?).map(Operand::Event)
}

/// `where` as a conjunction of `lhs == rhs`, split on ` and `.
pub(crate) fn parse_where(clause: &str) -> Result<Vec<(Operand, Operand)>, RuleError> {
    clause
        .split(" and ")
        .map(|conjunct| {
            let (left, right) = conjunct.split_once(" == ")?;
            Some((parse_operand(left)?, parse_operand(right)?))
        })
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| RuleError {
            clause: clause.to_owned(),
        })
}

/// `where` with each `inputs.<name>` and `outputs.<step>.<output>` operand replaced by the
/// quoted literal of its text value (REQ-WFEXEC-65), or `None` when the clause is outside the
/// grammar, an operand's value is absent or not text, or a value cannot be spelled as a
/// quoted literal (it holds a `"` or `\`, or would split the clause).
fn substitute_run_values(
    clause: &str,
    inputs: &ResolvedInputs,
    steps: &BTreeMap<String, StepState<CapturedValue>>,
) -> Option<String> {
    let literal = |text: &str| {
        (!text.contains(['"', '\\']) && !text.contains(" and ") && !text.contains(" == "))
            .then(|| format!("\"{text}\""))
    };
    let side = |text: &str| match parse_operand(text)? {
        Operand::Inputs(name) => match inputs.get(&name)? {
            ResolvedInput::Text(value) => literal(value),
            ResolvedInput::Json(_) | ResolvedInput::File(_) | ResolvedInput::Attachments(_) => None,
        },
        Operand::Outputs { step, output } => {
            match steps.get(&step).and_then(step_outputs)?.get(&output)? {
                CapturedValue::Text(value) => literal(value.as_str()),
                CapturedValue::Json(_) | CapturedValue::File(_) | CapturedValue::GitBranch(_) => {
                    None
                }
            }
        }
        _ => Some(text.to_owned()),
    };
    let substituted = clause
        .split(" and ")
        .map(|conjunct| {
            let (left, right) = conjunct.split_once(" == ")?;
            Some(format!("{} == {}", side(left)?, side(right)?))
        })
        .collect::<Option<Vec<_>>>()?
        .join(" and ");
    let conjuncts = parse_where(&substituted).ok()?;
    conjuncts
        .iter()
        .flat_map(|(left, right)| [left, right])
        .all(|operand| !matches!(operand, Operand::Inputs(_) | Operand::Outputs { .. }))
        .then_some(substituted)
}

fn resolve(operand: &Operand, run_id: &str, wait: &WaitRecord, event: &Value) -> Option<Value> {
    match operand {
        Operand::Event(path) => path
            .iter()
            .try_fold(event, |value, segment| value.get(segment))
            .cloned(),
        Operand::RunId => Some(Value::String(run_id.to_owned())),
        Operand::Wait(WaitField::Id) => Some(Value::String(wait.id())),
        Operand::Wait(WaitField::Kind) => Some(Value::String(wait.kind.name().to_owned())),
        Operand::Wait(WaitField::Since) => Some(Value::String(wait.since.clone())),
        Operand::Wait(WaitField::Then) => Some(Value::String(wait.then.clone())),
        Operand::Wait(WaitField::Step) => match wait.step.as_slice() {
            [step] => Some(step.clone()),
            steps => Some(Value::Array(steps.to_vec())),
        },
        Operand::Wait(WaitField::Payload(path)) => {
            let (first, rest) = path.split_first()?;
            rest.iter()
                .try_fold(wait.payload.get(first)?, |value, segment| {
                    value.get(segment)
                })
                .cloned()
        }
        Operand::Literal(literal) => Some(Value::String(literal.clone())),
        Operand::Bool(literal) => Some(Value::Bool(*literal)),
        // The wait record carries these resolved to literals; one left in a rule is unbound.
        Operand::Inputs(_) | Operand::Outputs { .. } => None,
    }
}

/// A wait record's payload (REQ-WFEXEC-61, REQ-WFEXEC-62): for a `stop` wait the stop
/// control's `by`, `at` and `payload`; otherwise the waiting step's committed outputs, its
/// `outcome` and each `text` or `json` value, and `child`, the child run's directory
/// relative to the run directory, when one ran.
pub fn wait_payload(
    wait: &EnteredWait,
    steps: &BTreeMap<String, StepState<CapturedValue>>,
    child: Option<&str>,
    stop: Option<&ControlEvent>,
) -> Map<String, Value> {
    let mut payload = Map::new();
    if wait.kind == WaitKind::Stop {
        if let Some(stop) = stop {
            payload.insert("by".to_owned(), Value::String(stop.by.clone()));
            payload.insert("at".to_owned(), Value::String(stop.at.clone()));
            if let Some(control) = &stop.payload {
                payload.insert("payload".to_owned(), Value::Object(control.clone()));
            }
        }
        return payload;
    }
    for (name, value) in steps
        .get(wait.step())
        .and_then(step_outputs)
        .into_iter()
        .flatten()
    {
        match value {
            CapturedValue::Text(text) => {
                payload.insert(name.clone(), Value::String(text.as_str().to_owned()));
            }
            CapturedValue::Json(json) => {
                payload.insert(name.clone(), json.value().clone());
            }
            CapturedValue::File(_) | CapturedValue::GitBranch(_) => {}
        }
    }
    if let Some(child) = child {
        payload.insert("child".to_owned(), Value::String(child.to_owned()));
    }
    payload
}

/// The first event, in order, that resolves the wait through `rule` (REQ-CONTINUE-74,
/// INV-ENGINE-21): a control or an observed external event whose `source` is the rule's,
/// whose `event` is one the rule names, and for which every equality of `where` holds, with
/// `event.<path>` read from the event's JSON, `run.id` the run's id, `wait.*` the record's
/// fields, `wait.payload.<path>` read from its payload and `true` and `false` JSON booleans.
/// A side that does not resolve never equals anything. A `where` outside the grammar is
/// `RuleError` whatever the events are.
pub fn evaluate<'a, E: Event>(
    rule: &WaitRule,
    run_id: &str,
    wait: &WaitRecord,
    events: &'a [E],
) -> Result<Option<&'a E>, RuleError> {
    let conjuncts = parse_where(&rule.where_)?;
    Ok(events.iter().find(|candidate| {
        candidate.source() == rule.source
            && rule.event.iter().any(|event| event == candidate.event())
            && {
                let event = candidate.as_json();
                conjuncts.iter().all(|(left, right)| {
                    matches!(
                        (
                            resolve(left, run_id, wait, &event),
                            resolve(right, run_id, wait, &event),
                        ),
                        (Some(left), Some(right)) if left == right
                    )
                })
            }
    }))
}

/// The quoted literal a conjunct of `clause` equates with `event.<path>`, on either side, or
/// `None` when no conjunct does; a condition source reads what to observe from it
/// (REQ-WFEXEC-65). A clause outside the grammar is `RuleError`.
pub fn event_literal(clause: &str, path: &[&str]) -> Result<Option<String>, RuleError> {
    Ok(parse_where(clause)?
        .into_iter()
        .find_map(|conjunct| match conjunct {
            (Operand::Event(event), Operand::Literal(literal))
            | (Operand::Literal(literal), Operand::Event(event))
                if event.iter().map(String::as_str).eq(path.iter().copied()) =>
            {
                Some(literal)
            }
            _ => None,
        }))
}

/// The `control.stop` in force before `step` (REQ-WFEXEC-62, INV-ENGINE-21): the last
/// `control.stop` or `control.lift` whose `payload.before` names the step, when it is a stop.
pub fn stop_in_force<'a>(events: &'a [ControlEvent], step: &str) -> Option<&'a ControlEvent> {
    events
        .iter()
        .rfind(|control| {
            matches!(control.event.as_str(), CONTROL_STOP | CONTROL_LIFT)
                && control.before() == Some(step)
        })
        .filter(|control| control.event == CONTROL_STOP)
}

/// `<NNNN>-<entry>.json` as its ordinal and entry name.
fn parse_numbered_file_name(name: &[u8]) -> Option<(u64, &str)> {
    let name = std::str::from_utf8(name).ok()?;
    let (ordinal, rest) = name.split_once('-')?;
    let suffix = rest.strip_suffix(".json")?;
    (ordinal.len() == 4
        && ordinal.bytes().all(|byte| byte.is_ascii_digit())
        && !suffix.is_empty()
        && suffix
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte == b'_'))
    .then_some(())?;
    let ordinal = ordinal
        .parse::<u64>()
        .ok()
        .filter(|ordinal| *ordinal >= 1)?;
    Some((ordinal, suffix))
}

/// A directory of a run root that holds one record per file `<NNNN>-<entry>.json`, appended
/// whole and never rewritten: `controls/` (REQ-LOCALRUN-178) and `mirror/`
/// (REQ-LOCALRUN-193). `decode` answers the record a file holds when it is one for its
/// entry name, and `invalid` is the failure a file that is not one reads as.
pub(crate) struct NumberedRecords<T> {
    directory: &'static str,
    temporary: &'static str,
    invalid: fn() -> LocalRunDirectoryError,
    decode: fn(&str, &[u8]) -> Option<T>,
}

impl<T> NumberedRecords<T> {
    pub(crate) const fn new(
        directory: &'static str,
        temporary: &'static str,
        invalid: fn() -> LocalRunDirectoryError,
        decode: fn(&str, &[u8]) -> Option<T>,
    ) -> Self {
        Self {
            directory,
            temporary,
            invalid,
            decode,
        }
    }

    fn invalid_at(&self, records: &OwnedFd, name: &[u8]) -> LocalRunDirectoryError {
        LocalRunDirectoryError::StateFile {
            path: file_locator(
                records,
                std::ffi::OsStr::new(&*String::from_utf8_lossy(name)),
            ),
            operation: "validate",
            source: Box::new((self.invalid)()),
        }
    }

    /// Every record under `records`, in ordinal order, refusing an entry that is not a
    /// record for its name, and a repeated ordinal.
    fn read_open(&self, records: &OwnedFd) -> Result<Vec<(u64, T)>, LocalRunDirectoryError> {
        let mut entries = directory_entries(records)?
            .iter()
            .map(|name| {
                let (ordinal, suffix) =
                    parse_numbered_file_name(name).ok_or_else(|| self.invalid_at(records, name))?;
                let file = std::str::from_utf8(name).map_err(|_| self.invalid_at(records, name))?;
                let bytes = read_regular_file(records, file)?;
                (self.decode)(suffix, &bytes)
                    .map(|record| (ordinal, record))
                    .ok_or_else(|| self.invalid_at(records, name))
            })
            .collect::<Result<Vec<_>, _>>()?;
        entries.sort_by_key(|(ordinal, _)| *ordinal);
        if entries.windows(2).any(|pair| pair[0].0 == pair[1].0) {
            return Err(LocalRunDirectoryError::StateFile {
                path: file_locator(records, "."),
                operation: "validate",
                source: Box::new((self.invalid)()),
            });
        }
        Ok(entries)
    }

    /// The records of the run at `run_directory` with their ordinals, in ordinal order,
    /// none before the first is appended.
    pub(crate) fn read(
        &self,
        run_directory: &Path,
    ) -> Result<Vec<(u64, T)>, LocalRunDirectoryError> {
        let (_, root) = open_run_root(run_directory)?;
        match statat(&root, self.directory, AtFlags::SYMLINK_NOFOLLOW) {
            Ok(stat) if FileType::from_raw_mode(stat.st_mode) == FileType::Directory => {}
            Ok(_) => {
                return Err(LocalRunDirectoryError::StateFile {
                    path: file_locator(&root, self.directory),
                    operation: "validate layout",
                    source: Box::new(LocalRunDirectoryError::StateInvalid),
                });
            }
            Err(Errno::NOENT) => return Ok(Vec::new()),
            Err(source) => return Err(file_error(&root, self.directory, "stat", source)),
        }
        let records = open_directory_at(&root, self.directory)?;
        self.read_open(&records)
    }

    /// Appends `record` to the run at `run_directory` as `<directory>/<NNNN>-<entry>.json`,
    /// `<NNNN>` one past the highest ordinal there: the directory is created owner-only on
    /// first use, appenders are serialized by an exclusive lock on it, and the file is
    /// written whole under `.private/` and renamed into place, owner-only and never
    /// rewritten. Returns the file's path.
    pub(crate) fn append(
        &self,
        run_directory: &Path,
        entry: &str,
        record: &impl Serialize,
    ) -> Result<PathBuf, LocalRunDirectoryError> {
        let (normalized, root) = open_run_root(run_directory)?;
        let private = open_directory_at(&root, PRIVATE_DIRECTORY)?;
        let records = create_or_open_directory(&root, self.directory)?;
        flock(&records, FlockOperation::LockExclusive)
            .map_err(|source| file_error(&root, self.directory, "lock", source))?;
        let ordinal = self
            .read_open(&records)?
            .last()
            .map_or(1, |(ordinal, _)| ordinal + 1);
        if ordinal > NUMBERED_ORDINAL_LIMIT {
            return Err(LocalRunDirectoryError::StateConflict);
        }
        let name = format!("{ordinal:04}-{entry}.json");
        let bytes = encode_json(record)?;
        match unlinkat(&private, self.temporary, AtFlags::empty()) {
            Ok(()) | Err(Errno::NOENT) => {}
            Err(source) => {
                return Err(file_error(
                    &private,
                    self.temporary,
                    "remove stale temporary",
                    source,
                ));
            }
        }
        let written = write_new_immutable_file(&private, self.temporary, &bytes).and_then(|()| {
            renameat_with(
                &private,
                self.temporary,
                &records,
                &name,
                RenameFlags::NOREPLACE,
            )
            .map_err(|source| file_error(&records, &name, "append", source))
        });
        if written.is_err() {
            let _ = unlinkat(&private, self.temporary, AtFlags::empty());
        }
        written?;
        sync_directory(&records)?;
        sync_directory(&root)?;
        Ok(normalized.join(self.directory).join(name))
    }
}

fn control_event_invalid() -> LocalRunDirectoryError {
    LocalRunDirectoryError::ControlEventInvalid
}

/// A control file holds a control event whose event name is its file's entry name.
fn decode_control(entry: &str, bytes: &[u8]) -> Option<ControlEvent> {
    serde_json::from_slice::<ControlEvent>(bytes)
        .ok()
        .filter(|event| event.is_valid() && event.name() == Some(entry))
}

const CONTROLS: NumberedRecords<ControlEvent> = NumberedRecords::new(
    CONTROLS_DIRECTORY,
    CONTROL_TEMPORARY_FILE,
    control_event_invalid,
    decode_control,
);

/// The controls of the run at `run_directory` in ordinal order (REQ-LOCALRUN-178), none
/// before the first is appended. A file under `controls/` that is not a control event is
/// refused at its path with `ControlEventInvalid`.
pub fn read_controls(run_directory: &Path) -> Result<Vec<ControlEvent>, LocalRunDirectoryError> {
    Ok(read_numbered_controls(run_directory)?
        .into_iter()
        .map(|(_, event)| event)
        .collect())
}

/// `read_controls` with each control's ordinal, the `<NNNN>` of its file name.
pub fn read_numbered_controls(
    run_directory: &Path,
) -> Result<Vec<(u64, ControlEvent)>, LocalRunDirectoryError> {
    CONTROLS.read(run_directory)
}

/// Appends `event` to the run at `run_directory` as `controls/<NNNN>-<event>.json`
/// (REQ-LOCALRUN-178) as `NumberedRecords::append` writes a record. Returns the file's
/// path. An event outside the published schema is `ControlEventInvalid` and writes nothing.
pub fn append_control(
    run_directory: &Path,
    event: &ControlEvent,
) -> Result<PathBuf, LocalRunDirectoryError> {
    let suffix = event
        .name()
        .filter(|_| event.is_valid())
        .ok_or(LocalRunDirectoryError::ControlEventInvalid)?;
    CONTROLS.append(run_directory, suffix, event)
}

/// A step position (REQ-WFDEF-4, REQ-WFDEF-5) as `show` and `list` print it: its
/// identifiers joined by `.` and each fan-out element `{"item": n}` written `[n]` after its
/// step, as `build[2]`.
pub fn position_path(position: &[Value]) -> String {
    let mut path = String::new();
    for element in position {
        if let Some(step) = element.as_str() {
            if !path.is_empty() {
                path.push('.');
            }
            path.push_str(step);
        } else if let Some(item) = element.get("item").and_then(Value::as_u64) {
            path.push('[');
            path.push_str(&item.to_string());
            path.push(']');
        }
    }
    path
}
#[cfg(test)]
mod tests;
