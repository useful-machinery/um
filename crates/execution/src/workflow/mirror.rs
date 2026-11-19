//! The root workflow's `mirror` block (REQ-WFFORMAT-101): what Linear sees of a run of the
//! root. Decoding keeps the authored strings; the template and guard grammars here parse
//! them, which `useful workflow definition validate` does with a location for each failure.
//! The `Mirror` driver performs a run's hooks through a `MirrorTarget` the CLI implements
//! and appends each write's record under the run's `mirror/` (REQ-LOCALRUN-193).

use std::collections::BTreeMap;
use std::fmt;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::document::BindingKey;
use super::local_run::{LocalRunDirectoryError, MIRROR_DIRECTORY};
use super::schema_common::{parse_canonical_utc_timestamp, utc_timestamp};
use super::wait::{NumberedRecords, WaitRecord};

/// The decoded `mirror.linear` block. Templates and guards stay as written, since a bad one
/// is a tree diagnostic rather than a decoding failure; they parse once the tree is valid.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MirrorBlock {
    /// The root input whose value is the issue identifier: the name after `inputs.`.
    pub issue: String,
    pub on_start: Option<String>,
    pub on_stage: Option<String>,
    pub on_wait: Option<String>,
    /// The state each outcome moves the issue to, keyed by outcome name.
    pub on_outcome: BTreeMap<String, MirrorState>,
}

impl MirrorBlock {
    /// Each declared comment hook with its template as written.
    pub fn templates(&self) -> impl Iterator<Item = (MirrorHook, &str)> {
        [
            (MirrorHook::Start, &self.on_start),
            (MirrorHook::Stage, &self.on_stage),
            (MirrorHook::Wait, &self.on_wait),
        ]
        .into_iter()
        .filter_map(|(hook, template)| template.as_deref().map(|template| (hook, template)))
    }
}

/// One `onOutcome` entry: the binding whose bound value is the state, and its guard.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MirrorState {
    /// The binding key, without `$`.
    pub state: String,
    /// The guard as written; it parses with [`MirrorGuard::parse`].
    pub unless: Option<String>,
}

/// The points of a run at which the mirror writes.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum MirrorHook {
    Start,
    Stage,
    Wait,
    Outcome,
}

impl MirrorHook {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Start => "start",
            Self::Stage => "stage",
            Self::Wait => "wait",
            Self::Outcome => "outcome",
        }
    }

    /// The template's key in the block, as a diagnostic names it.
    pub fn template_key(self) -> &'static str {
        match self {
            Self::Start => "onStart.comment",
            Self::Stage => "onStage.comment",
            Self::Wait => "onWait.comment",
            Self::Outcome => "onOutcome",
        }
    }
}

/// The variables a hook's template may name (REQ-WFFORMAT-101).
pub fn hook_variables(hook: MirrorHook) -> &'static [&'static str] {
    match hook {
        MirrorHook::Start | MirrorHook::Outcome => {
            &["operator", "run.id", "run.url", "run.reference"]
        }
        MirrorHook::Stage => &[
            "operator",
            "run.id",
            "run.url",
            "run.reference",
            "stage",
            "summary",
        ],
        MirrorHook::Wait => &[
            "operator",
            "run.id",
            "run.url",
            "run.reference",
            "wait.kind",
            "wait.summary",
        ],
    }
}

/// A comment template: literal text and `{{<variable>}}` placeholders.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Template {
    segments: Vec<TemplateSegment>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TemplateSegment {
    Literal(String),
    Variable(String),
}

/// A `{{` that does not open exactly `{{[a-z][A-Za-z0-9.]*}}`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TemplateError {
    /// The byte offset of the malformed placeholder.
    pub offset: usize,
}

impl fmt::Display for TemplateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "malformed template placeholder at byte {}",
            self.offset
        )
    }
}

impl std::error::Error for TemplateError {}

impl Template {
    pub fn parse(text: &str) -> Result<Self, TemplateError> {
        let mut segments = Vec::new();
        let mut rest = text;
        let mut offset = 0;
        while let Some(start) = rest.find("{{") {
            if start > 0 {
                segments.push(TemplateSegment::Literal(rest[..start].to_owned()));
            }
            let body = &rest[start + 2..];
            let name = body
                .find("}}")
                .map(|end| &body[..end])
                .filter(|name| is_variable_name(name))
                .ok_or(TemplateError {
                    offset: offset + start,
                })?;
            segments.push(TemplateSegment::Variable(name.to_owned()));
            let consumed = start + 2 + name.len() + 2;
            rest = &rest[consumed..];
            offset += consumed;
        }
        if !rest.is_empty() {
            segments.push(TemplateSegment::Literal(rest.to_owned()));
        }
        Ok(Self { segments })
    }

    pub fn segments(&self) -> &[TemplateSegment] {
        &self.segments
    }

    pub fn variables(&self) -> impl Iterator<Item = &str> {
        self.segments.iter().filter_map(|segment| match segment {
            TemplateSegment::Variable(name) => Some(name.as_str()),
            TemplateSegment::Literal(_) => None,
        })
    }
}

fn is_variable_name(name: &str) -> bool {
    let mut characters = name.chars();
    characters
        .next()
        .is_some_and(|first| first.is_ascii_lowercase())
        && characters.all(|character| character.is_ascii_alphanumeric() || character == '.')
}

/// An `unless` guard: `issue.state != $key` or `issue.state == $key`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MirrorGuard {
    pub operator: MirrorGuardOperator,
    /// The binding key as written, without `$`: a flat key or `<id>.<name>` (REQ-WFDEF-37).
    pub binding: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MirrorGuardOperator {
    Equal,
    NotEqual,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MirrorGuardError {
    /// The guard compares with a quoted literal where a `$binding` is expected.
    QuotedLiteral,
    /// The guard is outside the grammar.
    Invalid,
}

impl fmt::Display for MirrorGuardError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::QuotedLiteral => "the guard compares with a quoted literal",
            Self::Invalid => "the guard is not `issue.state != $key` or `issue.state == $key`",
        })
    }
}

impl std::error::Error for MirrorGuardError {}

impl MirrorGuard {
    pub fn parse(text: &str) -> Result<Self, MirrorGuardError> {
        if text.contains(['"', '\'']) {
            return Err(MirrorGuardError::QuotedLiteral);
        }
        let tokens = text.split_whitespace().collect::<Vec<_>>();
        let [subject, operator, binding] = tokens.as_slice() else {
            return Err(MirrorGuardError::Invalid);
        };
        let operator = match *operator {
            "==" => MirrorGuardOperator::Equal,
            "!=" => MirrorGuardOperator::NotEqual,
            _ => return Err(MirrorGuardError::Invalid),
        };
        let binding = binding
            .strip_prefix('$')
            .filter(|key| BindingKey::parse(key).is_some())
            .filter(|_| *subject == "issue.state")
            .ok_or(MirrorGuardError::Invalid)?;
        Ok(Self {
            operator,
            binding: binding.to_owned(),
        })
    }
}

/// Why a mirror write failed (REQ-LOCALRUN-193): one closed `code` and a `diagnostic` that
/// names the cause and never a credential or a response body beyond the target's error
/// message.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MirrorFailure {
    pub code: &'static str,
    pub diagnostic: String,
}

impl MirrorFailure {
    pub const CREDENTIALS_UNAVAILABLE: &'static str = "mirror_credentials_unavailable";
    pub const BINDINGS_UNAVAILABLE: &'static str = "mirror_bindings_unavailable";
    pub const ISSUE_UNAVAILABLE: &'static str = "mirror_issue_unavailable";
    pub const STATE_UNKNOWN: &'static str = "mirror_state_unknown";
    pub const WRITE_REJECTED: &'static str = "mirror_write_rejected";
    pub const UNAVAILABLE: &'static str = "mirror_unavailable";
    /// Every code a failed write may record.
    pub const CODES: [&'static str; 6] = [
        Self::CREDENTIALS_UNAVAILABLE,
        Self::BINDINGS_UNAVAILABLE,
        Self::ISSUE_UNAVAILABLE,
        Self::STATE_UNKNOWN,
        Self::WRITE_REJECTED,
        Self::UNAVAILABLE,
    ];

    fn new(code: &'static str, diagnostic: impl Into<String>) -> Self {
        Self {
            code,
            diagnostic: diagnostic.into(),
        }
    }

    pub fn credentials_unavailable(diagnostic: impl Into<String>) -> Self {
        Self::new(Self::CREDENTIALS_UNAVAILABLE, diagnostic)
    }

    pub fn bindings_unavailable(diagnostic: impl Into<String>) -> Self {
        Self::new(Self::BINDINGS_UNAVAILABLE, diagnostic)
    }

    pub fn issue_unavailable(diagnostic: impl Into<String>) -> Self {
        Self::new(Self::ISSUE_UNAVAILABLE, diagnostic)
    }

    pub fn state_unknown(diagnostic: impl Into<String>) -> Self {
        Self::new(Self::STATE_UNKNOWN, diagnostic)
    }

    pub fn write_rejected(diagnostic: impl Into<String>) -> Self {
        Self::new(Self::WRITE_REJECTED, diagnostic)
    }

    pub fn unavailable(diagnostic: impl Into<String>) -> Self {
        Self::new(Self::UNAVAILABLE, diagnostic)
    }
}

impl fmt::Display for MirrorFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code, self.diagnostic)
    }
}

impl std::error::Error for MirrorFailure {}

/// Where the mirror writes: the execution crate declares it and the CLI implements it over
/// Linear. `issue_state` answers the issue's state name and `set_state` moves it to the state
/// of a name; `issue_state_id` and `set_state_id` do the same by the provider's state id, for
/// a revision with a resolution record (REQ-WFDEF-32). `comments` answers the bodies of the
/// issue's newest comments, at most 50, for the start rule (REQ-LOCALRUN-216).
pub trait MirrorTarget {
    fn issue_state(&self, issue: &str) -> Result<String, MirrorFailure>;
    fn issue_state_id(&self, issue: &str) -> Result<String, MirrorFailure>;
    fn comments(&self, issue: &str) -> Result<Vec<String>, MirrorFailure>;
    fn comment(&self, issue: &str, body: &str) -> Result<(), MirrorFailure>;
    fn set_state(&self, issue: &str, state: &str) -> Result<(), MirrorFailure>;
    fn set_state_id(&self, issue: &str, state_id: &str) -> Result<(), MirrorFailure>;
}

/// A bound state as the target is asked for it: by its bound name, or by the id the
/// revision's resolution record gives its key.
enum StateSelector {
    Name(String),
    Id(String),
}

/// What a write asked the target to do.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "lowercase", deny_unknown_fields)]
pub enum MirrorAction {
    Comment {
        body: String,
    },
    /// `state` is the bound value of `binding`, absent when it could not be read.
    State {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        state: Option<String>,
        binding: String,
    },
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum MirrorWriteOutcome {
    Written,
    Skipped,
    Failed,
}

/// Why a write was skipped (REQ-LOCALRUN-193).
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MirrorSkipReason {
    /// The outcome's `unless` guard held.
    GuardHeld,
    /// The start comment was left unwritten because the issue already links the run.
    StartAnnounced,
}

/// One `mirror/<NNNN>-<hook>.json` (REQ-LOCALRUN-193).
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MirrorWrite {
    pub schema_version: u64,
    pub hook: MirrorHook,
    /// A canonical UTC RFC 3339 timestamp.
    pub at: String,
    pub issue: String,
    pub action: MirrorAction,
    pub outcome: MirrorWriteOutcome,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<MirrorSkipReason>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostic: Option<String>,
}

const MIRROR_WRITE_SCHEMA_VERSION: u64 = 1;
const MIRROR_TEMPORARY_FILE: &str = "mirror.json.tmp";

/// How a hook's write ended.
enum Performed {
    Written,
    Skipped(MirrorSkipReason),
    Failed(MirrorFailure),
}

impl From<Result<(), MirrorFailure>> for Performed {
    fn from(result: Result<(), MirrorFailure>) -> Self {
        result.map_or_else(Self::Failed, |()| Self::Written)
    }
}

impl MirrorWrite {
    fn new(
        hook: MirrorHook,
        issue: &str,
        action: MirrorAction,
        performed: Performed,
    ) -> Result<Self, LocalRunDirectoryError> {
        let at = utc_timestamp(useful_support::utc_now())
            .map_err(|_| LocalRunDirectoryError::MirrorWriteInvalid)?;
        let (outcome, reason, failure) = match performed {
            Performed::Written => (MirrorWriteOutcome::Written, None, None),
            Performed::Skipped(reason) => (MirrorWriteOutcome::Skipped, Some(reason), None),
            Performed::Failed(failure) => (MirrorWriteOutcome::Failed, None, Some(failure)),
        };
        let (code, diagnostic) = failure.map_or((None, None), |failure| {
            (Some(failure.code.to_owned()), Some(failure.diagnostic))
        });
        Ok(Self {
            schema_version: MIRROR_WRITE_SCHEMA_VERSION,
            hook,
            at,
            issue: issue.to_owned(),
            action,
            outcome,
            reason,
            code,
            diagnostic,
        })
    }

    /// The facts this crate writes and relies on: schema version 1, a canonical `at`, a
    /// state action exactly on the outcome hook, a `reason` exactly on a skipped write and a
    /// closed `code` with a `diagnostic` exactly on a failed one.
    pub fn is_valid(&self) -> bool {
        let consistent = match self.outcome {
            MirrorWriteOutcome::Written => {
                self.reason.is_none() && self.code.is_none() && self.diagnostic.is_none()
            }
            MirrorWriteOutcome::Skipped => {
                self.reason.is_some() && self.code.is_none() && self.diagnostic.is_none()
            }
            MirrorWriteOutcome::Failed => {
                self.reason.is_none()
                    && self
                        .code
                        .as_deref()
                        .is_some_and(|code| MirrorFailure::CODES.contains(&code))
                    && self.diagnostic.is_some()
            }
        };
        self.schema_version == MIRROR_WRITE_SCHEMA_VERSION
            && parse_canonical_utc_timestamp(&self.at).is_some()
            && matches!(self.action, MirrorAction::State { .. })
                == (self.hook == MirrorHook::Outcome)
            && consistent
    }
}

fn mirror_write_invalid() -> LocalRunDirectoryError {
    LocalRunDirectoryError::MirrorWriteInvalid
}

/// A mirror file holds a valid write whose hook is its file's entry name.
fn decode_write(entry: &str, bytes: &[u8]) -> Option<MirrorWrite> {
    serde_json::from_slice::<MirrorWrite>(bytes)
        .ok()
        .filter(|write| write.is_valid() && write.hook.as_str() == entry)
}

const WRITES: NumberedRecords<MirrorWrite> = NumberedRecords::new(
    MIRROR_DIRECTORY,
    MIRROR_TEMPORARY_FILE,
    mirror_write_invalid,
    decode_write,
);

/// Appends `write` to the run at `run_directory` as `mirror/<NNNN>-<hook>.json`
/// (REQ-LOCALRUN-193), as `wait::append_control` appends a control. Returns the file's
/// path; a write that is not valid is `MirrorWriteInvalid` and writes nothing.
pub(crate) fn append_write(
    run_directory: &Path,
    write: &MirrorWrite,
) -> Result<PathBuf, LocalRunDirectoryError> {
    if !write.is_valid() {
        return Err(LocalRunDirectoryError::MirrorWriteInvalid);
    }
    WRITES.append(run_directory, write.hook.as_str(), write)
}

/// The mirror writes of the run at `run_directory` in ordinal order (REQ-LOCALRUN-193),
/// none when it has no `mirror/`. A file there that is not a write is refused at its path
/// with `MirrorWriteInvalid`.
pub fn read_mirror_writes(
    run_directory: &Path,
) -> Result<Vec<MirrorWrite>, LocalRunDirectoryError> {
    Ok(read_numbered_mirror_writes(run_directory)?
        .into_iter()
        .map(|(_, write)| write)
        .collect())
}

/// `read_mirror_writes` with each write's ordinal, the `<NNNN>` of its file name.
pub fn read_numbered_mirror_writes(
    run_directory: &Path,
) -> Result<Vec<(u64, MirrorWrite)>, LocalRunDirectoryError> {
    WRITES.read(run_directory)
}

/// What a run's mirror holds of the run (REQ-LOCALRUN-191): the values of the `operator`,
/// `run.id`, `run.url` and `run.reference` placeholders, each composed by the caller, the
/// record's `external.url` when it has one, which the start rule looks for on the issue
/// (REQ-LOCALRUN-216), and the run directory the mirror's records go under. `url` and
/// `reference` fall back to the run directory and the `show` command, so only
/// `external_url` says whether the record has an external URL.
#[derive(Clone, Debug)]
pub struct MirrorRun {
    pub operator: String,
    pub id: String,
    pub url: String,
    pub reference: String,
    pub external_url: Option<String>,
    pub directory: PathBuf,
}

/// A run's mirror (REQ-LOCALRUN-193): the root's block, the issue identifier, the bound
/// values of its revision's bindings snapshot (or why they cannot be had), the resolved
/// provider ids when the revision has a resolution record, both keyed by the text a
/// reference names after `$` (a flat key, or `<id>.<name>` for an integration binding,
/// REQ-WFDEF-37), and the run
/// (`MirrorRun`): the operator, the run's id, URL and reference, and the run directory the
/// records go under. With resolved ids the outcome's guard and state write go by id, else
/// by the bound name (REQ-LOCALRUN-192). Each hook performs the block's write when the
/// block declares it and appends one record of how it ended; its only error is that
/// record's own write failing, and a failed write is a record, never an error.
#[derive(Clone, Debug)]
pub struct Mirror {
    block: MirrorBlock,
    issue: String,
    bindings: Result<BTreeMap<String, String>, MirrorFailure>,
    resolved_ids: Option<BTreeMap<String, String>>,
    run: MirrorRun,
}

impl Mirror {
    pub fn new(
        block: MirrorBlock,
        issue: String,
        bindings: Result<BTreeMap<String, String>, MirrorFailure>,
        run: MirrorRun,
    ) -> Self {
        Self {
            block,
            issue,
            bindings,
            resolved_ids: None,
            run,
        }
    }

    /// The mirror matching and writing states by the provider ids of the revision's
    /// resolution record (REQ-WFDEF-32), by the reference key the bound values are keyed by.
    #[must_use]
    pub fn with_resolved_ids(mut self, resolved_ids: BTreeMap<String, String>) -> Self {
        self.resolved_ids = Some(resolved_ids);
        self
    }

    /// `onStart`, once when `useful workflow run create` has made the run, skipped when the issue
    /// already links the run (`start_announced`, REQ-LOCALRUN-216).
    pub fn start(&self, target: &dyn MirrorTarget) -> Result<(), LocalRunDirectoryError> {
        self.comment(target, MirrorHook::Start, &[])
    }

    /// `onStage`, when a `workflow` step's child exits with `outcome`: `stage` is the step id
    /// or its position path from the root, which for a nested run's stage runs through the
    /// nested run's step (`parts[2].partReview`), and `summary` the child's `summary` text
    /// export when it made one available.
    pub fn stage(
        &self,
        target: &dyn MirrorTarget,
        stage: &str,
        outcome: &str,
        summary: Option<&str>,
    ) -> Result<(), LocalRunDirectoryError> {
        self.comment(
            target,
            MirrorHook::Stage,
            &[("stage", stage), ("summary", summary.unwrap_or(outcome))],
        )
    }

    /// `onWait`, when an attempt has ended in the wait `record`.
    pub fn wait(
        &self,
        target: &dyn MirrorTarget,
        record: &WaitRecord,
    ) -> Result<(), LocalRunDirectoryError> {
        let summary = match record.payload.get("summary") {
            Some(Value::String(summary)) => summary.clone(),
            _ => record.step_path(),
        };
        self.comment(
            target,
            MirrorHook::Wait,
            &[
                ("wait.kind", record.kind_name()),
                ("wait.summary", &summary),
            ],
        )
    }

    /// Whether `onOutcome` maps `outcome`, so that `outcome` performs a write
    /// (REQ-LOCALRUN-204).
    pub fn maps_outcome(&self, outcome: &str) -> bool {
        self.block.on_outcome.contains_key(outcome)
    }

    /// `onOutcome.<outcome>`, once the run record reads `exited` with `outcome`: the issue
    /// moves to the bound state unless the entry's guard holds over the issue's state, read
    /// once; both by id when the mirror has resolved ids. An outcome the block does not map
    /// (`maps_outcome`) writes nothing.
    pub fn outcome(
        &self,
        target: &dyn MirrorTarget,
        outcome: &str,
    ) -> Result<(), LocalRunDirectoryError> {
        let Some(entry) = self.block.on_outcome.get(outcome) else {
            return Ok(());
        };
        let action = MirrorAction::State {
            state: self.bound(&entry.state).ok(),
            binding: entry.state.clone(),
        };
        let guarded = self.selector(&entry.state).and_then(|state| {
            self.guard_holds(target, entry.unless.as_deref())
                .map(|holds| (state, holds))
        });
        let performed = match guarded {
            Err(failure) => Performed::Failed(failure),
            Ok((_, true)) => Performed::Skipped(MirrorSkipReason::GuardHeld),
            Ok((StateSelector::Name(state), false)) => target.set_state(&self.issue, &state).into(),
            Ok((StateSelector::Id(state_id), false)) => {
                target.set_state_id(&self.issue, &state_id).into()
            }
        };
        self.record(MirrorHook::Outcome, action, performed)
    }

    fn comment(
        &self,
        target: &dyn MirrorTarget,
        hook: MirrorHook,
        variables: &[(&str, &str)],
    ) -> Result<(), LocalRunDirectoryError> {
        let template = match hook {
            MirrorHook::Start => &self.block.on_start,
            MirrorHook::Stage => &self.block.on_stage,
            MirrorHook::Wait => &self.block.on_wait,
            MirrorHook::Outcome => &None,
        };
        let Some(template) = template else {
            return Ok(());
        };
        let body = self.render(template, variables);
        let performed = match &self.bindings {
            Ok(_) if hook == MirrorHook::Start && self.start_announced(target) => {
                Performed::Skipped(MirrorSkipReason::StartAnnounced)
            }
            Ok(_) => target.comment(&self.issue, &body).into(),
            Err(failure) => Performed::Failed(failure.clone()),
        };
        self.record(hook, MirrorAction::Comment { body }, performed)
    }

    /// Whether one of the issue's newest comments already links the run (REQ-LOCALRUN-216):
    /// it contains the record's non-empty external URL, as the target answers it or with
    /// every backslash removed, which undoes Markdown's escapes. A run without one reads
    /// nothing, and a read that fails is no announcement and leaves no trace.
    fn start_announced(&self, target: &dyn MirrorTarget) -> bool {
        let Some(url) = self
            .run
            .external_url
            .as_deref()
            .filter(|url| !url.is_empty())
        else {
            return false;
        };
        target.comments(&self.issue).is_ok_and(|bodies| {
            bodies
                .iter()
                .any(|body| body.contains(url) || body.replace('\\', "").contains(url))
        })
    }

    /// The template with each placeholder replaced by its value; one the hook does not
    /// supply renders empty, and a template outside the grammar renders as written.
    fn render(&self, template: &str, variables: &[(&str, &str)]) -> String {
        let Ok(parsed) = Template::parse(template) else {
            return template.to_owned();
        };
        let value = |name: &str| match name {
            "operator" => Some(self.run.operator.as_str()),
            "run.id" => Some(self.run.id.as_str()),
            "run.url" => Some(self.run.url.as_str()),
            "run.reference" => Some(self.run.reference.as_str()),
            _ => variables
                .iter()
                .find(|(variable, _)| *variable == name)
                .map(|(_, value)| *value),
        };
        parsed
            .segments()
            .iter()
            .map(|segment| match segment {
                TemplateSegment::Literal(text) => text.as_str(),
                TemplateSegment::Variable(name) => value(name).unwrap_or_default(),
            })
            .collect()
    }

    /// The bound value of binding `key` in the revision's snapshot.
    fn bound(&self, key: &str) -> Result<String, MirrorFailure> {
        let bindings = self.bindings.as_ref().map_err(Clone::clone)?;
        bindings.get(key).cloned().ok_or_else(|| {
            MirrorFailure::bindings_unavailable(format!(
                "the binding `{key}` is not in the revision's bindings snapshot"
            ))
        })
    }

    /// The bound state of binding `key` as the target is asked for it: its id in the
    /// resolution record when the mirror has one (`mirror_state_unknown` for a key the
    /// record gives none), else its bound name.
    fn selector(&self, key: &str) -> Result<StateSelector, MirrorFailure> {
        let bound = self.bound(key)?;
        let Some(resolved_ids) = &self.resolved_ids else {
            return Ok(StateSelector::Name(bound));
        };
        resolved_ids
            .get(key)
            .cloned()
            .map(StateSelector::Id)
            .ok_or_else(|| {
                MirrorFailure::state_unknown(format!(
                    "the binding `{key}` has no resolved id in the revision's resolution record"
                ))
            })
    }

    /// Whether the `unless` guard holds over the issue's state, read once and compared by id
    /// when the mirror has resolved ids; no guard never holds and reads nothing.
    fn guard_holds(
        &self,
        target: &dyn MirrorTarget,
        unless: Option<&str>,
    ) -> Result<bool, MirrorFailure> {
        let Some(unless) = unless else {
            return Ok(false);
        };
        let guard = MirrorGuard::parse(unless).map_err(|error| {
            MirrorFailure::bindings_unavailable(format!("the outcome's guard is invalid: {error}"))
        })?;
        let in_state = match self.selector(&guard.binding)? {
            StateSelector::Name(state) => target.issue_state(&self.issue)? == state,
            StateSelector::Id(state_id) => target.issue_state_id(&self.issue)? == state_id,
        };
        Ok(match guard.operator {
            MirrorGuardOperator::Equal => in_state,
            MirrorGuardOperator::NotEqual => !in_state,
        })
    }

    fn record(
        &self,
        hook: MirrorHook,
        action: MirrorAction,
        performed: Performed,
    ) -> Result<(), LocalRunDirectoryError> {
        let write = MirrorWrite::new(hook, &self.issue, action, performed)?;
        append_write(&self.run.directory, &write).map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::fs;
    use std::os::unix::fs::PermissionsExt as _;

    use serde_json::{Map, Value, json};

    use super::*;
    use crate::workflow::wait::{WaitKind, derive_rule};

    const ROOT_WORKFLOW: &[u8] = include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/workflow/v1/valid/root-workflow.yaml"
    ));

    #[derive(Clone, Debug, Eq, PartialEq)]
    enum Call {
        IssueState(String),
        IssueStateId(String),
        Comments(String),
        Comment(String, String),
        SetState(String, String),
        SetStateId(String, String),
    }

    /// A target that records each call and answers as told.
    struct FakeTarget {
        state: Result<String, MirrorFailure>,
        state_id: Result<String, MirrorFailure>,
        comments: Result<Vec<String>, MirrorFailure>,
        writes: Result<(), MirrorFailure>,
        calls: RefCell<Vec<Call>>,
    }

    impl FakeTarget {
        fn new() -> Self {
            Self {
                state: Ok("Engineering".to_owned()),
                state_id: Ok("state-engineering".to_owned()),
                comments: Ok(Vec::new()),
                writes: Ok(()),
                calls: RefCell::default(),
            }
        }

        fn in_state(state: &str) -> Self {
            Self {
                state: Ok(state.to_owned()),
                ..Self::new()
            }
        }

        fn calls(&self) -> Vec<Call> {
            self.calls.borrow().clone()
        }
    }

    impl MirrorTarget for FakeTarget {
        fn issue_state(&self, issue: &str) -> Result<String, MirrorFailure> {
            self.calls
                .borrow_mut()
                .push(Call::IssueState(issue.to_owned()));
            self.state.clone()
        }

        fn issue_state_id(&self, issue: &str) -> Result<String, MirrorFailure> {
            self.calls
                .borrow_mut()
                .push(Call::IssueStateId(issue.to_owned()));
            self.state_id.clone()
        }

        fn comments(&self, issue: &str) -> Result<Vec<String>, MirrorFailure> {
            self.calls
                .borrow_mut()
                .push(Call::Comments(issue.to_owned()));
            self.comments.clone()
        }

        fn comment(&self, issue: &str, body: &str) -> Result<(), MirrorFailure> {
            self.calls
                .borrow_mut()
                .push(Call::Comment(issue.to_owned(), body.to_owned()));
            self.writes.clone()
        }

        fn set_state(&self, issue: &str, state: &str) -> Result<(), MirrorFailure> {
            self.calls
                .borrow_mut()
                .push(Call::SetState(issue.to_owned(), state.to_owned()));
            self.writes.clone()
        }

        fn set_state_id(&self, issue: &str, state_id: &str) -> Result<(), MirrorFailure> {
            self.calls
                .borrow_mut()
                .push(Call::SetStateId(issue.to_owned(), state_id.to_owned()));
            self.writes.clone()
        }
    }

    /// A directory shaped like a run root as far as the mirror needs: `.private/` beside it.
    fn run_root() -> tempfile::TempDir {
        let root = tempfile::tempdir().unwrap();
        fs::create_dir(root.path().join(".private")).unwrap();
        fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).unwrap();
        root
    }

    fn sketch_block() -> MirrorBlock {
        crate::workflow::decode(ROOT_WORKFLOW)
            .expect("the root fixture decodes")
            .root
            .and_then(|root| root.mirror)
            .expect("the root fixture carries the sketch's mirror block")
    }

    fn engineering_bindings() -> BTreeMap<String, String> {
        [
            ("connection", "local"),
            ("linearTeam", "AUTO"),
            ("ownedState", "Engineering"),
            ("backlog", "Backlog"),
            ("canceled", "Canceled"),
            ("done", "Done"),
            ("repository", "useful-machinery/platform"),
        ]
        .into_iter()
        .map(|(key, value)| (key.to_owned(), value.to_owned()))
        .collect()
    }

    /// The run `run_x` of `saeed`, its records under `directory`.
    fn mirror_run(directory: &Path) -> MirrorRun {
        MirrorRun {
            operator: "saeed".to_owned(),
            id: "run_x".to_owned(),
            url: "file:///tmp/run_x".to_owned(),
            reference: "useful workflow run show run_x".to_owned(),
            external_url: None,
            directory: directory.to_owned(),
        }
    }

    fn mirror(
        root: &tempfile::TempDir,
        bindings: Result<BTreeMap<String, String>, MirrorFailure>,
    ) -> Mirror {
        Mirror::new(
            sketch_block(),
            "AUTO-143".to_owned(),
            bindings,
            mirror_run(root.path()),
        )
    }

    fn approval_wait() -> WaitRecord {
        WaitRecord {
            kind: WaitKind::Approval,
            since: "2026-11-08T08:00:00Z".to_owned(),
            step: vec![Value::String("review".to_owned())],
            then: "merge".to_owned(),
            on_hand_back: None,
            rule: derive_rule(WaitKind::Approval, false).expect("approval derives a rule"),
            payload: Map::new(),
        }
    }

    fn record_json(root: &tempfile::TempDir, name: &str) -> Value {
        serde_json::from_slice(&fs::read(root.path().join("mirror").join(name)).unwrap()).unwrap()
    }

    fn mode(path: &std::path::Path) -> u32 {
        fs::symlink_metadata(path).unwrap().permissions().mode() & 0o7777
    }

    // REQ-LOCALRUN-193
    #[test]
    fn each_declared_hook_writes_through_the_target_and_appends_its_record_in_call_order() {
        let root = run_root();
        let mirror = mirror(&root, Ok(engineering_bindings()));
        let target = FakeTarget::new();
        assert_eq!(read_mirror_writes(root.path()).unwrap(), []);

        mirror.start(&target).unwrap();
        assert_eq!(
            target.calls(),
            [Call::Comment(
                "AUTO-143".to_owned(),
                "Engineering started by saeed. Run: file:///tmp/run_x".to_owned()
            )]
        );
        let start = record_json(&root, "0001-start.json");
        assert_eq!(start["schemaVersion"], 1);
        assert_eq!(start["hook"], "start");
        assert_eq!(start["issue"], "AUTO-143");
        assert_eq!(
            start["action"],
            json!({"kind": "comment", "body": "Engineering started by saeed. Run: file:///tmp/run_x"})
        );
        assert_eq!(start["outcome"], "written");
        assert!(
            super::super::schema_common::parse_canonical_utc_timestamp(
                start["at"].as_str().unwrap()
            )
            .is_some(),
            "{start}"
        );
        assert_eq!(
            start.as_object().unwrap().keys().collect::<Vec<_>>(),
            ["action", "at", "hook", "issue", "outcome", "schemaVersion"]
        );

        mirror
            .stage(&target, "design", "blocked", Some("Three slices"))
            .unwrap();
        mirror.stage(&target, "build", "built", None).unwrap();
        mirror.wait(&target, &approval_wait()).unwrap();
        mirror.outcome(&target, "landed").unwrap();
        assert_eq!(
            target.calls()[1..],
            [
                Call::Comment("AUTO-143".to_owned(), "design: Three slices".to_owned()),
                Call::Comment("AUTO-143".to_owned(), "build: built".to_owned()),
                Call::Comment(
                    "AUTO-143".to_owned(),
                    "Waiting for approval: review".to_owned()
                ),
                Call::SetState("AUTO-143".to_owned(), "Done".to_owned()),
            ]
        );
        assert_eq!(
            record_json(&root, "0005-outcome.json")["action"],
            json!({"kind": "state", "state": "Done", "binding": "done"})
        );

        let mirror_directory = root.path().join("mirror");
        assert_eq!(mode(&mirror_directory), 0o700);
        for entry in fs::read_dir(&mirror_directory).unwrap() {
            let path = entry.unwrap().path();
            assert_eq!(mode(&path) & 0o277, 0, "{}", path.display());
        }
        assert_eq!(
            fs::read_dir(root.path().join(".private")).unwrap().count(),
            0
        );
        let numbered = read_numbered_mirror_writes(root.path()).unwrap();
        assert_eq!(
            numbered
                .iter()
                .map(|(ordinal, write)| (*ordinal, write.hook, write.outcome))
                .collect::<Vec<_>>(),
            [
                (1, MirrorHook::Start, MirrorWriteOutcome::Written),
                (2, MirrorHook::Stage, MirrorWriteOutcome::Written),
                (3, MirrorHook::Stage, MirrorWriteOutcome::Written),
                (4, MirrorHook::Wait, MirrorWriteOutcome::Written),
                (5, MirrorHook::Outcome, MirrorWriteOutcome::Written),
            ]
        );
        assert_eq!(
            read_mirror_writes(root.path()).unwrap(),
            numbered
                .into_iter()
                .map(|(_, write)| write)
                .collect::<Vec<_>>()
        );

        // A file under `mirror/` that is not a write for its name makes the writes unreadable.
        fs::write(
            mirror_directory.join("0006-stage.json"),
            fs::read(mirror_directory.join("0001-start.json")).unwrap(),
        )
        .unwrap();
        match read_mirror_writes(root.path()) {
            Err(LocalRunDirectoryError::StateFile { path, source, .. })
                if path.ends_with("mirror/0006-stage.json")
                    && matches!(*source, LocalRunDirectoryError::MirrorWriteInvalid) => {}
            other => panic!("a misnamed write was not refused: {other:?}"),
        }
    }

    // REQ-LOCALRUN-193
    #[test]
    fn a_guarded_outcome_reads_the_issue_once_and_skips_while_the_guard_holds() {
        let root = run_root();
        let mirror = mirror(&root, Ok(engineering_bindings()));

        let moved = FakeTarget::in_state("Backlog");
        mirror.outcome(&moved, "withdrawn").unwrap();
        assert_eq!(moved.calls(), [Call::IssueState("AUTO-143".to_owned())]);
        let skipped = record_json(&root, "0001-outcome.json");
        assert_eq!(skipped["outcome"], "skipped");
        assert_eq!(skipped["reason"], "guard_held");
        assert_eq!(
            skipped["action"],
            json!({"kind": "state", "state": "Backlog", "binding": "backlog"})
        );

        let owned = FakeTarget::in_state("Engineering");
        mirror.outcome(&owned, "withdrawn").unwrap();
        assert_eq!(
            owned.calls(),
            [
                Call::IssueState("AUTO-143".to_owned()),
                Call::SetState("AUTO-143".to_owned(), "Backlog".to_owned()),
            ]
        );
        assert_eq!(
            record_json(&root, "0002-outcome.json")["outcome"],
            "written"
        );

        let unreadable = FakeTarget {
            state: Err(MirrorFailure::issue_unavailable(
                "the issue AUTO-143 was not found",
            )),
            ..FakeTarget::new()
        };
        mirror.outcome(&unreadable, "canceled").unwrap();
        assert_eq!(
            unreadable.calls(),
            [Call::IssueState("AUTO-143".to_owned())]
        );
        let failed = record_json(&root, "0003-outcome.json");
        assert_eq!(failed["outcome"], "failed");
        assert_eq!(failed["code"], "mirror_issue_unavailable");
        assert_eq!(failed["diagnostic"], "the issue AUTO-143 was not found");
    }

    /// The provider ids a revision's resolution record gives the sketch's state bindings.
    fn resolved_ids() -> BTreeMap<String, String> {
        [
            ("linearTeam", "team-auto"),
            ("ownedState", "state-engineering"),
            ("backlog", "state-backlog"),
            ("canceled", "state-canceled"),
            ("done", "state-done"),
        ]
        .into_iter()
        .map(|(key, value)| (key.to_owned(), value.to_owned()))
        .collect()
    }

    // REQ-LOCALRUN-192, REQ-WFDEF-32: with resolved ids the guard compares the issue's state
    // id with the bound state's id and the write sends the id, whatever the states are named
    // now; the record keeps the bound value.
    #[test]
    fn with_resolved_ids_the_guard_compares_and_the_write_sends_state_ids() {
        let root = run_root();
        let mirror = mirror(&root, Ok(engineering_bindings())).with_resolved_ids(resolved_ids());

        let target = FakeTarget::new();
        mirror.outcome(&target, "landed").unwrap();
        assert_eq!(
            target.calls(),
            [Call::SetStateId(
                "AUTO-143".to_owned(),
                "state-done".to_owned()
            )]
        );
        let landed = record_json(&root, "0001-outcome.json");
        assert_eq!(landed["outcome"], "written");
        assert_eq!(
            landed["action"],
            json!({"kind": "state", "state": "Done", "binding": "done"})
        );

        // The owned state was renamed: its id still matches, so the guard does not hold.
        let renamed = FakeTarget {
            state: Ok("In engineering".to_owned()),
            ..FakeTarget::new()
        };
        mirror.outcome(&renamed, "withdrawn").unwrap();
        assert_eq!(
            renamed.calls(),
            [
                Call::IssueStateId("AUTO-143".to_owned()),
                Call::SetStateId("AUTO-143".to_owned(), "state-backlog".to_owned()),
            ]
        );
        assert_eq!(
            record_json(&root, "0002-outcome.json")["outcome"],
            "written"
        );

        // Another state now named as the owned one was: its id differs, so the guard holds.
        let moved = FakeTarget {
            state_id: Ok("state-triage".to_owned()),
            ..FakeTarget::new()
        };
        mirror.outcome(&moved, "withdrawn").unwrap();
        assert_eq!(moved.calls(), [Call::IssueStateId("AUTO-143".to_owned())]);
        let skipped = record_json(&root, "0003-outcome.json");
        assert_eq!(skipped["outcome"], "skipped");
        assert_eq!(skipped["reason"], "guard_held");
    }

    // REQ-LOCALRUN-192, REQ-WFDEF-32: with a resolution record, a bound state or guard key it
    // gives no id is `mirror_state_unknown` before any request.
    #[test]
    fn with_resolved_ids_a_bound_key_without_an_id_is_state_unknown() {
        for missing in ["done", "ownedState", "backlog"] {
            let root = run_root();
            let mut ids = resolved_ids();
            ids.remove(missing);
            let mirror = mirror(&root, Ok(engineering_bindings())).with_resolved_ids(ids);
            let target = FakeTarget::new();
            let outcome = if missing == "done" {
                "landed"
            } else {
                "withdrawn"
            };
            mirror.outcome(&target, outcome).unwrap();
            assert_eq!(target.calls(), [], "{missing}");
            let failed = record_json(&root, "0001-outcome.json");
            assert_eq!(failed["outcome"], "failed", "{missing}");
            assert_eq!(failed["code"], "mirror_state_unknown", "{missing}");
        }
    }

    // REQ-LOCALRUN-193
    #[test]
    fn a_hook_the_block_does_not_declare_writes_and_records_nothing() {
        let root = run_root();
        let mirror = mirror(&root, Ok(engineering_bindings()));
        let target = FakeTarget::new();
        mirror.outcome(&target, "failed").unwrap();
        mirror.outcome(&target, "superseded").unwrap();
        assert_eq!(target.calls(), []);
        assert!(!root.path().join("mirror").exists());

        let mut block = sketch_block();
        block.on_start = None;
        let quiet = Mirror::new(
            block,
            "AUTO-143".to_owned(),
            Ok(engineering_bindings()),
            mirror_run(root.path()),
        );
        quiet.start(&target).unwrap();
        assert_eq!(target.calls(), []);
        assert_eq!(read_mirror_writes(root.path()).unwrap(), []);
    }

    // REQ-LOCALRUN-193
    #[test]
    fn a_failed_write_is_recorded_with_its_code_and_never_fails_the_hook() {
        let root = run_root();
        let mirror = mirror(&root, Ok(engineering_bindings()));
        let target = FakeTarget {
            writes: Err(MirrorFailure::write_rejected(
                "Linear answered 400: Entity not found",
            )),
            ..FakeTarget::new()
        };
        assert_eq!(mirror.start(&target), Ok(()));
        assert_eq!(target.calls().len(), 1);
        let failed = record_json(&root, "0001-start.json");
        assert_eq!(failed["outcome"], "failed");
        assert_eq!(failed["code"], "mirror_write_rejected");
        assert_eq!(
            failed["diagnostic"],
            "Linear answered 400: Entity not found"
        );
        assert_eq!(failed.get("reason"), None);
    }

    // REQ-LOCALRUN-193
    #[test]
    fn a_mirror_without_its_bindings_records_every_hook_failed_and_calls_no_target() {
        let root = run_root();
        let mirror = mirror(
            &root,
            Err(MirrorFailure::bindings_unavailable(
                "definition_record_invalid: bindings snapshot does not match its digest",
            )),
        );
        let target = FakeTarget::new();
        mirror.start(&target).unwrap();
        mirror.stage(&target, "design", "blocked", None).unwrap();
        mirror.wait(&target, &approval_wait()).unwrap();
        mirror.outcome(&target, "landed").unwrap();
        mirror.outcome(&target, "withdrawn").unwrap();
        assert_eq!(target.calls(), []);
        let writes = read_mirror_writes(root.path()).unwrap();
        assert_eq!(writes.len(), 5);
        for write in &writes {
            assert_eq!(write.outcome, MirrorWriteOutcome::Failed, "{write:?}");
            assert_eq!(write.code.as_deref(), Some("mirror_bindings_unavailable"));
        }
        assert_eq!(
            writes[0].action,
            MirrorAction::Comment {
                body: "Engineering started by saeed. Run: file:///tmp/run_x".to_owned()
            }
        );

        // A bound value missing from a hand-edited snapshot fails the same way.
        let mut partial = engineering_bindings();
        partial.remove("ownedState");
        let other = run_root();
        let mirror = Mirror::new(
            sketch_block(),
            "AUTO-143".to_owned(),
            Ok(partial),
            mirror_run(other.path()),
        );
        mirror.outcome(&target, "withdrawn").unwrap();
        assert_eq!(target.calls(), []);
        assert_eq!(
            record_json(&other, "0001-outcome.json")["code"],
            "mirror_bindings_unavailable"
        );
    }

    /// The Cloud run page a run's record names as its `external.url`.
    const RUN_PAGE: &str = "https://usefulmachinery.test/acme/runs/run_x";

    /// The sketch's mirror for a run whose record has `external_url`, which `run.url` and
    /// `run.reference` then are, as `mirror_for_run` composes them.
    fn external_mirror(
        root: &tempfile::TempDir,
        bindings: Result<BTreeMap<String, String>, MirrorFailure>,
        external_url: Option<&str>,
    ) -> Mirror {
        let mut run = mirror_run(root.path());
        if let Some(url) = external_url {
            url.clone_into(&mut run.url);
            url.clone_into(&mut run.reference);
        }
        run.external_url = external_url.map(str::to_owned);
        Mirror::new(sketch_block(), "AUTO-143".to_owned(), bindings, run)
    }

    /// A target whose issue holds the comments `bodies`.
    fn commented(bodies: &[&str]) -> FakeTarget {
        FakeTarget {
            comments: Ok(bodies.iter().map(|body| (*body).to_owned()).collect()),
            ..FakeTarget::new()
        }
    }

    /// The sketch's start comment for a run whose `run.url` is `url`.
    fn start_text(url: &str) -> String {
        format!("Engineering started by saeed. Run: {url}")
    }

    // REQ-LOCALRUN-216: a comment holding the run's external URL, as Linear returns it or
    // with its backslashes removed, leaves the start comment unwritten and recorded
    // `skipped` `start_announced` with the body it would have written.
    #[test]
    fn a_start_comment_is_skipped_when_the_issue_already_links_the_run() {
        for announced in [
            format!("Cloud started the run: {RUN_PAGE}"),
            "Cloud started [run\\_x](https://usefulmachinery.test/acme/runs/run\\_x).".to_owned(),
        ] {
            let root = run_root();
            let mirror = external_mirror(&root, Ok(engineering_bindings()), Some(RUN_PAGE));
            let target = commented(&["An earlier note.", &announced]);
            mirror.start(&target).unwrap();
            assert_eq!(
                target.calls(),
                [Call::Comments("AUTO-143".to_owned())],
                "{announced}"
            );
            let write = record_json(&root, "0001-start.json");
            assert_eq!(write["hook"], "start", "{write}");
            assert_eq!(write["outcome"], "skipped", "{write}");
            assert_eq!(write["reason"], "start_announced", "{write}");
            assert_eq!(
                write["action"],
                json!({"kind": "comment", "body": start_text(RUN_PAGE)})
            );
            assert_eq!(read_mirror_writes(root.path()).unwrap().len(), 1);
        }
    }

    // REQ-LOCALRUN-216: comments that do not hold the URL, or a read that fails, leave the
    // start comment to be written as without the rule, and nothing of the read is recorded.
    #[test]
    fn a_start_comment_is_written_when_no_comment_links_the_run_or_none_can_be_read() {
        for target in [
            commented(&[&start_text("https://usefulmachinery.test/acme/runs/run_y")]),
            FakeTarget {
                comments: Err(MirrorFailure::unavailable(
                    "Linear answered IssueComments with HTTP 500.",
                )),
                ..FakeTarget::new()
            },
        ] {
            let root = run_root();
            let mirror = external_mirror(&root, Ok(engineering_bindings()), Some(RUN_PAGE));
            mirror.start(&target).unwrap();
            assert_eq!(
                target.calls(),
                [
                    Call::Comments("AUTO-143".to_owned()),
                    Call::Comment("AUTO-143".to_owned(), start_text(RUN_PAGE)),
                ]
            );
            let write = record_json(&root, "0001-start.json");
            assert_eq!(write["outcome"], "written", "{write}");
            assert_eq!(
                write.as_object().unwrap().keys().collect::<Vec<_>>(),
                ["action", "at", "hook", "issue", "outcome", "schemaVersion"]
            );
        }
    }

    // REQ-LOCALRUN-216: a run without an external URL, or with an empty one, and every other
    // hook never ask for the comments, and a mirror without its bindings asks nothing.
    #[test]
    fn only_the_start_of_a_run_with_an_external_url_and_its_bindings_reads_the_comments() {
        for external_url in [None, Some("")] {
            let root = run_root();
            let mirror = external_mirror(&root, Ok(engineering_bindings()), external_url);
            let target = commented(&[&start_text(RUN_PAGE)]);
            mirror.start(&target).unwrap();
            let body = start_text(external_url.unwrap_or("file:///tmp/run_x"));
            assert_eq!(
                target.calls(),
                [Call::Comment("AUTO-143".to_owned(), body)],
                "{external_url:?}"
            );
            assert_eq!(record_json(&root, "0001-start.json")["outcome"], "written");
        }

        let root = run_root();
        let mirror = external_mirror(&root, Ok(engineering_bindings()), Some(RUN_PAGE));
        let target = commented(&[&start_text(RUN_PAGE)]);
        mirror.stage(&target, "design", "blocked", None).unwrap();
        mirror.wait(&target, &approval_wait()).unwrap();
        assert_eq!(
            target.calls(),
            [
                Call::Comment("AUTO-143".to_owned(), "design: blocked".to_owned()),
                Call::Comment(
                    "AUTO-143".to_owned(),
                    "Waiting for approval: review".to_owned()
                ),
            ]
        );

        let root = run_root();
        let mirror = external_mirror(
            &root,
            Err(MirrorFailure::bindings_unavailable(
                "definition_record_invalid: bindings snapshot does not match its digest",
            )),
            Some(RUN_PAGE),
        );
        let target = commented(&[&start_text(RUN_PAGE)]);
        mirror.start(&target).unwrap();
        assert_eq!(target.calls(), []);
        let write = record_json(&root, "0001-start.json");
        assert_eq!(write["outcome"], "failed", "{write}");
        assert_eq!(write["code"], "mirror_bindings_unavailable", "{write}");
    }

    // REQ-LOCALRUN-193
    #[test]
    fn a_wait_payload_summary_names_the_wait_and_a_fan_out_stage_its_position() {
        let root = run_root();
        let mirror = mirror(&root, Ok(engineering_bindings()));
        let target = FakeTarget::new();
        let mut answer = approval_wait();
        answer.kind = WaitKind::Answer;
        answer
            .payload
            .insert("summary".to_owned(), json!("two questions on scope"));
        mirror.wait(&target, &answer).unwrap();
        let mut element = approval_wait();
        element.payload.insert("summary".to_owned(), json!(3));
        element.step.push(json!({"item": 2}));
        mirror.wait(&target, &element).unwrap();
        assert_eq!(
            target.calls(),
            [
                Call::Comment(
                    "AUTO-143".to_owned(),
                    "Waiting for answer: two questions on scope".to_owned()
                ),
                Call::Comment(
                    "AUTO-143".to_owned(),
                    "Waiting for approval: review[2]".to_owned()
                ),
            ]
        );
    }

    // REQ-WFFORMAT-101
    #[test]
    fn templates_parse_exactly_lowercase_dotted_placeholders() {
        let template = Template::parse("Engineering started by {{operator}}. Run: {{run.url}}")
            .expect("the sketch's onStart template parses");
        assert_eq!(
            template.segments(),
            [
                TemplateSegment::Literal("Engineering started by ".to_owned()),
                TemplateSegment::Variable("operator".to_owned()),
                TemplateSegment::Literal(". Run: ".to_owned()),
                TemplateSegment::Variable("run.url".to_owned()),
            ]
        );
        assert_eq!(
            Template::parse("{{operator}}")
                .unwrap()
                .variables()
                .collect::<Vec<_>>(),
            ["operator"]
        );
        for malformed in [
            "{{ operator }}",
            "{{Operator}}",
            "{{}}",
            "{{operator",
            "a {{1x}}",
        ] {
            assert!(Template::parse(malformed).is_err(), "{malformed}");
        }
    }

    // REQ-WFFORMAT-101
    #[test]
    fn guards_parse_to_the_operator_and_binding_key() {
        assert_eq!(
            MirrorGuard::parse("issue.state != $ownedState"),
            Ok(MirrorGuard {
                operator: MirrorGuardOperator::NotEqual,
                binding: "ownedState".to_owned(),
            })
        );
        assert_eq!(
            MirrorGuard::parse("issue.state == $ownedState"),
            Ok(MirrorGuard {
                operator: MirrorGuardOperator::Equal,
                binding: "ownedState".to_owned(),
            })
        );
        assert_eq!(
            MirrorGuard::parse("issue.state != \"Engineering\""),
            Err(MirrorGuardError::QuotedLiteral)
        );
        for invalid in [
            "issue.state = $ownedState",
            "issue.team != $ownedState",
            "issue.state != ownedState",
            "issue.state != $Owned",
            "issue.state!=$ownedState",
            "issue.state != $ownedState and true",
        ] {
            assert_eq!(
                MirrorGuard::parse(invalid),
                Err(MirrorGuardError::Invalid),
                "{invalid}"
            );
        }
    }

    // REQ-WFDEF-37: a guard takes a `$<id>.<name>` binding, the integration before the last
    // `.`, and passes its key on as written.
    #[test]
    fn guards_take_an_integration_binding_as_written() {
        assert_eq!(
            MirrorGuard::parse("issue.state != $linear.ownedState"),
            Ok(MirrorGuard {
                operator: MirrorGuardOperator::NotEqual,
                binding: "linear.ownedState".to_owned(),
            })
        );
        for key in ["linear.canceled", "gitlab.com.group"] {
            assert_eq!(
                MirrorGuard::parse(&format!("issue.state == ${key}")).map(|guard| guard.binding),
                Ok(key.to_owned())
            );
        }
        for key in ["$linear.", "$.canceled", "$linear.Canceled"] {
            assert_eq!(
                MirrorGuard::parse(&format!("issue.state == {key}")),
                Err(MirrorGuardError::Invalid),
                "{key}"
            );
        }
    }
}
