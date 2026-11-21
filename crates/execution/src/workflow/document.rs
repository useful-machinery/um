use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

use super::admission::{CLAUDE_CODE_LOGIN_VARIABLE, CODEX_LOGIN_VARIABLE};
use super::condition::TerminalDisposition;
use super::mirror::MirrorBlock;
use super::validated::{ValidatedHarness, WorkflowValueType};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WorkflowDocument {
    pub(crate) schema_version: u8,
    pub(crate) description: Option<String>,
    pub(crate) environment_passthrough: BTreeSet<String>,
    pub(crate) inputs: BTreeMap<String, InputDeclaration>,
    pub(crate) agent_profiles: BTreeMap<String, AgentProfile>,
    pub(crate) steps: BTreeMap<String, StepDefinition>,
    pub(crate) step_order: Vec<String>,
    pub(crate) finalizers: BTreeMap<String, FinalizerDefinition>,
    pub(crate) finalizer_order: Vec<String>,
    pub(crate) exports: BTreeMap<String, ExportDefinition>,
    /// The declared outcome names (REQ-WFFORMAT-87); empty when the workflow declares none.
    pub(crate) outcomes: Vec<String>,
    pub(crate) outcome: Option<OutcomeDeclaration>,
    pub(crate) name: Option<String>,
    pub(crate) version: Option<String>,
    /// The root input whose value is a run's subject (REQ-WFFORMAT-100).
    pub(crate) subject: Option<String>,
    pub(crate) root: Option<RootBlocks>,
    /// The outputs of each `workflow` step, derived from its child's summary when the
    /// document is validated (REQ-WFFORMAT-91); decoding leaves it empty.
    pub(crate) derived_outputs: BTreeMap<String, BTreeMap<String, Output>>,
}

/// The root-only blocks of a workflow file (REQ-WFFORMAT-82). A run acts on none of them
/// but `mirror` (REQ-WFFORMAT-86) and, through admission, `requires` (REQ-WFEXEC-55,
/// REQ-WFEXEC-78); an absent block is empty, since the schema admits no empty block.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RootBlocks {
    pub programs: Vec<String>,
    pub exclude: Vec<String>,
    pub requires: Option<RootRequires>,
    pub bindings: BTreeMap<String, Binding>,
    pub triggers: BTreeMap<String, Trigger>,
    pub mirror: Option<MirrorBlock>,
}

impl RootBlocks {
    /// The blocks a workflow file other than a tree's root may not carry: all of these but
    /// a `requires` that holds no `tools`, the override of REQ-WFDEF-35 (REQ-WFFORMAT-89).
    pub(crate) fn without_override(&self) -> Self {
        let mut blocks = self.clone();
        if blocks
            .requires
            .as_ref()
            .is_some_and(|requires| requires.tools.is_empty())
        {
            blocks.requires = None;
        }
        blocks
    }
}

/// The `requires` block: the tools admission probes, and the integrations the workflow
/// declares with whose credentials act for them, kept as declared (REQ-WFDEF-34).
/// Admission probes `tools` alone and keeps the environment names of each declared
/// integration's id for every step (REQ-WFEXEC-78); the engine reads no `as` or `bindings`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RootRequires {
    pub tools: Vec<String>,
    /// The block's `as`, the default for every integration; `None` when not written.
    pub acting_as: Option<IntegrationAs>,
    /// The declared integrations by provider id; empty when the block names none.
    pub integrations: BTreeMap<IntegrationProvider, Integration>,
}

/// A provider id of `requires.integrations`: the schema's closed list (REQ-WFDEF-34). The
/// variants are declared in the bytewise order of their ids, so a map keyed by them iterates
/// as the ids sort.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub enum IntegrationProvider {
    #[serde(rename = "buildkite")]
    Buildkite,
    #[serde(rename = "claude_code")]
    ClaudeCode,
    #[serde(rename = "codex")]
    Codex,
    #[serde(rename = "github")]
    Github,
    #[serde(rename = "gitlab.com")]
    GitlabCom,
    #[serde(rename = "linear")]
    Linear,
    #[serde(rename = "mergify")]
    Mergify,
}

impl IntegrationProvider {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Buildkite => "buildkite",
            Self::ClaudeCode => "claude_code",
            Self::Codex => "codex",
            Self::Github => "github",
            Self::GitlabCom => "gitlab.com",
            Self::Linear => "linear",
            Self::Mergify => "mergify",
        }
    }

    /// The environment names a run delivers to every step for a declared integration
    /// (REQ-WFEXEC-78, INV-ENGINE-43): the token and its file a runner offers an OAuth
    /// provider's grant token under (REQ-GRANT-87), the variable a runner offers a coding
    /// agent's login as (REQ-GRANT-44, REQ-GRANT-37), and the token and its file a runner
    /// offers a token service's token under (REQ-INTEGRATION-12), the names Cloud lists for
    /// a reviewed token row. None is a name admission strips as runner-private.
    pub fn environment_names(self) -> &'static [&'static str] {
        match self {
            Self::Buildkite => &["BUILDKITE_API_TOKEN", "BUILDKITE_API_TOKEN_FILE"],
            Self::ClaudeCode => &[CLAUDE_CODE_LOGIN_VARIABLE],
            Self::Codex => &[CODEX_LOGIN_VARIABLE],
            Self::Github => &["GH_TOKEN", "GH_TOKEN_FILE"],
            Self::GitlabCom => &["GITLAB_TOKEN", "GITLAB_TOKEN_FILE"],
            Self::Linear => &[
                "USEFUL_LINEAR_ACCESS_TOKEN",
                "USEFUL_LINEAR_ACCESS_TOKEN_FILE",
            ],
            Self::Mergify => &["MERGIFY_TOKEN", "MERGIFY_TOKEN_FILE"],
        }
    }
}

/// Whose credentials act for an integration: a member's or the organization's
/// (REQ-WFDEF-34).
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum IntegrationAs {
    #[serde(rename = "member")]
    Member,
    #[serde(rename = "org")]
    Org,
}

/// One entry of `requires.integrations` (REQ-WFDEF-34).
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Integration {
    /// The entry's own `as`, overriding the block's; `None` when not written.
    pub acting_as: Option<IntegrationAs>,
    /// The entry's bindings by name; empty when it declares none.
    pub bindings: BTreeMap<String, IntegrationBinding>,
}

/// An integration binding (REQ-WFDEF-34): a kind its provider lists, which the schema
/// closes per provider before this is decoded, and an optional default.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IntegrationBinding {
    pub kind: String,
    pub default: Option<String>,
}

/// The key of a binding reference, the text after `$` (REQ-WFDEF-37): flat when it holds no
/// `.`, otherwise the integration before its last `.` and the binding's name after it, so a
/// provider id that holds a dot, such as `gitlab.com`, stays unambiguous. Every reader of a
/// reference splits it here, and `Display` joins it back: a name is an identifier and holds no
/// `.`, so the joined key splits to the same pair.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BindingKey<'a> {
    /// A key of the root's flat `bindings` block.
    Flat(&'a str),
    /// A binding declared under `requires.integrations.<integration>.bindings`.
    Integration { integration: &'a str, name: &'a str },
}

impl<'a> BindingKey<'a> {
    /// Splits `key` at its last `.`, whether or not it is well formed.
    pub fn split(key: &'a str) -> Self {
        match key.rsplit_once('.') {
            Some((integration, name)) => Self::Integration { integration, name },
            None => Self::Flat(key),
        }
    }

    /// The split of a well-formed key, the text the schema's `BindingReference` admits after
    /// `$`: an identifier, or a lowercase dotted integration name, `.` and an identifier.
    pub fn parse(key: &'a str) -> Option<Self> {
        let split = Self::split(key);
        let well_formed = match split {
            Self::Flat(key) => useful_support::is_identifier(key),
            Self::Integration { integration, name } => {
                useful_support::is_identifier(name)
                    && integration
                        .bytes()
                        .next()
                        .is_some_and(|byte| byte.is_ascii_lowercase())
                    && integration.bytes().all(|byte| {
                        byte.is_ascii_lowercase()
                            || byte.is_ascii_digit()
                            || matches!(byte, b'_' | b'.')
                    })
            }
        };
        well_formed.then_some(split)
    }
}

impl fmt::Display for BindingKey<'_> {
    /// The key as a reference writes it after `$`: the flat key, or `<integration>.<name>`.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Flat(key) => formatter.write_str(key),
            Self::Integration { integration, name } => write!(formatter, "{integration}.{name}"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Binding {
    pub kind: BindingKind,
    pub default: Option<BindingValue>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum BindingKind {
    #[serde(rename = "connection")]
    Connection,
    #[serde(rename = "linear.team")]
    LinearTeam,
    #[serde(rename = "linear.state")]
    LinearState,
    #[serde(rename = "github.repository")]
    GithubRepository,
    /// One organization member.
    #[serde(rename = "member")]
    Member,
    /// A list of organization members.
    #[serde(rename = "members")]
    Members,
    /// One workflow definition of the same project, named by a definition trigger
    /// (REQ-WFDEF-39).
    #[serde(rename = "definition")]
    Definition,
}

impl BindingKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Connection => "connection",
            Self::LinearTeam => "linear.team",
            Self::LinearState => "linear.state",
            Self::GithubRepository => "github.repository",
            Self::Member => "member",
            Self::Members => "members",
            Self::Definition => "definition",
        }
    }
}

/// A binding's default or bound value: a string for every kind but `members`, whose value is
/// a list (REQ-WFDEF-3, REQ-WFDEF-20). Serialized as written, a list as a JSON array.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(untagged)]
pub enum BindingValue {
    Text(String),
    List(Vec<String>),
}

impl BindingValue {
    /// Whether this value fits `kind`: a non-empty string, or for `members` a list of
    /// non-empty strings, empty included.
    pub fn fits(&self, kind: BindingKind) -> bool {
        match (self, kind) {
            (Self::List(items), BindingKind::Members) => items.iter().all(|item| !item.is_empty()),
            (Self::List(_), _) | (Self::Text(_), BindingKind::Members) => false,
            (Self::Text(text), _) => !text.is_empty(),
        }
    }

    /// The string of a value of any kind but `members`.
    pub fn as_text(&self) -> Option<&str> {
        match self {
            Self::Text(text) => Some(text),
            Self::List(_) => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Trigger {
    /// Starts a run of the workflow. No local command acts on `stop_before`, `owner`,
    /// `busy_behavior`, a schedule rule or a definition rule (REQ-WFDEF-30, REQ-WFDEF-31,
    /// REQ-WFDEF-33, REQ-WFDEF-39).
    Start {
        on: StartRule,
        subject: Option<String>,
        inputs: BTreeMap<String, String>,
        stop_before: Option<StopBefore>,
        owner: Option<OwnerDeclaration>,
        /// None when the trigger declares none, which reads as `skip`.
        busy_behavior: Option<BusyBehavior>,
    },
    /// Acts on the live run for the event's subject.
    Control {
        on: TriggerRule,
        control: TriggerControlAction,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TriggerRule {
    /// As written: a binding reference, `$` followed by the binding key, or the bare id of a
    /// declared integration (REQ-WFDEF-38).
    pub source: String,
    pub event: String,
    pub condition: Option<String>,
}

/// What a start trigger evaluates: a provider event, a timetable (REQ-WFDEF-31) or the end
/// of another definition's run (REQ-WFDEF-39).
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StartRule {
    Event(TriggerRule),
    /// A cron expression and its time zone, both as written.
    Schedule {
        cron: String,
        timezone: String,
    },
    /// A run of the bound definition that ended with one of `outcomes`.
    Definition {
        /// A binding reference as written, `$` followed by the binding key.
        definition: String,
        /// The outcome names in the order written; one name written alone is a list of one.
        outcomes: Vec<String>,
    },
}

/// A start trigger's `stopBefore` (REQ-WFDEF-30): the event's label list and the prefix
/// that, followed by a root step's name, stops the run before that step.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct StopBefore {
    /// An `event.<path>` as written.
    pub labels: String,
    pub prefix: String,
}

/// A start trigger's `busyBehavior` (REQ-WFDEF-33): what a start does while the subject's
/// slot is busy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BusyBehavior {
    Skip,
    QueueLatest,
}

impl BusyBehavior {
    /// The value as written.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Skip => "skip",
            Self::QueueLatest => "queueLatest",
        }
    }
}

/// A start trigger's `owner` (REQ-WFDEF-30), the short form read as `from` alone.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct OwnerDeclaration {
    pub from: OwnerSource,
    /// A binding reference of kind `members`, `$` followed by the binding key.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub only: Option<String>,
    /// A binding reference of kind `members`, `$` followed by the binding key.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub except: Option<String>,
}

/// Who a run belongs to, as written: serialized as the string or list the root declares.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(untagged)]
pub enum OwnerSource {
    /// One `event.<path>`, a provider user.
    Event(String),
    /// `event.<path>` values taken in order.
    Events(Vec<String>),
    /// A binding reference of kind `member`, `$` followed by the binding key.
    Binding(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TriggerControlAction {
    Named(String),
    Conditional {
        when: String,
        then: String,
        otherwise: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ExportDefinition {
    pub(crate) source: SourceReference,
    pub(crate) presentation: Option<ExportPresentation>,
}

/// An export's source or `outcome.from` as written: one output reference, or a `firstOf` of
/// two to sixteen distinct output references in the order they are tried (REQ-WFFORMAT-107,
/// REQ-WFFORMAT-108). Validation checks the two forms by different rules, so the list of one
/// a single reference resolves to is not written here.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SourceReference {
    Reference(OutputReference),
    FirstOf(Vec<OutputReference>),
}

/// The `outcome` block (REQ-WFFORMAT-88): the output that names a succeeded run's outcome
/// and the name a failed run takes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OutcomeDeclaration {
    pub(crate) from: SourceReference,
    pub(crate) on_failure: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ExportPresentation {
    pub(crate) title: Option<PresentationSource>,
    pub(crate) description: Option<PresentationSource>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PresentationSource {
    Literal(String),
    Reference(OutputReference),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum InputDeclaration {
    Text {
        optional: bool,
        default: Option<String>,
    },
    Json {
        schema: Option<String>,
        optional: bool,
        default: Option<JsonDefault>,
    },
    File {
        media_type: Option<String>,
    },
    Attachments {
        optional: bool,
    },
}

/// A JSON input's default: a file path relative to the workflow file, or the literal value
/// when the YAML value is not text (REQ-WFFORMAT-28).
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum JsonDefault {
    File(String),
    Literal(Value),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StepDefinition {
    pub(crate) body: NodeBody,
    pub(crate) control_dependencies: Vec<String>,
    pub(crate) recovery: Option<StepRecovery>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StepRecovery {
    pub(crate) retries: u8,
    pub(crate) handler: Option<RecoveryHandler>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RecoveryHandler {
    Command {
        argv: Vec<String>,
        cwd: Option<String>,
        environment_passthrough: Option<BTreeSet<String>>,
    },
    Agent {
        profile: String,
        prompt: String,
        cwd: Option<String>,
        environment_passthrough: Option<BTreeSet<String>>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FinalizerDefinition {
    pub(crate) body: NodeBody,
    pub(crate) after: Vec<String>,
    pub(crate) when: BTreeSet<FinalizationTrigger>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd)]
#[serde(rename_all = "snake_case")]
pub enum FinalizationTrigger {
    Succeeded,
    Failed,
    Cancelled,
}

impl FinalizationTrigger {
    pub(crate) fn all() -> BTreeSet<Self> {
        [Self::Succeeded, Self::Failed, Self::Cancelled]
            .into_iter()
            .collect()
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AgentProfile {
    pub(crate) harness: ValidatedHarness,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum NodeBody {
    Command(CommandNode),
    Agent(AgentNode),
    Workflow(WorkflowNode),
}

/// A step that runs another workflow file of the same tree as a child run
/// (REQ-WFFORMAT-89). It has no `CommonNode`: its outputs are derived from the child and
/// its edges, not dataflow, order it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WorkflowNode {
    pub(crate) workflow: String,
    pub(crate) inputs: BTreeMap<String, ValueReference>,
    pub(crate) edges: BTreeMap<String, EdgeTarget>,
    pub(crate) for_each: Option<ForEach>,
}

/// A `workflow` step's `forEach` (REQ-WFFORMAT-98): the list the step runs one child for
/// each element of, and the child outcome taken for an empty list. The reference string
/// form decodes with `empty` absent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ForEach {
    pub(crate) items: ValueReference,
    pub(crate) empty: Option<String>,
}

/// Where an `on` edge leads (REQ-WFFORMAT-92): a step, which may be the step itself or an
/// earlier one, an exit, a limited edge (REQ-WFFORMAT-96) or a wait. A wait names the step
/// a hand-back continues at in `on_hand_back`, required for `approval` and refused
/// otherwise, and carries `rule` exactly when it is `external` (REQ-WFFORMAT-97).
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum EdgeTarget {
    Step(String),
    Exit(String),
    /// `{then, limit, else}`: `then` as authored, which validation requires to be a step,
    /// and `else_` an unlimited target.
    Limited {
        then: String,
        limit: u32,
        else_: Box<EdgeTarget>,
    },
    Wait {
        kind: WaitKind,
        then: Box<EdgeTarget>,
        on_hand_back: Option<String>,
        rule: Option<WaitRule>,
    },
}

/// A wait's rule `{source, event, where}` (REQ-WFFORMAT-97, REQ-WFDEF-5): authored on an
/// `external` wait, derived for the local kinds. `event` is read as one name or a list and
/// always written as a list.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WaitRule {
    pub source: String,
    #[serde(deserialize_with = "one_or_many")]
    pub event: Vec<String>,
    #[serde(rename = "where")]
    pub where_: String,
}

pub(crate) fn one_or_many<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<String>, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum OneOrMany {
        One(String),
        Many(Vec<String>),
    }
    Ok(match OneOrMany::deserialize(deserializer)? {
        OneOrMany::One(event) => vec![event],
        OneOrMany::Many(events) => events,
    })
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WaitKind {
    Answer,
    Approval,
    Repair,
    Stop,
    External,
    Children,
}

impl WaitKind {
    /// The kind's name as the format and the records spell it (REQ-WFFORMAT-92).
    pub const fn name(self) -> &'static str {
        match self {
            Self::Answer => "answer",
            Self::Approval => "approval",
            Self::Repair => "repair",
            Self::Stop => "stop",
            Self::External => "external",
            Self::Children => "children",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CommandNode {
    pub(crate) common: CommonNode,
    pub(crate) inputs: BTreeMap<String, ValueReference>,
    pub(crate) argv: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AgentNode {
    pub(crate) common: CommonNode,
    pub(crate) agent: Agent,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FailurePolicy {
    #[default]
    Required,
    Advisory,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CommonNode {
    pub(crate) failure_policy: FailurePolicy,
    pub(crate) condition: Option<ConditionPredicate>,
    pub(crate) cwd: Option<String>,
    /// The node's own `environmentPassthrough` (REQ-WFFORMAT-102): `None` when absent, so
    /// the node receives every name the root declares, distinct from an empty list.
    pub(crate) environment_passthrough: Option<BTreeSet<String>>,
    pub(crate) outputs: BTreeMap<String, Output>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ConditionPredicate {
    All(Vec<ConditionPredicate>),
    Any(Vec<ConditionPredicate>),
    Not(Box<ConditionPredicate>),
    Equals([ConditionOperand; 2]),
    Exists(ConditionSelector),
    Disposition {
        node: String,
        is: TerminalDisposition,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ConditionOperand {
    Reference {
        reference: ValueReference,
        pointer: Option<String>,
    },
    Literal(Value),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ConditionSelector {
    pub(crate) reference: ValueReference,
    pub(crate) pointer: String,
}

/// A `{ref}` value. `Wait` (`wait.<field>`, a field of the control that resolved the wait
/// leading to the step) and `ControlsAnswers` (`controls.answers`, the answers recorded
/// for the step's questions) decode only in a `workflow` step's inputs (REQ-WFFORMAT-95),
/// as does `Item` (`item`, or `item.<field>` with `field` set), the element a `forEach`
/// step's child runs for (REQ-WFFORMAT-99). `FirstOf` (`{firstOf}`, two to sixteen distinct
/// output references in the order they are tried) decodes only in an ordinary step's
/// `inputs` and agent message (REQ-WFFORMAT-105).
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ValueReference {
    Input { name: String },
    Output(OutputReference),
    FinalizationContext,
    Wait { field: String },
    ControlsAnswers,
    Item { field: Option<String> },
    FirstOf(Vec<OutputReference>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OutputReference {
    pub(crate) node: String,
    pub(crate) output: String,
}

/// A declared output, or one a `workflow` step derives from its child: `ChildExport` is
/// a `text` or `json` export of the child and `ChildOutcome` the child's recorded outcome
/// (REQ-WFFORMAT-91).
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Output {
    TextPath { path: String },
    TextAgentResponse,
    JsonPath { path: String, schema: String },
    JsonAgentResult { schema: String },
    FilePath { path: String, media_type: String },
    GitBranchWorkspace,
    ChildExport { value_type: WorkflowValueType },
    ChildOutcome,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Agent {
    pub(crate) profile: String,
    pub(crate) system_prompt: String,
    pub(crate) message: AgentMessage,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AgentMessage {
    pub(crate) text: Vec<MessageSource>,
    pub(crate) attachments: Vec<MessageSource>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum MessageSource {
    File { path: String },
    Reference(ValueReference),
}
