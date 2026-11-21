use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};

use super::claude_code::ClaudeCodeConfig;
use super::codex::CodexConfig;
use super::condition::ResolvedPredicate;
use super::document::{FailurePolicy, FinalizationTrigger, Output, RootBlocks, WaitKind, WaitRule};
use super::evidence::Prerequisite;
use super::pi::PiConfig;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum WorkflowValueType {
    Text,
    AttachmentCollection,
    Json,
    File,
    GitBranch,
}

/// Every declared root input with its value type; it types `inputs.<name>` references.
pub(crate) type DeclaredInputs = BTreeMap<String, WorkflowValueType>;

/// A declared input's default (REQ-WFFORMAT-28). A JSON default file holds its canonical
/// source-root-relative path once resolved.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ValidatedInputDefault {
    Text(String),
    JsonLiteral(serde_json::Value),
    JsonFile(String),
}

#[derive(
    Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowNodeRole {
    Step,
    Finalizer,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkflowNode {
    pub(crate) id: String,
    pub(crate) role: WorkflowNodeRole,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidatedWorkflow {
    pub(crate) schema_version: u8,
    pub(crate) description: Option<String>,
    pub(crate) environment_passthrough: BTreeSet<String>,
    pub steps: BTreeMap<String, ValidatedStep>,
    pub recoveries: BTreeMap<String, Option<ValidatedStepRecovery>>,
    pub source_order: Vec<String>,
    pub presentation_order: Vec<String>,
    pub finalizers: BTreeMap<String, ValidatedFinalizer>,
    pub finalizer_source_order: Vec<String>,
    pub finalizer_presentation_order: Vec<String>,
    /// Each export's sources in written order (REQ-WFFORMAT-65, REQ-WFFORMAT-107).
    pub exports: BTreeMap<String, OutputSources>,
    pub export_presentation: BTreeMap<String, ValidatedExportPresentation>,
    /// The declared outcomes and their source (REQ-WFFORMAT-87, REQ-WFFORMAT-88); `None`
    /// leaves the implicit outcomes `succeeded` and `failed`.
    pub outcome: Option<ValidatedOutcome>,
    pub(crate) declared_inputs: DeclaredInputs,
    /// Declared inputs marked `optional: true` (REQ-WFFORMAT-84).
    pub(crate) optional_inputs: BTreeSet<String>,
    pub(crate) input_defaults: BTreeMap<String, ValidatedInputDefault>,
    pub(crate) input_json_schema_paths: BTreeMap<String, String>,
    pub(crate) input_file_media_types: BTreeMap<String, String>,
    /// Profiles replaced at invocation time, with their pinned effective configuration.
    pub(crate) replaced_agent_profiles: BTreeMap<String, ValidatedHarness>,
    /// The root workflow's definition name, a lowercase identifier (REQ-WFFORMAT-7).
    pub name: Option<String>,
    /// A label for people (REQ-WFFORMAT-7).
    pub version: Option<String>,
    /// The declared `text` root input whose value is a run's subject (REQ-WFFORMAT-100).
    pub subject: Option<String>,
    /// The root-only blocks (REQ-WFFORMAT-82); a run acts on none of them but `mirror`
    /// (REQ-WFFORMAT-86) and, through admission, `requires` (REQ-WFEXEC-55, REQ-WFEXEC-78).
    pub root: Option<RootBlocks>,
    /// How the ordinary steps are started (REQ-WFFORMAT-93).
    pub scheduling: Scheduling,
}

/// A workflow without `workflow` steps starts a step when its prerequisites are met; one
/// with them is edge-scheduled from its first step (REQ-WFFORMAT-93).
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Scheduling {
    Dataflow,
    Edges { entry: String },
}

impl ValidatedWorkflow {
    /// The step or finalizer named `id`; the two share one namespace.
    pub fn node(&self, id: &str) -> Option<&ValidatedStep> {
        self.steps
            .get(id)
            .or_else(|| self.finalizers.get(id).map(|finalizer| &finalizer.body))
    }

    /// The workflow's root `environmentPassthrough` names (REQ-WFFORMAT-102); a node's own
    /// list can only narrow it.
    pub fn environment_passthrough(&self) -> &BTreeSet<String> {
        &self.environment_passthrough
    }

    /// Whether a run may omit the declared input, because it is optional or defaulted.
    pub(crate) fn input_may_be_omitted(&self, name: &str) -> bool {
        self.optional_inputs.contains(name) || self.input_defaults.contains_key(name)
    }
}

/// A workflow's declared outcomes: `source` holds the `text` outputs of required ordinary
/// steps that `outcome.from` names, in written order, a list of one for a single reference,
/// whose step then declares no `condition` (REQ-WFFORMAT-88, REQ-WFFORMAT-108); `on_failure`
/// is one of `names`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidatedOutcome {
    pub names: Vec<String>,
    pub source: OutputSources,
    pub on_failure: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidatedExportPresentation {
    pub title: Option<ValidatedPresentationField>,
    pub description: Option<ValidatedPresentationField>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ValidatedPresentationField {
    Literal(String),
    Reference(ResolvedOutputSource),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidatedFinalizer {
    pub body: ValidatedStep,
    pub(crate) when: BTreeSet<FinalizationTrigger>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidatedStepRecovery {
    pub(crate) retries: u8,
    pub handler: Option<ValidatedRecoveryHandler>,
}

// Source and validated handlers deliberately remain separate: validation pins an agent
// harness, while source syntax must not carry one. Sharing the type would blur that boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ValidatedRecoveryHandler {
    Command {
        argv: Vec<String>,
        cwd: Option<String>,
        environment_passthrough: Option<BTreeSet<String>>,
    },
    Agent {
        profile: String,
        prompt: String,
        cwd: Option<String>,
        harness: ValidatedHarness,
        environment_passthrough: Option<BTreeSet<String>>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ValidatedStep {
    Command(ValidatedCommandStep),
    Agent(ValidatedAgentStep),
    Workflow(ValidatedWorkflowStep),
}

impl ValidatedStep {
    pub(crate) fn common(&self) -> &ValidatedCommonStep {
        match self {
            Self::Command(step) => &step.common,
            Self::Agent(step) => &step.common,
            Self::Workflow(step) => &step.common,
        }
    }

    pub(crate) fn common_mut(&mut self) -> &mut ValidatedCommonStep {
        match self {
            Self::Command(step) => &mut step.common,
            Self::Agent(step) => &mut step.common,
            Self::Workflow(step) => &mut step.common,
        }
    }
}

/// A `workflow` step (REQ-WFFORMAT-89 to REQ-WFFORMAT-92). `common.outputs` holds the
/// derived outputs, `outcome` included, and `common.prerequisites` is empty: edges, not
/// dataflow, order an edge-scheduled run.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidatedWorkflowStep {
    pub common: ValidatedCommonStep,
    /// The child workflow file, source-root-relative.
    pub workflow: String,
    /// The canonical paths the child's resolution reached, the child file included, whose
    /// bytes a continuation compares for an inherited step (REQ-CONTINUE-12).
    pub(crate) child_paths: BTreeSet<String>,
    pub(crate) inputs: BTreeMap<String, ValidatedChildInput>,
    pub(crate) edges: BTreeMap<String, ValidatedEdgeTarget>,
    pub(crate) for_each: Option<ValidatedForEach>,
}

/// A validated `forEach` (REQ-WFFORMAT-98): `items` a `json` root input or a `json`
/// export of an earlier `workflow` step, and `empty` one of the child's outcomes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ValidatedForEach {
    pub(crate) items: ResolvedValueReference,
    pub(crate) empty: Option<String>,
}

/// A mapped child input; `may_be_omitted` when the child declares it optional or
/// defaulted, so an unavailable value omits it rather than failing the step.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ValidatedChildInput {
    pub(crate) reference: ResolvedValueReference,
    pub(crate) may_be_omitted: bool,
}

/// A validated `on` target (REQ-WFFORMAT-92): a step of the workflow, which may be the
/// producer itself or an earlier step, an exit, a limited edge or a wait.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ValidatedEdgeTarget {
    Step(String),
    Exit(String),
    /// `{then, limit, else}` (REQ-WFFORMAT-96): `then` a step, `else_` never limited.
    Limited {
        then: String,
        limit: u32,
        else_: Box<ValidatedEdgeTarget>,
    },
    Wait {
        kind: WaitKind,
        then: Box<ValidatedEdgeTarget>,
        /// The step a hand-back continues at; present exactly for `approval`.
        on_hand_back: Option<String>,
        /// The authored rule, present exactly for `external` (REQ-WFFORMAT-97), its `where`
        /// parsed and its `inputs.` and `outputs.` operands checked.
        rule: Option<WaitRule>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidatedCommandStep {
    pub common: ValidatedCommonStep,
    pub(crate) inputs: BTreeMap<String, ResolvedValueReference>,
    pub(crate) argv: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidatedAgentStep {
    pub common: ValidatedCommonStep,
    pub agent: ValidatedAgent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ResolvedDirectPrerequisite {
    pub(crate) producer: String,
    pub(crate) control: bool,
    pub(crate) disposition_control: bool,
    pub(crate) data: bool,
    pub(crate) condition_data: bool,
    /// The producer is a source of a `firstOf` in the consumer's body (REQ-WFFORMAT-39),
    /// independent of `data`: a source may be skipped and passed over, so it is never a
    /// data prerequisite unless the consumer also names it by a single reference.
    pub(crate) first_of: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidatedCommonStep {
    pub failure_policy: FailurePolicy,
    pub(crate) condition: Option<ResolvedPredicate>,
    pub(crate) condition_values: BTreeMap<String, ResolvedValueSource>,
    pub(crate) prerequisites: Vec<ResolvedDirectPrerequisite>,
    pub(crate) evidence_prerequisites: Vec<Prerequisite>,
    pub(crate) cwd: Option<String>,
    /// The node's own `environmentPassthrough`, each name one the root declares; `None`
    /// when the node declares none (REQ-WFFORMAT-102).
    pub(crate) environment_passthrough: Option<BTreeSet<String>>,
    pub(crate) outputs: BTreeMap<String, ValidatedOutput>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ResolvedValueReference {
    pub(crate) source: ResolvedValueSource,
    pub(crate) value_type: WorkflowValueType,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ResolvedValueSource {
    Input(String),
    Output(ResolvedOutputSource),
    FinalizationContext,
    /// `wait.<field>`, a `workflow` step's input only (REQ-WFFORMAT-95).
    Wait(String),
    /// `controls.answers`, a `workflow` step's input only (REQ-WFFORMAT-95).
    ControlsAnswers,
    /// `item`, or `item.<field>` with `field` set: the element a `forEach` step's child
    /// runs for, a `workflow` step's input only (REQ-WFFORMAT-99).
    Item {
        field: Option<String>,
    },
    /// A `firstOf`, its sources in the order they are tried; an ordinary step's `inputs`
    /// and agent message only (REQ-WFFORMAT-105). The reference's `value_type` is the kind
    /// every source shares (REQ-WFFORMAT-106).
    FirstOf(Vec<ResolvedOutputSource>),
}

impl ResolvedValueSource {
    /// The step outputs the value may be taken from: its output reference, every source of a
    /// `firstOf`, and none for a value that names no output.
    pub(crate) fn output_sources(&self) -> &[ResolvedOutputSource] {
        match self {
            Self::Output(output) => std::slice::from_ref(output),
            Self::FirstOf(sources) => sources,
            Self::Input(_)
            | Self::FinalizationContext
            | Self::Wait(_)
            | Self::ControlsAnswers
            | Self::Item { .. } => &[],
        }
    }
}

/// The list of an agent step's message an entry belongs to.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum MessageList {
    Text,
    Attachments,
}

/// Where a `firstOf` is bound in an ordinary step's body (REQ-WFFORMAT-105): a command
/// step's input by name, or an agent message entry by list and zero-based index. The order
/// is inputs by name, then text entries, then attachments, each by index.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum FirstOfBinding {
    Input(String),
    Message { list: MessageList, index: usize },
}

/// The source each `firstOf` of a step's body took when the step was started
/// (REQ-WFCOND-45, INV-ENGINE-44).
pub(crate) type ChosenSources = BTreeMap<FirstOfBinding, ResolvedOutputSource>;

impl ValidatedStep {
    /// Each `firstOf` in the step's body with its sources in written order; empty for a
    /// `workflow` step, whose body never holds one.
    pub(crate) fn first_of_bindings(&self) -> BTreeMap<FirstOfBinding, &[ResolvedOutputSource]> {
        match self {
            Self::Command(command) => command
                .inputs
                .iter()
                .filter_map(|(name, reference)| match &reference.source {
                    ResolvedValueSource::FirstOf(sources) => {
                        Some((FirstOfBinding::Input(name.clone()), sources.as_slice()))
                    }
                    _ => None,
                })
                .collect(),
            Self::Agent(agent) => {
                let message = &agent.agent.message;
                [
                    (MessageList::Text, &message.text),
                    (MessageList::Attachments, &message.attachments),
                ]
                .into_iter()
                .flat_map(|(list, entries)| {
                    entries
                        .iter()
                        .enumerate()
                        .filter_map(move |(index, entry)| match entry {
                            ValidatedMessageSource::Reference {
                                source: ResolvedValueSource::FirstOf(sources),
                                ..
                            } => {
                                Some((FirstOfBinding::Message { list, index }, sources.as_slice()))
                            }
                            _ => None,
                        })
                })
                .collect()
            }
            Self::Workflow(_) => BTreeMap::new(),
        }
    }
}

/// The single source a `firstOf` bound at `binding` resolves to: the chosen source, when
/// the list holds it. Anything else leaves the `firstOf` in place, which every reader of a
/// body refuses as an unavailable input.
fn chosen_source(
    binding: &FirstOfBinding,
    source: &ResolvedValueSource,
    chosen: &ChosenSources,
) -> Option<ResolvedValueSource> {
    let ResolvedValueSource::FirstOf(sources) = source else {
        return None;
    };
    chosen
        .get(binding)
        .filter(|choice| sources.contains(choice))
        .cloned()
        .map(ResolvedValueSource::Output)
}

impl ValidatedCommandStep {
    /// The step with each `firstOf` input replaced by the source the reducer chose for it, so
    /// the input is materialized as a single reference's (INV-ENGINE-44).
    pub(crate) fn with_chosen_sources(&self, chosen: &ChosenSources) -> Cow<'_, Self> {
        if chosen.is_empty() {
            return Cow::Borrowed(self);
        }
        let mut resolved = self.clone();
        for (name, reference) in &mut resolved.inputs {
            if let Some(source) = chosen_source(
                &FirstOfBinding::Input(name.clone()),
                &reference.source,
                chosen,
            ) {
                reference.source = source;
            }
        }
        Cow::Owned(resolved)
    }
}

impl ValidatedAgentStep {
    /// The step with each `firstOf` message entry replaced by the source the reducer chose
    /// for it, so the entry is read as a single reference's (INV-ENGINE-44).
    pub(crate) fn with_chosen_sources(&self, chosen: &ChosenSources) -> Cow<'_, Self> {
        if chosen.is_empty() {
            return Cow::Borrowed(self);
        }
        let mut resolved = self.clone();
        let message = &mut resolved.agent.message;
        for (list, entries) in [
            (MessageList::Text, &mut message.text),
            (MessageList::Attachments, &mut message.attachments),
        ] {
            for (index, entry) in entries.iter_mut().enumerate() {
                if let ValidatedMessageSource::Reference { source, .. } = entry
                    && let Some(chosen) =
                        chosen_source(&FirstOfBinding::Message { list, index }, source, chosen)
                {
                    *source = chosen;
                }
            }
        }
        Cow::Owned(resolved)
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ResolvedOutputSource {
    pub(crate) node: WorkflowNode,
    pub output: String,
    pub(crate) value_type: WorkflowValueType,
}

impl ResolvedOutputSource {
    pub(crate) fn reference(&self) -> String {
        format!("outputs.{}.{}", self.node.id, self.output)
    }
}

/// The sources an export or the outcome takes its value from, in written order: one for a
/// single reference, two to sixteen for a `firstOf` (REQ-WFFORMAT-107, REQ-WFFORMAT-108). It
/// is never empty, and every source has the kind of the first (REQ-WFFORMAT-106).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutputSources {
    first: ResolvedOutputSource,
    rest: Vec<ResolvedOutputSource>,
}

impl OutputSources {
    /// The one source a single reference names.
    pub(crate) fn single(source: ResolvedOutputSource) -> Self {
        Self {
            first: source,
            rest: Vec::new(),
        }
    }

    /// The sources in written order; `None` for an empty list.
    pub(crate) fn from_list(sources: Vec<ResolvedOutputSource>) -> Option<Self> {
        let mut sources = sources.into_iter();
        let first = sources.next()?;
        Some(Self {
            first,
            rest: sources.collect(),
        })
    }

    /// The first source in written order, which is the only one of a single reference.
    pub fn first(&self) -> &ResolvedOutputSource {
        &self.first
    }

    /// Every source in written order.
    pub fn iter(&self) -> impl Iterator<Item = &ResolvedOutputSource> + Clone {
        std::iter::once(&self.first).chain(&self.rest)
    }

    /// The kind every source shares, which is the value's type.
    pub fn value_type(&self) -> WorkflowValueType {
        self.first.value_type
    }

    /// The source the value resolves to once the run has ended (INV-ENGINE-45): the source
    /// the choice takes, or else the first that neither succeeded nor was skipped, or the
    /// first source when every source was skipped. `None` while a source is not terminal.
    pub(crate) fn settled_source(
        &self,
        ended: impl Fn(&ResolvedOutputSource) -> SourceEnd,
    ) -> Option<&ResolvedOutputSource> {
        match choose_first_of(self.iter(), ended) {
            FirstOfChoice::Waiting => None,
            FirstOfChoice::Chosen(source) => Some(source),
            FirstOfChoice::Blocked(sources) => {
                Some(sources.first().copied().unwrap_or(&self.first))
            }
        }
    }
}

/// How the step of a `firstOf` source stands, as the choice reads it (INV-ENGINE-44).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SourceEnd {
    /// The step has not reached a terminal state.
    Pending,
    /// The step succeeded or was inherited as succeeded.
    Succeeded,
    /// The step was skipped or inherited as skipped.
    Skipped,
    /// The step ended any other way: failed, blocked, not run or cancelled.
    Unsupplied,
}

/// How a `firstOf` stands, from the node states of its sources alone (REQ-WFCOND-45,
/// REQ-WFCOND-46, INV-ENGINE-44).
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum FirstOfChoice<'a> {
    /// A source has not reached a terminal state.
    Waiting,
    /// Every source is terminal, none ended other than succeeded or skipped, and this is the
    /// first in written order that succeeded or was inherited as succeeded.
    Chosen(&'a ResolvedOutputSource),
    /// The sources that leave the `firstOf` without a value, in written order: each that
    /// ended failed, blocked, not run or cancelled, as soon as one has, or every source when
    /// all of them were skipped.
    Blocked(Vec<&'a ResolvedOutputSource>),
}

/// The choice of a `firstOf` (INV-ENGINE-44), the one rule a step's body, an export and the
/// archived view apply; `ended` answers how each source's step stands.
pub(crate) fn choose_first_of<'a, Sources>(
    sources: Sources,
    ended: impl Fn(&ResolvedOutputSource) -> SourceEnd,
) -> FirstOfChoice<'a>
where
    Sources: IntoIterator<Item = &'a ResolvedOutputSource>,
    Sources::IntoIter: Clone,
{
    let sources = sources.into_iter();
    let unsupplied = sources
        .clone()
        .filter(|source| ended(source) == SourceEnd::Unsupplied)
        .collect::<Vec<_>>();
    if !unsupplied.is_empty() {
        return FirstOfChoice::Blocked(unsupplied);
    }
    if sources
        .clone()
        .any(|source| ended(source) == SourceEnd::Pending)
    {
        return FirstOfChoice::Waiting;
    }
    sources
        .clone()
        .find(|source| ended(source) == SourceEnd::Succeeded)
        .map_or_else(
            || FirstOfChoice::Blocked(sources.collect()),
            FirstOfChoice::Chosen,
        )
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ValidatedOutput {
    pub(crate) definition: Output,
    pub(crate) value_type: WorkflowValueType,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidatedAgent {
    pub(crate) profile: String,
    pub(crate) system_prompt: String,
    pub(crate) message: ValidatedAgentMessage,
    pub harness: ValidatedHarness,
}

impl ValidatedAgent {
    /// The name of the agent profile the node selects.
    pub fn profile_name(&self) -> &str {
        &self.profile
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ValidatedHarness {
    Pi(PiConfig),
    ClaudeCode(ClaudeCodeConfig),
    Codex(CodexConfig),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ValidatedAgentMessage {
    pub(crate) text: Vec<ValidatedMessageSource>,
    pub(crate) attachments: Vec<ValidatedMessageSource>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ValidatedMessageSource {
    File {
        path: String,
    },
    Reference {
        source: ResolvedValueSource,
        value_type: WorkflowValueType,
    },
}
