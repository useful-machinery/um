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
    pub exports: BTreeMap<String, ResolvedOutputSource>,
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
    /// The root-only blocks (REQ-WFFORMAT-82); no run reads them.
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

/// A workflow's declared outcomes: `source` is a `text` output of a required,
/// unconditional ordinary step, and `on_failure` is one of `names`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidatedOutcome {
    pub names: Vec<String>,
    pub source: ResolvedOutputSource,
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
