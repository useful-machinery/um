use std::collections::{BTreeMap, BTreeSet};

use serde::Deserialize;
use serde_json::Value;

use super::document::{
    Agent, AgentMessage, AgentNode, AgentProfile, Binding, BindingKind, CommandNode, CommonNode,
    ConditionOperand, ConditionPredicate, ConditionSelector, EdgeTarget, ExportDefinition,
    ExportPresentation, FailurePolicy, FinalizationTrigger, FinalizerDefinition, ForEach,
    InputDeclaration, JsonDefault, MessageSource, NodeBody, OutcomeDeclaration, Output,
    OutputReference, PresentationSource, RecoveryHandler, RootBlocks, RootRequires, StepDefinition,
    StepRecovery, Trigger, TriggerControlAction, TriggerRule, ValueReference, WaitKind, WaitRule,
    WorkflowDocument, WorkflowNode,
};
use super::mirror::{MirrorBlock, MirrorState};
use super::validated::ValidatedHarness;
use super::{claude_code, codex, pi};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct WorkflowDto {
    #[serde(rename = "schemaVersion")]
    schema_version: u8,
    description: Option<String>,
    #[serde(
        rename = "environmentPassthrough",
        default,
        deserialize_with = "deserialize_environment_passthrough"
    )]
    environment_passthrough: BTreeSet<String>,
    #[serde(default)]
    inputs: BTreeMap<String, InputDeclarationDto>,
    #[serde(rename = "agentProfiles", default)]
    agent_profiles: BTreeMap<String, AgentProfileDto>,
    steps: BTreeMap<String, StepDto>,
    #[serde(default)]
    finalizers: BTreeMap<String, FinalizerDto>,
    #[serde(default)]
    exports: BTreeMap<String, ExportDto>,
    #[serde(default)]
    outcomes: Vec<String>,
    outcome: Option<OutcomeDto>,
    name: Option<String>,
    version: Option<String>,
    subject: Option<String>,
    programs: Option<Vec<String>>,
    exclude: Option<Vec<String>>,
    requires: Option<RequiresDto>,
    bindings: Option<BTreeMap<String, BindingDto>>,
    triggers: Option<BTreeMap<String, TriggerDto>>,
    mirror: Option<MirrorDto>,
}

#[derive(Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
enum InputDeclarationDto {
    #[serde(rename = "text")]
    Text {
        optional: Option<bool>,
        default: Option<String>,
    },
    #[serde(rename = "json")]
    Json {
        schema: Option<String>,
        optional: Option<bool>,
        // A present `null` is a literal default, distinct from an absent one.
        #[serde(default, deserialize_with = "deserialize_present_value")]
        default: Option<Value>,
    },
    #[serde(rename = "file")]
    File {
        #[serde(rename = "mediaType")]
        media_type: Option<String>,
    },
    #[serde(rename = "attachments")]
    Attachments { optional: Option<bool> },
}

fn deserialize_present_value<'de, D>(deserializer: D) -> Result<Option<Value>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Value::deserialize(deserializer).map(Some)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RequiresDto {
    tools: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BindingDto {
    kind: BindingKind,
    default: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MirrorDto {
    linear: MirrorLinearDto,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct MirrorLinearDto {
    issue: String,
    on_start: Option<MirrorCommentDto>,
    on_stage: Option<MirrorCommentDto>,
    on_wait: Option<MirrorCommentDto>,
    #[serde(default)]
    on_outcome: BTreeMap<String, MirrorStateDto>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MirrorCommentDto {
    comment: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MirrorStateDto {
    state: String,
    unless: Option<String>,
}

impl MirrorDto {
    /// The block with its strings as written: the schema fixed `issue` as `inputs.<name>`
    /// and each state as `$<key>`; templates and guards are checked with the tree.
    fn into_block(self) -> MirrorBlock {
        let linear = self.linear;
        let unprefixed = |value: String, prefix: &str| match value.strip_prefix(prefix) {
            Some(rest) => rest.to_owned(),
            None => value,
        };
        let comment = |hook: Option<MirrorCommentDto>| hook.map(|hook| hook.comment);
        MirrorBlock {
            issue: unprefixed(linear.issue, "inputs."),
            on_start: comment(linear.on_start),
            on_stage: comment(linear.on_stage),
            on_wait: comment(linear.on_wait),
            on_outcome: linear
                .on_outcome
                .into_iter()
                .map(|(outcome, state)| {
                    (
                        outcome,
                        MirrorState {
                            state: unprefixed(state.state, "$"),
                            unless: state.unless,
                        },
                    )
                })
                .collect(),
        }
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
enum TriggerDto {
    Start(StartTriggerDto),
    Control(ControlTriggerDto),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StartTriggerDto {
    on: TriggerRuleDto,
    subject: Option<String>,
    inputs: BTreeMap<String, String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ControlTriggerDto {
    on: TriggerRuleDto,
    control: TriggerControlDto,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TriggerRuleDto {
    source: String,
    event: String,
    #[serde(rename = "where")]
    condition: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TriggerControlDto {
    // The schema admits only `bySubject`.
    #[serde(rename = "run")]
    _run: String,
    action: TriggerControlActionDto,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum TriggerControlActionDto {
    Named(String),
    Conditional(ConditionalControlActionDto),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConditionalControlActionDto {
    when: String,
    then: String,
    #[serde(rename = "else")]
    otherwise: String,
}

impl TriggerRuleDto {
    fn into_rule(self) -> TriggerRule {
        TriggerRule {
            source: self.source,
            event: self.event,
            condition: self.condition,
        }
    }
}

impl TriggerDto {
    fn into_trigger(self) -> Trigger {
        match self {
            Self::Start(start) => Trigger::Start {
                on: start.on.into_rule(),
                subject: start.subject,
                inputs: start.inputs,
            },
            Self::Control(control) => Trigger::Control {
                on: control.on.into_rule(),
                control: match control.control.action {
                    TriggerControlActionDto::Named(name) => TriggerControlAction::Named(name),
                    TriggerControlActionDto::Conditional(action) => {
                        TriggerControlAction::Conditional {
                            when: action.when,
                            then: action.then,
                            otherwise: action.otherwise,
                        }
                    }
                },
            },
        }
    }
}

impl InputDeclarationDto {
    fn into_declaration(self) -> InputDeclaration {
        match self {
            Self::Text { optional, default } => InputDeclaration::Text {
                optional: optional == Some(true),
                default,
            },
            Self::Json {
                schema,
                optional,
                default,
            } => InputDeclaration::Json {
                schema,
                optional: optional == Some(true),
                default: default.map(|value| match value {
                    Value::String(path) => JsonDefault::File(path),
                    literal => JsonDefault::Literal(literal),
                }),
            },
            Self::File { media_type } => InputDeclaration::File { media_type },
            Self::Attachments { optional } => InputDeclaration::Attachments {
                optional: optional == Some(true),
            },
        }
    }
}

#[derive(Deserialize)]
#[serde(tag = "kind")]
enum StepDto {
    #[serde(rename = "cmd")]
    Command {
        #[serde(flatten)]
        body: CommandNodeDto,
        #[serde(rename = "dependsOn", default)]
        control_dependencies: Vec<String>,
        recovery: Option<RecoveryDto>,
    },
    #[serde(rename = "agent")]
    Agent {
        #[serde(flatten)]
        body: AgentNodeDto,
        #[serde(rename = "dependsOn", default)]
        control_dependencies: Vec<String>,
        recovery: Option<RecoveryDto>,
    },
    #[serde(rename = "workflow")]
    Workflow {
        workflow: String,
        #[serde(default)]
        inputs: BTreeMap<String, ReferenceDto>,
        on: BTreeMap<String, EdgeTargetDto>,
        #[serde(rename = "forEach", default)]
        for_each: Option<ForEachDto>,
    },
}

/// `forEach` is a reference string like `outcome.from`, not a `{ref}` object, or the
/// object `{items, empty}` (REQ-WFFORMAT-98).
#[derive(Deserialize)]
#[serde(untagged)]
enum ForEachDto {
    Reference(String),
    Object(ForEachObjectDto),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ForEachObjectDto {
    items: String,
    #[serde(default)]
    empty: Option<String>,
}

impl ForEachDto {
    fn into_for_each(self) -> Option<ForEach> {
        let (items, empty) = match self {
            Self::Reference(items) => (items, None),
            Self::Object(ForEachObjectDto { items, empty }) => (items, empty),
        };
        Some(ForEach {
            items: parse_value_reference(&items)?,
            empty,
        })
    }
}

/// Untagged variants are tried in order; the objects deny unknown fields, so a `limit` on
/// a wait or a limited `else` decodes as neither.
#[derive(Deserialize)]
#[serde(untagged)]
enum EdgeTargetDto {
    Limited(LimitedTargetDto),
    Wait(WaitTargetDto),
    Target(String),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LimitedTargetDto {
    then: String,
    limit: u32,
    #[serde(rename = "else")]
    else_: ElseTargetDto,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum ElseTargetDto {
    Wait(WaitTargetDto),
    Target(String),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WaitTargetDto {
    wait: WaitKind,
    then: String,
    #[serde(default, rename = "onHandBack")]
    on_hand_back: Option<String>,
    #[serde(default)]
    rule: Option<WaitRule>,
}

impl EdgeTargetDto {
    fn into_target(self) -> EdgeTarget {
        match self {
            Self::Target(target) => parse_edge_target(target),
            Self::Wait(wait) => wait.into_target(),
            Self::Limited(limited) => EdgeTarget::Limited {
                then: limited.then,
                limit: limited.limit,
                else_: Box::new(match limited.else_ {
                    ElseTargetDto::Target(target) => parse_edge_target(target),
                    ElseTargetDto::Wait(wait) => wait.into_target(),
                }),
            },
        }
    }
}

impl WaitTargetDto {
    fn into_target(self) -> EdgeTarget {
        EdgeTarget::Wait {
            kind: self.wait,
            then: Box::new(parse_edge_target(self.then)),
            on_hand_back: self.on_hand_back,
            rule: self.rule,
        }
    }
}

/// `exit.<outcome>` names an outcome of the workflow; anything else is a step id, which
/// never contains a `.`.
fn parse_edge_target(target: String) -> EdgeTarget {
    match target.strip_prefix("exit.") {
        Some(outcome) => EdgeTarget::Exit(outcome.to_owned()),
        None => EdgeTarget::Step(target),
    }
}

#[derive(Deserialize)]
#[serde(tag = "kind")]
enum FinalizerDto {
    #[serde(rename = "cmd")]
    Command {
        #[serde(flatten)]
        body: CommandNodeDto,
        #[serde(default)]
        after: Vec<String>,
        #[serde(default = "all_finalization_triggers")]
        when: BTreeSet<FinalizationTrigger>,
    },
    #[serde(rename = "agent")]
    Agent {
        #[serde(flatten)]
        body: AgentNodeDto,
        #[serde(default)]
        after: Vec<String>,
        #[serde(default = "all_finalization_triggers")]
        when: BTreeSet<FinalizationTrigger>,
    },
}

fn all_finalization_triggers() -> BTreeSet<FinalizationTrigger> {
    FinalizationTrigger::all()
}

#[derive(Deserialize)]
struct CommandNodeDto {
    #[serde(flatten)]
    common: CommonNodeDto,
    #[serde(default)]
    inputs: BTreeMap<String, ReferenceDto>,
    command: CommandDto,
}

#[derive(Deserialize)]
struct AgentNodeDto {
    #[serde(flatten)]
    common: CommonNodeDto,
    agent: AgentDto,
}

#[derive(Deserialize)]
struct CommonNodeDto {
    #[serde(rename = "failurePolicy", default)]
    failure_policy: FailurePolicy,
    condition: Option<ConditionPredicateDto>,
    cwd: Option<String>,
    #[serde(
        rename = "environmentPassthrough",
        default,
        deserialize_with = "deserialize_node_environment_passthrough"
    )]
    environment_passthrough: Option<BTreeSet<String>>,
    #[serde(default)]
    outputs: BTreeMap<String, OutputDto>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CommandDto {
    argv: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RecoveryDto {
    retries: u8,
    handler: Option<RecoveryHandlerDto>,
}

#[derive(Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
enum RecoveryHandlerDto {
    #[serde(rename = "cmd")]
    Command {
        command: CommandDto,
        cwd: Option<String>,
        #[serde(
            rename = "environmentPassthrough",
            default,
            deserialize_with = "deserialize_node_environment_passthrough"
        )]
        environment_passthrough: Option<BTreeSet<String>>,
    },
    #[serde(rename = "agent")]
    Agent {
        profile: String,
        prompt: String,
        cwd: Option<String>,
        #[serde(
            rename = "environmentPassthrough",
            default,
            deserialize_with = "deserialize_node_environment_passthrough"
        )]
        environment_passthrough: Option<BTreeSet<String>>,
    },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct AgentProfileDto {
    harness: HarnessDto,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AgentDto {
    profile: String,
    #[serde(rename = "systemPrompt")]
    system_prompt: String,
    message: AgentMessageDto,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AgentMessageDto {
    text: Vec<MessageSourceDto>,
    #[serde(default)]
    attachments: Vec<MessageSourceDto>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum MessageSourceDto {
    File {
        file: String,
    },
    Reference {
        #[serde(rename = "ref")]
        reference: String,
    },
}

#[derive(Deserialize)]
#[serde(tag = "kind")]
enum HarnessDto {
    #[serde(rename = "pi")]
    Pi { config: Value },
    #[serde(rename = "claude_code")]
    ClaudeCode { config: Value },
    #[serde(rename = "codex")]
    Codex { config: Value },
}

#[derive(Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
enum OutputDto {
    #[serde(rename = "text")]
    Text {
        #[serde(rename = "from")]
        source: TextOutputSourceDto,
        path: Option<String>,
    },
    #[serde(rename = "json")]
    Json {
        #[serde(rename = "from")]
        source: JsonOutputSourceDto,
        path: Option<String>,
        schema: String,
    },
    #[serde(rename = "file")]
    File {
        #[serde(rename = "from")]
        source: PathOutputSourceDto,
        path: String,
        #[serde(rename = "mediaType")]
        media_type: String,
    },
    #[serde(rename = "git_branch")]
    GitBranch {
        #[serde(rename = "from")]
        source: WorkspaceOutputSourceDto,
    },
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum TextOutputSourceDto {
    Path,
    AgentResponse,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum JsonOutputSourceDto {
    Path,
    AgentResult,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum PathOutputSourceDto {
    Path,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum WorkspaceOutputSourceDto {
    Workspace,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum ConditionPredicateDto {
    All(ConditionAllDto),
    Any(ConditionAnyDto),
    Not(ConditionNotDto),
    Equals(ConditionEqualsDto),
    Exists(ConditionExistsDto),
    Disposition(ConditionDispositionDto),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConditionAllDto {
    all: Vec<ConditionPredicateDto>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConditionAnyDto {
    any: Vec<ConditionPredicateDto>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConditionNotDto {
    not: Box<ConditionPredicateDto>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConditionEqualsDto {
    equals: [ConditionOperandDto; 2],
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConditionExistsDto {
    exists: ConditionSelectorDto,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConditionDispositionDto {
    disposition: ConditionDispositionValueDto,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConditionDispositionValueDto {
    node: String,
    is: super::condition::TerminalDisposition,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum ConditionOperandDto {
    Reference(ConditionReferenceDto),
    Literal(ConditionLiteralDto),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConditionReferenceDto {
    #[serde(rename = "ref")]
    reference: String,
    pointer: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConditionLiteralDto {
    value: Value,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConditionSelectorDto {
    #[serde(rename = "ref")]
    reference: String,
    pointer: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReferenceDto {
    #[serde(rename = "ref")]
    reference: String,
}

fn deserialize_environment_passthrough<'de, D>(
    deserializer: D,
) -> Result<BTreeSet<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let names = Vec::<String>::deserialize(deserializer)?;
    let mut allowed = BTreeSet::new();
    for name in names {
        let mut bytes = name.bytes();
        if !bytes
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == b'_')
            || !bytes.all(|c| c.is_ascii_alphanumeric() || c == b'_')
            || super::admission::is_managed_runner_private_environment_name(std::ffi::OsStr::new(
                &name,
            ))
            || !allowed.insert(name.clone())
        {
            return Err(serde::de::Error::custom(
                "invalid or duplicate environment passthrough name",
            ));
        }
    }
    Ok(allowed)
}

/// A node's `environmentPassthrough`, under the root list's name rules; present as
/// `Some` even when empty, so an absent list stays distinct (REQ-WFFORMAT-102).
fn deserialize_node_environment_passthrough<'de, D>(
    deserializer: D,
) -> Result<Option<BTreeSet<String>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    deserialize_environment_passthrough(deserializer).map(Some)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExportDto {
    #[serde(rename = "ref")]
    reference: String,
    presentation: Option<ExportPresentationDto>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OutcomeDto {
    from: String,
    #[serde(rename = "onFailure")]
    on_failure: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExportPresentationDto {
    title: Option<PresentationSourceDto>,
    description: Option<PresentationSourceDto>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum PresentationSourceDto {
    Literal(PresentationLiteralDto),
    Reference(ReferenceDto),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PresentationLiteralDto {
    value: String,
}

impl PresentationSourceDto {
    fn into_source(self) -> Option<PresentationSource> {
        match self {
            Self::Literal(literal) => Some(PresentationSource::Literal(literal.value)),
            Self::Reference(reference) => {
                parse_output_reference(&reference.reference).map(PresentationSource::Reference)
            }
        }
    }
}

impl WorkflowDto {
    pub(super) fn into_document(
        self,
        step_order: Vec<String>,
        finalizer_order: Vec<String>,
    ) -> Option<WorkflowDocument> {
        let inputs = self
            .inputs
            .into_iter()
            .map(|(name, declaration)| (name, declaration.into_declaration()))
            .collect();
        let root = (self.programs.is_some()
            || self.exclude.is_some()
            || self.requires.is_some()
            || self.bindings.is_some()
            || self.triggers.is_some()
            || self.mirror.is_some())
        .then(|| RootBlocks {
            programs: self.programs.unwrap_or_default(),
            exclude: self.exclude.unwrap_or_default(),
            requires: self.requires.map(|requires| RootRequires {
                tools: requires.tools,
            }),
            bindings: self
                .bindings
                .unwrap_or_default()
                .into_iter()
                .map(|(key, binding)| {
                    (
                        key,
                        Binding {
                            kind: binding.kind,
                            default: binding.default,
                        },
                    )
                })
                .collect(),
            triggers: self
                .triggers
                .unwrap_or_default()
                .into_iter()
                .map(|(name, trigger)| (name, trigger.into_trigger()))
                .collect(),
            mirror: self.mirror.map(MirrorDto::into_block),
        });
        let agent_profiles = AgentProfileDto::into_agent_profiles(self.agent_profiles)?;
        let steps = self
            .steps
            .into_iter()
            .map(|(name, step)| step.into_step().map(|step| (name, step)))
            .collect::<Option<_>>()?;
        let finalizers = self
            .finalizers
            .into_iter()
            .map(|(name, finalizer)| {
                finalizer
                    .into_finalizer()
                    .map(|finalizer| (name, finalizer))
            })
            .collect::<Option<_>>()?;
        let exports = self
            .exports
            .into_iter()
            .map(|(name, reference)| {
                let source = parse_output_reference(&reference.reference)?;
                let presentation = if let Some(presentation) = reference.presentation {
                    let title = match presentation.title {
                        Some(title) => Some(title.into_source()?),
                        None => None,
                    };
                    let description = match presentation.description {
                        Some(description) => Some(description.into_source()?),
                        None => None,
                    };
                    if title.is_none() && description.is_none() {
                        return None;
                    }
                    Some(ExportPresentation { title, description })
                } else {
                    None
                };
                Some((
                    name,
                    ExportDefinition {
                        source,
                        presentation,
                    },
                ))
            })
            .collect::<Option<_>>()?;

        let outcome = match self.outcome {
            Some(outcome) => Some(OutcomeDeclaration {
                from: parse_output_reference(&outcome.from)?,
                on_failure: outcome.on_failure,
            }),
            None => None,
        };

        Some(WorkflowDocument {
            schema_version: self.schema_version,
            description: self.description,
            environment_passthrough: self.environment_passthrough,
            inputs,
            agent_profiles,
            steps,
            step_order,
            finalizers,
            finalizer_order,
            exports,
            outcomes: self.outcomes,
            outcome,
            name: self.name,
            version: self.version,
            subject: self.subject,
            root,
            derived_outputs: BTreeMap::new(),
        })
    }
}

impl StepDto {
    fn into_step(self) -> Option<StepDefinition> {
        let (body, control_dependencies, recovery) = match self {
            Self::Command {
                body,
                control_dependencies,
                recovery,
            } => (body.into_body()?, control_dependencies, recovery),
            Self::Agent {
                body,
                control_dependencies,
                recovery,
            } => (body.into_body()?, control_dependencies, recovery),
            Self::Workflow {
                workflow,
                inputs,
                on,
                for_each,
            } => (
                NodeBody::Workflow(WorkflowNode {
                    workflow,
                    inputs: parse_references(inputs, parse_workflow_step_reference)?,
                    edges: on
                        .into_iter()
                        .map(|(outcome, target)| (outcome, target.into_target()))
                        .collect(),
                    for_each: match for_each {
                        Some(for_each) => Some(for_each.into_for_each()?),
                        None => None,
                    },
                }),
                Vec::new(),
                None,
            ),
        };
        Some(StepDefinition {
            body,
            control_dependencies,
            recovery: recovery.map(RecoveryDto::into_recovery),
        })
    }
}

impl FinalizerDto {
    fn into_finalizer(self) -> Option<FinalizerDefinition> {
        let (body, after, when) = match self {
            Self::Command { body, after, when } => (body.into_body()?, after, when),
            Self::Agent { body, after, when } => (body.into_body()?, after, when),
        };
        Some(FinalizerDefinition { body, after, when })
    }
}

impl CommandNodeDto {
    fn into_body(self) -> Option<NodeBody> {
        Some(NodeBody::Command(CommandNode {
            common: self.common.into_common_node()?,
            inputs: parse_references(self.inputs, parse_value_reference)?,
            argv: self.command.argv,
        }))
    }
}

impl AgentNodeDto {
    fn into_body(self) -> Option<NodeBody> {
        Some(NodeBody::Agent(AgentNode {
            common: self.common.into_common_node()?,
            agent: self.agent.into_agent()?,
        }))
    }
}

impl CommonNodeDto {
    fn into_common_node(self) -> Option<CommonNode> {
        let outputs = self
            .outputs
            .into_iter()
            .map(|(name, output)| output.into_output().map(|output| (name, output)))
            .collect::<Option<_>>()?;

        Some(CommonNode {
            failure_policy: self.failure_policy,
            condition: match self.condition {
                Some(condition) => Some(condition.into_predicate()?),
                None => None,
            },
            cwd: self.cwd,
            environment_passthrough: self.environment_passthrough,
            outputs,
        })
    }
}

impl ConditionPredicateDto {
    fn into_predicate(self) -> Option<ConditionPredicate> {
        match self {
            Self::All(value) => Some(ConditionPredicate::All(
                value
                    .all
                    .into_iter()
                    .map(Self::into_predicate)
                    .collect::<Option<_>>()?,
            )),
            Self::Any(value) => Some(ConditionPredicate::Any(
                value
                    .any
                    .into_iter()
                    .map(Self::into_predicate)
                    .collect::<Option<_>>()?,
            )),
            Self::Not(value) => Some(ConditionPredicate::Not(Box::new(
                value.not.into_predicate()?,
            ))),
            Self::Equals(value) => Some(ConditionPredicate::Equals(
                value
                    .equals
                    .map(ConditionOperandDto::into_operand)
                    .into_iter()
                    .collect::<Option<Vec<_>>>()?
                    .try_into()
                    .ok()?,
            )),
            Self::Exists(value) => Some(ConditionPredicate::Exists(ConditionSelector {
                reference: parse_value_reference(&value.exists.reference)?,
                pointer: value.exists.pointer,
            })),
            Self::Disposition(value) => Some(ConditionPredicate::Disposition {
                node: value.disposition.node,
                is: value.disposition.is,
            }),
        }
    }
}

impl ConditionOperandDto {
    fn into_operand(self) -> Option<ConditionOperand> {
        match self {
            Self::Reference(value) => Some(ConditionOperand::Reference {
                reference: parse_value_reference(&value.reference)?,
                pointer: value.pointer,
            }),
            Self::Literal(value) => Some(ConditionOperand::Literal(value.value)),
        }
    }
}

fn parse_references(
    references: BTreeMap<String, ReferenceDto>,
    parse: fn(&str) -> Option<ValueReference>,
) -> Option<BTreeMap<String, ValueReference>> {
    references
        .into_iter()
        .map(|(name, reference)| parse(&reference.reference).map(|reference| (name, reference)))
        .collect()
}

impl RecoveryDto {
    fn into_recovery(self) -> StepRecovery {
        StepRecovery {
            retries: self.retries,
            handler: self.handler.map(RecoveryHandlerDto::into_handler),
        }
    }
}

impl RecoveryHandlerDto {
    fn into_handler(self) -> RecoveryHandler {
        match self {
            Self::Command {
                command,
                cwd,
                environment_passthrough,
            } => RecoveryHandler::Command {
                argv: command.argv,
                cwd,
                environment_passthrough,
            },
            Self::Agent {
                profile,
                prompt,
                cwd,
                environment_passthrough,
            } => RecoveryHandler::Agent {
                profile,
                prompt,
                cwd,
                environment_passthrough,
            },
        }
    }
}

impl AgentProfileDto {
    /// Types each profile's native harness configuration, for the workflow and the
    /// replacement decoder alike. The structural schema already closes each config, so
    /// this refuses only what the schema refuses.
    pub(super) fn into_agent_profiles(
        profiles: BTreeMap<String, Self>,
    ) -> Option<BTreeMap<String, AgentProfile>> {
        profiles
            .into_iter()
            .map(|(name, profile)| {
                let harness = profile.harness.into_harness()?;
                Some((name, AgentProfile { harness }))
            })
            .collect()
    }
}

impl HarnessDto {
    fn into_harness(self) -> Option<ValidatedHarness> {
        match self {
            Self::Pi { config } => pi::resolve_config(&config).map(ValidatedHarness::Pi),
            Self::ClaudeCode { config } => {
                claude_code::resolve_config(&config).map(ValidatedHarness::ClaudeCode)
            }
            Self::Codex { config } => codex::resolve_config(&config).map(ValidatedHarness::Codex),
        }
    }
}

impl AgentDto {
    fn into_agent(self) -> Option<Agent> {
        Some(Agent {
            profile: self.profile,
            system_prompt: self.system_prompt,
            message: AgentMessage {
                text: message_sources(self.message.text)?,
                attachments: message_sources(self.message.attachments)?,
            },
        })
    }
}

fn message_sources(sources: Vec<MessageSourceDto>) -> Option<Vec<MessageSource>> {
    sources
        .into_iter()
        .map(|source| match source {
            MessageSourceDto::File { file } => Some(MessageSource::File { path: file }),
            MessageSourceDto::Reference { reference } => {
                parse_value_reference(&reference).map(MessageSource::Reference)
            }
        })
        .collect()
}

impl OutputDto {
    fn into_output(self) -> Option<Output> {
        match self {
            Self::Text {
                source: TextOutputSourceDto::Path,
                path: Some(path),
            } => Some(Output::TextPath { path }),
            Self::Text {
                source: TextOutputSourceDto::AgentResponse,
                path: None,
            } => Some(Output::TextAgentResponse),
            Self::Json {
                source: JsonOutputSourceDto::Path,
                path: Some(path),
                schema,
            } => Some(Output::JsonPath { path, schema }),
            Self::Json {
                source: JsonOutputSourceDto::AgentResult,
                path: None,
                schema,
            } => Some(Output::JsonAgentResult { schema }),
            Self::File {
                source: PathOutputSourceDto::Path,
                path,
                media_type,
            } => Some(Output::FilePath { path, media_type }),
            Self::GitBranch {
                source: WorkspaceOutputSourceDto::Workspace,
            } => Some(Output::GitBranchWorkspace),
            Self::Text { .. } | Self::Json { .. } => None,
        }
    }
}

fn parse_value_reference(reference: &str) -> Option<ValueReference> {
    if reference == "finalization.context" {
        return Some(ValueReference::FinalizationContext);
    }
    if let Some(name) = reference.strip_prefix("inputs.") {
        return Some(ValueReference::Input {
            name: name.to_owned(),
        });
    }

    parse_output_reference(reference).map(ValueReference::Output)
}

/// A `workflow` step's inputs also read the control that resolved a wait and the
/// answers recorded for the step (REQ-WFFORMAT-95), and the element a `forEach` step's
/// child runs for (REQ-WFFORMAT-99); the structural schema admits these forms nowhere
/// else.
fn parse_workflow_step_reference(reference: &str) -> Option<ValueReference> {
    if reference == "controls.answers" {
        return Some(ValueReference::ControlsAnswers);
    }
    if reference == "item" {
        return Some(ValueReference::Item { field: None });
    }
    if let Some(field) = reference.strip_prefix("item.") {
        return um_support::is_identifier(field).then(|| ValueReference::Item {
            field: Some(field.to_owned()),
        });
    }
    if let Some(field) = reference.strip_prefix("wait.") {
        return Some(ValueReference::Wait {
            field: field.to_owned(),
        });
    }
    parse_value_reference(reference)
}

fn parse_output_reference(reference: &str) -> Option<OutputReference> {
    let mut segments = reference.split('.');
    if segments.next() != Some("outputs") {
        return None;
    }
    let node = segments.next()?.to_owned();
    let output = segments.next()?.to_owned();
    if segments.next().is_some() {
        return None;
    }

    Some(OutputReference { node, output })
}
