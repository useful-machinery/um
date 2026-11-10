# Workflow V1 authoring reference

Reference ID: `um.workflow.v1.authoring`

Language version: `1`

Authoring guide: <https://docs.usefulmachinery.com/agent/workflow-authoring.md>

Full language reference: <https://docs.usefulmachinery.com/reference/workflow-v1.md>

Raw schema: <https://docs.usefulmachinery.com/schemas/workflow-v1.schema.json>

Embedded in `um`: Workflow V1 authoring and validation, not execution or
repository changes.

## Authoring loop

1. **Inspect read-only.** Find the repository root, applicable instruction files,
   source-control state, existing workflow conventions and assets, the supported CLI,
   and the exact workflow source root and YAML path.
2. **Design the definition.** Identify command and agent nodes, control and data edges,
   profiles, static support files, outputs, exports, failure policies, step recovery, and
   finalizers.
3. **Propose one bounded change.** Name every file, configured harness/model/effort
   value, graph behavior, and the exact validation command. State that authoring and
   validation do not authorize execution, repository checks, or source-control changes.
4. **Author the approved files.** Recheck each destination before writing. Preserve
   collisions and partial assets. Write referenced prompts, attachments, and result
   schemas together with the YAML.
5. **Validate without running.** Invoke `workflow validate --json` with an explicit
   source root and workflow path. Treat only its structured `valid` outcome as success.
6. **Report and stop.** Report the path, outcome, graph, outputs, exports, and
   finalizers. Do not run it.

## Mental model

- A definition is one closed YAML 1.2 document with `schemaVersion: 1`. Unknown fields,
  kinds, references, and enum values are invalid.
- The workflow source root is an explicit validation boundary containing the YAML and
  retained static files. It is not inferred from a checkout or the YAML directory.
- The execution root is separate. Commands, agents, working directories, and runtime
  outputs operate below it when a run is separately authorized.
- Ordinary steps form a DAG. After all ordinary steps are terminal, matching finalizers
  form a second DAG. A workflow whose steps run child workflows is instead walked along
  the edges its steps declare (see [Child workflows and edges](#child-workflows-and-edges)).
- References carry typed values and infer data dependencies. They do not interpolate
  text, read files implicitly, or grant authority.
- Definition validation resolves source bytes and checks the contract. It does not run
  nodes, inspect credentials, contact providers, or prove harness/model availability.

## Core document and graph

| Root field | Required | Meaning |
| --- | --- | --- |
| `schemaVersion` | yes | Integer `1`. |
| `description` | no | Human metadata with no execution effect. |
| `name` | no | Lowercase identifier naming a tree's definition; not a run identity. |
| `version` | no | Label for people, 1 to 64 characters. |
| `subject` | no | Root only: the declared `text` input naming the run's subject. |
| `inputs` | no | Named Text, JSON, File, or attachments; optional JSON schema or exact File media type; may be `optional` or carry a `default`. |
| `environmentPassthrough` | no | Unique host environment variable names explicitly forwarded to all steps, agents, and recovery handlers. |
| `agentProfiles` | no | Workflow-local harness configurations. |
| `steps` | yes | At least one ordinary `cmd`, `agent`, or `workflow` node. |
| `finalizers` | no | Nodes considered after the ordinary phase. |
| `exports` | no | Stable names selecting declared outputs. |
| `outcomes`, `outcome` | no | Declared run outcome names and the `text` output that chooses one; declared together. |
| `programs`, `exclude`, `requires`, `bindings`, `triggers`, `mirror` | no | Root-only tree blocks: program directories, digest excludes, required `tools`, typed bindings with optional text `default`, start or control triggers on a `$binding` source, and the Linear writes a run makes. |

Identifiers use lower camel case, contain 1 through 64 ASCII letters or digits, and
match `^[a-z][A-Za-z0-9]*$`. Ordinary step and finalizer IDs share a namespace. V1
rejects duplicate or non-string keys, anchors, aliases, merge keys, custom tags,
multiple YAML documents, `%YAML` and unknown directives, and floating-point numbers
(write `1`, not `1.0`). A workflow file is at most 1 MiB and nests values at most
100 levels deep.

Execution inherits only `PATH`, `HOME`, `USER`, `LOGNAME`, `LANG`, `LC_ALL`,
`LC_CTYPE`, `TERM`, and `TMPDIR` from the launching process by default. Add
other names at the workflow root, for example `environmentPassthrough: [MY_TOKEN]`.
Names must be unique ASCII shell variable names (letter or underscore, then
letters, digits, or underscores). Missing names remain unset. Engine-reserved
`UM_*` and `SCHERZO_*`, along with runner-private Git, SSH, and GitHub credential/helper names
cannot be passed through. Runner source revision metadata is injected by the
engine, not inherited from the host. Treat every declared name as available to
all workflow code, including recovery. Guard and recovery scratch directories
use `TMPDIR` when set (otherwise the system temporary directory); the guard's
launch manifest never records environment values.

The `cmd` and `agent` node kinds may declare `failurePolicy`, `recovery`, `dependsOn`,
`cwd`, and `outputs`. `failurePolicy` is `required` by default or may be `advisory`.
`recovery.retries` is 1 through 10. It either omits `handler` for an immediate unchanged
target recheck, names one closed command handler, or names one fresh-agent handler with
an existing profile and retained prompt. A command handler reads the bounded private
Recovery Context Schema 1 path from `UM_RECOVERY_CONTEXT` and writes exactly one
closed, at-most-16-KiB `recheck` or `gave_up` decision document to the fresh private
`UM_RECOVERY_RESULT` path. An agent handler's profile and compatible installed
harness are admission and runner-placement requirements even when recovery never
activates.

- `dependsOn` names unique ordinary steps and creates control-only ordering.
- An `outputs.<step>.<output>` reference creates a direct inferred data edge. Naming
  the same producer in `dependsOn` is valid and retains both data and control origins.
- The combined graph must be acyclic. Workflow inputs and exports do not create graph edges.
- A required failure stops new ordinary starts and fails the ordinary outcome. An
  advisory failure remains visible, permits control-only dependents, and does not fail
  the workflow by itself.
- A required node cannot consume an advisory node's output. Advisory nodes cannot be
  export sources.
- A recovery handler is not a node and has no inputs, outputs, dependencies, failure
  policy, or nested recovery. Only terminal target success commits outputs; finalizers
  cannot declare recovery and wait for ordinary recovery quiescence.
- Rechecks and handlers are fresh physical invocations in the same execution root.
  `gave_up` or handler failure stops recovery. Recovery has no condition-specific retry,
  delay/backoff, handler retry, capture-only retry, rollback, or session continuation.
- Recovery effects are at-least-once. Authors supply stable domain idempotency keys
  through existing explicit inputs or admitted environment, not Useful Machinery lifecycle IDs.

## Conditional steps and finalizers

A node `condition` uses `all`, `any`, `not`, `equals`, `exists`, or `disposition`.
Values include named Text/JSON inputs and outputs, finalizer context, or terminal
dispositions. Equality never coerces kinds; JSON Pointer follows RFC 6901.

Resolution retains every referenced output and disposition in the static graph, even if
evaluation short-circuits. Evaluation occurs at most once before body readiness. True
proceeds; false commits terminal `skipped` with value-free `condition_false` evidence.
Unavailable condition data blocks its gate; unavailable body data after true blocks the
body gate. An evaluated missing Pointer fails in phase `condition` with code
`json_pointer_missing`; a skipped export is unavailable as `source_skipped`.

Limits: depth 16; 256 nodes per condition; 64 children per `all` or `any`; 4,096 nodes per
workflow; and 1,024 exact prerequisite descriptors per node. A condition cannot read files,
environment, credentials, logs, diagnostics, providers, clocks, randomness, or mutable
source.

A command uses a nonempty argument vector:

```yaml
steps:
  check:
    kind: cmd
    cwd: packages/api
    command:
      argv: ["./scripts/check.sh", "--unit"]
```

The vector is passed directly to the operating system. There is no shell parsing,
interpolation, glob expansion, redirection, pipeline construction, or implicit shell.
Put complex shell behavior in a repository-owned script. Commands alone accept named
`inputs`; each bound value is materialized under the run-time `UM_STEP_INPUTS`
directory with a `manifest.json`.

## Paths and references

Validation receives the source root through `--source-root`. The selected workflow path
must resolve inside it.

| Field | Base and rule |
| --- | --- |
| `agent.systemPrompt` or recovery-agent `handler.prompt` | YAML directory; retained UTF-8 file inside the source root. |
| Message `{ file: ... }` | YAML directory; retained file inside the source root; text files are UTF-8. |
| JSON output `schema` | YAML directory; retained, self-contained Draft 2020-12 schema inside the source root. |
| Node `cwd` | Execution root; defaults to that root and cannot contain `..`. |
| Path output `path` | Execution root, not node `cwd`; cannot contain `..`. |

Static paths may use `..` only when normalization and symbolic-link resolution remain
inside the source root. Runtime paths may not use parent traversal. Static source bytes
are pinned during validation and are not reread from mutable workflow source at run time.

A reference object contains exactly one `ref` field:

```yaml
inputs:
  plan:
    ref: outputs.plan.result
```

| Reference | Type and availability |
| --- | --- |
| `inputs.<name>` | The exact required declared Text, JSON, File, or attachment-collection value. |
| `outputs.<node>.<output>` | The declared output's exact type. |
| `finalization.context` | Engine-owned JSON; finalizer command input, agent attachment, or condition only. Context schema version 2 contains stable run/attempt IDs and bounded classified failed/blocked ordinary-step facts; unavailable diagnostics are null, not inferred. Resuming an attempt retains identity; retry/continuation uses a new attempt ID. |

Nodes reference inputs and ordinary outputs; finalizers may also reference finalizer
outputs and `finalization.context`. Inputs add no edges. Conditions admit only Text/JSON
values. An input is required unless `optional: true` or defaulted. A JSON default string
is a path relative to the workflow file, any other value a literal; a Text default is
text. Never both; File inputs take neither. Supplied empties are explicit values. Files require a supplied
valid media type. Its parameter separators are only space or tab, and parameter values
permit Unicode but not `;`, U+0000 through U+001F, or U+007F. A declared `mediaType`
matches exactly—no normalization, inference, wildcards, or sniffing. Exports reference
only outputs. Types never convert.

## Agent profiles

Agent nodes select one root profile. Inline harness definitions and fallback profiles
are invalid.

| Harness `kind` | Runtime profile | Required native config |
| --- | --- | --- |
| `pi` | PiJsonV1 | Nonempty `model`; `thinking`: `off`, `minimal`, `low`, `medium`, `high`, `xhigh`, or `max`. |
| `claude_code` | ClaudeCodeStreamJsonV1 | Nonempty `model`; `effort`: `low`, `medium`, `high`, `xhigh`, or `max`. |
| `codex` | CodexAppServerV1 | Nonempty `model` and nonempty native `effort`. |

Each config is closed. Do not guess a model, translate effort values between harnesses,
or add a fallback. Every agent node starts a fresh harness process, session, and
conversation.

For `claude_code`, session-specific transcript/resource symlinks live under
`CLAUDE_CONFIG_DIR` (default `$HOME/.claude`) in project-history directories created
as needed. Existing session entries are never replaced. After process-tree quiescence,
only unchanged links created by Useful Machinery are removed; empty directories may remain.
Missing or empty transcripts emit the `Claude Code native transcript capture missing`
warning. Retained transcripts are diagnostic only.

```yaml
inputs:
  request:
    kind: text
  updates:
    kind: attachments

agentProfiles:
  coding:
    harness:
      kind: pi
      config:
        model: provider/model
        thinking: high

steps:
  review:
    kind: agent
    agent:
      profile: coding
      systemPrompt: ../prompts/review-system.md
      message:
        text:
          - file: ../prompts/review-request.md
          - ref: inputs.request
        attachments:
          - ref: inputs.updates
    outputs:
      response:
        kind: text
        from: agent_response
```

`profile`, `systemPrompt`, and nonempty `message.text` are required. Text entries are
ordered and joined with two newlines. Attachments preserve order and type. Profile
validation checks syntax only; it does not install a harness or query a provider.

## Outputs and finalizers

| Semantic `kind` | Acquisition `from` | Producer | Required fields |
| --- | --- | --- | --- |
| `text` | `path` | command or agent | `path` |
| `text` | `agent_response` | agent | none |
| `json` | `path` | command or agent | `path`, `schema` |
| `json` | `agent_result` | agent | `schema` |
| `file` | `path` | command or agent | `path`, `mediaType` |
| `git_branch` | `workspace` | command or agent | none |

Every output contains both discriminators and matches exactly one row. A node may declare
any number of distinct paths, at most one native agent source, and at most one workspace
source. Commands cannot use native agent sources. Every JSON schema must be a
self-contained UTF-8 JSON Schema Draft 2020-12 document with this root dialect:

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema"
}
```

That `$schema` belongs in the separate result-schema JSON file. Do not add a root
`$schema` property to workflow YAML; the closed workflow root rejects it.

`exports` gives stable names to outputs without conversion. Export sources must be
required nodes. A finalizer export also requires `succeeded` in that finalizer's trigger
set.

`outcomes` lists between 1 and 64 distinct names a run may end with. Each name
matches `^[a-z][a-z0-9_]*$`, has at most 64 characters, and is not one of the engine
outcomes `withdrawn`, `canceled`, or `superseded`. `outcome` names where the choice comes
from: `from` is an `outputs.<step>.<output>` reference and `onFailure` is the name a
failed run takes. Declare both keys or neither; `onFailure` must be one of `outcomes`, or
validation fails with `invalid_outcome_declaration`. The source must be a declared `text`
output of a required ordinary step without a `condition`, otherwise validation fails with
`invalid_outcome_source`. Without the declaration a workflow has the implicit outcomes
`succeeded` and `failed`.

```yaml
outcomes: [planned, blocked, failed]
outcome:
  from: outputs.decide.verdict
  onFailure: failed
```

Finalizers replace ordinary `dependsOn` with:

- `when`: a nonempty unique set of `succeeded`, `failed`, and/or `cancelled`; omission
  selects all three ordinary outcomes.
- `after`: unique finalizer IDs that must become terminal first.

Finalizer output references infer edges within the finalizer DAG. A consumer's `when`
set must be a subset of a referenced finalizer producer's set. A required finalizer
issue turns an ordinary success into failure; advisory finalizer issues stay visible but
do not replace the ordinary outcome.

## Child workflows and edges

A step of `kind: workflow` runs another workflow file of the same tree as a child run
and routes on the outcome the child ends with. It accepts exactly `workflow`, `inputs`,
`on` and `forEach`.

```yaml
outcomes: [landed, failed]
steps:
  design:
    kind: workflow
    workflow: design.yaml
    inputs:
      ticket:
        ref: inputs.ticket
    on:
      planned: build
      blocked: exit.failed
      failed: exit.failed
  build:
    kind: workflow
    workflow: build.yaml
    inputs:
      plan:
        ref: outputs.design.plan
    on:
      built: exit.landed
      failed: exit.failed
```

- `workflow` names a `.yaml` or `.yml` file directly under the source root, other than
  `workflow.yaml` and the file itself (`child_workflow_not_in_tree`); an unreadable file
  is `child_workflow_unavailable`. The child is validated as its own workflow file and a
  failure there is `child_workflow_invalid`, whose remedy names the child's own code and
  location. A child may not declare `workflow` steps (`child_workflow_nested`) or carry
  `name`, `version`, `subject`, or a root-only block (`child_workflow_root_blocks`). The
  child file and the static files it names join the parent's closure and digest. These
  failures are at location `step_workflow`.
- `inputs` maps the child's input names to references of the parent's inputs or of an
  earlier step's outputs. Every child input that is neither optional nor defaulted must
  be mapped (`child_input_missing`), only declared names may be mapped
  (`child_input_undeclared`), kinds must match (`child_input_type_mismatch`), and a child
  `file` or `attachments` input cannot be mapped (`child_input_unsupported`). These
  failures are at location `step_input`.
- A `workflow` step's `inputs` may also reference `wait.<field>`, a `text` value: the
  named field of the control that resolved the wait whose `then` or `onHandBack` is this
  step. `controls.answers` is a `json` value: the answers recorded for this step's
  questions. Either may be absent when the step starts, so the child input it maps onto
  must be optional or defaulted (`control_reference_requires_optional` at `step_input`),
  and kinds must match as above. `cmd` and `agent` steps accept neither form.
- On a `forEach` step, `inputs` may also reference `item`, the element, or
  `item.<field>`, a top-level field of an object element. Either takes the kind of the
  `text` or `json` child input it maps onto; a `text` input needs a JSON string, checked
  when the step starts. Without `forEach` it is `item_reference_requires_for_each`.
- The step declares no `outputs`. Its outputs are the child's `text` and `json` exports
  with their types, plus the `text` output `outcome`, the child's recorded outcome. A
  child may not export `outcome` (`child_export_reserved`). Other names, including a
  child's `file` or `git_branch` exports, fail `unknown_output` where they are referenced.
- `on` maps each of the child's outcomes (its `outcomes`, or `succeeded` and `failed`) to
  one target: a step ID, `exit.<outcome>` naming one of this workflow's outcomes
  (`succeeded` or `failed` when it declares none), a limited edge, or a wait
  `{wait: answer|approval|repair|external, then: <target>}`. An `approval`
  wait also names `onHandBack: <step>`, the step a hand-back continues at
  (`wait_hand_back_required` without it); any other wait may not
  (`wait_hand_back_unsupported`). Validation reports `edge_outcome_undeclared`,
  `edge_outcome_missing`, `edge_target_unknown`, `edge_exit_undeclared`,
  `edge_limit_target_invalid`, `wait_rule_required`, `wait_rule_unsupported`,
  `wait_rule_invalid` and `wait_rule_reference_invalid` at location `step_edge`, which
  names the step and the outcome. A wait's `then` and `onHandBack` are checked as
  targets; a step reached only through one of them, or through a limited edge's `then`
  or `else`, is reachable.
- A step target may be a later step, which starts in the same attempt, or the step
  itself or an earlier step, from which the run continues in a new attempt.
- A limited edge `{then: <step>, limit: <1..64>, else: <target>}` takes `then` while the
  run has taken it fewer than `limit` times, then `else`: a step ID, an exit or a wait,
  never another limited edge. An exit as `then` is `edge_limit_target_invalid`.
- An `external` wait carries `rule: {source, event, where}` and no other wait may
  (`wait_rule_required`, `wait_rule_unsupported`). `source` names a provider other than
  `um`, `event` one event name or a list, and `where` equalities joined by ` and ` over
  `event.<path>`, quoted literals, `true`, `false`, `inputs.<name>` (a `text` input) and
  `outputs.<step>.<output>` (a `text` export of an earlier step), for example
  `event.pullRequest == outputs.design.pullRequest`. Anything else in the rule is
  `wait_rule_invalid`; an operand naming another value is `wait_rule_reference_invalid`.

`forEach` runs the child once per element of a `json` list, in order. It is a reference
or `{items: <reference>, empty: <outcome>}`, `empty` the outcome for an empty list:

```yaml
  build:
    kind: workflow
    workflow: build.yaml
    forEach: outputs.design.parts
    inputs:
      part: { ref: item.name }
    on: { built: exit.landed, failed: exit.failed }
```

It names a `json` root input or a `json` export of an earlier step; anything else is
`for_each_reference_invalid`, and an `empty` the child lacks `for_each_empty_undeclared`,
both at location `step_for_each`.

A workflow with a `workflow` step is edge-scheduled. Every ordinary step in it must be a
`workflow` step (`edge_scheduling_kind`), the first step in document order is the entry,
and every other step must be the target of some edge (`step_unreachable`); both failures
are at location `step`. References supply values but add no dependency. `outcomes` may be
declared alone, because the `exit` edge a run reaches chooses the outcome; an `outcome`
block is `invalid_outcome_declaration`. Finalizers work as in any workflow. Cloud does
not run workflows with `workflow` steps.

## Complete example

This definition demonstrates direct command vectors, an inferred file-data edge, an
export, and an all-outcome finalizer. Command programs and runtime output files are
checked only when a separately authorized run uses them.

<!-- workflow-reference-fixture:installed-workflow:begin -->
```yaml
# yaml-language-server: $schema=https://docs.usefulmachinery.com/schemas/workflow-v1.schema.json
schemaVersion: 1
description: Build and summarize a report, then always clean up.

steps:
  build:
    kind: cmd
    command:
      argv: ["./scripts/build.sh"]
    outputs:
      report:
        kind: file
        from: path
        path: artifacts/report.json
        mediaType: application/json

  summarize:
    kind: cmd
    inputs:
      report:
        ref: outputs.build.report
    command:
      argv: ["./scripts/summarize.sh"]
    outputs:
      summary:
        kind: text
        from: path
        path: artifacts/summary.txt

finalizers:
  cleanup:
    kind: cmd
    when: [succeeded, failed, cancelled]
    inputs:
      context:
        ref: finalization.context
    command:
      argv: ["./scripts/cleanup.sh"]

exports:
  summary:
    ref: outputs.summarize.summary
```
<!-- workflow-reference-fixture:installed-workflow:end -->

The `build -> summarize` edge is inferred from the output reference. Adding
`dependsOn: [build]` to `summarize` would be valid but unnecessary because the data edge
already requires `build` to succeed. `cleanup` is required by default and is considered
after every ordinary outcome.

## Safety boundaries

- Begin with read-only inspection. Do not activate an untrusted repository environment
  merely to inspect it.
- Preserve existing, partial, and unrelated files. Show collisions and obtain approval
  before replacing content.
- Authoring files, validating a definition, running a workflow, executing repository
  checks, installing tools, using the network, and mutating source control are separate
  authority boundaries.
- Never inspect or reproduce provider credentials. A valid definition proves neither
  credential readiness nor harness/model availability.
- Pass commands and paths through an argument-vector process interface. Never
  interpolate repository text or paths into shell source.
- Inputs and profiles are data, not permission to access files, processes, networks,
  tools, secrets, credentials, or repositories.

Workflow V1 has no workflow-declared retry, timeout, loop, matrix, fan-out, concurrency,
secret, environment, or workspace field.

## Schema retrieval and authoritative validation

Print the installed raw structural contract, unchanged and without network access:

```sh
um workflow schema
```

The schema checks document structure only. Validate a complete materialized source
bundle with the installed CLI:

```sh
um workflow validate \
  --source-root <ROOT> \
  <WORKFLOW_FILE> \
  --json
```

`workflow validate --json` is authoritative for graph, type, reference, path, policy,
and static-source validation. It returns one structured result. Accept the definition
only when the process succeeds and the closed `outcome` is `valid`. Validation does not
execute commands or agents, inspect credentials, check model access, or contact
Useful Machinery.

For the complete online contract, use the public
[authoring guide](https://docs.usefulmachinery.com/agent/workflow-authoring.md),
[language reference](https://docs.usefulmachinery.com/reference/workflow-v1.md), and
[raw schema](https://docs.usefulmachinery.com/schemas/workflow-v1.schema.json).
