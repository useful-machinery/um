# Useful Machinery CLI

**SDLC as Code.** Useful Machinery turns your software development lifecycle into
repeatable workflows, versioned alongside your code.

Define commands and coding agents in one repository-owned YAML file, connect typed
outputs to downstream steps, run independent work concurrently, and retain a result you
can inspect instead of reconstructing what happened from terminal scrollback.

The CLI is the open-source command-line and runner executable for Useful Machinery. It can
run workflows locally with Pi, Claude Code, and Codex, or serve them from an enrolled
outbound runner. The project is early, but the local workflow engine and its authoring,
execution, and inspection tools are available today.

## Get started

Open your repository in your coding agent and paste this prompt:

```text
Help me get started with Useful Machinery in this repository.

Retrieve and follow the official onboarding guide:
https://docs.usefulmachinery.com/agent/onboarding.md

Treat the guide as the source of truth. Inspect my setup first, explain what you recommend, and ask before installing software or modifying files.
```

The guide helps your agent inspect your setup, verify or propose installation of
the official CLI, and offer a first repository workflow before guiding you through
Cloud account setup.

A starter workflow follows `implement → check → review`: you supply the task,
an agent implements it, your repository checks run, and another agent reviews the
result. Local workflow authoring and validation do not require a Cloud account.

You can also read the [official onboarding guide](https://docs.usefulmachinery.com/agent/onboarding.md)
directly. Follow it for current installation and setup guidance.

## Why SDLC as Code?

Coding-agent automation often begins as a loose sequence of shell commands, prompts,
and copy-and-paste handoffs. That approach becomes difficult to repeat, supervise, and
recover once work spans several tools or agents. Keep workflows, prompts, checks,
and output schemas in source control so changes to your engineering process can
be reviewed alongside changes to your software. Useful Machinery gives the work a runtime:

- **One graph for commands and agents.** Express control flow and typed data flow in a
  workflow dependency graph instead of coordinating scripts and model sessions by
  hand.
- **Parallel work with explicit handoffs.** Let independent steps run concurrently, pass
  text, schema-validated JSON, and files to downstream work, and export Git branch
  results when a workflow changes a repository.
- **Durable execution.** Follow progress in the terminal, inspect retained runs later,
  recover bounded step failures, and retry eligible runs explicitly.
- **Repository-owned automation.** Version workflows, prompts, attachments, and result
  schemas beside the code they operate on.
- **Validate before you spend.** Resolve the complete workflow bundle and check its
  graph, references, types, files, and policy offline before starting commands or
  consuming model tokens.

## What you can build

Use Useful Machinery to:

- run tests, linters, builds, and several independent code reviews in parallel, then
  combine their findings in one final artifact/PR;
- have one agent extract schema-validated facts and pass them directly to another agent
  for planning, review, or synthesis;
- mix deterministic command steps with agent work, while preserving typed outputs for
  downstream steps and callers;
- add bounded recovery to flaky work and finalizers for cleanup or outcome-specific
  reporting; and
- retain machine-readable results and portable artifacts for later inspection or use by
  other tools.

The bundles under [`examples/workflows/`](examples/workflows/) include ready-to-run
recipes for command data flow, parallel agents, structured results, recovery,
cancellation, advisory failures, attachments, and finalizers.

## Explore

| I want to… | Start here |
| --- | --- |
| See available features and limitations | [Current capabilities](#current-capabilities) |
| Find workflow examples | [Workflow examples](examples/workflows/README.md) |
| Learn workflow syntax and semantics | [Workflow V1 reference](https://docs.usefulmachinery.com/reference/workflow-v1.md) |
| Validate and run a local workflow | [Validation](#local-workflow-validation) and [execution](#local-workflow-execution) |
| Publish and manage Cloud workflow definitions | [Cloud workflow definitions](#cloud-workflow-definitions) |
| Submit and inspect Cloud runs | [Cloud runs](#cloud-runs) |
| Understand this public mirror | [Contributing](#contributing) |

## A workflow at a glance

This workflow makes a repository check and two agent reviews eligible to run
concurrently. The final agent waits for the check and receives both review outputs:

```mermaid
flowchart LR
    C["Repository check<br/>(command)"] --> S["Synthesize<br/>(agent)"]
    R1["Correctness review<br/>(agent)"] -->|text output| S
    R2["Security review<br/>(agent)"] -->|text output| S
    S --> O["Exported report"]
```

In Useful Machinery workflows, output references create the data-flow edges automatically:

```yaml
# yaml-language-server: $schema=https://docs.usefulmachinery.com/schemas/workflow-v1.schema.json
schemaVersion: 1

description: Check a repository, review it in parallel, and combine the findings.

agentProfiles:
  coding:
    harness:
      kind: pi
      config:
        model: provider/model
        thinking: high

steps:
  check:
    kind: cmd
    command:
      argv: ["./scripts/check"]

  correctnessReview:
    kind: agent
    agent:
      profile: coding
      systemPrompt: prompts/reviewer-system.md
      message:
        text:
          - file: prompts/review-correctness.md
    outputs:
      notes:
        kind: text
        from: agent_response

  securityReview:
    kind: agent
    agent:
      profile: coding
      systemPrompt: prompts/reviewer-system.md
      message:
        text:
          - file: prompts/review-security.md
    outputs:
      notes:
        kind: text
        from: agent_response

  summarize:
    kind: agent
    dependsOn: [check]
    agent:
      profile: coding
      systemPrompt: prompts/reviewer-system.md
      message:
        text:
          - file: prompts/summarize-reviews.md
          - ref: outputs.correctnessReview.notes
          - ref: outputs.securityReview.notes
    outputs:
      report:
        kind: text
        from: agent_response

exports:
  report:
    ref: outputs.summarize.report
```

Replace the example model with one available through your Pi installation and add the
referenced prompt files. Then validate the bundle without executing it, run it in a new
durable run directory, and return to that run later:

```sh
um workflow validate \
  --source-root . \
  .um/workflows/review.yaml

um workflow run create \
  --path . \
  --workflow .um/workflows/review.yaml \
  --execution-root . \
  --run-dir ~/.um/runs/review-001

um workflow run status ~/.um/runs/review-001
um workflow run view ~/.um/runs/review-001
```

During execution, Useful Machinery schedules ready steps, supervises each command or fresh agent
session, captures declared outputs, and publishes one durable terminal result. The same
run handle works for noninteractive status, archived inspection, and an explicit retry
when the retained state is eligible.

## Current capabilities

The current release supports:

- offline Workflow V1 validation, an installed authoring reference, and structural JSON
  Schema output;
- local command, Pi, Claude Code, and Codex DAG execution with typed data flow,
  concurrency, recovery, finalizers, imports, exports, durable status, retry, and
  archived inspection;
- portable Artifact Set V1 validation without the original run or source checkout;
- OAuth device login, renewable human sessions, linked sign-in identity management,
  explicit service API-key authentication, service-principal creation and credential rotation,
  human-service delegation management, logout, account signup, display-name management, account and organization deletion
  scheduling, organization discovery, profile management and owner audit history,
  invitation issuance and lifecycle management, the
  current principal's invitation inbox, one-page member-directory reads, actor-bound GitHub App setup,
  installation and repository discovery, complete project and repository configuration,
  mixed named-input and inputless Cloud run creation, explicit single-use Run Input Set
  administration, retained-input inspection/download/deletion, terminal waiting, and
  Artifact Set download and inspection;
- runner and runner-pool administration, including personal pools, pool status and pool
  join keys, plus prerequisite diagnostics; and
- enrollment, or joining a pool with a join key when the service first starts, and service
  operation for an outbound runner that connects only to its Cloud-issued endpoint and
  waits for explicit start authorization.

The Cloud management surface is not complete. The CLI can discover and manage accessible
organizations, manage invitations and membership, connect GitHub App installations,
discover authorized repositories, manage projects and runner pools, create mixed-input
or inputless runs for a ready project, administer Run Input Sets, inspect/download/delete
retained run inputs, wait for terminal status, and download verified Artifact Sets. It
does not yet guide the rest of Cloud onboarding.

## Service principals and API keys

Create a service principal while signed in as a human, and direct its show-once initial
API key to a new private file:

```sh
um service-principal create \
  --display-name "Build agent" \
  --api-key-file ./build-agent.key
```

The destination is created with mode `0600` and is never overwritten. Use
`--api-key-file -` only for a deliberate secret pipeline; it writes only the API key to
standard output and sends status to standard error. `--json` cannot be combined with
that stdout form, and JSON output never contains an API key. A signal before request
dispatch stops creation or issuance; after dispatch, the CLI completes the bounded
request and key delivery despite a later `SIGINT` or `SIGTERM` so a committed one-time
key is not discarded. If destination write or synchronization fails,
`api_key_delivery_failed` preserves the known non-secret metadata, destination, and safe
recovery classification. The receipt reports destination cleanup as `removed`,
`not_applicable` for standard output, or `uncertain`. File cleanup is attempted immediately; `uncertain` means the key may remain
at the destination and the operator must protect, inspect, and remove it before deciding
whether to revoke the known credential. The CLI does not revoke it automatically. If an
idempotent replay returns credential metadata without the show-once key, the CLI reports
`api_key_unavailable`, preserves the metadata in JSON, attempts to remove the reserved
output file, and does not issue or invent a replacement.

Service credentials are explicit per invocation. The CLI does not persist a service API
key or fall back between a service key and the human credential store:

```sh
um service-principal credential list \
  --service-api-key-file ./build-agent.key

um service-principal credential issue \
  --service-api-key-file ./build-agent.key \
  --api-key-file ./build-agent-next.key

um service-principal credential revoke \
  crd_01k0z6r1w8f4jy2m7q9v3x5abc \
  --service-api-key-file ./build-agent-next.key \
  --yes
```

Input key files must be regular, non-symbolic-link files owned by the current user with
mode exactly `0600`. A trailing newline is accepted. `--service-api-key-file -` reads one
key from standard input. Keys are redacted from debug values and are never emitted in
JSON, diagnostics, or ordinary logs. Credential listing returns non-secret metadata
only; issuance requires an existing platform API key, and revocation requires a
different active key because a credential cannot revoke itself.

Cloud-management leaves whose public API operation permits a service actor also accept
`--service-api-key-file PATH|-`. This includes service-capable account profile and
self-deletion operations; current-principal status and identity management;
organization, invitation, GitHub, project, publication, run, retained-input, Artifact
Set, runner-pool, and runner-administration operations. Identity linking by a service
also requires `--workload-token-file PATH|-` containing a fresh token from a configured
workload issuer. Both files use the same private-file checks and cannot both consume
standard input.

Human-only operations remain human-only: login, logout, signup, service-principal
creation, account-deletion cancellation, and organization-deletion request or
cancellation do not accept service API-key authentication. A service creating an
organization must additionally pass `--delegator-principal-id` with the exact human
principal from an active delegation. Delegation is attribution only: the service must
still hold its own required organization membership and role for every organization
operation, and the CLI never borrows authority from the human credential store.

Service API keys are also separate from runner credentials. They can authorize runner
administration when the service has the required organization role, but they cannot
enroll a runner, start `runner serve`, authenticate the runner connection, or replace a
runner activation or runner credential.

## Delegations

A signed-in human proposes a delegation to one exact active service principal:

```sh
um delegation propose prn_01k0z6r1w8f4jy2m7q9v3x5abc
```

Proposal is deliberately human-only. Acceptance is deliberately service-only and always
requires the nominated service's explicit API-key input; the CLI never substitutes a
stored human session:

```sh
um delegation accept dlg_01k0z6r1w8f4jy2m7q9v3x5abc \
  --service-api-key-file ./build-agent.key
```

Either exact participant may list, show, or end its relationships. Omit
`--service-api-key-file` to use the renewable human session, or supply it to act as the
service. Listing returns one page and exposes an opaque next cursor when another page is
available:

```sh
um delegation list --limit 50
um delegation list --cursor <CURSOR> \
  --service-api-key-file ./build-agent.key
um delegation show dlg_01k0z6r1w8f4jy2m7q9v3x5abc
um delegation end dlg_01k0z6r1w8f4jy2m7q9v3x5abc --yes
```

Ended relationships remain visible in delegation history with their terminal timestamp
and reason. Accepting or ending an already ended relationship returns the stable
`delegation_transition_unavailable` outcome; an inaccessible relationship returns
`not_found` without revealing whether it exists. Delegations do not expire under the
current public API contract. An expired or revoked actor credential instead returns
`unauthenticated`. Delegation records attribution only and never lends organization
membership or any other authority.

## Public API contract

Download the customer OpenAPI contract from its stable public URL:

<https://docs.usefulmachinery.com/openapi/public-api.yaml>

The hosted file describes the API deployed at `https://api.usefulmachinery.com`. A particular CLI
build remains self-contained: it uses the generated Rust client committed under
`crates/api/src/generated/` and does not fetch the hosted contract at build time or run time. Each
generated source header records the OpenAPI Generator version and the canonical contract's
path, and the contract a CLI build used is the one committed beside the client at that
build's source commit, even when the deployed API contract has since changed.

## Local workflow validation

Use `um workflow validate` to resolve a checked-out Workflow V1 bundle
without running it:

```sh
um workflow validate \
  --source-root ./my-repository \
  ./my-repository/.um/workflows/check.yaml
```

`workflow validate` accepts the same `--agent-profiles <PATH>` replacement document as
`workflow run create`, applies it identically, and reports the replaced profile names without
retaining the document.

Ready-to-run command and agent bundles are available under
[`examples/workflows/`](examples/workflows/). They cover basic DAGs, command input and
output dataflow, recovery, advisory and required failures, cancellation, all three agent
harnesses, structured agent results, attachments, and workflow finalizers. Definition
validation is offline; running an agent example requires its selected harness and
provider credentials and may consume billed tokens.

`--source-root` is required and defines the complete directory boundary for the
selected workflow YAML and all static prompts, message files, attachments, and result
schemas. The workflow file is an ordinary host path resolved from the process's initial
working directory, then canonicalized and required to remain within that explicit root.
The CLI does not infer the boundary from an enclosing repository or the YAML file's
directory.

For concise, version-aligned authoring guidance from the installed executable, run:

```sh
um workflow reference
```

The command writes its version-aligned embedded Markdown unchanged to standard output.
It includes the supported authoring loop, language semantics, safety boundaries, a
checked example, and these stable public handoffs:

- [Workflow V1 authoring guide](https://docs.usefulmachinery.com/agent/workflow-authoring.md)
- [Workflow V1 language reference](https://docs.usefulmachinery.com/reference/workflow-v1.md)
- [Workflow V1 raw schema](https://docs.usefulmachinery.com/schemas/workflow-v1.schema.json)

It needs no source checkout, workflow file, configuration, credentials, or network
access.

Retrieve the self-contained Workflow V1 JSON Schema from an installed executable
without a source checkout:

```sh
um workflow schema > workflow-v1.schema.json
```

The command writes raw JSON Schema bytes without a wrapper or preamble and preserves the
canonical asset's single terminal newline. It needs no workflow file, configuration,
credentials, or network access. Configure a JSON Schema-aware YAML editor or validation
tool to use the emitted file or the stable public raw-schema URL above; every schema
reference resolves within the document. Do not add a `$schema` property to the workflow
document.

The emitted schema checks document structure only. `workflow validate` remains the
complete workflow definition validator: it additionally checks the step graph,
references, path containment, static files, types, and policy. Passing a schema check
alone therefore does not establish that the CLI will accept the complete workflow
definition and its referenced files.

A successful human result reports the normalized source-root-relative workflow path,
the SHA-256 digest of the resolved source closure, step count, and required named input
interface. It never prints static file contents. Add `--json` for one schema-version-1
result with `valid` or `invalid` as its closed `outcome`; invalid results contain one
bounded CLI-owned diagnostic rather than parser or schema-library error text.

This command performs definition resolution only. It does not admit or start a run,
execute command or agent steps, check harness or model availability, read human or
runner credentials, or contact Useful Machinery. A zero exit status means only that the
local definition resolved successfully.

## Remote artifact metadata and download

List one page of a run's sealed Artifact Set without downloading it:

```sh
um artifact list acme-research run_01k0z6r1w8f4jy2m7q9v3x5abc \
  --limit 50
```

The result includes whole-set identity, seal and retention times, total member and byte
counts, and each member's portable path, media type, size, and SHA-256 digest. Members
are in portable-path order. `--limit` accepts 1 through 200, and `--cursor` continues
from the opaque `next cursor` value; the limit may change between pages. Add `--json`
for a schema-version-1 result with `outcome: "listed"` and the page under `artifactSet`.

Listing calls only the metadata inventory operation. It does not request exact download
capabilities, read artifact bytes, or create output files. A missing run and an
unattached, deleted, or not-yet-sealed Artifact Set are deliberately indistinguishable
and produce `outcome: "unavailable"`; an elapsed retention window produces
`outcome: "expired"`. Both states return a nonzero status, as do authorization,
network, invalid-cursor, and invalid-response outcomes.

Download and verify the complete set only when its bytes are needed:

```sh
um artifact download acme-research \
  run_01k0z6r1w8f4jy2m7q9v3x5abc --output ./downloaded-attempt-result
```

Download continues through every inventory page, requests fresh exact capabilities in
bounded batches, verifies each member and the complete portable set, and atomically
creates the requested output directory only after complete validation. Add `--json` for
one schema-version-1 receipt with `outcome: "downloaded"`, the Run and Artifact Set IDs,
verified member and byte totals, the committed destination, and deployment. A failed
transfer or verification emits no receipt and never commits the destination.

## Portable artifact validation

Validate one copied or downloaded Artifact Set V1 directory without its original run,
workflow source, or execution checkout:

```sh
um artifact validate ./downloaded-attempt-result
```

The command validates the complete closed `result.json` contract and every declared
export. It checks the exact root and `exports/` inventory, aliases, portable carrier
paths, confined regular-file identity, sizes, SHA-256 digests, media types, UTF-8 text,
compact ordered JSON, and changed or zero-delta `git_branch` artifacts. Git validation
checks the closed semantic metadata, carrier-presence rule, bundle-v2 header and profile,
pack framing and checksum, and reconstructible object facts without a destination
repository. Unknown kinds are rejected. There is no single-export selector or repair
mode.

Validation opens the selected directory read-only, follows no symbolic link beneath its
opened root, and leaves the set unchanged. It does not read workflow-run state,
credentials, or configuration, and it does not contact Useful Machinery, a provider, a Git
remote, or any other network service.

Add `--json` for one Artifact Validate Result Schema 1 document. A valid result has
`outcome: "valid"`, `exitStatus: 0`, the canonical artifact-directory path, and bounded
summary counts. An invalid result has `outcome: "invalid"`, `exitStatus: 1`, and the
complete contractually bounded deterministic diagnostic sequence instead of a summary.
Human mode reports the same diagnostic codes and ordering. Command-line usage errors
return 2 and do not inspect the artifact directory.

## Local workflow execution

Use `um workflow run create` to execute a mixed command, PiJsonV1,
ClaudeCodeStreamJsonV1, and CodexAppServerV1 agent Workflow V1 DAG in an existing
caller-owned directory and create one durable local run
directory:

```sh
um workflow run create \
  --path ./my-repository \
  --workflow .um/workflows/check.yaml \
  --execution-root ./my-checkout \
  --run-dir ./runs/check-001 \
  --max-parallel 2
```

The run directory must not exist and must be disjoint from the execution root. The CLI
normalizes it from its nearest existing parent and creates any missing parent suffix.
First-workflow onboarding uses the owner-private `~/.um/runs/` durable state root
by default; retained runs are application state and do not belong under `~/.config`.
The CLI retains immutable workflow and named-input bytes, durable closed run and attempt
state, and attempt 1's atomic result beneath `attempts/000001/result`. Every agent
invocation receives a fresh profile directory under
`attempts/<attempt>/diagnostics/` with immutable attempt, step, invocation, profile, and
exact-version metadata. PiJsonV1 and ClaudeCodeStreamJsonV1 additionally retain fresh
native session storage there; those sessions are never resumed, and Claude's temporary
ambient history links are removed when the invocation quiesces. CodexAppServerV1 starts
one fresh ephemeral thread and uses one transient owner-private SQLite directory beneath
invocation staging. App Server events remain authoritative, the SQLite directory is
removed after settlement, and no Codex-native thread or file is retained. A Codex
protocol failure may retain the same bounded structural rejection document as the other
profiles. The run-directory path is the local run handle. Add `--json` for one terminal
schema-version-1 object on stdout while the live presentation remains on stderr, or
`--plain` to force the line presentation on stdout. The `--json` object of `run create`,
`run retry` and `run resume` conforms to
[`schemas/workflow-run-result-v1.schema.json`](schemas/workflow-run-result-v1.schema.json),
whose completed-attempt `result` refers to the view schema's `WorkflowResult`.

Retained harness diagnostics can contain sensitive prompts, model output, tool activity,
paths, and extension or provider state. Native sessions or diagnostic files may be
incomplete or malformed after failure, cancellation, or forced termination and are
diagnostic only: workflow status, results, retry, and recovery never read or reopen them
as authority. They remain owner-private after every terminal outcome, are not included in
published attempt results, have no stable viewer or download interface, and disappear
only when the owning durable run directory is removed.

An ordinary command or agent step may declare `recovery.retries` from 1 through 10 and
optionally one `cmd` or fresh `agent` handler. Without a handler, Useful Machinery immediately
rechecks the unchanged target. A handler can only return the closed `recheck` or
`gave_up` decision through its engine-owned private result transport; it is not a DAG
node and publishes no output. Every target recheck and handler is a fresh physical
invocation in the same execution root. Provisional target failures remain history, and
only the target invocation supplying terminal success commits outputs. Repeated external
effects are at-least-once: authors supply stable domain idempotency keys through existing
explicit inputs or admitted environment, never from Useful Machinery invocation or round IDs.

Every declared root input is required. Supply Text with `--input-text <NAME> <TEXT>` or
`--input-text-file <NAME> <PATH|->`, JSON with `--input-json <NAME> <JSON>` or
`--input-json-file <NAME> <PATH|->`, and one File with
`--input-file <NAME> <MEDIA_TYPE> <PATH>`; append ordered attachment members with
`--input-attachment <NAME> <MEDIA_TYPE> <PATH>`, or provide a present empty collection
with `--input-attachments-empty <NAME>`. JSON is strict and may be constrained by a
retained workflow schema. File media types are explicit and optional declaration constraints
match exactly. All bindings are acquired before execution, and one Text or JSON file input
may exclusively claim standard input. Workflow commands always receive closed standard input.

`--agent-profiles <PATH>` replaces declared agent profiles for one invocation without
editing the workflow. The file is a closed YAML document with `schemaVersion: 1` and an
`agentProfiles` map whose keys name profiles the selected workflow declares and whose
values are complete harness definitions in the ordinary Workflow V1 grammar. Each named
profile is replaced as a whole, so every step, finalizer, and recovery handler that
references it runs on the replacement; unnamed profiles keep their declarations. A
replacement may select a different harness kind, and only the harnesses the effective
profiles select must be installed. The document never changes the source-closure digest.
Its exact bytes are retained with the run and reapplied by `run retry` and `run resume`, and
`result.json` records the digest and effective configuration under
`workflow.agentProfileReplacement`. The structural contract is
[`schemas/agent-profile-replacement-v1.schema.json`](schemas/agent-profile-replacement-v1.schema.json).

```sh
um workflow run create \
  --path . \
  --workflow .um/workflows/review.yaml \
  --execution-root . \
  --run-dir ~/.um/runs/review-002 \
  --agent-profiles ~/.um/profiles/claude.yaml
```

Without `--plain` or `--json`, run uses the interactive terminal interface when stdin and
stdout are terminals, `TERM` is present and neither empty nor `dumb`, and stdin is not
reserved by a Text or JSON input. Every other combination keeps the plain line stream.
Stderr terminal status does not select the interface. `NO_COLOR` disables semantic color
under `--color=auto` but does not disable interactive mode; `--color=always` overrides it.

The interface keeps a stable step selection, an inspector, and bounded per-step logs.
Use `Up`/`Down` or `j`/`k` to select a step, `Enter` and `Escape` to enter and leave its
full log, and `?` for all log navigation controls. Wide and narrow terminals use
side-by-side or stacked layouts. Resizing recomputes the layout without leaving
interactive mode; below 64 columns or 20 rows a resize notice replaces the panes while
execution, `Ctrl-C`, and eventual `q` handling continue.

`Ctrl-C` requests the existing orderly `user_request` cancellation and does not abandon
owned child work. `q` is ignored while execution, publication, cleanup, or run ownership
is active. Once the retained attempt is complete, the interface remains open for
post-run inspection until `q`; Useful Machinery then restores raw mode, the alternate screen, and
the cursor before invoking the shared standard plain-summary renderer. The summary and
process status are therefore the same contracts used by forced plain output. Inspect the
durable run later with `workflow run status <RUN_DIR>`; retry is always explicit.

A Claude Code profile is configured independently from Pi:

```yaml
agentProfiles:
  claude:
    harness:
      kind: claude_code
      config:
        model: claude-opus-4-1
        effort: high
```

`model` is a nonempty native Claude model string. `effort` is one of `low`, `medium`,
`high`, `xhigh`, or `max`; both fields are required and additional configuration is
rejected. Useful Machinery does not query a model catalog, install Claude Code, or supply a
fallback model or harness.

A Codex profile is also independent:

```yaml
agentProfiles:
  codex:
    harness:
      kind: codex
      config:
        model: gpt-5.4
        effort: xhigh
```

Both Codex values are required nonempty native strings, and additional configuration is
rejected. Useful Machinery does not read provider credentials during definition resolution,
installation discovery, rejection presentation, or doctor checks.

Local execution snapshots the inherited environment after resolution and removes
`UM_` variables before launching commands or agents. Other caller-provided values are
retained unless the closed harness profile fixes them. ClaudeCodeStreamJsonV1 removes
`CLAUDE_CODE_PROJECT_DIR_NAME` to keep retained-session routing authoritative and applies
its documented native controls; values such as `GH_TOKEN`, `GITHUB_TOKEN`, and Git or SSH
credential-helper configuration remain available for local repository setup and cloning.
The local caller owns that authority; Runner Serve applies a separate managed
credential-isolation policy. The CLI inspects the resolved agent steps and validates
each required harness once: `pi` for PiJsonV1, `claude` for
ClaudeCodeStreamJsonV1, and `codex` for CodexAppServerV1. Each search selects the first
executable candidate in inherited `PATH` and pins its canonical absolute path, exact
observed version, profile, and capabilities for the run. Each harness admits a
canonical stable release in its qualified range, `>=2.1.234 <2.2.0` for Claude Code, and
also admits, as untested, any capability-compatible stable release newer than its
qualification release (Pi 1.3.0, Claude Code 2.1.321, Codex 0.163.0); every native
Claude Code initialization frame must report the pinned observed version exactly. After
admission succeeds and before any step runs, the command writes one stderr advisory line
per selected untested harness naming the harness, the installed version, and the
qualification release, in every presentation mode and never per agent step; `--json`
stdout remains the result document alone. Later `PATH` changes cannot switch an admitted
executable. Command-only workflows probe no harness; each single-harness workflow
requires no unrelated installation. A mixed workflow requires exactly its selected
harnesses and never substitutes or falls back between them. Workflow definitions,
imports, and remote values cannot supply an executable or alter selection. The adapter
does not read Useful Machinery human or runner credentials and does not contact Useful Machinery; an
admitted agent harness may use the provider and other host authority selected by its
closed profile and inherited environment.

Workflow V1 outputs declare semantic `kind` separately from acquisition `from`. The six
rows are `text/path`, `text/agent_response`, `json/path`, `json/agent_result`,
`file/path`, and `git_branch/workspace`; consumers and exports observe only the semantic
kind. A Git branch output is export-only and cannot bind a downstream command input or
agent message. Before any step
starts, local admission requires the execution root to be exactly one SHA-1 Git worktree
root and pins its current commit as the baseline. Successful capture requires a clean
committed head descended from that baseline. A changed head publishes one Git bundle
carrier; an unchanged head publishes the semantic zero-delta result without a carrier.
Repository remotes, destination branches, providers, credentials, and publication policy
are not workflow fields.

## Local workflow status

Inspect the durable state and retry eligibility of an existing local run without changing
it:

```sh
um workflow run status ./runs/check-001
```

The run-directory positional is an ordinary host path resolved from the process's
initial working directory. Retry uses the same durable handle explicitly:

```sh
um workflow run retry ./runs/check-001 --execution-root ./my-checkout
```

To reexecute a selected ordinary step and its downstream ordinary dependents,
inherit the other eligible ordinary steps by reference, and rerun all finalizers:

```sh
um workflow run resume ./runs/check-001 --from build --from verify
um workflow run resume ./runs/check-001 --from verify \
  --workflow ./revised-workflow.yaml --execution-root ./another-checkout
```

`--workflow` selects a replacement within the original retained source-root boundary;
without it the immediately prior attempt's retained definition is used. The execution
root is selected independently and defaults to the prior attempt's root. Accepted
`--json` writes the terminal result as one stdout object conforming to
[`schemas/workflow-run-result-v1.schema.json`](schemas/workflow-run-result-v1.schema.json)
and emits the same accepted partition record to stderr before execution; a refused
continuation writes its rejection document under the same schema. The initial
run inputs and primary-workspace Git baseline remain immutable. A continuation is
allowed only from an eligible latest failed, cancelled, or interrupted attempt, after
all retained process groups are proven quiescent. Before actions run, plain output
reports the ordered reexecuted and inherited partitions. Executed steps, recovery
handlers, and finalizers can read the credential-free `UM_CONTINUATION_CONTEXT`
file; initial runs and explicit retries do not receive this variable. A continued
step starts fresh; an inherited step has neither a new invocation nor a copied log.
A required body-data consumer of an inherited skipped output is rejected, while
conditions, advisory consumers, finalizers, and exports retain their normal
unavailable-value behavior. `workflow run status` reports only base continuation
eligibility, not request-specific admission; an unavailable or incomparable historical
workspace snapshot yields `workspaceModified: "unknown"`.

Status reads only the closed `local-run.json` and `state.json` contracts and an existing
regular `run.lock`. For activated step recovery, plain and JSON snapshots report the
active or terminal target/handler role, round, target execution, decision, invocation
accounting and usage, raw terminal disposition, output ownership, and retry eligibility.
It never scans `.private`, creates the lock, acquires execution
ownership, waits for an owner, signals child work, or reads standard input. Its
non-acquiring lock query combines with two matching state revisions to distinguish an
active owner, a settled run, an abandoned nonterminal attempt, and ownership that cannot
be proven safely. An exact live process-group identity remains `ownership_unproven`; an
unlocked outstanding start action without its required process-guard registration makes
the run directory invalid rather than retry-eligible. The retry projection is eligible
or uses the first applicable closed reason in this order: `run_locked`,
`ownership_unproven`, `latest_attempt_succeeded`, then `latest_attempt_rejected`.

Without a mode option, and with `--plain`, one complete human snapshot is written to
stdout and operational diagnostics use stderr. `--json` writes exactly one ANSI-free
object conforming to
[`schemas/workflow-status-result-v1.schema.json`](schemas/workflow-status-result-v1.schema.json)
to stdout; run-directory, schema, lock-query, and unstable-snapshot failures are encoded
in that object rather than duplicated on stderr. Status has no TUI or live presentation
stream.

`--color` accepts `auto`, `always`, or `never`. It affects only semantic tokens in plain
output; `auto` requires terminal stdout, a usable `TERM`, and an unset or empty
`NO_COLOR`. JSON never contains ANSI. A complete snapshot returns 0 regardless of run
or retry disposition, an operational or output failure returns 1, and a command-line
usage error returns 2. SIGINT or SIGTERM before completed output returns 130 or 143,
respectively.

## Local workflow archived view

View a successfully published terminal attempt from one stable read-only snapshot:

```sh
um workflow run view ./runs/check-001 [--attempt 1] [--plain | --json]
```

Omitting `--attempt` selects the current attempt from the snapshot. Without an explicit
mode, terminal stdin plus terminal stdout plus a usable `TERM` opens the existing frozen
archived TUI; every other stream arrangement prints a plain completed-attempt summary.
`--plain` and `--json` force their modes regardless of terminal capability and conflict
with each other. The command never opens `/dev/tty` or falls back after selecting the
TUI.

The plain summary contains retained attempt and execution identity, lifecycle, duration,
node disposition, direct typed causes, activated Recovery Summary Schema 1,
per-invocation usage and output ownership, terminal counts, outcome, cancellation, and
finalization facts. It does not replay events or print retained stdout or stderr,
commands, exports, artifact contents, or agent observations, and it claims no ordering
between retained streams.

`--json` writes exactly one ANSI-free document conforming to
[`schemas/workflow-view-result-v1.schema.json`](schemas/workflow-view-result-v1.schema.json).
A successful view returns 0 and embeds the complete validated immutable selected result,
including when the workflow outcome is failed or cancelled. Expected acquisition and
attempt-selection failures return 1 as the schema's closed error variant on stdout
without duplicate stderr prose. Usage errors return 2. Local serialization, output, or
terminal failures return 1 and do not append a replacement document after a partial
prefix.

The loader validates the retained workflow, published result, cross-document identity,
artifact set, and retained budget before exposing either the safe archived projection or
the complete wire result. It opens only the state-recorded publication once, never
acquires execution ownership, and does not retarget if a retry advances the run.

Use `q` to restore an active TUI and return 0 without a workflow-run summary. SIGINT and
SIGTERM interrupt any mode with status 130 or 143; noninteractive rendering, writing,
and flushing remain abandonable even when stdout is full. Viewer signals never cancel a
workflow. `--color` affects only plain and TUI styling; JSON is always ANSI-free.

## Version inspection

Use `um --version` or `um version` for conventional one-line
output. Use `um version --json` for the schema-version-1 structured contract:

```json
{
  "schemaVersion": 1,
  "command": "um",
  "version": "0.10.0",
  "executablePath": "/resolved/path/to/um",
  "buildIdentity": "unknown"
}
```

Packaged builds replace the local `unknown` build identity with their source revision.
Official GitHub release binaries report the public commit identified by the release tag
as `buildIdentity`.
The schema does not define a release channel.

## Human authentication

Use `um auth login` to authenticate through a browser on the same machine or
another machine. The CLI prints an activation URL and user code, never opens a browser,
and never listens for an inbound callback. Add `--json` to receive newline-delimited
schema-version-1 events. Use `--force` to start a new device authorization transaction
without checking an existing credential with the API.

Use `um auth status` to ask the selected deployment whether the current
identity is authenticated, requires signup, is forbidden, is unauthenticated, or is
unreachable. `forbidden` (exit 1) means the deployment accepts the sign-in but refuses its
account, as it does for an account that is suspended, deleted, or scheduled for deletion;
the stored sign-in is kept, and `um account deletion cancel` stops a scheduled deletion
before its deadline. Add `--json` for the schema-version-1 structured result.
Authenticated, signup-required, and forbidden results preserve any server actions as
complete opaque JSON values. The CLI does not
validate action IDs or guide origins, fetch guides, infer commands, or execute actions.
Status always contacts the public API, including when no local credential exists.

Manage the OIDC identities linked to the signed-in account under `auth identity`:

```sh
# List one page; the current local-session identity is marked when that page contains it.
um auth identity list --limit 50

# Continue when the result includes a next cursor.
um auth identity list --limit 50 --cursor "$NEXT_CURSOR"

# Prove control of another identity in a fresh browser/device flow and link it.
um auth identity link

# Remove a non-current identity by the opaque ID returned by list.
um auth identity remove idn_01k0z6r1w8f4jy2m7q9v3x5abc --yes
```

`auth identity link` keeps the session that passed preflight as the acting identity
and uses a separately issued access token only as the proposed identity proof. It
requests fresh browser/device authorization without `offline_access` and never writes
the proof to the credential store. The final request stays bound to the preflight
session; if that credential is rejected after another local sign-in replaces it, the
command stops instead of retargeting the proof. Standard rejected-credential cleanup
can remove an invalid acting session, and the JSON terminal event then reports
`localSessionIdentity` as `removed` rather than `unchanged`. The deployment still
enforces the proof audience, approved issuer, identity type, and ten-minute issuance
window. An identity already linked to any account produces the same
identity-unavailable result; linking never merges accounts.

Human-authenticated identity removal uses the current local session and never starts a
weaker proof path. The deployment requires that session's access token to have been issued within ten
minutes and refuses to remove either its exact identity or the last usable identity. To
remove the identity currently used on this device, first find the item marked `current`
in `list`. Listing is oldest-first and returns one page, so follow each `nextCursor` with
`--cursor` until the marked identity appears. Record that ID, run
`um auth login --force`, and choose a different identity already linked to
the same account. That fresh login replaces the local session. Removing the former
identity then leaves the replacement local session signed in and unchanged. A removal
command never deletes or revokes the current local credential. With
`--service-api-key-file`, removal instead uses the selected service authority and never
reads or changes the local human session. Deployment workload-identity policy and
service-actor authorization still apply.

Identity list and remove use one schema-version-1 JSON document with `--json`. Linking
uses newline-delimited schema-version-1 activation and terminal-result events because it
waits for browser authorization. Listing returns exactly one page and preserves
`nextCursor`; `--limit` accepts 1 through 200. Human list items and successful link
results show a sign-in method (GitHub, Google, GitLab.com or Other); JSON identities
add `provider` (`github`, `google`, `gitlab.com` or `unknown`). This is only a
presentation hint based on the exact Useful Machinery Auth0 issuer and recognized
subject namespaces. A different issuer or unrecognized namespace is `unknown`, still
listable and removable by `idn_` ID. Workload identities omit `provider`. The API's
exact issuer, subject, current flag and optional email provenance remain unchanged.
For example, a linked human identity without an email claim may appear as:

```json
{"id":"idn_01k0z6r1w8f4jy2m7q9v3x5abd","kind":"oidc","issuer":"https://auth.usefulmachinery.com/","subject":"oauth2|um-gitlab-com-signin|um-gitlab-com:123456","createdAt":"2026-09-05T12:00:00Z","current":false,"provider":"gitlab.com"}
```

The example is synthetic: the GitLab.com namespace was observed in an isolated
tenant, not yet qualified against the production connection. These labels do not
enable a provider or link accounts by email.

One successful login establishes a renewable human session. The CLI stores the one-hour
access token, its expiration, and a rotating refresh token in
`~/.um/credentials.json`, then silently renews access for every human-authenticated
command while the refresh session remains valid. Auth0 expires refresh sessions after 30
days idle or 90 days total.

Use `um auth logout` to remove the human credential for the active deployment
and ask Auth0 to revoke its refresh token. The result distinguishes confirmed and
unconfirmed server revocation; local removal still completes when Auth0 is unreachable.
The human store remains separate from workflow-run state and all runner credentials.

Development deployments that use HTTP require `--allow-insecure-http` on each networked
leaf command. This includes authentication and linked identity management, account,
organization, GitHub connection, project, Cloud run, and runner administration commands;
the option is not global.

## Grants

`um grant` manages what Useful Machinery may do as you at other providers. A grant is an
OAuth grant (`github`, `gitlab.com`, `linear`), your consent at the provider, whose ID begins
`pgr_`, or a native grant (`claude_code`, `codex`), a coding agent's own login imported for
one organization, whose ID begins `agl_`. Each grant is used only on the runners you authorize
for it. The commands take your signed-in session only, never a service API key.

```sh
# Make a grant through the provider's own sign-in. For github, gitlab.com and linear this
# starts the provider's consent, prints its URL, opens it in your browser when this machine has
# one (not over SSH, and on Linux only with a display), and waits until the grant is active;
# approving the URL in any browser completes it. --no-wait returns once the consent has
# started. When you already hold a grant of the provider, the command asks before replacing it
# on a terminal and otherwise exits 2 unless you pass --replace; the new sign-in becomes that
# grant's next generation, keeping its ID and the runners it is authorized for.
um grant create linear
um grant create github --replace --json

# For codex this runs `codex login` in a private temporary Codex home (mode 0700) that it
# removes afterwards, and imports the login it writes; your own Codex home is never read or
# changed. With --device, or where this machine has no browser (over SSH, or on Linux without
# a display), it runs `codex login --device-auth`, whose code you enter in any browser.
# --login names that sign-in, the default, and is Codex's alone. --from-file imports an
# existing auth.json from an owned mode-0600 file, or - for standard input, instead. The grant
# is for your only organization, or the one --organization names. A coding agent's sign-in
# has no consent to wait for, so --no-wait and --timeout are refused for codex and claude_code.
um grant create codex
um grant create codex --device --organization acme
um grant create codex --from-file ~/backup/auth.json --replace

# For claude_code this runs `claude setup-token` with a private temporary configuration
# directory (mode 0700) that it removes afterwards; your own Claude Code configuration is never
# read or changed. Its prompts appear in your terminal and what you type, such as a code to
# paste, reaches it; the token it prints is imported and never shown. Without a terminal on
# standard input the command fails at once, starting no sign-in, and names --token-file -.
# --token-file imports a token claude setup-token printed from an owned mode-0600 file, or -
# for standard input, instead.
um grant create claude_code
um grant create claude_code --token-file - --organization acme < token.txt

# Without a provider, or with --all, run the sign-in of each provider you hold no active or
# reconnect_required grant of, one after another: github, gitlab.com, linear, claude_code,
# codex. Each provider's name is printed on standard error before its prompts. A sign-in that
# fails does not stop the ones after it; a provider this deployment does not serve is reported
# provider_unavailable and passed over. The coding agents' grants are for your only
# organization, or the one --organization names; when you are a member of none or of several
# and name none, a coding agent you hold nowhere is reported organization_required and passed
# over, and the consents still run. The command ends with one outcome per provider and exits 1
# when any sign-in failed or every provider selected was passed over; an interrupt or a
# termination signal during a sign-in ends it there with status 130 or 143. --device applies to
# the Codex sign-in; --no-wait and --timeout apply to the consents. --from-file and --token-file
# need a provider, and --replace a provider or --workflow. With --json each provider's events
# are followed by a summary event.
um grant create
um grant create --all --organization acme --json

# With --workflow FILE, which repeats, sign in to only the providers those workflows and the
# workflows their workflow steps run call for, derived as um runner doctor --workflow derives
# them; the kind gitlab means gitlab.com. Before any sign-in the command prints the workflow
# files, each provider to sign in to and each passed over as held with what calls for it, and
# the declarations that may use credentials the workflows do not name, for which no sign-in is
# run. Without --source-root each file is bounded by the root of the Git repository that holds
# it, and a file in no repository is refused with exit 2. A file that does not resolve is
# reported as um workflow validate reports it, with exit 1. --workflow does not take a provider.
# A provider every grant of which you hold needs reconnecting (its consent lapsed, or a runner
# reported its login refused) is planned to reconnect: on a terminal the command asks first,
# before any sign-in, and --replace replaces it without asking, storing the sign-in as the
# grant's next generation so it keeps its ID and its runners; it never replaces a healthy
# grant. One you keep is reported reconnect_required, naming um grant create PROVIDER
# --replace. With --json the plan is a first workflow_requirements event, whose selected,
# reconnect and held arrays name each provider and what calls for it.
um grant create --workflow workflow.yaml --workflow ../other/workflow.yaml
um grant create --workflow workflow.yaml --source-root . --json
um grant create --workflow workflow.yaml --replace

# List every grant you hold: its ID, provider, kind, the account it acts as (an OAuth grant's
# linked identity, a native grant's organization), its state and generation, and each runner
# it is authorized for with that runner's availability and next action and, for a native
# grant, the last use the runner reported and when. Then name each provider you hold nothing
# for. It reads every page; there is no --limit or --cursor.
um grant list

# Show one grant in detail: for a native grant what each runner was last delivered and
# reported, its last use and when, and for each runner whose authorization has ended the one
# that ended last, with its removal state and stop, also after that runner is authorized
# again; for an OAuth grant its scopes, last use and expiries. Name it by its ID, or by its
# provider when you hold one grant of it.
um grant list agl_01k0z6r1w8f4jy2m7q9v3x5abc
um grant list codex --json

# Authorize one runner, named by the rnr_ ID that `um runner list acme` prints, to use a
# grant. For an OAuth grant, every run on that runner that you start can then be issued tokens
# from it; for a native grant, every run that runner serves, whichever member caused it, can
# use the login, and every step on that runner can read it. The command reads the grant, says
# which applies, and does nothing without --yes. A Codex grant serves one runner at a time.
um grant authorize codex rnr_01k0z6r1w8f4jy2m7q9v3x5abc --yes

# Leave the runner out to choose it from a list. At a terminal it shows the runners the grant
# can be authorized for and is not yet, each by name, pool, online or offline, mode when not
# enabled, and ID, online enabled runners first; Enter takes the highlighted one. It then
# states what authorizing does, naming the runner, and asks before it acts unless --yes was
# given: any answer but y, or Esc at the list, sends nothing and exits 2. Where the command
# cannot prompt (no terminal, or --json), it lists the same runners and exits 2 having sent
# nothing. `um grant authorize --all` offers the runners of your organizations, and
# `um grant deauthorize GRANT` only the runners the grant is authorized for.
um grant authorize codex

# Offered more than one runner, the list ends with a choice of all of them, except for a Codex
# grant, which serves one runner at a time, and for --all. With all chosen the command states
# how many runners it covers, acts on each in turn and prints one outcome per runner; it exits
# 1 when any runner was refused, and a failure that is not one runner's refusal ends it there.
um grant authorize claude_code

# Authorize that runner for every grant of yours that can serve it: each OAuth grant and each
# native grant of the runner's organization. It prints one outcome per grant (`authorized`,
# `already_authorized`, `other_organization` for a native grant of another organization, or
# the refusal, such as `authorized_elsewhere` for a Codex grant authorized for another runner)
# and exits 1 when any grant was refused. A failure that is not one grant's refusal, such as a
# rate limit, ends it at that grant: it prints the outcomes so far, then the failure naming
# that grant, and exits 1.
um grant authorize --all rnr_01k0z6r1w8f4jy2m7q9v3x5abc --yes

# End that runner's authorization. For a native grant Cloud also asks the run that runner is
# serving, whichever member started it, to stop, and the command prints whether the runner has
# confirmed removing its copy and the run asked to stop; for an OAuth grant no run is stopped
# and no further token is issued there.
um grant deauthorize codex rnr_01k0z6r1w8f4jy2m7q9v3x5abc --yes

# Revoke a grant everywhere. An OAuth grant is erased and revoked at the provider where the
# provider offers it. A native grant is erased and the command prints each runner it was
# authorized for with its removal state and the run asked to stop there; a confirmation covers
# only the copy that runner kept, and the session at the provider stays signed in until you
# sign out there.
um grant revoke github --yes
```

`list --json` prints one schema-version-1 document with outcome `listed`, `grants` (each with
`id`, `provider`, `kind`, `account` with its `kind`, `id` and, where it is known, `name`,
`label` where there is one, `state`, `generation`, `createdAt` and `authorizations`, each with
`runnerId`, `organizationId`, `availability` and `nextAction` and, for a native grant whose
runner reported a last use, `lastUse` (`accepted` or `refused`) and, where Cloud holds it,
`lastUseAt`, or the grant's own `availability` and `nextAction` when no runner is authorized)
and `notConnected`; `list GRANT --json` prints outcome `shown` with `grant`. A provider name
you hold no grant of reports `grant_not_found` and exits 1; one you hold several grants of
exits 2 and names their IDs.

`create --json` prints newline-delimited events: `consent_required` with `sessionId`,
`provider`, `authorizationUrl` and `expiresAt`, then `consent_result` with its `outcome`
(`completed` with `grant`, or `failed`, `expired` or `recovery_required` with `reason`, which
exit 1). A consent that cannot start reports `provider_unavailable` or `consent_in_progress`.
For `codex` it prints one `import_result` event with outcome `imported`, or `replaced` when
it stored the next generation of a grant you hold, and `grant`; a sign-in that made no login
reports `provider_login_failed` and exits 1, and nothing is stored. A source option the
provider does not take, `--organization` for an OAuth provider, and a missing organization
when you are a member of several exit 2 before anything is run or sent.

`authorize`, `deauthorize` and `revoke` take `--yes`; without it they send nothing, state what
acting does for a grant of either kind and exit 2. `authorize` and `deauthorize` given no
`RUNNER` read the runners they could act on instead; at a terminal they show them as a list to
choose from and, without `--yes`, ask before acting, and otherwise exit 2 naming them; `--json`
prints outcome `runner_required` with `grantId` (absent for `--all`) and `runners`, each with `id`,
`name`, `pool`, `connectivity`, `mode` and `organizationId`. With no runner to offer they
exit 1 with `runner_not_found`, `already_authorized`, `authorized_elsewhere` or
`authorization_not_found`. Each sends its request with one
idempotency key and replays it once when the answer is lost. `authorize --json` prints outcome
`authorized` with `grantId` and `authorization`; `authorize --all --json` prints outcome
`authorized`, or `refused` when any grant was refused, with `runnerId` and `grants`, each with
`grantId`, `provider`, `kind`, `outcome` and, when it was authorized, `authorization`; a
failure that ends it follows that document with the failure's own.
`deauthorize --json` prints outcome `deauthorized` with `grantId`, `authorizationId`,
`runnerId` and, for a native grant, `removal` and `stop`; `revoke --json` prints outcome
`revoked` with `grantId`, `provider`, `kind` and, for a native grant, `endedAuthorizations`.
A refusal exits 1 with its outcome (`runner_not_found`, `runner_holds_login`,
`already_authorized`, `authorized_elsewhere`, `authorization_not_found`, `grant_not_found` or
`provider_unavailable`) and a remedy.

Every runner authorization `list` prints, and every provider you hold nothing for, carries one
`availability` (`available`, `unverified`, `reconnect_required`, `pending`, `delivery_failed`,
`insufficient_permission`, `not_delivered` or `not_connected`) and one `nextAction`, and the
text form names the command or step that next action means: `um grant create PROVIDER
--replace` to reconnect, `um grant authorize GRANT RUNNER --yes` for a grant no runner is
authorized for, `um runner grant sync` on the runner's host after a failed
delivery, and `um grant create PROVIDER` for a provider you hold nothing for. A runner that
has not yet reported installing what Cloud holds is `pending` with `wait_for_runner`, and a
login in place that the runner has neither checked with its provider nor had a step use at
this generation is `unverified`.

On the runner's host, `um runner grant sync` asks the live runner, through the control socket
its runner home's configuration names or, with `--config PATH`, that file's, to set up the
agent logins authorized for it now and prints each provider's readiness; see
[Runner serve](#runner-serve).

The earlier commands remain for one release with their arguments, output and exit statuses
unchanged, but are absent from help, and each first prints a line on standard error naming
the command that replaces it: `um auth grant create` and `um auth agent-login import` are
`um grant create PROVIDER`; `um auth agent-login reconnect` is
`um grant create PROVIDER --replace`; `um auth grant list`, `um auth agent-login list` and
`um auth credential status` are `um grant list`; `um auth agent-login show` is
`um grant list GRANT`; `um auth agent-login authorize`, `deauthorize` and `revoke` and
`um auth grant revoke` are the `um grant` leaf of the same name; and
`um runner agent-login sync` is `um runner grant sync`. `um auth grant uses` has no
replacement.

## Account management

OAuth login does not implicitly create a Useful Machinery account. When authentication
status is `signup_required` and the deployment advertises signup, use
`um account signup` after the customer explicitly approves account creation.
Add `--json` for a schema-version-1 structured result. The CLI authenticates the request
with the existing human credential and retries an ambiguous transport failure once with
the same opaque idempotency key.

Set or clear the authenticated account's optional display name with exactly one of these
options:

```sh
um account update --display-name "Ada Lovelace"
um account update --clear-display-name
```

The deployment removes surrounding Unicode whitespace and accepts 1 through 200 Unicode
scalar values without control characters. An equivalent normalized name and clearing an
already absent name are successful no-ops. Successful human output says that the display
name is `set` or `cleared`; `--json` reports the same stable `outcome` and the active
`principal`. The principal omits `displayName` after a clear.

Each invocation generates a fresh opaque idempotency key. After an ambiguous transport
failure, the CLI retries once with the same key and exact merge patch. If the result still
cannot be confirmed, check `um auth status` before issuing another update.
Structured failures report one of `invalid_display_name`, `unauthenticated`, `forbidden`,
`idempotency_conflict`, `request_too_large`, `unsupported_media_type`, or `unreachable`.

Schedule deletion of the authenticated human account only after reviewing the 30-day
window and confirming explicitly:

```sh
um account deletion request --yes
```

A successful request reports the principal ID, `deletion_pending` state, request time,
deadline, and update time. It conditionally removes the exact local human credential
that authorized the request; it never removes a credential that another process replaced
while the request was in progress. The result reports `localCredential` as `removed` or
`changed`; a local cleanup error reports `removal_unconfirmed`, returns nonzero after
showing the confirmed schedule, and directs the user to local sign-out. An unconfirmed
API request retains the local credential and must not be repeated
until the lifecycle state has been confirmed with the deployment operator.

If the reply to the request is lost, the command repeats the identical request with the same
request identity. When the deployment refuses that repeat as `forbidden`, the first request
may already have scheduled deletion, so the command reports `acceptance_unknown` (exit 4)
instead and keeps the local credential. Run `um auth status` to check the account's state,
and `um account deletion cancel` only to keep an account whose deletion is scheduled.

Supplying `--service-api-key-file PATH|-` to the request deletes the authenticated
service principal immediately instead of creating a schedule. A successful JSON result
reports `outcome: "deleted"`; the command does not read, remove, or update a human
credential store.

Cancel before the deadline with a fresh browser proof from the same still-linked identity:

```sh
um account deletion cancel
```

Cancellation does not use or require the stored local session. It requests a fresh
browser/device access token without `offline_access`, sends that token only as the API
bearer, and never stores it or a refresh token. A successful cancellation reports the
returned active lifecycle transition while leaving the credential store unchanged; run
`um auth login` afterward to establish a renewable local session. This
separation is intentional: an old stored credential, a refreshed background session, or
a different linked identity does not replace the API's distinct, later, same-identity
proof requirement.

`account deletion request --json` emits one schema-version-1 result. Cancellation emits
newline-delimited schema-version-1 `activation_required` and terminal `result` events
because browser authorization is part of the command. A repeated request or cancellation
with a new request identity reports `transition_unavailable` when the lifecycle is
already in the requested state or the transition is otherwise unavailable. Scheduling
also reports `human_owner_required` when deleting the account would leave an organization
without an effective human owner, and `acceptance_unknown` when the deployment refuses the
repeat of a request whose reply was lost; that result carries no `schedule` or
`localCredential`. Browser proof rejection reports
`reauthentication_required`, and a cancellation whose proof belongs to an account the
deployment refuses, such as one that is suspended or already erased, reports `forbidden`
(exit 1); no outcome weakens the linked-identity or issuance-time checks.

## Organization management

Organization commands default to the selected local human OAuth credential. Supported
commands accept `--service-api-key-file` to use explicit service authority instead,
without reading the human credential store; runner credentials are never used. Service
organization creation also requires `--delegator-principal-id` naming an authorized human
principal, and the deployment enforces the delegation contract. Organization deletion
request and cancellation remain human-only; cancellation uses the fresh proof flow
below. Other operations do not start login or signup. Organization references must be an
exact `org_` ID or lowercase URL-safe slug. The CLI rejects invalid references locally
and passes accepted references to the deployment without normalization.

```sh
# Discover your organization memberships and their organization IDs and slugs.
um organization list --limit 50

# Create an organization. The deployment may assign the slug when it is omitted.
um organization create \
  --display-name "Acme Research" \
  --slug acme-research

# Read an accessible active organization by ID or exact slug.
um organization show acme-research

# Update the display name, slug, or both. At least one option is required.
um organization update acme-research \
  --display-name "Acme Labs" \
  --slug acme-labs

# Schedule deletion for 30 days as a current active human owner.
um organization deletion request acme-labs --yes

# Cancel before the deadline with a fresh browser proof.
um organization deletion cancel acme-labs

# Read one active member-directory page. Both pagination options are optional.
um organization member list acme-labs \
  --limit 50 \
  --cursor opaque-continuation

# Read one owner-only page containing active, suspended, and ended memberships.
um organization member history acme-labs --limit 50

# Read one owner-only page of privacy-safe audit records.
um organization audit list acme-labs \
  --limit 50 \
  --cursor opaque-continuation

# Change another member's organization role.
um organization member update acme-labs \
  mem_01k0z6r1w8f4jy2m7q9v3x5abc \
  --role owner

# Permanently end another member's membership.
um organization member remove acme-labs \
  mem_01k0z6r1w8f4jy2m7q9v3x5abc \
  --yes

# Permanently end your own membership.
um organization leave acme-labs --yes
```

Add `--json` to any of these leaves for its schema-version-1 result. Organization
listing returns one oldest-first page of the selected principal's membership history.
Every row includes the membership and organization IDs plus lifecycle state. The
organization name and slug appear only while both the organization and membership are
active; suspended and ended rows retain history without exposing mutable organization
profile data. Pass the returned `nextCursor` back with `--cursor` to continue.

Organization, active-member, and membership-history listing each return exactly one
page, preserve `nextCursor`, and do not follow it automatically. Their `--limit` options
accept 1 through 200. Active-member listing remains available to effective members;
membership history and member changes require an active owner. History preserves
lifecycle fields while leaving an inactive principal's omitted display name absent.

Organization deletion request and cancellation require a current active human owner.
Cancellation additionally requires the same linked identity that scheduled deletion to
present a different browser-issued bearer with a strictly later issuer time. As with
account cancellation, the CLI obtains that proof through a fresh browser/device flow
without `offline_access`, sends it only as the cancellation bearer, and does not persist
it. Organization lifecycle commands never replace or remove the account's local human
credential. A successful request reports the 30-day `deletion_pending` schedule; a
successful cancellation reports the returned `active` or `suspended` transition.

Request uses one schema-version-1 result with `--json`; cancellation uses
newline-delimited `activation_required` and terminal `result` events. Owner denial stays
`forbidden` on request and is deliberately included in `reauthentication_required` on
cancellation so the CLI does not disclose whether a private target, actor, or proof would
otherwise qualify. A cancellation whose proof belongs to an account the deployment
refuses, such as a suspended account or one itself scheduled for deletion, reports
`forbidden` with exit 1. Missing and inaccessible organization targets remain
indistinguishable. `transition_unavailable` handles a schedule or cancellation that is
already in the requested state or otherwise unavailable.

Audit listing also returns exactly one oldest-first page and preserves `nextCursor`, but
its `--limit` accepts 1 through 100. Only an active organization owner can use it. Human
output identifies each available record's actor, action, target, occurrence time, change
count, and retention snapshot. A record whose details cannot be projected remains in
place with `details: unavailable`, its occurrence and retention metadata, and the
redacted warning reason; the CLI does not infer actor, action, target, or hidden subject
details. `--json` emits the same closed records, `detailsStatus`, retention snapshots,
optional warnings, and cursor in a schema-version-1 `listed` result.

Role update accepts only `owner` or `member`. It does not expose membership suspension
or reactivation. Member removal and self-leave are terminal and require `--yes`. The
deployment enforces self-targeting, active-owner authorization, valid transitions, and
the requirement to retain an effective human owner.

Create, organization update, role update, removal, and leave generate a fresh opaque
idempotency key per process invocation. After an ambiguous transport failure, the CLI
retries once with the same key and serialized request. If both attempts are ambiguous,
it reports `unreachable` because the mutation result cannot be confirmed. It does not
persist the key or retry a contracted HTTP response. Do not issue a new mutation merely
because an earlier result was unconfirmed.

These commands use the selected human or explicitly supplied service authority within
the API's actor restrictions. Authentication status may carry a server-advertised
`organization.create` action, but the CLI only transports that value; a trusted external
guide owns action selection, explanation, and approval.

## Organization invitations

Invitation commands use the selected human OAuth credential by default and accept
`--service-api-key-file PATH|-` when an API operation permits a service actor. The
deployment authorizes the exact acting principal. Active organization owners can issue
invitations to either an active principal ID or an email address, inspect retained
invitation history, and revoke an outstanding invitation:

```sh
# Issue to exactly one target kind.
um organization invitation issue acme-labs \
  --principal-id prn_01k0z6r1w8f4jy2m7q9v3x5abc
um organization invitation issue acme-labs \
  --email teammate@example.com

# Read one owner-only page, including terminal history.
um organization invitation list acme-labs --limit 50

# Revoke an outstanding invitation.
um organization invitation revoke acme-labs \
  inv_01k0z6r1w8f4jy2m7q9v3x5abc --yes
```

The current principal can list directly targeted outstanding invitations, preview an
invitation, and accept or decline it:

```sh
um invitation list --limit 50
um invitation preview inv_01k0z6r1w8f4jy2m7q9v3x5abc
um invitation accept inv_01k0z6r1w8f4jy2m7q9v3x5abc
um invitation decline inv_01k0z6r1w8f4jy2m7q9v3x5abc --yes
```

Email-targeted invitation links contain a bearer capability. Supply it only through a
private regular file owned by the current Unix user, or through explicit standard input:

```sh
um invitation preview inv_01k0z6r1w8f4jy2m7q9v3x5abc \
  --capability-file ~/.config/um/invitation.capability
um invitation accept inv_01k0z6r1w8f4jy2m7q9v3x5abc \
  --capability-file - < ~/.config/um/invitation.capability
```

Capability files must use mode `0600`, must not be symlinks, and must be owned by the
invoking user. The capability's embedded
invitation ID must match the command argument. The CLI sends the value only in the API
request body, zeroizes the capability input buffer after use, and never includes it in
normal human or JSON output. This does not guarantee erasure of copies made by the
HTTP or TLS stack. Avoid command-line arguments, environment variables, shell history,
logs, and issue reports for capability values.

Preview, accept, and decline intentionally collapse expired, revoked, consumed,
inaccessible, and wrong-capability cases into `invitation_unavailable`; they do not reveal
which condition applied. Owner history retains `outstanding`, `accepted`, `declined`,
`revoked`, and `expired` states. Each list reads one page, preserves `nextCursor`, and does
not follow it automatically; limits accept 1 through 200.

Issue, revoke, accept, and decline generate a fresh opaque idempotency key and retry one
ambiguous transport failure with the same serialized request and key. They never retry a
contracted HTTP response. `--json` emits schema-version-1 results. Authentication and
failures use exit 3 when sign-in is required, exit 4 when the deployment is unreachable
or rate limited, and exit 1 for other rejected or unavailable outcomes.

## GitHub connections

GitHub commands use the selected human OAuth credential by default and accept
`--service-api-key-file PATH|-` when the operation permits a service actor. The deployment
requires the exact acting principal to be a current active organization owner. The CLI
uses a service credential only when explicitly selected and never substitutes a runner
credential, GitHub personal access token, or local Git credential.

Setup is deliberately split around browser consent. The CLI neither opens a browser nor
listens for an inbound callback. Begin a ten-minute, single-use setup session:

```sh
um connection github setup begin acme-labs
```

Open the reported URL in a browser and approve the GitHub account and repositories.
GitHub then returns the browser to a Useful Machinery setup page that shows the decimal
installation ID and the completion command. Run that command with the ID to complete the
same session as the same Useful Machinery principal:

```sh
um connection github setup complete \
  acme-labs \
  ghs_01k0z6r1w8f4jy2m7q9v3x5abc \
  --provider-installation-id 12345678
```

Completion uses the explicitly selected principal credential, while the deployment
enforces the setup session's exact actor and organization binding and verifies the
installation with the platform GitHub App credential. An expired or rejected completion
does not let the CLI substitute another actor, account, repository list, or provider
credential. Begin a
new setup session when the old session has expired.

List the organization's stable installation bindings and their current `active`,
`disconnected`, or `revoked` state:

```sh
um connection github installation list acme-labs
```

Discover the repositories currently authorized through one active binding. This is a
live provider-backed read and may update existing repository availability projections;
it does not bind a project or create repository connections eagerly:

```sh
um connection github repository list \
  acme-labs \
  ghi_01k0z6r1w8f4jy2m7q9v3x5abc
```

Remove an installation binding from Useful Machinery with its stable binding ID:

```sh
um connection github installation remove \
  acme-labs \
  ghi_01k0z6r1w8f4jy2m7q9v3x5abc --yes
```

Disconnection marks the binding and its repository connections unavailable for new
source use; it does not uninstall the GitHub App or rewrite accepted run provenance. A
still-valid disconnected installation can be reactivated only through a new verified
browser setup session. A revoked provider installation is terminal.

Add `--json` to any GitHub leaf for one schema-version-1 result. Setup begin returns
`pending` and the session, setup complete returns `completed`, installation remove
returns `disconnected`, and both list commands return `listed`. Expected failures use a
closed outcome without copying provider responses or API problem prose. Setup completion
and disconnection retry one ambiguous transport failure with the same session or binding
identity because the API defines exact replay; setup initiation and reads make one
request attempt. A later completion retry must reuse the same setup session and provider
installation ID.

## Project actions

Organization owners configure Google Docs workflow actions under the provider-neutral
`project action` namespace. The configuration file is a closed JSON object: create
requires `displayName`, `workflowPath`, and explicit `enabled`. The optional
`definitionId` names the published Cloud workflow definition whose tree holds
`workflowPath`; an action without one cannot be launched. An action that names a
definition selects no publication. See
[`examples/action-config.json`](examples/action-config.json) for a complete example.
Bindings may be `user`, `document` (`field` is `id` or `url`), or `literal`
(`kind` is `text` with a string value or `json` with any JSON value, including null).
The optional `publication` is null or `{"exportName":"name"}`. Configured
bindings and publication are validated against the workflow at form load and launch,
not during CLI configuration. This command does not edit a Google document or launch
an action.

```sh
um project action create ORG PROJECT --config-file examples/action-config.json
um project action list ORG PROJECT --limit 50 --cursor CURSOR
um project action show ORG PROJECT ACTION
um project action update ORG PROJECT ACTION --config-file patch.json --expected-version 3
um project action enable ORG PROJECT ACTION
um project action disable ORG PROJECT ACTION
um project action delete ORG PROJECT ACTION --yes
```

A patch contains only changed configuration fields, never `enabled` or
`expectedVersion`; the positive version comes from the flag. A `publication: null`
patch clears the selection, while omission leaves it unchanged. A patched
`definitionId` replaces the definition; a definition cannot be removed. Enable, disable,
and delete use no version or preliminary read. `--config-file -` reads standard input;
it cannot share that input with `--service-api-key-file -`. List is paginated with
an opaque next cursor. Show returns a nonsecret tombstone for a recently deleted
action. Delete needs `--yes` and succeeds on an empty `204` response. `--json` uses
the schema-version-1 envelope with `action`, `items`, or `tombstone` data and a
stable outcome. An ambiguous transport reply is retried with the same automatic
request key and body. If the result remains uncertain, inspect the action before
issuing another mutation; the key is reported for diagnosis but cannot be supplied
as a CLI flag. A rejected replay after a lost reply does not establish that the
first mutation failed: the CLI reports `result_uncertain`, the replay outcome,
and the original key rather than advising another create. A source-built CLI
is not a published or installed release.

## Triggers and schedules

`um project trigger` and `um project schedule` are removed. Triggers that target a
`workflowPath` and their Linear lifecycle policy are retired in Cloud: declare start, control
and schedule triggers in a published workflow definition's root instead, and use the root's
`mirror` for issue state. Retained trigger and schedule records
stay readable through the public API; `um run linear show` and `um run linear history` still
read a run's retained lifecycle binding.

## GitLab.com connections and credential candidates

GitLab setup is CLI-only for now. Credential activation and GitLab run admission remain
closed pending qualification; creating a connection or submitting a candidate does
not enable execution. See the [GitLab.com setup guide](https://docs.usefulmachinery.com/agent/gitlab-setup.md)
for supported token kinds, Developer access, expiry and the retained-record limit.

```sh
um connection gitlab create ORG https://gitlab.com --json
um connection gitlab list ORG --json
um connection gitlab show ORG CONNECTION --json
um connection gitlab credential create ORG CONNECTION --expected-version VERSION \
  --kind service-account --token-file /path/to/private-token --json
um connection gitlab credential show ORG CONNECTION CANDIDATE --json
um connection gitlab credential activate ORG CONNECTION CANDIDATE \
  --expected-provider-user-id ID --dedicated-automation --json
um connection gitlab credential cancel ORG CONNECTION CANDIDATE --json
um connection gitlab remove ORG CONNECTION --expected-version VERSION --yes --json
```

Pass the credential only from an owned, regular, non-symlink mode-0600 file or
`--token-file -` (standard input); never pass it as an argument. The CLI submits
once and returns the candidate's current state. Show it separately for review.
Activation checks a verified identity and requires explicit attestation (or an
interactive identity prompt). The API currently fails real-token activation closed.
Local disconnect does not revoke the upstream token: revoke it separately at GitLab.
Every mutation accepts `--idempotency-key` to replay an uncertain response; without
one the CLI generates and prints a key, including on ambiguous transport failures.
A completed activation can be replayed while the server retains its receipt, even
if candidate metadata has expired: reuse the same key with the explicitly reviewed
provider ID and `--dedicated-automation`. A cancellation acknowledgement does not
prove a terminal candidate was cancelled or undo activation. Neither create nor
show polls. GitLab credential commands distinguish usage (exit 2), authentication
(3), unavailable transport, provider or service (4), conflict (6), candidate
unavailability (7), and identity mismatch (8). With `--json`, a failure is one JSON
document on standard output whose `outcome` names it, such as `conflict`,
`provider_unavailable` or `unreachable`, and which carries the request's
`idempotencyKey`.

Once an active connection has a usable credential, discover repositories by numeric
provider ID, then explicitly select one before binding a project. Use `--selected`
to inspect retained selections even when upstream discovery is unavailable.
`--search`, `--repository-id`, and `--selected` are exclusive list filters;
`--limit` and `--cursor` paginate the list. Each link/remove requires the current
connection version. Repeat `--repository-id REPOSITORY` in a single link/remove
command to change multiple IDs atomically; each command supports `--idempotency-key` for replay and
`--delegator-principal-id` for service-action attribution.

```sh
um connection gitlab repository list ORG CONNECTION --json
um connection gitlab repository list ORG CONNECTION --selected --json
um connection gitlab repository link ORG CONNECTION --repository-id REPOSITORY --expected-version VERSION --json
um project create ORG --name PROJECT_NAME --gitlab-connection-id CONNECTION \
  --repository-id REPOSITORY --json
um project repository set ORG PROJECT --gitlab-connection-id CONNECTION \
  --repository-id REPOSITORY --json
um connection gitlab repository remove ORG CONNECTION --repository-id REPOSITORY \
  --expected-version VERSION --yes --json
```

`--gitlab-connection-id` and `--installation-id` are mutually exclusive.
The project shows the GitLab binding and closed admission diagnostic; binding
cannot enable GitLab execution. The [setup guide](https://docs.usefulmachinery.com/agent/gitlab-setup.md)
explains selection, credential limitations and local disconnect.

## Linear connections

An active organization owner can connect a Linear workspace without supplying a Linear
credential to the CLI. The deployment manages the OAuth exchange and stores the grant;
the CLI prints only a consent URL, session identity, and nonsecret connection status.
It does not open a browser or listen for the callback. Use the selected human session
(or explicitly pass `--service-api-key-file PATH|-` for the same authorized principal):

```sh
um connection linear authorization create acme-labs
um connection linear authorization create acme-labs --connection-id lcn_01k0z6r1w8f4jy2m7q9v3x5abc
# To request write consent explicitly on a new or read-only connection:
um connection linear authorization create acme-labs --enable-state-updates
```

Open the URL in a browser as the initiating owner. By default, the command polls the
owner-bound authorization session until completion, failure, expiry, or recovery-required
status (up to 15 minutes). Use `--timeout 2m` to stop waiting earlier, or `--no-wait`
to return immediately after the consent URL. Neither stopping the CLI nor losing
access cancels callback processing. Save the session ID and inspect it later as the
same owner; do not start another session merely because polling stopped:

```sh
um connection linear authorization show acme-labs las_01k0z6r1w8f4jy2m7q9v3x5abc
um connection linear authorization wait acme-labs las_01k0z6r1w8f4jy2m7q9v3x5abc --timeout 5m
um connection linear list acme-labs
um connection linear show acme-labs lcn_01k0z6r1w8f4jy2m7q9v3x5abc
um connection linear team list acme-labs lcn_01k0z6r1w8f4jy2m7q9v3x5abc
um connection linear state list acme-labs lcn_01k0z6r1w8f4jy2m7q9v3x5abc LINEAR_TEAM_ID
um connection linear remove acme-labs lcn_01k0z6r1w8f4jy2m7q9v3x5abc --yes
um connection linear delete acme-labs lcn_01k0z6r1w8f4jy2m7q9v3x5abc --yes
```

Remove erases the credential but reserves the workspace binding; delete also
releases that binding. Reauthorization targets the exact connection ID. Once write
consent has been verified, a plain reauthorization requests it again even after
credential loss; it is not a downgrade. Session `requestedScopes` and connection
`stateUpdatesConsented` are nonsecret inspection metadata. Discovery pages require
owner access and suggest states without installing rules or writing to Linear. Use `--json`
for schema-version-1 newline-delimited events when starting or waiting and a single
JSON line for inspection/lifecycle commands. A lost start response is retried only
with the same internally generated request key during that invocation. If commitment
remains unknown, inspect existing connections as the initiating owner before
deliberately starting another request; the CLI cannot recover an unknown session
by replaying a key in a later invocation. A known session ID remains available on
timeout, interruption, or polling failure. `recovery_required` is not success; inspect the
connection and start a deliberate reauthorization if appropriate. No response includes
Linear tokens, authorization codes, or PKCE verifiers.

## Project management

Project commands use the selected human OAuth credential by default and accept
`--service-api-key-file PATH|-` when an API operation permits a service actor. Project
IDs, GitHub installation binding IDs, and provider repository IDs remain exact authority
inputs; repository full names are display metadata only. Runner pools can be selected by ID
or exact name. Start by discovering those values:

```sh
# Discover the current account's organization memberships.
um organization list

# Discover active and retained GitHub installation bindings for one organization.
um connection github installation list acme-labs

# Discover the repositories currently authorized for one installation.
um connection github repository list \
  acme-labs \
  ghi_01k0z6r1w8f4jy2m7q9v3x5abc

# Discover existing runner pools and copy a pool ID or exact name when needed.
um runner pool list acme-labs
```

Create a project from one discovered repository. Omit `--default-branch` to select the
provider-observed default branch, and omit `--pool-id` to create a valid project
that reports `runner_pool_unassigned` until configured. `--pool-id` names the project's
first pool; add more with `um project runner-pool set`:

```sh
um project create acme-labs \
  --name um \
  --installation-id ghi_01k0z6r1w8f4jy2m7q9v3x5abc \
  --repository-id 123456789 \
  --default-branch release \
  --pool-id rpl_01k0z6r1w8f4jy2m7q9v3x5abc

# Page projects and show one complete configuration and readiness projection.
um project list acme-labs --limit 50
um project show \
  acme-labs \
  prj_01k0z6r1w8f4jy2m7q9v3x5abc

# Rename without changing execution configuration.
um project rename \
  acme-labs \
  prj_01k0z6r1w8f4jy2m7q9v3x5abc \
  --name um-release
```

Repository and runner-pool settings are independent. `set` assigns or atomically
replaces the current value. Repeated removals are API-defined no-ops.

A project runs on a set of the organization's runner pools, with no order among them, and
one pool may serve several projects. `um project runner-pool set` replaces the whole set
with the one or more pools it names, up to 20, each by ID or exact name; naming the same
set again changes nothing. `um project runner-pool remove` empties the set, after which the
project reports `runner_pool_unassigned`. `um project show` lists every pool of the set. A
member's personal pool cannot serve a project, and naming one is refused:

```sh
# Read, replace, update, or remove the repository binding.
um project repository show \
  acme-labs prj_01k0z6r1w8f4jy2m7q9v3x5abc
um project repository set \
  acme-labs prj_01k0z6r1w8f4jy2m7q9v3x5abc \
  --installation-id ghi_01k0z6r1w8f4jy2m7q9v3x5abc \
  --repository-id 123456789
um project repository update \
  acme-labs prj_01k0z6r1w8f4jy2m7q9v3x5abc \
  --default-branch stable
um project repository remove \
  acme-labs prj_01k0z6r1w8f4jy2m7q9v3x5abc --yes

# Replace the project's runner pools with the pools named, or remove them all.
um project runner-pool set \
  acme-labs prj_01k0z6r1w8f4jy2m7q9v3x5abc \
  rpl_01k0z6r1w8f4jy2m7q9v3x5abc builders
um project runner-pool remove \
  acme-labs prj_01k0z6r1w8f4jy2m7q9v3x5abc --yes
```

Human output always includes the complete project's `ready` or `blocked` verdict and its
ordered blockers. JSON mode wraps the public project representation in a
schema-version-1 command result and preserves `nextCursor` on one-page list operations.
Every mutation generates one fresh opaque idempotency key. An ambiguous connection or
response-body failure is retried once with the same key and serialized request, and an
access-token refresh within the invocation also retains them. Owner authorization and
same-organization repository and runner-pool checks remain server-enforced.

### Signed project webhooks

After a qualified maintenance cutover, owners can create a subscription with
`um project webhook create ORG PROJECT --config-file config.json`; use `-` for
bounded standard input, unless the service credential also reads standard input.
For example, the file may contain
`{"url":"https://receiver.example.com/hook","eventTypes":["run.failed"],"contextKeys":[]}`.
The secret in the first successful create or rotate response must be saved
privately at once; later inspection and idempotent replay return metadata only.
JSON create/rotate results retain the operation `outcome` and report
`secretDisclosure: "shown_once"` only when a validated signing secret is included;
`"metadata_only"` explicitly means no secret was recovered. Human output gives
the same warning. Never treat a metadata-only response as a new signing key.
`um project webhook update ORG PROJECT WEBHOOK --expected-version 1 --config-file patch.json`
replaces only supplied selections and never reads the version implicitly. Empty
`contextKeys` clears that selection. URL changes require a new subscription.

Members may `list ORG PROJECT`, `show ORG PROJECT WEBHOOK`,
`delivery list ORG PROJECT WEBHOOK --limit 20 --cursor CURSOR --run-id RUN_ID --state failed`
and `delivery show ORG PROJECT WEBHOOK DELIVERY`. Owners may also `enable`,
`disable`, `delete`, `rotate-secret`, `revoke-previous-secret`, and `test` with
`ORG PROJECT WEBHOOK`, or `delivery replay ORG PROJECT WEBHOOK DELIVERY`.
Deletion requires `--yes` and stops future sends; it does not erase retained history.
One page is returned with an opaque `nextCursor`; `--json` selects schema-version-1
results. The server enforces tenant and owner authorization. An uncertain
mutation outcome is not permission to issue a new key blindly: inspect the
resource first. For uncertain mutation failures, JSON retains the generated
`idempotencyKey`, known `projectId`, optional `webhookId` and `deliveryId`, and
`nextAction: "inspect_resource"`; human output also prints these non-secret
identities. Keep the original key for diagnosis, but do not retry in a new
invocation without first inspecting retained state. A completed replay returns
`metadataOnly: true` and cannot recover a signing secret. See
[receiver and recovery guidance](../docs/operations/webhook-delivery.md) for
raw-body signature verification, deduplication, overlap and retention.

## Cloud workflow definitions

`um workflow definition publish --cloud ORGANIZATION --project PROJECT` records the
revision locally and then publishes the tree to that project's Cloud definition: it
uploads and seals the tree under its digest and publishes the revision with its bindings.
A home's first publication creates the Cloud definition, or reuses the project's definition
whose current revision has the same tree, bindings and executable bits, and records its id
locally; later ones publish to it, and `--cloud-new` creates another. A Cloud refusal leaves
the local revision recorded and exits 1.

```sh
um workflow definition publish .um/engineering --bindings bindings.json \
  --cloud acme-labs --project prj_01k0z6r1w8f4jy2m7q9v3x5abc --json
```

`--to` and `--new` choose the local definition under the home, and `--cloud-to` and
`--cloud-new` choose the Cloud one. Cloud and the home assign their ids separately, in the
same `def_` shape, and they are not interchangeable: `--to` takes an id
`um workflow definition list` prints, and `--cloud-to` one
`um workflow definition cloud list` prints. To publish the next revision of an existing
Cloud definition from a home that has never published it, find its id and name it once:

```sh
um workflow definition cloud list acme-labs prj_01k0z6r1w8f4jy2m7q9v3x5abc
um workflow definition publish .um/engineering --bindings bindings.json \
  --cloud acme-labs --project prj_01k0z6r1w8f4jy2m7q9v3x5abc --cloud-to DEFINITION
```

The home then records `DEFINITION` as the local definition's Cloud id, so later
publications from it need no target. Without `--cloud-to`, such a home reuses the definition
only when its publication is identical to the definition's current revision, and otherwise
creates a second Cloud definition.

With `--json`, `cloud.reused` is `true` when the publication reused an identical definition;
a reused definition records no revision, and `cloud.revision` is its current one.
`cloud.treeReused` is `true` when Cloud already held the tree sealed under its digest, so
nothing was uploaded; tree reuse is storage and says nothing of the definition. When more
than one of the project's definitions is identical, Cloud refuses with
`definition-reuse-ambiguous` and the report names their ids (`cloud.workflow.definitionIds`):
choose one with `--cloud-to`, or create another with `--cloud-new`.

`um workflow definition cloud` reads and changes a project's Cloud definitions, and lists
and shows a definition's start evaluations, newest first:

```sh
um workflow definition cloud list acme-labs prj_01k0z6r1w8f4jy2m7q9v3x5abc --limit 20
um workflow definition cloud show acme-labs DEFINITION \
  --project-id prj_01k0z6r1w8f4jy2m7q9v3x5abc
# enable, disable, archive and unarchive take the same arguments.
um workflow definition cloud disable acme-labs DEFINITION \
  --project-id prj_01k0z6r1w8f4jy2m7q9v3x5abc
um workflow definition cloud set-principal acme-labs DEFINITION PRINCIPAL \
  --project-id prj_01k0z6r1w8f4jy2m7q9v3x5abc
um workflow definition cloud evaluation list acme-labs DEFINITION \
  --project-id prj_01k0z6r1w8f4jy2m7q9v3x5abc --state failed
um workflow definition cloud evaluation show acme-labs DEFINITION EVALUATION \
  --project-id prj_01k0z6r1w8f4jy2m7q9v3x5abc
```

With `--cloud ORGANIZATION`, `um workflow run show`, `resume` and `release` take a Cloud
run ID as `RUN` and read or write no local run: `show` reads the run with its definition,
subject claim, workflow state, latest progress, exits and newest trigger deliveries;
`resume` asks Cloud to continue it, as `um run continue` does; and `release` asks Cloud,
as an organization owner, to release the subject claim of the exited run. The control
leaves take the same option ([Cloud runs](#cloud-runs)).

```sh
um workflow run show run_01k0z6r1w8f4jy2m7q9v3x5abc --cloud acme-labs --json
um workflow run resume run_01k0z6r1w8f4jy2m7q9v3x5abc --cloud acme-labs --from build
um workflow run release run_01k0z6r1w8f4jy2m7q9v3x5abc --cloud acme-labs
```

## Cloud runs

List materialized runs within one organization (newest first) with bounded,
opaque-cursor pages. Filters combine with AND; repeat exact context matches:

```sh
um run list acme-labs --project-id prj_01k0z6r1w8f4jy2m7q9v3x5abc \
  --state-group active --created-after 2025-01-01T00:00:00Z \
  --integration-context source=linear --integration-context linearIssue=LIV-123 \
  --limit 50 --json
um run list acme-labs --project-id prj_01k0z6r1w8f4jy2m7q9v3x5abc \
  --state-group active --created-after 2025-01-01T00:00:00Z \
  --integration-context source=linear --integration-context linearIssue=LIV-123 \
  --limit 50 --cursor 'CURSOR_FROM_NEXT_CURSOR' --json
```

Text output includes run IDs, state, timestamps, project, workflow, context,
placement when available, and the next cursor; `--json` returns `items` and
`nextCursor` as in the API. Context is private: avoid shell-history exposure when
using context filters, and do not put context in logs. For ticket dispatch, write
`{"source":"linear","linearIssue":"LIV-123"}` to a protected file and supply
`workflow run create --cloud --integration-context-file PATH`.

Run commands use the selected human OAuth credential by default and accept
`--service-api-key-file PATH|-` when an API operation permits a service actor. They use
the configured Useful Machinery deployment. `um workflow` runs and inspects local workflow
definitions and run directories; with `--cloud ORGANIZATION` it also publishes a definition
to Cloud, creates a run of a Cloud definition, and reads and controls a definition-backed
Cloud run, and `um workflow definition cloud` manages Cloud definitions (see
[Cloud workflow definitions](#cloud-workflow-definitions)).

Start a Cloud run of a workflow published as a Cloud definition with
`um workflow run create ID --cloud ORGANIZATION --project PROJECT`; `um workflow definition
publish DIR --bindings FILE --cloud ORGANIZATION --project PROJECT` prints the id. `ID` is
sent as the Cloud definition id, and nothing under the `um` home or any execution root is
read. `--workflow FILE` names a workflow file of the definition's tree, relative to its root,
and the root workflow is run when it is omitted. The command takes `--source-branch`,
`--workspace-branch` or `--workspace-commit`, `--display-name`, the named-input options or
`--input-set-id`, `--integration-context-file`, `--max-parallel` from 1 to 64, sent only when
given, `--no-owner`, and `--wait` with `--timeout`, uses one idempotency key per invocation,
and writes one `create` result. A run you create is owned by you unless `--no-owner` starts
it with no owner, in the project's runner pools. The local options (`--path`,
`--execution-root`, `--run-dir`, `--agent-profiles`, `--stop-before`, `--owner`,
`--external`) are usage errors beside `--cloud`, as the Cloud options are without it. A run
of a definition names no publication: publish an export of a succeeded run with
`um publication create`.

```sh
# Admit an inputless run of the definition's root workflow without waiting for execution.
um workflow run create def_01k0z6r1w8f4jy2m7q9v3x5abc \
  --cloud acme-labs --project prj_01k0z6r1w8f4jy2m7q9v3x5abc \
  --source-branch main \
  --display-name "Release checks"

# Or run a workflow file of its tree and bind mixed values, including ordered and
# explicitly empty collections.
um workflow run create def_01k0z6r1w8f4jy2m7q9v3x5abc \
  --cloud acme-labs --project prj_01k0z6r1w8f4jy2m7q9v3x5abc \
  --workflow flows/review.yaml \
  --input-text-file request ./request.txt \
  --input-json settings '{"mode":"review"}' \
  --integration-context-file ./integration-context.json \
  --input-attachment evidence text/plain ./notes.txt \
  --input-attachment evidence application/pdf ./report.pdf \
  --input-attachments-empty optionalEvidence

# Create an open immutable set, upload its object members, and then seal it.
um run input-set create acme-labs \
  --project-id prj_01k0z6r1w8f4jy2m7q9v3x5abc \
  --input-text-file request ./request.txt \
  --input-attachment evidence text/plain ./notes.txt \
  --input-attachment evidence application/pdf ./report.pdf \
  --json > input-set.json
input_set_id=$(jq -er '.inputSet.id' input-set.json)

um run input-set upload acme-labs "$input_set_id" \
  --member-file inputs/request ./request.txt \
  --member-file inputs/evidence/000000 ./notes.txt \
  --member-file inputs/evidence/000001 ./report.pdf
um run input-set seal acme-labs "$input_set_id"

um workflow run create def_01k0z6r1w8f4jy2m7q9v3x5abc \
  --cloud acme-labs --project prj_01k0z6r1w8f4jy2m7q9v3x5abc \
  --input-set-id "$input_set_id"

# Read the latest public projection.
um run show \
  acme-labs \
  run_01k0z6r1w8f4jy2m7q9v3x5abc

# Observe settlement for up to 30 minutes without changing the run.
um run show acme-labs run_01k0z6r1w8f4jy2m7q9v3x5abc \
  --wait --timeout 30m

# Read the retained Linear lifecycle binding a retired trigger admitted the run with;
# outcome not_found when none.
um run linear show acme-labs run_01k0z6r1w8f4jy2m7q9v3x5abc --json
# List the binding's retained desired-state updates, newest first; --cursor continues a page.
um run linear history acme-labs run_01k0z6r1w8f4jy2m7q9v3x5abc --limit 50 --json

# Continue from selected steps on the same retained runner boot. Admission is not execution.
um run continue acme-labs run_01k0z6r1w8f4jy2m7q9v3x5abc \
  --from build --from test --expected-version 7 --json
# Observe the immutable admission receipt with the current Run until it settles.
um run continue acme-labs run_01k0z6r1w8f4jy2m7q9v3x5abc \
  --from build --wait --timeout 30m

# Retry an eligible terminal run; observe the durable receipt until it resolves.
um run retry acme-labs run_01k0z6r1w8f4jy2m7q9v3x5abc \
  --expected-version 7 --timeout 30m

# Request graceful cancellation; force is a separate explicit, separately keyed choice.
um run cancel acme-labs run_01k0z6r1w8f4jy2m7q9v3x5abc \
  --wait --timeout 30m
```

A definition-backed Cloud run's wait takes the local run controls with
`--cloud ORGANIZATION`. Each leaf but `stop` reads the run's current wait, refuses
`run_not_waiting` or `wait_kind_mismatch` before sending anything, and asks Cloud under one
request key to carry the control to the runner executing the run, which applies it as the
leaf applies it to a local run. The output reports Cloud's acceptance, not the runner's
application; `wait_not_current` means the run's wait changed after the read.

```sh
um workflow run answer run_01k0z6r1w8f4jy2m7q9v3x5abc --cloud acme-labs \
  --question scope --text "Only the public API"
um workflow run approve run_01k0z6r1w8f4jy2m7q9v3x5abc --cloud acme-labs
um workflow run hand-back run_01k0z6r1w8f4jy2m7q9v3x5abc --cloud acme-labs \
  --notes "Split the migration"
um workflow run retry run_01k0z6r1w8f4jy2m7q9v3x5abc --cloud acme-labs --json
um workflow run stop run_01k0z6r1w8f4jy2m7q9v3x5abc --cloud acme-labs \
  --before review --reason "Hold for review"
um workflow run lift run_01k0z6r1w8f4jy2m7q9v3x5abc --cloud acme-labs --before review
```

Without an input flag, `workflow run create --cloud` admits an inputless run. The mixed-input flags may be
repeated in one invocation: Text uses `--input-text NAME TEXT` or
`--input-text-file NAME PATH`, JSON uses `--input-json NAME JSON` or
`--input-json-file NAME PATH`, a singular File uses
`--input-file NAME MEDIA_TYPE PATH`, and ordered attachment members use repeated
`--input-attachment NAME MEDIA_TYPE PATH`. `--input-attachments-empty NAME` preserves a
present collection with no members. A name may be bound once, except that attachment
members append to one collection in argument order. `--integration-context-file PATH`
supplies a private immutable flat JSON object whose keys and values are strings. It is
returned by run readers but is not a workflow input. Omitting the flag and supplying an
empty object have the same run semantics. The object allows at most 32 entries, 64 UTF-8
bytes per nonempty key, 1 KiB per value, and 16 KiB of compact JSON; empty values are
valid, U+0000 is not, and duplicate members reject locally. The encoded-size limit uses
compact UTF-8 JSON with the shortest JSON-required escapes, so `<`, `>`, `&`, U+2028, and U+2029
retain their literal UTF-8 size. The source document, including escapes and formatting,
is limited to 128 KiB so file and standard-input reads remain bounded.

One Text or JSON file source or the integration-context file may use `-` to claim standard
input. Exactly one such operand may claim standard input in an invocation; competing claims
reject before stdin is read or Cloud is contacted. File and attachment paths always name
regular files.

Text and JSON values are limited to 1 MiB, each File or attachment member to 64 MiB, all
values together to 256 MiB, named inputs to 256, and attachment members to 256. Text must
be UTF-8. JSON must be strict UTF-8 JSON with duplicate keys and excessive nesting
rejected before Cloud access. Bytes and explicit valid media types are preserved exactly,
including zero-length values. The CLI computes the canonical manifest digest, allocates
one input set, requests upload capabilities in batches of at most 100, uploads every
logical member, seals the set, and only then creates the run. A storage `412 Precondition
Failed` is treated only as evidence that seal verification is required; connection and
timeout failures are not treated as upload evidence. Storage uploads have a five-minute
request timeout, separate from the 20-second metadata-request timeout.

Use `run input-set create` to acquire local sources, compute the immutable manifest, and
allocate an open set. It returns the set metadata and ID without requesting upload
capabilities, writing object storage, or sealing. `run input-set show` inspects a set and
manifest. `run input-set upload` maps each selected logical member ID to a caller-owned
regular file with repeated `--member-file MEMBER PATH` flags. It rereads the authoritative
manifest, requires each file's exact size and digest, and reports accepted members
separately from `412` members that still require authoritative sealing. This permits a
later invocation to upload all object members or resume only missing members, including
one member such as `inputs/evidence/000001` from a larger attachment collection. Empty
attachment collections require no upload. `run input-set seal` completes authoritative
verification.
`run input-set delete ... --yes` makes a set logically unavailable and schedules exact-key
content cleanup. Once a set is sealed, pass
its exact ID to `workflow run create --cloud --input-set-id`; this conflicts with every acquisition flag
and never restages content.

Create output reports the accepted Run ID and whether the deployment replayed the
request. One invocation keeps each mutation's idempotency key through an ambiguous API
transport retry and an access-token refresh; keys are not persisted for a later invocation.
If preparation stops after input-set allocation, the CLI reports its ID and recovery
steps on stderr even with `--json`; inspect or resume that set explicitly before
allocating another. An interrupt after run dispatch reports unknown acceptance rather
than claiming no run was created.

Retained content is explicit run-scoped administration:

```sh
um run input show acme-labs "$run_id" --json
# Omit --member to download every member, or repeat it for an exact subset.
um run input download acme-labs "$run_id" \
  --member inputs/request \
  --output ./retained-inputs --json
um run input delete acme-labs "$run_id" --yes
```

A download first validates the retained inventory, requests capabilities in batches of at
most 100, and verifies every selected size and SHA-256 digest. Omit `--member` to select
all members, or repeat `--member MEMBER` to select exact logical IDs; selected members
retain canonical manifest order. Members are written beneath the new destination using
their deterministic logical IDs, such as `inputs/request` and
`inputs/evidence/000000`. Private staging is atomically renamed only after all selected
members verify, and an existing destination is never replaced. Interruption before the
commit phase or any integrity failure leaves no committed destination; the CLI attempts
to unlink its private staging, which is filesystem cleanup rather than physical media
erasure. An interruption racing the atomic commit reports unknown commitment and
directs the caller to inspect the destination. Capability URLs, input bytes, and bearer
credentials are never included in command output. Retention expiry is reported as
`gone`. Successful deletion makes content logically unavailable and schedules exact-key
cleanup; it does not attest physical erasure and does not remove the run's integration
context. Both deletion commands require literal
`--yes`, use fresh idempotency keys, and report an unknown commitment if interrupted
after dispatch.

`workflow run create --cloud`, `run show`, and `run cancel` accept `--wait` and optional
`--timeout DURATION` (positive integral `ms`, `s`, `m`, `h`, or bare seconds).
`--timeout` requires `--wait`. Create and cancel budgets begin after acceptance; show
starts immediately. The remaining wait budget caps credential refresh, read retries,
and each observation request; it does not bound the accepted mutation. Reads continue
through admitted creation. Without `--wait`, create returns on durable run acceptance;
with `--wait`, it observes execution and artifact attachment. Show-wait also observes the
automatic publication handoff and its linked Publication, which only a run created by
workflow path before its retirement can have, under the same timeout budget. A successful
Run with pending handoff is not settled; waiting never creates an attempt or switches
to a later manual attempt. Cancel observes only its receipt and terminal Run. A cancellation receipt being accepted or resolved is not by itself
proof of a stopped Run. Force requires explicit `--force`; to escalate use a new
idempotency key. Reuse `--idempotency-key` with the same mode and Run to reconcile an
uncertain response. Signals and timeouts stop only local observation.

`run continue ORGANIZATION RUN --from STEP [--from STEP ...]` selects distinct steps
in the supplied order. The run always continues with the definition it was created
from; a receipt admitted earlier under a replaced definition still shows that
definition's commit and workflow path. `--expected-version N` guards admission. A generated key is reused for at most one
ambiguous transport replay of the identical request and principal. If both attempts
are ambiguous, acceptance is unknown: inspect the run before another request; a 404
is not proof of non-admission. A 201 contains an immutable accepted attempt and
partition, **not** proof of execution. Without `--wait` it returns on admission;
with `--wait` it reads the immutable receipt alongside the latest authorized Run
until terminal state. `--timeout` requires `--wait` and begins after acceptance.
JSON emits `operation: continue`, `outcome`, `request` (the immutable receipt),
`run` (the current projection), `idempotencyKey` when acceptance is unknown, and
`code` on failure. Admission rejection includes a `diagnostic` array of safe
violation codes and optional node/producer/reference/prior-state details in
JSON, also shown in human output. A `410` `path-run-retired` answer, for a run Cloud no
longer continues because it was created by workflow path, is outcome `error` with code
`path_run_retired` and exit 1 for `run continue` and `workflow run resume --cloud` alike.
The Run shows selected/inherited/reexecuted steps, effective source digest, both workspace paths, pending/ready/unavailable preparation,
modified true/false/unknown, interruption cause, and portable-result absence.
The Run partition is not an output-producer inventory; direct producer identities
for inherited output values are preserved in retained and portable results, not
inferred from a preceding attempt in the Run summary. Missing portable results
cannot supply that evidence. Signals and timeouts stop observation only, never submit a second continuation.

`run retry ORGANIZATION RUN` always observes its retry receipt; `--expected-version`
requires a positive integer and `--timeout` limits observation after acceptance.
A single generated key is reused for ambiguous transport replay. POST acceptance alone
never reports application: only an applied GET receipt succeeds. JSON retry results
contain `schemaVersion`, `operation: retry`, `deployment`, `organizationRef`, `runId`,
`outcome`, nullable `requestId`, `idempotencyKey`, `receipt`, and `code`.
Outcomes are `applied`, exact-code `rejected`, `subject_busy` and `subject_settled`
(another run holds the run's subject; `holderRunId` in JSON names it), `retry_pending`,
`timed_out`, `observation_stopped`, `acceptance_unknown`, and `error`. A `409`
`path-run-retired` answer, for a run Cloud no longer retries because it was created by
workflow path, is outcome `error` with code `path_run_retired`; publish the workflow as a
definition and start a run of it instead.
Rejection, conflict, and timeout exit 1; usage exits 2; authentication
exits 3; unresolved transport exits 4; SIGINT/SIGTERM exit 130/143. A known
interruption retains the request ID; unknown submission retains the key. Neither
cancels or creates another retry. Inspect the run and request before trying again.

For `workflow run create --cloud`, `run show`, and `run cancel`, `--json` emits one
schema-version-1 object with `operation`, `deployment`, `organizationRef`,
nullable `runId`, `outcome`, full nullable `run`,
`publication`, `cancellationRequest`, nullable create `replayed`, and nullable `error`.
The error includes `code`, `idempotencyKey`, `requestedMode`, and `requestId` (nullable).
A create that Cloud definitely refuses at source admission (a `409` or `422`
`workspace-selection-unsupported`, `workflow-source-unavailable`, `workflow-source-invalid`,
`workspace-source-unavailable`, or `workspace-membership-unproven` problem) exits 1 with
code `admission_rejected`; its error adds `httpStatus`, the full `problemType` URI, and
`correlationId`, the response's `X-Request-ID` (omitted when absent or not a `req_` id), and
human mode prints the same facts with a remedy. Such a refusal created no run and is never
`acceptance_unknown`, which a create reports with its key and exit 4 only when the outcome
is unconfirmed: a transport failure, a `5xx`, or an unreadable response after dispatch.
Progress goes to stderr. Human show results show the Run's automatic publication
handoff and any observed Publication state, outcome, receipt disposition, failure, and
redacted confirmed pull-request URL separately from execution. On failure, inspect the existing publication or handoff
and provider effects before creating another publication attempt. Human cancellation
errors retain the requested mode, known receipt, Run state, interruption, and artifact
delivery evidence for same-key reconciliation.
Create-wait exits nonzero unless the run succeeded; show-wait and cancel-wait
exit zero on observed settlement, regardless of the Run's execution result. Timeout
exits 1; SIGINT/SIGTERM exit 130/143 without sending a new cancellation request.

### Agent named-input-to-artifact loop

An authenticated agent can bind a prompt file to a workflow's named Text input and
complete the run using only stable JSON command surfaces and the downloaded local result:

```sh
set -euo pipefail
organization=acme-labs
project=prj_01k0z6r1w8f4jy2m7q9v3x5abc
definition=def_01k0z6r1w8f4jy2m7q9v3x5abc
request_path=./request.txt
artifact_path=./downloaded-run

um workflow run create "$definition" \
  --cloud "$organization" --project "$project" \
  --input-text-file request "$request_path" \
  --json > create.json
run_id=$(jq -er 'select(.outcome == "accepted") | .runId' create.json)

um run show "$organization" "$run_id" \
  --wait --timeout 30m --json > observed.json
jq -e '.outcome == "settled" and .run.state == "succeeded"' observed.json >/dev/null

um artifact download "$organization" "$run_id" \
  --output "$artifact_path" --json > download.json
jq -e '.outcome == "downloaded"' download.json >/dev/null
jq . "$artifact_path/result.json"
```

The input bytes remain at the caller-selected source path; they do not enter command
arguments or JSON receipts. `result.json` is inspected only after the complete Artifact
Set has been downloaded, verified, and committed at the requested destination.

## GitHub artifact publications

Publication commands use the selected human OAuth credential by default and accept
`--service-api-key-file PATH|-` when the operation permits a service actor. They turn one
available `git_branch` export from a succeeded Cloud run into a ready-for-review pull
request. Run success and export declaration alone do not publish anything automatically;
Publication commands create explicit post-run attempts. A run of a Cloud definition delivers
no export to publish yet, so they apply to runs that already have one: runs created by
workflow path before that form was retired, some of which named an export at creation and
were handed off to an automatic attempt.

The complete Publication command surface is:

```sh
# Create one attempt for an exact export and retain its identity.
um publication create acme-labs "$run_id" \
  --export changes --json > publication-create.json
publication_id=$(jq -er '.publication.id' publication-create.json)

# Create another attempt and wait locally for its terminal outcome.
um publication create acme-labs "$run_id" \
  --export changes --wait --timeout 30m

# Read the current attempt, optionally waiting for a terminal snapshot.
um publication show \
  acme-labs "$run_id" "$publication_id" --wait --timeout 30m

# List one oldest-first page of attempts beneath the run.
um publication list \
  acme-labs "$run_id" --limit 50
```

To update an existing open pull request from the run's connected GitHub repository,
select its repository-local number. Cloud captures its head when the request is accepted;
use the optional hash when the caller has already inspected that exact revision:

```sh
um publication create acme-labs "$run_id" --export changes \
  --change-request 42 --expected-head "$inspected_head" --wait --json
```

An accepted update advances only the request's own source ref by a guarded fast-forward;
it does not create another pull request or change its title, body, review state, labels,
reviewers, draft status or base. Existing approvals **may still count** after the update,
and preconfigured GitHub auto-merge or workflows **may proceed** under repository rules.
Reviewers and repository owners must enforce their own approval policy; a successful
Publication proves the guarded ref effect, not fresh human approval. A stale hash or
ineligible request is refused at acceptance; a later race is reported on the separate
Publication without changing the run's outcome.

An explicit publication workflow first waits for a run that has an export, then creates and
waits for the selected Publication:

```sh
set -euo pipefail
organization=acme-labs

um run show "$organization" "$run_id" \
  --wait --timeout 30m --json > run-observed.json
jq -e '.outcome == "settled" and .run.state == "succeeded"' run-observed.json >/dev/null

um publication create "$organization" "$run_id" \
  --export changes --wait --timeout 30m --json > publication.json
jq -e '.outcome == "succeeded"' publication.json >/dev/null
publication_id=$(jq -er '.publication.id' publication.json)
```

For a run handed off to an automatic attempt, `run show --wait --json` follows the handoff
and that attempt and reports `.run.state`, `.run.publication`, and `.publication` in its one
final JSON object; it exits zero on a terminal failure but retains the failure fields.
`no_changes` and `pull_request_already_merged` are successful Publication outcomes,
not newly created pull requests. Neither timeout nor a signal cancels remote work.

Without `--wait`, `publication create` returns after Cloud accepts the attempt. With
`--wait`, it polls the accepted Publication and exits zero only for a succeeded outcome.
The caller may supply an opaque `--idempotency-key`; otherwise the CLI generates one and
preserves it across the invocation's bounded acceptance retries and terminal output. The
command never accepts a repository, base, destination branch, pull-request metadata, or
branch-only mode. Cloud derives those values from the run and either reports
`no_changes`, creates or exactly reuses an open pull request, reports an exact
already-merged pull request, or stores a structured failure.

`publication show` is a read: without `--wait`, it emits the current snapshot; with
`--wait`, it polls until any terminal snapshot. Both forms exit zero when they retrieve a
stored failed Publication, so automation using `show --wait --json` must inspect
`outcome`. `publication list` returns only the requested page and preserves `nextCursor`
in JSON instead of following it automatically. `--timeout` requires `--wait`; omitting it
waits until terminal state or a process signal. Timeout, SIGINT, and SIGTERM stop only
local observation: they do not cancel, retry, delete, merge, or otherwise mutate the
Publication or its provider effects. Waiting JSON output is one complete document for
terminal, timeout, or fatal read outcomes and is empty when a process signal wins.

Publication consumes the run's existing private Artifact Set and grants no public
artifact access or download URL. Plain output contains only the public Publication
projection and credential-free, query-free provider links; it does not stream provider
logs or expose credentials.

## Runner pools, registrations, and join keys

A runner pool is a named group of runners that projects run on. Every human member of an
organization also has a personal pool, named by the service, that shows its member in
`um runner pool show`; it serves that member's own runners and cannot serve a project.
Owners administer every pool. A member administers the runners in their own personal pool:
registering, renaming, changing the mode of and deleting them, and issuing their
activations and join keys. Creating, renaming and deleting pools and moving runners between
pools stay with owners. Commands taking a pool or runner accept its ID or exact name.

```sh
# Show a pool, its member when it is personal, its waiting runs, and how many of its
# runners are online, busy, idle and offline.
um runner pool show acme-labs builders

# Register a runner in a named pool, in a member's personal pool by principal ID, or,
# naming neither, in your own personal pool. The activation file must not exist.
um runner create acme-labs --pool-id builders --activation-file ./runner.activation.json
um runner create acme-labs --member prn_01k0z6r1w8f4jy2m7q9v3x5abc \
  --activation-file ./runner.activation.json
um runner create acme-labs --activation-file ./runner.activation.json
```

`um runner pool show` counts a runner busy while it holds unfinished work and idle while it
is enabled, connected and free, from one snapshot; `--json` carries the counts and waiting
runs as `status`. Against a Cloud release that does not report pool status, it shows the
pool alone. `--pool-id` and `--member` cannot be combined, and a service actor must name one
of them, since it has no personal pool.

A join key lets hosts become runners in a pool without an activation each. Any number of
hosts may join with one key until it expires or is revoked; each becomes a new runner with
a name the service chooses and its own credential, generated on the host.

```sh
# Issue a key for a pool (or, naming none, your personal pool) that admits hosts for
# seven days. A runner that joined with it is removed after one hour offline. The key's
# one line of JSON goes to standard output, ready to paste at `um runner serve --join-key`.
um runner join-key issue acme-labs \
  --pool-id builders \
  --expires-in 7d \
  --remove-after 1h

# Or write the key to a new protected file, for hosts that start unattended.
um runner join-key issue acme-labs \
  --pool-id builders \
  --expires-in 7d \
  --remove-after 1h \
  --join-key-file ./builders.join-key.json

# List keys with their pool, issuer, state (issued, revoked or expired), expiry and
# removal time. Continue with --cursor when a next cursor is shown.
um runner join-key list acme-labs --pool-id builders

# Stop new joins. The runners that joined with the key stay in the pool.
um runner join-key revoke acme-labs rjk_01k0z6r1w8f4jy2m7q9v3x5abc --yes
```

`--expires-in` runs from `1m` to `365d` and `--remove-after` from `5m` to `30d`, each a
whole number of minutes, hours or days; both are fixed at issue, and a value outside its
bounds is refused before anything is sent. The key is written once, as one line of JSON:
with no `--join-key-file`, or with `--join-key-file -`, to standard output, with the
command's status on standard error; with `--join-key-file PATH`, like an activation, to a
new file only you can read, created before the key is issued. Cloud cannot show the key
again. `--json` needs `--join-key-file PATH` and is refused before anything is sent
otherwise; it reports the key's metadata and the file's path, never the key itself. The
key stays a secret on the screen and in the clipboard: keep it in a secret store and out of
version control, logs and transcripts.

A runner that joined with a key is removed automatically once it has been offline for the
key's removal time, counted from when Cloud last saw it, or from its join if it never
connected, and holds no reserved or unfinished work; its runs and records are kept. A
disabled runner is not removed, and re-enabling, renaming or moving a runner starts its
offline time again. A runner registered with an activation is never removed this way, and
revoking or expiring a key removes no runner. `um runner show` names the key a runner
joined with. A workspace a runner kept for a failed run does not keep the runner: once a
runner is removed or deleted, the runs whose workspaces it kept can no longer be continued
with `um run continue`. Choose `--remove-after` to cover the longest outage after which
failed runs should still be continuable on the key's runners.

## Runner doctor

Use `um runner doctor` to inspect the local prerequisites currently known to
the runner. The default set contains only `environment.command.git`. It executes the
`git` resolved from the runner process's `PATH`, requires a parseable version at least
`2.29.0`, and reports a pass or failure for that check. Select
`execution.harness.pi-json-v1` explicitly to check the `pi` inherited through the same
operator-controlled `PATH`, select `execution.harness.claude-code-stream-json-v1` to 
check `claude`, or `execution.harness.codex-app-server-v1` to check `codex` 
independently. A successful result does not mean the runner is ready to serve
assignments: runner configuration, machine identity, connectivity, and other 
execution requirements are not all checked yet.

```sh
# Run the default checks.
um runner doctor

# Run a named check. Repeat --check to select more than one registered check.
um runner doctor --check environment.command.git

# Validate the Pi installation selected from inherited PATH only.
um runner doctor \
  --check execution.harness.pi-json-v1

# Validate Claude Code independently, or repeat --check to inspect multiple harnesses.
um runner doctor \
  --check execution.harness.claude-code-stream-json-v1

# Validate Codex independently.
um runner doctor \
  --check execution.harness.codex-app-server-v1

# Check the agent-login readiness the runner serving from the runner home recorded.
um runner doctor --check credentials.agent-logins

# The same check for a service installation that names its configuration.
um runner doctor \
  --check credentials.agent-logins --config /etc/um/runner.json

# List IDs without running any checks.
um runner doctor --list-checks

# Emit the schema-version-1 JSON report.
um runner doctor --json
```

With `--workflow FILE` the doctor runs no check and instead lists the credentials a workflow
tree needs: it resolves each named workflow and every child workflow
its `workflow` steps name, as `um workflow validate` does, and reports one requirement per
kind (`github`, `gitlab`, `linear`, `claude_code`, `codex`) with every declaration that calls
for it, followed by the nodes and forwarded variables whose credentials it cannot see. Agent
profiles on Claude Code or Codex and credential-like names in the root
`environmentPassthrough` count; nothing runs, no harness sign-in or variable value is read,
and Cloud is not contacted, so every requirement reads `unverified` with a next action.
Repeat `--workflow` to cover several files; `--json` prints the same as one document. Each
file is bounded by the root of the Git repository that holds it, files of several repositories
are listed together, and a file in no repository is refused; `--source-root ROOT` names the
boundary instead.

With `--readiness --organization-id ORGANIZATION` the doctor also asks Useful Machinery, as
the signed-in member or, with `--service-api-key-file`, a service principal, about the
organization's connections for the providers the workflows need: its Linear and GitLab
connections and its GitHub App installations. A provider reads `missing`,
`reconnect_required` (an inactive connection, or a GitLab credential that expired) or, for
Linear, `insufficient_permission` with the `um connection` command that fixes it, and the
command exits 1; a connection Useful Machinery holds ready stays `unverified` with
`cloud: ready`, because the credential reaches a runner only for a run. A read the caller may
not make leaves its requirement `unverified` with code `not_permitted`; only an organization
owner may list GitHub App installations. Add `--project-id PROJECT` to take the repository's
provider from the project's repository instead of the `origin` remote: an unavailable
repository reads `reconnect_required` with code `repository_unavailable`, or `unverified`
with code `provider_unavailable` while the deployment's GitLab admission is closed, and names
`um project repository show`.

With `--grants` the doctor also reads your own grants and shows, beneath each requirement, what
you hold of its provider: each grant with, for every runner it is authorized for, its
availability and next action as `um grant list` reports them, or `not_connected` with the
`um grant create` command that makes one. Add `--runner-id RUNNER`, the runner's ID as
`um runner list` prints it, to see each grant's status on that runner alone, or
`not_connected` with the `um grant authorize` command when it is not authorized there;
`--organization-id` keeps only that organization's coding agent grants. Your grants never
change a requirement's state or the exit status, and `--grants` uses your own sign-in, never a
service API key.

With `--connect` the doctor signs you in to what the workflows need and then checks again. It
runs the flow `um grant create --workflow` runs over the same files: it prints the plan of the
providers to sign in to, to reconnect and already held, runs the same sign-ins in the same
order, and prints the same summary. `--organization-id ORGANIZATION` names the organization of
a coding agent's grant, and `--replace` replaces each grant that needs reconnecting without
asking. Once the sign-ins end it reads your grants again and prints the `--grants` report,
with the organization's connections when `--readiness` is given; add `--runner-id RUNNER` to
see each grant's status on that runner. With `--json` every line is one JSON document: the
events `um grant create --workflow --json` prints, then the report as a last event `report`.
A failure before the summary, or a signal that stops a sign-in, ends the command with that
status and no report. Otherwise it exits 1 when a sign-in failed, when every provider selected
was passed over, or when `--readiness` finds a requirement failing, and 0 otherwise.
`--connect` uses your own sign-in, never a service API key. The doctor runs no step of the
workflow in any mode, and it does not authorize a runner: the report names the
`um grant authorize` command that does. Without `--readiness`, `--grants` or `--connect`
nothing is contacted.

```sh
# List the credentials a workflow tree needs without running it.
um runner doctor --workflow workflow.yaml

# Bound the files by a directory other than their Git repository.
um runner doctor --workflow workflow.yaml --source-root .

# Also check the organization's connections for the providers it needs.
um runner doctor --workflow workflow.yaml \
  --readiness --organization-id acme

# Take the repository's provider from a project instead of the origin remote.
um runner doctor --workflow workflow.yaml \
  --readiness --organization-id acme --project-id PROJECT

# Show your grants for the providers it needs, and their status on one runner.
um runner doctor --workflow workflow.yaml --grants --runner-id RUNNER

# Sign in to what it lacks, replace what needs reconnecting, and report again.
um runner doctor --workflow workflow.yaml --connect --replace --runner-id RUNNER
```

A selected harness check identifies its execution profile and compatibility policy in
both report formats, including when the harness is missing or incompatible. Human
reports label an available observed version separately from a supported range or exact
required version. When a range has a repository qualification release, the report shows
that qualification version separately rather than presenting it as the only admitted
release. A passing harness check also reports the detail `qualification`:
`supported_range` for a release in the qualified range, or `untested` for a stable
release newer than the qualification release. An untested installation still passes
with code `ok`, and the command exits 0 when every selected check passes.

The opt-in `credentials.agent-logins` check reads the readiness document
`um runner serve` records beside `runnerStatePath`, found through `--config PATH` or,
without it, the runner home's configuration (see [Runner serve](#runner-serve)); a runner
home that holds no configuration is read as its default layout, with the state at
`runner/state.json`. No other check reads the configuration or anything under the `um`
home, so the default checks run with neither `UM_HOME` nor `HOME` set. It passes when every agent login Cloud authorized
for the runner is `ready` and was not refused at its last use, or Cloud authorized none
(`revoked`, or `not_authorized` with no authorization in force), and otherwise fails with that
provider's state as its code, such as `foreign_login`, `not_authorized` for a login whose
authorization stands while Cloud refuses its owner, or `reconnect_required` for a `ready`
login the provider refused at its last use. Its details name each provider's availability and next action as
`<provider>_availability` and `<provider>_next_action`. It fails
`readiness_unknown` when the runner has recorded no readiness yet, a runner home that holds
no configuration included, `config_invalid` for a configuration it cannot load, and
`config_required` when no `--config` is given and the `um` home cannot be located because
neither `UM_HOME` nor `HOME` is set. It takes no lock, creates nothing, never reads the
runner credential, contacts nothing, and reports no login material.

Checks are registered statically by components compiled into this executable. The
command does not load plugins or change runner configuration, and it reads human
credentials and contacts Useful Machinery only for `--readiness`, `--grants` or
`--connect`. It executes `git --version` with a five-second deadline, bounds
captured standard output, drains standard error without reporting it, and exposes only
a normalized numeric version in its report.

The Pi check searches inherited `PATH` in order, canonicalizes the first candidate named
`pi` that the current process can execute, and invokes that absolute path with one version
probe and one capability-help probe. The probes clear the child environment except for
the captured inherited `PATH`, fresh temporary Pi state and working-directory paths, and
the required isolation controls. Retaining `PATH` lets an environment-based launcher
resolve its interpreter without allowing another `pi` selection. It admits
canonical stable versions in `>=0.84.2 <0.88.0` or exactly `0.99.1`, `1.2.0`, or `1.3.0`;
the repository qualifies exact Pi 1.3.0, and any stable release newer than 1.3.0 is
admitted as untested when its capabilities pass. The capability probe accepts both the 0.84.2
`pi [options] [@files...] [messages...]` usage line and the 0.84.3+
`pi [options] [--] [@files...] [messages...]` line, while still requiring the JSON
event, custom-session-directory, extension, system-prompt append, and
invocation-scoped `--approve` capabilities required by `PiJsonV1`. It retains the exact
observed release, never falls through to another candidate after selection, and does not
inspect model metadata or
credentials, execute the caller's project, or read or write saved Pi project-trust
decisions. Missing, unexecutable, malformed, unsupported-version, and missing-capability
outcomes have distinct report codes.

The Claude Code check follows the same first-candidate and immutable-path rules for
`claude`, admitting canonical stable releases in its qualified range `>=2.1.234 <2.2.0`
and, as untested, any stable release newer than `2.1.321`. Useful Machinery does not
install or upgrade that executable. Its isolated `--version` and `--help` probes require the closed stream
input/output, partial-message, subagent-forwarding, explicit-session-identity,
permission-mode, setting-source, model, effort, append-system-prompt-file, and JSON-schema
capabilities used by `ClaudeCodeStreamJsonV1`. The report contains only status, profile,
observed version, supported range, exact repository qualification version,
qualification (`supported_range` or `untested`), closed capabilities, and the selected
absolute path. Qualification remains pinned to `2.1.321`
and does not claim that every admitted release or unexecuted host received exact-binary
conformance. The report never exposes environment values, credentials, or loaded Claude
settings. The JSON report has no `ready` field.

The Codex check selects `codex` independently, accepts stable `>=0.147.0 <0.164.0` and,
as untested, any stable release newer than `0.163.0`, and requires the generated App Server schema capability used by CodexAppServerV1. Its
isolated version and schema probes do not read ambient `CODEX_HOME`, provider
credentials, or native configuration. The report contains the exact observed version,
supported range, exact repository qualification version `0.163.0`, qualification
(`supported_range` or `untested`), closed capabilities, and canonical executable without starting a thread. At execution, async-delivery
messages, structured asynchronous questions, form elicitation, and bounded additive
events remain unattended and cannot settle, approve, assign a project, persist, or
commit a workflow value.

## Runner serve

`runner serve` uses the protected `rrc_` credential created by `runner enroll`, by a
signed-in registration, or by joining a pool with a join key when the service first
starts. The service never reads human OAuth credentials: `runner serve` uses the human
sign-in only to register a host that holds no enrolled state when `--personal` or
`--pool-id` is given, and the runner it then serves uses its own credential. Enrollment,
service startup, `runner status` and `runner grant sync` consume the same closed operator
configuration. Without `--config` it is the runner home's: the
directory `runner` under the `um` home (`$UM_HOME` when it is set and not empty, otherwise
`$HOME/.um`, a relative home taken from the working directory) holds the configuration and
the files it names:

```text
<um home>/runner/
├── config.json     operator configuration
├── state.json      enrolled state, written by runner enroll, a registration or a join
├── control.sock    control socket, created by a serving runner
└── work/           work root
```

`runner serve` and `runner enroll` given no `--config` first create what the home lacks, on
the host alone: the `um` home, `runner/` and `runner/work/`, each with mode `0700`, and
`runner/config.json` with mode `0600`, holding `schemaVersion` 1, `deploymentMode`
`production` and the absolute paths of `runner/state.json`, `runner/control.sock` and
`runner/work`. Initialization writes no state, journal, lock or socket, contacts no Cloud
deployment and registers or enrolls nothing; the command then proceeds as it does with
`--config` naming that file. What exists is kept: a configuration already there is used as
written and never rewritten, a directory already there keeps its mode and contents, and
when several starts initialize one home together exactly one configuration is published
and each of them reads it. They exit 1, having enrolled, joined and overwritten nothing,
when `runner` is not a directory owned by the current user with mode `0700` (a link, a
file, another owner or another mode), naming the owner and mode required; when
`runner/config.json` is a link, a directory or a document that is not an operator
configuration, reported as an invalid configuration; or when the configuration names a
`runnerStatePath` other than the home's `runner/state.json` while a file exists there, as
in a home that was moved or copied, reported as a conflict naming both paths.

`runner status` and `runner grant sync` given no `--config` create and change nothing under
the `um` home. When the runner home holds a configuration they ask the control socket it
names, and one that does not load is their failure; when it holds none they ask
`runner/control.sock`, so a home no runner serves from answers as an absent socket does,
with exit 4. A command that reads the configuration, given no `--config` while neither
`UM_HOME` nor `HOME` is set, exits 1 naming both variables and `--config`, having created
nothing.

```sh
um runner enroll --activation-file /protected/path/activation.json
um runner serve
um runner status
```

A start that finds no enrolled state and is given neither a join key nor `--personal` or
`--pool-id` exits 1 saying the host is not enrolled and naming `um runner enroll --activation-file <PATH>` and
`um runner serve --join-key-file <PATH>` as the ways to enroll it; it contacts no Cloud
deployment.

Another `UM_HOME` selects another runner home, and with it that home's workflow storage, so
a second runner on one host is the same commands with, for example,
`UM_HOME=/srv/um-two um runner serve`. The two homes serve at once, each with its own
identity, while a second `runner serve` on one home is refused as it is for one
`--config`. Where a person's sign-in is stored, a coding agent's own configuration
directory such as `CLAUDE_CONFIG_DIR`, and the Cloud organization a command addresses do
not follow `UM_HOME`.

A service installation names a configuration with absolute paths owned by its service
account with `--config PATH`, which is authoritative: the command uses that file as before
and neither reads nor creates anything in the runner home. The file must exist, and the
work root it names must exist before startup:

```json
{
  "schemaVersion": 1,
  "deploymentMode": "production",
  "runnerStatePath": "/var/lib/um/runner-state.json",
  "controlSocketPath": "/run/um/runner.sock",
  "workRoot": "/var/lib/um/work"
}
```

```sh
um runner serve --config /etc/um/runner.json
```

Production operators should follow the private monorepo's
`docs/operations/runner-v1-deployment.md` runbook for confidential activation transfer,
service-manager shutdown allowance, control-plane cutover, drain, and recovery. That
operator runbook is intentionally outside this public CLI source mirror.

A person signed in on the host with `um auth login` can instead register it when the
service first starts, with no activation file, in their own personal pool or, given the
pool's ID or exact name, in another pool:

```sh
um runner serve --config /etc/um/runner.json --personal --organization my-org
um runner serve --config /etc/um/runner.json --pool-id builders --organization my-org
```

`--organization` is required with `--personal` or `--pool-id` and refused without one of
them; `--personal`, `--pool-id` and a join key, `UM_RUNNER_JOIN_KEY` included, exclude one
another. Each violation is a usage error, exit 2, before the configuration is read. The
command takes no organization from the environment or the sign-in, never tries another
mode when the one given fails, never looks for an existing runner by its name and takes no
service API key. A member may register in their own personal pool and an owner in any pool,
as with `runner create`. `--allow-insecure-http`, accepted only with `--personal` or
`--pool-id`, permits a plain-HTTP API address for the registration requests; the
activation and connection addresses still follow `deploymentMode`.

On a host that holds no enrolled state, the start checks the configuration, its work root
included, before it reads the sign-in or contacts Cloud. It then registers a runner with a
name Cloud chooses, issues its activation, enrolls with it, writes enrolled state and
serves; the activation exists only in the host's enrollment journal until it is consumed,
and no activation file is written. It reports the runner's ID, name, pool and credential ID
on standard error. Without a sign-in it exits 3 having registered and written nothing. The
runner is a registration made by hand and is never removed automatically.

The registration's progress is kept in `.runner-registration.json`, in the directory of
the state file, a closed, versioned record private to the host's user that holds no
secret and is read and written only under the state lock. It names the deployment, the
organization and target as given, the pool's ID, the signed-in principal and the
registration's `Idempotency-Key`, then the registered runner, then that enrollment
completed. A start that finds the record and no enrolled state resumes that registration
with the same request and key, or issues an activation for the recorded runner, so one
host registers at most one runner however many starts are interrupted. An ambiguous
registration or issuance and a missing or rejected sign-in stop the start with the record
kept; a definite refusal of the registration removes it.

Enrolled state takes precedence: on a host whose state file is present, `runner serve`
serves with that state, registers and issues nothing, and reads no human deployment or
credential, also when `--personal` or `--pool-id` is still given. A start whose options
disagree with the host fails with exit 1 and a closed `code:` before anything is sent:
`registration_target_conflict` when the options name another organization, mode or pool
than the record, or the host was enrolled without a registration record;
`registration_unresolved` when the record cannot be resumed (another principal or
deployment, or staged more than six days ago), another command's enrollment or join is
unresolved, or a join key is given beside an unfinished registration; and
`registration_state_missing` when the record says enrollment completed and the state file
is absent. Each failure names the recorded runner's ID, the file to inspect and the way
forward. A host whose runner was deleted reports `authentication_failed` until its state
file and `.runner-registration.json` are removed.

A host can instead join a pool when the service first starts, with no activation, by
giving `runner serve` a join key (see
[Runner pools, registrations, and join keys](#runner-pools-registrations-and-join-keys)).
From a terminal, `--join-key` asks for the key's one line of JSON, as `runner join-key
issue` printed it, at a prompt that does not echo it:

```sh
um runner serve --config /etc/um/runner.json --join-key
```

The flag takes no value, so the key is never a command-line argument. The prompt reads one
line of at most 4096 bytes with echo turned off and restores the terminal's settings when
the read ends, an interrupt included. When standard input is not a terminal, the start
fails as a usage error, exit 2, before anything is staged or sent, and names the inputs of
an unattended start; it never waits for input. A line that is not a join key fails as an
unreadable key does and sends nothing.

An unattended host takes the key as a file or in the `UM_RUNNER_JOIN_KEY` environment
variable, which holds the key's one line of JSON, for example from a platform's secret
store:

```sh
um runner serve --join-key-file /protected/path/builders.join-key.json
UM_RUNNER_JOIN_KEY="$BUILDERS_JOIN_KEY" um runner serve --config /etc/um/runner.json
```

On a host that holds no enrolled state, the service generates its credential, joins the
key's pool as a new runner, writes enrolled state, and serves. On a host that already holds
enrolled state, it serves with that state and ignores the key, so the same command can
start the service every time. A host that holds enrolled state never reads the key and is
not prompted for one, and when its configuration or state does not load the start reports
that cause. On a host that holds no enrolled state, the service checks the host's
configuration, its work root included, before it reads the key, prompts for it or contacts
Cloud, so a host that cannot serve registers no runner. A key given with `--join-key-file`
is read as an activation is: from a regular file private to the current user, which is
left in place, or from piped standard input with `--join-key-file -`; an interactive
terminal is refused there, since `--join-key` is the terminal's input. `--join-key`,
`--join-key-file` and `UM_RUNNER_JOIN_KEY` exclude one another: giving the key in more than
one way is a usage error, and an expired key is refused before Cloud is contacted. The
service reads the key once, removes `UM_RUNNER_JOIN_KEY` from its own environment by
restarting itself without it, never writes the key to its state or keeps it for a later
start, and never passes it to a workflow step.

A key Cloud refuses, because its token is not recognized or the key is revoked or expired, or because its pool cannot
admit the runner (a personal pool whose member is not active, or an organization at its
runner limit), stops the start with a failure naming the refusal. An ambiguous outcome,
such as a lost response, is retried as the same request, so one start never registers two
runners. Until a join resolves, the host keeps a join journal beside its state. A start
with a different key than the journal names is refused with the journal's path and the
key it names: start the service once more with that key, which resends the same join, or
remove the journal after confirming in the pool that no runner joined from the host.
`runner enroll` with an activation is refused the same way while that journal exists.

A join resent from the journal, after an interrupted start or a lost response, may already
have registered a runner. When Cloud rejects its token, the journal stays and the failure
names it: the same key, however it is given, would be rejected again, so start the service
with the key as it was issued, or delete any runner that joined from the host in the pool,
remove the journal, and start with a key Cloud accepts. When Cloud refuses a resent join as gone,
the key may be revoked or expired, its issuer may no longer issue a key for the pool, or the
runner the earlier join registered may have been deleted; the journal is removed, and the
failure says that Cloud holds no runner for the host's join rather than that none was
registered.

The connection URL, runner ID, and current credential come only from the enrolled state
written by enrollment or by a join. There are no endpoint environment overrides or command-line aliases for the removed
Gateway, credential, transport-mode, workflow-mapping, source, workflow-path, and work
root options. `production` requires the Cloud-issued `wss://` connection URL.
`development` additionally permits `ws://` only for exact `localhost`, IPv4 loopback,
or `[::1]` hosts; it never permits a caller-selected endpoint. Deployment mode changes
transport security only. Every assignment supplies a pinned Cloud repository source,
and every runner materializes and verifies that source before admission.

Rotate a running host only with a replacement activation issued for its existing runner:

```sh
um runner enroll \
  --replace-credential \
  --activation-file /protected/path/replacement-activation.json
```

The command reports Cloud enrollment and live promotion separately. It stages the new
credential as pending without replacing the current credential, then asks Runner Serve
over the configured local socket to welcome and promote it in the existing boot. An
unreachable service or a pending authentication, protocol, network, or state-write
error leaves both credentials in protected state and returns nonzero; it never restarts
the service. Rerun the same command with the same protected activation file to retry a
staged credential without enrolling a third credential. On process startup, Runner
Serve prefers a usable current connection and then retries pending promotion. Keep the
old Cloud credential active until local status shows the replacement current and
`runner credential list` shows its `lastAuthenticatedAt`; then retire it with the fixed
grace or revoke it immediately if compromise is suspected. Never print an activation
artifact or runner state while transferring or verifying it.

Runner Serve installs the agent logins members have authorized for the runner when it
starts, every five minutes while it holds no assignment, and for each assignment it admits
before offering it any, so an assignment admitted after a reconnect gets the new generation
and one admitted after a revocation gets nothing of it, while an assignment already running
keeps what it was offered. It removes a login once Cloud no longer authorizes it, a Codex
login with the runner's whole Codex home. It keeps the logins in its own
`.runner-agent-login/` directory beside `runnerStatePath`, never in the service's `~/.codex`
or Claude Code configuration, and offers them to each assignment as `CODEX_HOME` and
`CLAUDE_CODE_OAUTH_TOKEN`: a workflow's steps use a delivered login only when the workflow's
`environmentPassthrough` names that variable. Steps given the runner's `CODEX_HOME` read
only its configuration there, not the service's own Codex configuration. While the service
sets `CODEX_HOME` or has a Claude Code login of its own, that provider is reported
`foreign_login` and nothing is installed for it. Runner Serve also checks a login with its
provider by running that coding agent's own program once with the login, outside any run,
with a fixed prompt that asks for a one-word answer: a login it installs when it starts or
while it holds no assignment, and every `ready` login when you ask it to set up now. A check
spends one model turn of the login owner's account at the provider, starts only while the
runner holds no assignment, and is never made for an assignment's own setup. `um runner
status` shows each provider's readiness once it has set up, and you can ask the live runner
to set up now:

```sh
um runner grant sync
um runner grant sync --json
```

The command goes only through the local control socket and prints one line per provider
with its state, its availability, when there is something to do its next action and the
step it names, and the login's last use and when
(`codex login:       ready (generation 3), availability available · last use: accepted at 2026-11-13T09:30:00Z`),
or with `--json` a schema-version-1 `synced` document with `agentLogins`, each provider
carrying `availability` and `nextAction` and, for a login with one, `lastUse` and `lastUseAt`.
The setup it asks for checks every `ready` login before it answers, so the command can take
about a minute longer for each login whose provider is slow to answer. A login is
`available` once a check or a step succeeded with it, `reconnect_required` once the provider
refused one, and `unverified` until either has happened at this generation; only the agent
login's owner can reconnect it. A check that gives no answer changes nothing: after the
providers' lines the command prints one line naming that provider and the cause
(`codex check:       no answer (program_unavailable)`), and `--json` lists it under
`unansweredChecks` with its `provider` and `cause`, one of `program_unavailable` (Runner
Serve found no supported `claude` or `codex` on its `PATH` when it started), `start_failed`,
`timed_out` (no answer within 60 seconds) or `harness_error`. It exits 1 when a provider
authorized for the runner is not `ready`, one Cloud refuses the owner of
(`insufficient_permission`) included, or its login was refused at its last use, and 4 when
Runner Serve is not reachable or holds an assignment (`runner_busy`); a check with no answer
does not change the exit status. A Runner Serve process started from an older `um` refuses
the request; restart it on this release.

Runner startup selects `pi`, `claude`, and `codex` independently from its inherited
operator-controlled `PATH`. Useful Machinery installs none of them. Each successful installation
is validated once and retained as an immutable executable, exact-version, profile, and
capability snapshot for the process lifetime: Pi admits `>=0.84.2 <0.88.0` or exactly `0.99.1`, `1.2.0`, or `1.3.0`, Claude Code
admits `>=2.1.234 <2.2.0`, and Codex admits capability-compatible stable
`>=0.147.0 <0.164.0` with exact repository qualification anchor `0.163.0`. Each also
keeps, as an untested compatible harness, a capability-compatible stable release newer
than its qualification release (Pi `1.3.0`, Claude Code `2.1.321`, Codex `0.163.0`), and
Runner Serve records one `runner.harness_untested` telemetry event per untested
installation on its stderr telemetry stream at startup, before its first connection
attempt. Admission and invocation never repeat a lookup or probe. A missing
or incompatible installation leaves only that harness unavailable, so Runner Serve
continues to accept command-only and other available-harness assignments. An assignment
requiring the unavailable harness is rejected before launch, and selection never falls
through to another executable or harness. Assignments and workflow data cannot influence
selection.

Runner agent steps use the same production dispatcher, cancellation precedence, process
containment, descendant-quiescence boundary, result authority, and atomic publication
path as local execution; no runner protocol or credential boundary changes by harness.
Claude execution still requires native initialization to report the startup version and
uses fixed unattended `bypassPermissions`. Codex execution uses the closed ephemeral
App Server profile and fixed no-approval policy. Neither is a sandbox. The runner owner
remains responsible for trusted native configuration and filesystem, process, network,
resource, credential, and secret isolation. Follow the harness rollout runbook, including
the required native qualification boundary, service restart, selected-harness no-fallback
recovery, and the separately deferred first credentialed production-runner canary.

Runner protocol v1 implements exactly
`workflow_v1_cloud_inputs_artifacts@1`, including ordinary-step recovery, every closed
Workflow V1 output/export, and terminal recovery evidence. Assignment admission consumes
the immutable resolver-carried transition, invocation, retention, and encoded-outbox
capacity bound to the workflow source closure; Runner Serve does not re-derive formulas.
Recovery transitions carry compact settling-invocation evidence, and the portable result
owns the complete closed summary/history. All ordinary frames remain at most 64 KiB, and
at most one terminal frame per assignment may use the 32 MiB terminal class.

The runner reconnects after retryable network, timeout, rate-limit, Gateway restart, and
server failures with jittered backoff while retaining boot-scoped assignment state.
Credential rejection is terminal authentication; unsupported subprotocol or malformed
Cloud protocol is terminal protocol. Terminal outcomes exit nonzero without exposing
bearer material. The work root must be an existing directory. An offer reserves one
assignment-private root but starts no source or input work. After the matching prepare
effect, Runner Serve checks out the pinned source, verifies the authoritative canonical
Run Input manifest, downloads every member through fresh exact capabilities into random
0600 temporary files without redirects, and admits exact named Text, semantic JSON,
singular File bytes and media types, and ordered attachment collections before semantic
acceptance. The fixed preparation deadline is not extended
by progress. Verified staging remains under the assignment root through execution and is
removed or quarantined by the existing release path. Cloud commands, agents, and
finalizers receive the exact recorded source branch and pinned commit as
`UM_SOURCE_BRANCH` and `UM_SOURCE_COMMIT_OID`; local execution receives neither.
After the later start effect, a token-free repository helper can obtain lease-bound read
access only for the credential-free primary `origin`. Runner Serve disables the helper,
revokes every known issuance after process quiescence, destroys local helper and token
state, verifies all retained carriers in private staging, and releases the checkout
before artifact upload or terminal acknowledgement. Author code can deliberately query
that helper and copy its bearer into author-controlled output; the platform does not
claim to prevent that behavior. Transport receipt and semantic
acceptance remain separate, and execution remains bounded by the latest received lease
across same-boot reconnects.

While the service runs, standard error contains newline-delimited JSON. Each outbound
attempt completes one `runner.gateway_connection` event with safe runner and boot IDs,
server host and port, protocol progress and counts, retry classification, selected
backoff when applicable, and a closed outcome and error type. Each offered effect
completes one `runner.effect_acknowledgement` event after transport confirmation or an
earlier safe ending. Its `success` outcome means only that the gateway confirmed the
runner's transport acknowledgement; the runner does not emit `runner.run` yet. Completed
JSON records enter a bounded non-blocking queue, so a stalled standard-error consumer
cannot delay runner protocol work; saturation or output failure drops and counts a
record.

`runner serve` can also send those same reviewed spans to a user-owned OTLP/HTTP
protobuf receiver. There is no default destination: export is enabled only when
`OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` or `OTEL_EXPORTER_OTLP_ENDPOINT` is non-empty. The
trace-specific endpoint is the complete request URL and takes precedence; `/v1/traces`
is appended to the generic endpoint. Remote endpoints must use HTTPS. HTTP is accepted
only for the exact `localhost` host or a loopback IP address. User information, queries,
fragments, remote cleartext, and non-HTTP protocols disable export without stopping the
runner.

The following standard OpenTelemetry environment variables are supported, with the
trace-specific value taking precedence over its generic equivalent:

- `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` / `OTEL_EXPORTER_OTLP_ENDPOINT`;
- `OTEL_EXPORTER_OTLP_TRACES_HEADERS` / `OTEL_EXPORTER_OTLP_HEADERS`, using the standard
  comma-separated `name=percent-encoded-value` form;
- `OTEL_EXPORTER_OTLP_TRACES_TIMEOUT` / `OTEL_EXPORTER_OTLP_TIMEOUT`, as an integer from
  1 through 30000 milliseconds; and
- `OTEL_EXPORTER_OTLP_TRACES_PROTOCOL` / `OTEL_EXPORTER_OTLP_PROTOCOL`, when set to
  `http/protobuf`.

For example, the receiver and its credential both remain under the operator's control:

```sh
OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=https://telemetry.example.test/v1/traces \
OTEL_EXPORTER_OTLP_TRACES_HEADERS='authorization=Bearer%20operator-owned-token' \
  um runner serve
```

`OTEL_SDK_DISABLED` is the sole remote-export privacy switch. Case-insensitive `true`
vetoes export even when every exporter variable is set; unset, empty, or
case-insensitive `false` permits an explicitly configured endpoint. Any other non-empty
value disables export and emits only a closed local diagnostic. The switch never
disables local JSON or W3C Trace Context propagation.

OTLP work uses a bounded, non-blocking queue, bounded requests and shutdown, and no
application retry. Receiver failures, rejection, stalls, saturation, malformed
configuration, and shutdown timeout cannot change WebSocket traffic, acknowledgements,
backoff, results, or exit status. Diagnostics are emitted at most once per closed
classification and contain no endpoint, header, response body, or raw exporter error.
Local JSON continues alongside export.

The WebSocket upgrade carries only the connection span's W3C `traceparent` (and a
non-empty `tracestate`, if one exists), never baggage. The gateway can extract it as the
session's remote parent. Effect acknowledgement spans remain independent roots and use
the existing runner, boot, run, assignment, and effect IDs for correlation. Exported
resource metadata contains only `service.name=um-runner`, the package version, and
the generated boot ID as `service.instance.id`; arbitrary resource environment
attributes are not imported.

Service events never contain the machine credential, endpoint path or query, raw
protocol frames, WebSocket close reasons, or arbitrary network errors. OTLP credentials
are supplied only by the user through the standard header variables; the runner ships
no Useful Machinery-owned endpoint, ingestion credential, Honeycomb behavior, or collector
requirement. Human and JSON output contracts for help, version, authentication, account,
and `runner doctor` remain unchanged and do not initialize runner telemetry.

Cloud rejections and local enqueue failures during artifact registration and confirmation
emit a local JSON diagnostic named `runner.artifact_delivery_failed`. Its assignment,
attempt, and delivery IDs identify
the affected transfer. `um.artifact.failure_origin` distinguishes `runner` from
`cloud`; `um.artifact.operation` identifies `registration` or `confirmation`,
independently of the public delivery phase. Local enqueue failures retain the closed
codes `outbox_capacity`, `outbox_encoding`, and `outbox_sequence`. Cloud failures retain
their closed response code and `um.protocol.request_message_id` for correlation
with protocol frame logs. These diagnostics exclude upload URLs, artifact contents,
and raw errors.

If a Cloud workflow result cannot be serialized for Artifact Set delivery, the
runner retains `result-publication-failure.json` under that assignment's private
run directory. It records the original workflow issue, structural step metadata,
the publication failure code, and the failed command's bounded output when
available. The file is mode `0600` and stays on the runner with the failed
workspace; it is not a portable Artifact Set or a publication receipt. Command
output may contain sensitive data, so inspect it only through operator access.

## Release policy

`release.toml` schema 2 contains only static policy: the initial release is `0.1.0`, the
source-development version is `0.0.0-dev`, and breaking impact before `1.0` is minor.
Native Cargo builds always report the development fallback. Nix development packages
inject a revision-bearing development version, while official release builds inject
their exact approved version.

Run `./scripts/check-release` to validate the policy and Cargo fallback. Impact and
version arithmetic in `./scripts/release-impact` is pure over this policy, explicit
impact, and an optional latest stable version. Source checks never fetch or inspect
public tags. The files in `changes/` are a frozen legacy archive; new intent is reviewed
in the canonical private journal before this standalone tree is mirrored.

Public `main` is a rolling, untagged release candidate rather than a version
reservation. Managed Buildkite advances it after validating the canonical source-evidence
artifact; GitHub Actions verifies the candidate contract, builds the x86-64 and ARM64
Linux and Apple Silicon macOS archives, and asks a human to approve the proposed version
and notes in the protected `cli-release` environment before the final write-scoped job
publishes the archives, `SHA256SUMS`, and build-provenance attestations on the
[Releases](https://github.com/useful-machinery/um/releases) page. A pre-tag failure is
repaired by advancing `main` with a corrected candidate. See
[`CONTRIBUTING.md`](CONTRIBUTING.md#release-policy-and-archived-notes) for the complete
candidate contract and repair rules.

Release binaries are not currently signed or notarized. Verify a downloaded archive
with the attached checksums and GitHub attestation before running it:

```sh
archive='um-<version>-<target>.tar.gz'

# Linux
sha256sum --ignore-missing --check SHA256SUMS

# macOS
shasum -a 256 --ignore-missing --check SHA256SUMS

gh attestation verify "$archive" \
  --repo useful-machinery/um
```

## Source boundary

Everything in this repository builds and tests using only its checked-in source and
declared external dependencies. The canonical check verifies that the public source is
self-contained.

## Contributing

> [!IMPORTANT]
> This repository is a read-only mirror. Public Discussions are welcome, but pull
> requests cannot be merged into the mirror directly.

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for feedback channels, development checks, and
security-reporting guidance.

## Development

The repository contains a standalone devenv environment with the pinned Rust toolchain.
Run the canonical check from the repository root:

```sh
./scripts/check
```

The command uses `scripts/strict-devenv` to remove the caller's environment before
Devenv constructs the declared test environment. This is the same entrypoint used by CI.
It preserves `CARGO_BUILD_JOBS`, `CARGO_HOME`, `CARGO_TARGET_DIR`, `CARGO_INCREMENTAL`,
and `RUSTC_WRAPPER` so build limits, a caller-owned registry and build cache, and
runner-owned compiler coordination survive the clean boundary instead of recompiling the
dependency graph from cold on every run. Jobs are per Cargo invocation. Incremental
compilation stays off unless the caller sets `CARGO_INCREMENTAL=1` for a persistent
target it reuses.
The check verifies public-source isolation, formatting, every target and feature on the
`rust-version` declared in `Cargo.toml`, checked-in Clippy policy, unit and integration
tests, and release-archive packaging of the tested binary.

## License

Useful Machinery CLI is licensed under the Apache License 2.0. See `LICENSE`.
