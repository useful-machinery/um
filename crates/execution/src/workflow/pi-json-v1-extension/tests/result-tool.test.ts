import assert from "node:assert/strict";
import test from "node:test";

import {
  createPiJsonV1Extension,
  createResultTool,
  decodeFrame,
  encodeFrame,
  type PiJsonV1ExtensionConfig,
  type ValidatePiResultV1Request,
} from "../fixtures/generated/pi-json-v1-extension.ts";

const fixedConfig: PiJsonV1ExtensionConfig = {
  toolName: "um_result_unit_fixed",
  socketPath: "/tmp/um/unit-fixed/result.sock",
  parameters: {
    type: "object",
    properties: {
      result: { type: "object" },
    },
    required: ["result"],
    additionalProperties: false,
  },
};

const fixedPayload = {
  summary: "fixed payload",
  files: ["src/main.rs"],
};

type ExtensionApi = Parameters<ReturnType<typeof createPiJsonV1Extension>>[0];
type ResultValidator = NonNullable<
  Parameters<typeof createPiJsonV1Extension>[1]
>;
type ResultToolContext = Parameters<
  ReturnType<typeof createResultTool>["execute"]
>[4];
type ToolCallGate = (
  event: {
    toolCallId: string;
    toolName: string;
    input: Record<string, unknown>;
  },
  context: { sessionManager: { getBranch(): readonly unknown[] } },
) => unknown;
type CompactionGate = () => unknown;
interface RegisteredResultTool {
  name: string;
  execute(
    toolCallId: string,
    argumentsValue: { result: unknown },
    signal: AbortSignal | undefined,
    onUpdate: undefined,
    context: {
      abort(): void;
      sessionManager: { getBranch(): readonly unknown[] };
    },
  ): Promise<{ terminate?: boolean }>;
}

function loadResultExtension(
  config: PiJsonV1ExtensionConfig,
  validate: ResultValidator,
): {
  gate: ToolCallGate;
  compact: CompactionGate;
  tool: RegisteredResultTool;
} {
  let gate: ToolCallGate | undefined;
  let compact: CompactionGate | undefined;
  let tool: RegisteredResultTool | undefined;
  const pi = {
    on(eventName: string, handler: unknown) {
      if (eventName === "tool_call") {
        gate = handler as ToolCallGate;
      } else if (eventName === "session_before_compact") {
        compact = handler as CompactionGate;
      }
    },
    registerTool(registeredTool: unknown) {
      tool = registeredTool as RegisteredResultTool;
    },
  } as unknown as ExtensionApi;

  createPiJsonV1Extension(config, validate)(pi);
  assert.ok(gate !== undefined);
  assert.ok(compact !== undefined);
  assert.ok(tool !== undefined);
  return { gate, compact, tool };
}

function branchContext(messages: readonly Record<string, unknown>[]): {
  sessionManager: { getBranch(): readonly unknown[] };
} {
  return {
    sessionManager: {
      getBranch: () =>
        messages.map((message) => ({ type: "message", message })),
    },
  };
}

function toolCallContext(content: readonly Record<string, unknown>[]): {
  sessionManager: { getBranch(): readonly unknown[] };
} {
  return branchContext([{ role: "assistant", content }]);
}

function resultToolCallContext(
  toolCallId: string,
  argumentsValue: unknown,
): { sessionManager: { getBranch(): readonly unknown[] } } {
  return toolCallContext([
    {
      type: "toolCall",
      id: toolCallId,
      name: fixedConfig.toolName,
      arguments: argumentsValue,
    },
  ]);
}

function resultToolContext(
  toolCallId: string,
  argumentsValue: unknown,
  abort: () => void,
): ResultToolContext {
  return {
    abort,
    ...resultToolCallContext(toolCallId, argumentsValue),
  } as unknown as ResultToolContext;
}

test("a valid fixed payload produces a terminating tool result", async () => {
  let capturedRequest: ValidatePiResultV1Request | undefined;
  const tool = createResultTool(
    fixedConfig,
    async (socketPath, request, signal) => {
      assert.equal(socketPath, fixedConfig.socketPath);
      assert.equal(signal, undefined);
      capturedRequest = request;
      return { kind: "Valid" };
    },
  );

  const result = await tool.execute(
    "tool-call-fixed",
    { result: fixedPayload },
    undefined,
    undefined,
    resultToolContext("tool-call-fixed", { result: fixedPayload }, () =>
      assert.fail("valid results must not abort Pi"),
    ),
  );

  assert.deepEqual(capturedRequest, {
    kind: "ValidatePiResultV1",
    toolCallId: "tool-call-fixed",
    toolName: fixedConfig.toolName,
    arguments: { result: fixedPayload },
  });
  assert.equal(result.terminate, true);
});

test("validator feedback is returned as a tool error", async () => {
  const tool = createResultTool(fixedConfig, async () => ({
    kind: "Rejected",
    feedback: "The fixed payload does not satisfy the retained schema.",
  }));

  await assert.rejects(
    tool.execute(
      "tool-call-fixed",
      { result: fixedPayload },
      undefined,
      undefined,
      resultToolContext("tool-call-fixed", { result: fixedPayload }, () =>
        assert.fail("rejected results must not abort Pi"),
      ),
    ),
    new Error("The fixed payload does not satisfy the retained schema."),
  );
});

// REQ-AGENT-83, REQ-AGENT-29, INV-HARNESS-16 (AUTO-263)
test("validation submits the arguments the session records, not the ones Pi executes the tool with", async () => {
  const sessionArguments = {
    result: { summary: "fixed payload", note: null, count: "3" },
  };
  const executedArguments = { result: { summary: "fixed payload", count: 3 } };
  let capturedRequest: ValidatePiResultV1Request | undefined;
  const { gate, tool } = loadResultExtension(
    fixedConfig,
    async (_socketPath, request) => {
      capturedRequest = request;
      return { kind: "Valid" };
    },
  );
  const toolCallId = "tool-call-recorded-fixed";
  const session = resultToolCallContext(toolCallId, sessionArguments);

  assert.equal(
    gate(
      {
        toolCallId,
        toolName: fixedConfig.toolName,
        input: executedArguments,
      },
      session,
    ),
    undefined,
  );
  await tool.execute(toolCallId, executedArguments, undefined, undefined, {
    abort: () => assert.fail("accepted results must not abort Pi"),
    ...session,
  });

  assert.deepEqual(capturedRequest?.arguments, sessionArguments);
});

// INV-HARNESS-16 (AUTO-263)
test("a result call the session does not record alone is refused before validation", async () => {
  let validations = 0;
  const { tool } = loadResultExtension(fixedConfig, async () => {
    validations += 1;
    return { kind: "Valid" };
  });

  await assert.rejects(
    tool.execute(
      "tool-call-unrecorded-fixed",
      { result: fixedPayload },
      undefined,
      undefined,
      {
        abort: () => assert.fail("an uncorrelated call must not abort Pi"),
        ...resultToolCallContext("tool-call-other-fixed", {
          result: fixedPayload,
        }),
      },
    ),
    new Error(
      "No result was accepted. The workflow result call could not be correlated.",
    ),
  );
  assert.equal(validations, 0);
});

test("compaction remains available until one result is accepted", async () => {
  let decision: "Rejected" | "Valid" = "Rejected";
  const { compact, tool } = loadResultExtension(fixedConfig, async () =>
    decision === "Valid"
      ? { kind: "Valid" }
      : { kind: "Rejected", feedback: "Correct the result." },
  );

  assert.equal(compact(), undefined);
  await assert.rejects(
    tool.execute(
      "tool-call-rejected-fixed",
      { result: fixedPayload },
      undefined,
      undefined,
      resultToolContext(
        "tool-call-rejected-fixed",
        { result: fixedPayload },
        () => assert.fail("rejection must not abort Pi"),
      ),
    ),
  );
  assert.equal(compact(), undefined);

  decision = "Valid";
  await tool.execute(
    "tool-call-accepted-fixed",
    { result: fixedPayload },
    undefined,
    undefined,
    resultToolContext(
      "tool-call-accepted-fixed",
      { result: fixedPayload },
      () => assert.fail("acceptance must not abort Pi"),
    ),
  );
  assert.deepEqual(compact(), { cancel: true });
});

test("a sibling result is blocked without affecting ordinary work or correction", async () => {
  let validations = 0;
  const { gate, tool } = loadResultExtension(fixedConfig, async () => {
    validations += 1;
    return { kind: "Valid" };
  });
  const blockedCallId = "tool-call-with-sibling-fixed";
  const siblingCallId = "sibling-tool-call-fixed";

  assert.equal(tool.name, fixedConfig.toolName);
  const siblingContext = toolCallContext([
    {
      type: "toolCall",
      id: blockedCallId,
      name: fixedConfig.toolName,
      arguments: { result: fixedPayload },
    },
    {
      type: "toolCall",
      id: siblingCallId,
      name: "read",
      arguments: { path: "fixed-input.txt" },
    },
  ]);
  const gateResult = gate(
    {
      toolCallId: blockedCallId,
      toolName: fixedConfig.toolName,
      input: { result: fixedPayload },
    },
    siblingContext,
  );
  assert.ok(gateResult !== null && typeof gateResult === "object");
  assert.deepEqual(gateResult, {
    block: true,
    reason:
      "No result was accepted. Call the workflow result tool by itself, without sibling tool calls.",
  });
  assert.equal(
    gate(
      { toolCallId: siblingCallId, toolName: "read", input: {} },
      siblingContext,
    ),
    undefined,
  );
  assert.equal(validations, 0);

  const correctedCallId = "tool-call-corrected-fixed";
  const correctedArguments = { result: fixedPayload };
  assert.equal(
    gate(
      {
        toolCallId: correctedCallId,
        toolName: fixedConfig.toolName,
        input: correctedArguments,
      },
      resultToolCallContext(correctedCallId, correctedArguments),
    ),
    undefined,
  );
  assert.equal(
    (
      await tool.execute(
        correctedCallId,
        correctedArguments,
        undefined,
        undefined,
        resultToolContext(correctedCallId, correctedArguments, () =>
          assert.fail("acceptance must not abort Pi"),
        ),
      )
    ).terminate,
    true,
  );
  assert.equal(validations, 1);
});

test("reused call identities and ambiguous argument objects are blocked", () => {
  const { gate } = loadResultExtension(fixedConfig, async () => ({
    kind: "Valid",
  }));
  const toolCallId = "tool-call-reused-fixed";
  const validContext = resultToolCallContext(toolCallId, {
    result: fixedPayload,
  });

  assert.equal(
    gate(
      {
        toolCallId,
        toolName: fixedConfig.toolName,
        input: { result: fixedPayload },
      },
      validContext,
    ),
    undefined,
  );
  const reused = gate(
    {
      toolCallId,
      toolName: fixedConfig.toolName,
      input: { result: fixedPayload },
    },
    validContext,
  );
  assert.equal((reused as { block?: unknown }).block, true);

  const ambiguous = gate(
    {
      toolCallId: "tool-call-extra-fixed",
      toolName: fixedConfig.toolName,
      input: { result: fixedPayload },
    },
    resultToolCallContext("tool-call-extra-fixed", {
      result: fixedPayload,
      extra: true,
    }),
  );
  assert.equal((ambiguous as { block?: unknown }).block, true);

  const duplicatedAcrossMessages = gate(
    {
      toolCallId: "tool-call-duplicated-fixed",
      toolName: fixedConfig.toolName,
      input: { result: fixedPayload },
    },
    branchContext([
      {
        role: "assistant",
        content: [
          {
            type: "toolCall",
            id: "tool-call-duplicated-fixed",
            name: "read",
            arguments: { path: "old.txt" },
          },
        ],
      },
      {
        role: "assistant",
        content: [
          {
            type: "toolCall",
            id: "tool-call-duplicated-fixed",
            name: fixedConfig.toolName,
            arguments: { result: fixedPayload },
          },
        ],
      },
    ]),
  );
  assert.deepEqual(duplicatedAcrossMessages, {
    block: true,
    reason:
      "No result was accepted. The workflow result call could not be correlated.",
  });
});

test("a validator transport failure aborts Pi instead of becoming recoverable", async () => {
  let aborted = false;
  const tool = createResultTool(fixedConfig, async () => {
    throw new Error("The validation socket closed unexpectedly.");
  });

  await assert.rejects(
    tool.execute(
      "tool-call-transport-failure-fixed",
      { result: fixedPayload },
      undefined,
      undefined,
      resultToolContext(
        "tool-call-transport-failure-fixed",
        { result: fixedPayload },
        () => {
          aborted = true;
        },
      ),
    ),
  );

  assert.equal(
    aborted,
    true,
    "validation-channel failures must abort rather than become correctable tool errors",
  );
});

test("a fatal validator response aborts Pi before surfacing the failure", async () => {
  let aborted = false;
  const { gate, tool } = loadResultExtension(fixedConfig, async () => ({
    kind: "Fatal",
    cause: "The fixed validation budget was exhausted.",
  }));
  const toolCallId = "tool-call-fatal-fixed";
  const argumentsValue = { result: fixedPayload };

  assert.equal(
    gate(
      {
        toolCallId,
        toolName: fixedConfig.toolName,
        input: argumentsValue,
      },
      resultToolCallContext(toolCallId, argumentsValue),
    ),
    undefined,
  );
  await assert.rejects(
    tool.execute(
      toolCallId,
      argumentsValue,
      undefined,
      undefined,
      resultToolContext(toolCallId, argumentsValue, () => {
        aborted = true;
      }),
    ),
  );

  assert.equal(aborted, true);
});

test("protocol framing preserves a fixed request payload", () => {
  const request: ValidatePiResultV1Request = {
    kind: "ValidatePiResultV1",
    toolCallId: "tool-call-fixed",
    toolName: fixedConfig.toolName,
    arguments: { result: fixedPayload },
  };

  const decoded: unknown = JSON.parse(
    decodeFrame(encodeFrame(request)).toString("utf8"),
  );
  assert.deepEqual(decoded, request);
});
