import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { createPiJsonV1InputExtension } from "../fixtures/generated/pi-json-v1-input.ts";

type ExtensionApi = Parameters<
  ReturnType<typeof createPiJsonV1InputExtension>
>[0];
type InputHandler = (event: { text: string }) => unknown;
type StartHandler = (
  event: { messageType?: "user" | "custom"; systemPrompt: string },
  ctx: { abort(): void },
) => unknown;

test("staged input applies only to a user run, never to a custom run", async () => {
  const directory = await mkdtemp(join(tmpdir(), "pi-input-"));
  try {
    const messagePath = join(directory, "message.md");
    const systemPath = join(directory, "system.md");
    await Promise.all([
      writeFile(messagePath, "submitted text"),
      writeFile(systemPath, "owned system text"),
    ]);
    let input: InputHandler | undefined;
    let start: StartHandler | undefined;
    const pi = {
      on(name: string, handler: unknown) {
        if (name === "input") input = handler as InputHandler;
        if (name === "before_agent_start") start = handler as StartHandler;
      },
    } as unknown as ExtensionApi;
    createPiJsonV1InputExtension({
      message: { marker: "<message>", path: messagePath },
      systemPrompt: { marker: "<system>", path: systemPath },
    })(pi);
    assert.ok(input);
    assert.ok(start);
    assert.deepEqual(input({ text: "begin <message>" }), {
      action: "transform",
      text: "begin submitted text",
    });
    assert.deepEqual(input({ text: "another <message>" }), {
      action: "continue",
    });

    let aborts = 0;
    const ctx = {
      abort() {
        aborts += 1;
      },
    };
    assert.deepEqual(
      start({ messageType: "user", systemPrompt: "<system>" }, ctx),
      {
        systemPrompt: "owned system text",
      },
    );
    assert.equal(aborts, 0);
    assert.deepEqual(start({ systemPrompt: "<system>" }, ctx), {
      systemPrompt: "owned system text",
    });
    assert.equal(aborts, 0);
    assert.equal(
      start({ messageType: "custom", systemPrompt: "<system>" }, ctx),
      undefined,
    );
    assert.equal(aborts, 1);
    assert.equal(
      start({ messageType: "user", systemPrompt: "missing marker" }, ctx),
      undefined,
    );
    assert.equal(aborts, 2);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
