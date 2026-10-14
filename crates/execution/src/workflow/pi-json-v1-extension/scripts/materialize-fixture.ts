import { mkdir, readFile, writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";

import { materializePiJsonV1Extension } from "../src/materialize.ts";
import type { PiJsonV1ExtensionConfig } from "../src/pi-json-v1-extension.ts";

const packageRoot = fileURLToPath(new URL("../", import.meta.url));
const templatePath = fileURLToPath(
  new URL("src/pi-json-v1-extension.ts", new URL("../", import.meta.url)),
);
const inputPath = fileURLToPath(
  new URL(
    "fixtures/materialization-input.json",
    new URL("../", import.meta.url),
  ),
);
const fixtureDirectory = fileURLToPath(
  new URL("fixtures/generated/", new URL("../", import.meta.url)),
);
const fixturePath = fileURLToPath(
  new URL(
    "fixtures/generated/pi-json-v1-extension.ts",
    new URL("../", import.meta.url),
  ),
);
const inputTemplatePath = fileURLToPath(
  new URL("src/pi-json-v1-input.ts", new URL("../", import.meta.url)),
);
const inputFixturePath = fileURLToPath(
  new URL(
    "fixtures/generated/pi-json-v1-input.ts",
    new URL("../", import.meta.url),
  ),
);

async function materializeInputFixture(): Promise<string> {
  const template = await readFile(inputTemplatePath, "utf8");
  const marker = '"__UM_PI_JSON_V1_INPUT_CONFIG_JSON__"';
  if (template.split(marker).length !== 2) {
    throw new Error("Input extension template must contain exactly one marker");
  }
  return template.replace(marker, JSON.stringify(JSON.stringify({})));
}

async function materializeFixture(): Promise<string> {
  const [template, input] = await Promise.all([
    readFile(templatePath, "utf8"),
    readFile(inputPath, "utf8"),
  ]);
  const config = JSON.parse(input) as PiJsonV1ExtensionConfig;
  return materializePiJsonV1Extension(template, config);
}

async function main(): Promise<void> {
  const argumentsValue = process.argv.slice(2);
  const check = argumentsValue.length === 1 && argumentsValue[0] === "--check";
  if (!check && argumentsValue.length !== 0) {
    throw new Error("usage: materialize-fixture.ts [--check]");
  }

  const [materialized, inputMaterialized] = await Promise.all([
    materializeFixture(),
    materializeInputFixture(),
  ]);
  if (!check) {
    await mkdir(fixtureDirectory, { recursive: true });
    await Promise.all([
      writeFile(fixturePath, materialized, "utf8"),
      writeFile(inputFixturePath, inputMaterialized, "utf8"),
    ]);
    return;
  }

  const [fixture, inputFixture] = await Promise.all([
    readFile(fixturePath, "utf8"),
    readFile(inputFixturePath, "utf8"),
  ]);
  if (fixture !== materialized || inputFixture !== inputMaterialized) {
    throw new Error(
      `Generated PiJsonV1 fixture is stale; run npm run fixture:generate from ${packageRoot}`,
    );
  }
}

await main();
