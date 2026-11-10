// SPDX-License-Identifier: MIT
// Identities handed off by the native acceptance gate, not global list positions.
import { readFile } from "node:fs/promises";
import { dirname, join } from "node:path";

const restartScenario = "native-restart-retains-authoritative-state";
const instruction =
  "Run tools/check-console.py with --browser-command, or provide its LEDGENCE_CONSOLE_STACK and matching LEDGENCE_CONSOLE_URL.";

function record(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}
function executionId(value: unknown, prefix: "task_" | "wf_"): string {
  if (
    typeof value !== "string" ||
    !value.startsWith(prefix) ||
    value.length > 128 ||
    !/^(task|wf)_[A-Za-z0-9_-]+$/u.test(value)
  )
    throw new Error("Invalid native execution seed identity.");
  return value;
}

export function decodeNativeExecutionSeeds(
  stack: unknown,
  results: unknown,
  consoleUrl: string,
) {
  if (
    !record(stack) ||
    typeof stack.console !== "string" ||
    new URL(stack.console).href !== new URL(consoleUrl).href
  )
    throw new Error(
      "Native seed evidence belongs to another Console. " + instruction,
    );
  if (!Array.isArray(results))
    throw new Error("Native acceptance results are missing. " + instruction);
  const matches = results.filter(
    (item: unknown) => record(item) && item.scenario === restartScenario,
  );
  const seed: unknown = matches[0];
  if (
    matches.length !== 1 ||
    !record(seed) ||
    seed.result !== "passed" ||
    !record(seed.detail)
  )
    throw new Error(
      "Exactly one passed native restart seed is required. " + instruction,
    );
  return {
    taskId: executionId(seed.detail.task_id, "task_"),
    workflowId: executionId(seed.detail.workflow_id, "wf_"),
  };
}

export async function nativeExecutionSeeds() {
  const path = process.env.LEDGENCE_CONSOLE_STACK;
  const consoleUrl = process.env.LEDGENCE_CONSOLE_URL;
  if (!path || !consoleUrl)
    throw new Error(
      "Native execution seed handoff is required. " + instruction,
    );
  const [stack, results] = await Promise.all([
    readFile(path, "utf8"),
    readFile(join(dirname(path), "results.json"), "utf8"),
  ]);
  return decodeNativeExecutionSeeds(
    JSON.parse(stack),
    JSON.parse(results),
    consoleUrl,
  );
}
