// SPDX-License-Identifier: MIT
import * as s from "./schema";
import { localStep } from "./resources";

export const operationDetail = s.refine(
  s.object({
    step: localStep,
    kind: s.enumeration("local", "model", "tool"),
    version: s.nullable(s.id),
    arguments: s.payload,
    result: s.payload,
  }),
  ({ kind, step, version, arguments: args }) =>
    kind === "local"
      ? version === null
      : version !== null &&
        new TextEncoder().encode(version).length <= 128 &&
        /^(model|tool):[^:]+:.+$/.test(step.callable) &&
        step.callable.startsWith(kind + ":") &&
        s.isJsonObject(args),
  "Invalid durable operation binding.",
);

export function operationFor(
  identity: { workflow_id: string; activation_id: string; step_key: string },
  node: {
    callable: string;
    accepted_at: number | null;
    accepting_attempt_id: string | null;
  },
) {
  return s.refine(
    operationDetail,
    ({ step }) =>
      step.workflow_id === identity.workflow_id &&
      step.activation_id === identity.activation_id &&
      step.step_key === identity.step_key &&
      step.callable === node.callable &&
      step.attempt_id === node.accepting_attempt_id &&
      step.accepted_at === node.accepted_at,
    "The durable operation does not match the selected local step.",
  );
}
