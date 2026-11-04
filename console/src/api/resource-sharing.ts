// SPDX-License-Identifier: MIT
import * as s from "./schema";
import { queueName, type QueueScope } from "./queue-policies";
import { ContractError } from "./codecs";

export const resourceName = s.refine(
  queueName,
  (v) => /^[A-Za-z0-9_.:-]+$/.test(v),
  "Invalid resource name.",
);
export const sequence = s.refine(
  s.decimal,
  (v) => v !== "0",
  "Expected a positive sequence.",
);
const scope = s.object({ tenant_id: queueName, namespace: queueName });
const inScope = (a: QueueScope, b: QueueScope) =>
  a.tenant_id === b.tenant_id && a.namespace === b.namespace;
const weight = s.integer(1, 1000);
const group = s.object({
  name: resourceName,
  weight,
  queues: s.array(queueName, 64),
});
const fair = s.refine(
  s.object({ groups: s.array(group, 16), default_group: resourceName }),
  (v) => {
    const names = new Set(v.groups.map((g) => g.name));
    const queues = v.groups.flatMap((g) => g.queues);
    return (
      names.has(v.default_group) &&
      names.size === v.groups.length &&
      queues.length <= 64 &&
      new Set(queues).size === queues.length &&
      new TextEncoder().encode(JSON.stringify(v)).length <= 12288
    );
  },
  "Invalid resource group configuration.",
);
const basePolicy = {
  enabled: s.boolean,
  max_occupancy: s.integer(1, 1000000),
  rate: s.nullable(
    s.object({
      numerator: s.integer(1, 1000000),
      period_ms: s.integer(1, 86400000),
      burst: s.integer(1, 1000000),
    }),
  ),
  offer_timeout_ms: s.integer(1000, 300000),
  start_timeout_ms: s.integer(100, 30000),
};
function policy(v: unknown) {
  if (s.isJsonObject(v) && "fair_sharing" in v)
    return s.object({ ...basePolicy, fair_sharing: fair })(v);
  return { ...s.object(basePolicy)(v), fair_sharing: null };
}
const safe = s.integer(0, Number.MAX_SAFE_INTEGER);
const snapshot = s.refine(
  s.object({
    resource: s.object({
      scope,
      name: resourceName,
      revision: s.integer(1, Number.MAX_SAFE_INTEGER),
      policy,
      updated_at: s.timestamp,
    }),
    offered: safe,
    occupied: safe,
    unknown: safe,
    quarantined: s.boolean,
  }),
  (v) => v.offered + v.unknown <= v.occupied,
  "Invalid resource occupancy.",
);
export function inspectResource(expected: QueueScope, name: string) {
  return s.refine(
    snapshot,
    (v) => inScope(v.resource.scope, expected) && v.resource.name === name,
    "Resource belongs to a different query.",
  );
}
export function resourceList(
  expected: QueueScope,
  after: string | null,
  limit: number,
) {
  return s.refine(
    s.object({
      items: s.array(snapshot, limit),
      next: s.nullable(resourceName),
    }),
    (page) =>
      page.items.every(
        ({ resource }, i) =>
          inScope(expected, resource.scope) &&
          resource.name > (page.items[i - 1]?.resource.name ?? after ?? ""),
      ) &&
      (page.next === null || page.next === page.items.at(-1)?.resource.name),
    "Invalid resource page.",
  );
}
function workflowSubject<K extends "workflow" | "provider">(kind: K) {
  return s.object({
    kind: s.enumeration(kind),
    workflow_id: queueName,
    origin_activation_id: queueName,
    key: queueName,
  });
}
const subject = s.variant({
  task: s.object({
    kind: s.enumeration("task"),
    task_id: queueName,
    generation: s.integer(1, 2147483647),
  }),
  workflow: workflowSubject("workflow"),
  provider: workflowSubject("provider"),
});
const decision = s.refine(
  s.object({
    scope,
    resource: resourceName,
    sequence,
    policy_revision: sequence,
    group: resourceName,
    weight,
    eligible_groups: s.integer(1, 16),
    cycle_round: weight,
    reason: s.enumeration("weighted", "sole_eligible"),
    queue: queueName,
    subject,
    offer_generation: sequence,
    offered_at: s.timestamp,
  }),
  (v) =>
    v.cycle_round <= v.weight &&
    (v.eligible_groups === 1) === (v.reason === "sole_eligible"),
  "Invalid resource offer decision.",
);
export function decisionsFor(
  expected: QueueScope,
  name: string,
  before: string | null,
  limit: number,
) {
  return (v: unknown) => {
    const page = s.object({
      items: s.array(decision, limit),
      next: s.nullable(sequence),
    })(v);
    let previous = before;
    for (const row of page.items) {
      if (
        !inScope(row.scope, expected) ||
        row.resource !== name ||
        (previous !== null && BigInt(row.sequence) >= BigInt(previous))
      )
        throw new ContractError(
          "Resource decisions disagree with the requested page.",
        );
      previous = row.sequence;
    }
    if (page.next !== null && page.next !== page.items.at(-1)?.sequence)
      throw new ContractError("Invalid decision cursor.");
    return page;
  };
}
