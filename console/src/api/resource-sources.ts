// SPDX-License-Identifier: MIT
import * as s from "./schema";
import { queueName as id, type QueueScope } from "./queue-policies";
import { resourceName, sequence } from "./resource-sharing";
import { admissionSnapshot } from "./admissions";
import { taskAdmissionFor } from "./task-admissions";
import { ContractError } from "./codecs";

const state = s.enumeration("possibly_dispatched", "unknown_occupied");
export type SourceState = s.Decoded<typeof state>;
const address = { workflow_id: id, origin_activation_id: id, key: id };
const subject = s.variant({
  task: s.object({
    kind: s.enumeration("task"),
    task_id: id,
    generation: s.integer(1, 2147483647),
  }),
  workflow: s.object({ kind: s.enumeration("workflow"), ...address }),
  provider: s.object({
    kind: s.enumeration("provider"),
    ...address,
    approval_activation_id: id,
    approval_key: id,
  }),
});
const sameScope = (a: QueueScope, b: QueueScope) =>
  a.namespace === b.namespace && a.tenant_id === b.tenant_id;
const row = s.object({
  scope: s.object({ tenant_id: id, namespace: id }),
  resource: resourceName,
  sequence: s.refine(
    sequence,
    (v) => BigInt(v) <= 9223372036854775807n,
    "Invalid source sequence.",
  ),
  revision: s.refine(
    sequence,
    (v) => BigInt(v) <= BigInt(Number.MAX_SAFE_INTEGER),
    "Invalid source revision.",
  ),
  updated_at: s.timestamp,
  state,
  subject,
});
export type SourceRow = s.Decoded<typeof row>;
export function sourcesFor(
  scope: QueueScope,
  name: string,
  filter: SourceState | null,
  before: string | null,
  limit: number,
) {
  return s.refine(
    s.object({
      items: s.array(row, limit),
      next_cursor: s.nullable(
        s.refine(
          s.string,
          (v) => v.length > 0 && v.length <= 4096,
          "Invalid source cursor.",
        ),
      ),
    }),
    (p) =>
      p.items.every((item, i) => {
        const prev = p.items[i - 1]?.sequence ?? before;
        return (
          sameScope(scope, item.scope) &&
          item.resource === name &&
          (!filter || item.state === filter) &&
          (!prev || BigInt(prev) > BigInt(item.sequence))
        );
      }) &&
      (!p.next_cursor || p.items.length > 0),
    "Source page does not match the requested resource, state or position.",
  );
}
export function sourceInspection(
  row: SourceRow,
  instance: string,
): {
  path: string;
  body: object;
  decode: s.Decoder<{ state: string; revision: number; source: string }>;
} {
  const a = row.subject;
  const invalid = () => {
    throw new ContractError(
      "The inspected record does not match this retained source.",
    );
  };
  if (a.kind === "task") {
    const decode = taskAdmissionFor(
      a.task_id,
      a.generation,
      row.resource,
      instance,
    );
    return {
      path: "task-admissions/inspect",
      body: { task_id: a.task_id, generation: a.generation },
      decode: (raw: unknown) => {
        const v = decode(raw);
        if (
          !sameScope(row.scope, v.occurrence.scope) ||
          !v.source ||
          BigInt(v.revision) < BigInt(row.revision)
        )
          return invalid();
        return {
          state: v.state,
          revision: v.revision,
          source: v.source.source_id,
        };
      },
    };
  }
  const { workflow_id, origin_activation_id, key } = a;
  return {
    path: "admissions/inspect",
    body: { workflow_id, origin_activation_id, key },
    decode: (raw: unknown) => {
      const v = admissionSnapshot(raw);
      if (
        !sameScope(row.scope, v.occurrence.scope) ||
        v.occurrence.workflow_id !== workflow_id ||
        v.occurrence.origin_activation_id !== origin_activation_id ||
        v.occurrence.key !== key ||
        v.binding.resource !== row.resource ||
        !v.source ||
        v.source.worker.audience.instance_id !== instance ||
        BigInt(v.revision) < BigInt(row.revision)
      )
        return invalid();
      if (a.kind === "provider") {
        if (!("provider" in v.binding)) return invalid();
        const approval = v.binding.provider.ticket.occurrence;
        if (
          approval.activation_id !== a.approval_activation_id ||
          approval.approval_key !== a.approval_key ||
          approval.workflow_id !== workflow_id ||
          !sameScope(approval.scope, row.scope)
        )
          return invalid();
      } else if (!("operation" in v.binding)) return invalid();
      return {
        state: v.state,
        revision: v.revision,
        source: v.source.source_id,
      };
    },
  };
}
