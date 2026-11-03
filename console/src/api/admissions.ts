// SPDX-License-Identifier: MIT
import * as s from "./schema";
import { ContractError } from "./codecs";
import { admissionOccurrence, program } from "./resources";
import { stringifyUserJson } from "./json";
import type { ExplorerNode } from "./explorer";
const positive = s.integer(1, Number.MAX_SAFE_INTEGER);
const digest = s.refine(
  s.string,
  (v) => /^sha256:[0-9a-f]{64}$/.test(v),
  "Invalid admission digest.",
);
const scope = s.object({ tenant_id: s.id, namespace: s.id });
const same = (a: unknown, b: unknown) =>
  stringifyUserJson(a) === stringifyUserJson(b);
const bounded = (v: unknown) =>
  new TextEncoder().encode(stringifyUserJson(v)).length <= 65536;
const operation = s.object({
  callable: s.id,
  kind: s.enumeration("model", "tool"),
  version: s.id,
  arguments: s.refine(
    s.payload,
    (v) => s.isJsonObject(v) && bounded(v),
    "Invalid admission arguments.",
  ),
});
const binding = s.object({
  resource: s.id,
  program: s.object({ program, digest, size: positive }),
  operation,
});
const offer = s.object({
  occurrence: admissionOccurrence,
  generation: positive,
  policy_revision: positive,
  activation_id: s.id,
  expires_at: s.timestamp,
  binding_digest: digest,
});
const source = s.refine(
  s.object({
    occurrence: admissionOccurrence,
    source_id: s.id,
    start_id: s.id,
    finish_id: s.id,
    generation: positive,
    binding_digest: digest,
    owner: s.object({
      scope,
      task_id: s.id,
      attempt_id: s.id,
      lease_id: s.id,
      generation: s.integer(1),
      worker_session_id: s.id,
      consumer_id: s.integer(),
    }),
    worker: s.object({
      audience: s.object({ authority_id: s.id, instance_id: s.id, scope }),
      issuer: s.id,
      subject: s.id,
      credential_id: s.id,
    }),
    accepted_at: s.timestamp,
    start_deadline_at: s.timestamp,
  }),
  (v) =>
    same(v.occurrence.scope, v.owner.scope) &&
    same(v.owner.scope, v.worker.audience.scope) &&
    v.start_deadline_at > v.accepted_at &&
    v.start_deadline_at - v.accepted_at <= 30000,
  "Inconsistent admission source.",
);
const finish = s.refine(
  s.object({
    source,
    output: s.refine(s.payload, bounded, "Admission output exceeds its limit."),
    accepted_at: s.timestamp,
    already_accepted: s.boolean,
  }),
  (v) => v.accepted_at >= v.source.accepted_at,
  "Finish predates its source.",
);
export const admissionSnapshot = s.refine(
  s.object({
    occurrence: admissionOccurrence,
    binding,
    state: s.enumeration(
      "waiting",
      "offered",
      "awaiting_rewait",
      "possibly_dispatched",
      "unknown_occupied",
      "finished",
      "closed_before_start",
      "administratively_released",
    ),
    revision: positive,
    created_at: s.timestamp,
    updated_at: s.timestamp,
    deadline_at: s.timestamp,
    offer: s.nullable(offer),
    source: s.nullable(source),
    finish: s.nullable(finish),
  }),
  (v) => {
    if (v.updated_at < v.created_at || v.deadline_at < v.created_at)
      return false;
    if (
      v.offer &&
      (!same(v.offer.occurrence, v.occurrence) ||
        v.offer.expires_at > v.deadline_at)
    )
      return false;
    if (
      v.source &&
      (!same(v.source.occurrence, v.occurrence) ||
        v.source.accepted_at > v.updated_at ||
        v.source.accepted_at < v.created_at ||
        v.source.accepted_at >= v.deadline_at)
    )
      return false;
    if (
      v.finish &&
      (v.finish.accepted_at > v.updated_at ||
        !v.source ||
        !same(v.finish.source, v.source))
    )
      return false;
    switch (v.state) {
      case "waiting":
      case "awaiting_rewait":
      case "closed_before_start":
        return v.offer === null && v.source === null && v.finish === null;
      case "offered":
        return v.offer !== null && v.source === null && v.finish === null;
      case "possibly_dispatched":
      case "unknown_occupied":
        return v.offer === null && v.source !== null && v.finish === null;
      case "finished":
        return v.offer === null && v.source !== null && v.finish !== null;
      case "administratively_released":
        return v.offer === null && v.source !== null;
    }
  },
  "Inconsistent admission snapshot.",
);
export function admissionFor(
  node: Extract<ExplorerNode, { kind: "admission_operation" }>,
  instanceId: string,
) {
  return (raw: unknown) => {
    const v = admissionSnapshot(raw);
    const f = v.finish;
    if (
      !same(v.occurrence, node.occurrence) ||
      v.binding.resource !== node.resource ||
      v.binding.operation.callable !== node.callable ||
      v.binding.operation.kind !== node.operation_kind ||
      v.binding.operation.version !== node.operation_version ||
      !f ||
      f.source.source_id !== node.source_id ||
      f.source.binding_digest !== node.binding_digest ||
      f.source.owner.task_id !== node.activation_id ||
      f.source.owner.attempt_id !== node.accepting_attempt_id ||
      f.accepted_at !== node.accepted_at ||
      f.source.worker.audience.instance_id !== instanceId
    )
      throw new ContractError(
        "Admission result does not match the selected operation.",
      );
    return v;
  };
}
