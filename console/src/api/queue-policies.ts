// SPDX-License-Identifier: MIT
import * as s from "./schema";
import { ContractError } from "./codecs";

function validText(value: string, maximum: number) {
  return (
    new TextEncoder().encode(value).length <= maximum &&
    ![...value].some((c) => {
      const n = c.codePointAt(0)!;
      return (
        n < 32 ||
        (n >= 127 && n <= 159) ||
        (n >= 0xd800 && n <= 0xdfff) ||
        (n >= 0xfdd0 && n <= 0xfdef) ||
        (n & 0xfffe) === 0xfffe
      );
    })
  );
}
export const queueName = s.refine(
  s.id,
  (value) => validText(value, 128),
  "Invalid queue name.",
);
const revision = s.refine(
  s.decimal,
  (n) => BigInt(n) > 0n,
  "Invalid revision.",
);
const scope = s.object({ tenant_id: s.id, namespace: s.id });
export type QueueScope = s.Decoded<typeof scope>;
const duration = s.integer(1, 2147483647);
const release = (v: unknown) =>
  s.isJsonObject(v) && v.mode === "confirm"
    ? s.object({ mode: s.enumeration("confirm") })(v)
    : s.object({ mode: s.enumeration("after_grace"), grace_ms: duration })(v);
const timeout = s.object({ after_ms: duration, release });
export const policy = s.object({
  max_concurrency: s.nullable(s.integer(0, 1000000)),
  timeout: s.nullable(timeout),
});
export type QueuePolicy = s.Decoded<typeof policy>;
const record = s.object({
  scope,
  queue: queueName,
  revision,
  policy,
  updated_at: s.timestamp,
});
const snapshot = s.refine(
  s.object({ record, occupied: s.decimal, uncertain: s.decimal }),
  (v) => BigInt(v.uncertain) <= BigInt(v.occupied),
  "Uncertain capacity exceeds occupied capacity.",
);
export type QueueSnapshot = s.Decoded<typeof snapshot>;
const hold = s.refine(
  s.object({
    scope,
    queue: queueName,
    task_id: queueName,
    attempt_id: queueName,
    worker_session_id: queueName,
    consumer_id: s.integer(),
    policy_revision: revision,
    grace_ms: s.nullable(duration),
    force_at: s.nullable(s.timestamp),
    revision,
    state: s.enumeration("reserved", "running", "uncertain", "released"),
    acquired_at: s.timestamp,
    deadline: s.nullable(s.timestamp),
    stop_requested_at: s.nullable(s.timestamp),
    released_at: s.nullable(s.timestamp),
    release_kind: s.nullable(
      s.enumeration("confirmed", "never_started", "operator", "timeout"),
    ),
  }),
  (v) =>
    (v.state === "released") === (v.released_at !== null) &&
    (v.state === "released") === (v.release_kind !== null) &&
    (v.stop_requested_at === null ||
      (v.deadline !== null && v.stop_requested_at >= v.deadline)) &&
    (v.released_at === null ||
      v.stop_requested_at === null ||
      v.released_at >= v.stop_requested_at) &&
    (v.grace_ms === null || v.deadline !== null) &&
    (v.force_at === null
      ? v.grace_ms === null || v.stop_requested_at === null
      : v.grace_ms !== null &&
        v.stop_requested_at !== null &&
        v.force_at === v.grace_ms + v.stop_requested_at) &&
    (v.release_kind !== "timeout" ||
      (v.force_at !== null &&
        v.released_at !== null &&
        v.released_at >= v.force_at)) &&
    [v.deadline, v.stop_requested_at, v.released_at].every(
      (t) => t === null || t >= v.acquired_at,
    ),
  "Inconsistent queue capacity hold.",
);
export type QueueHold = s.Decoded<typeof hold>;

function matches(actual: QueueScope, expected: QueueScope) {
  return (
    actual.namespace === expected.namespace &&
    actual.tenant_id === expected.tenant_id
  );
}
function assert(value: unknown, message: string): asserts value {
  if (!value) throw new ContractError(message);
}
export const inspectFor = (expected: QueueScope, queue: string) =>
  s.refine(
    snapshot,
    (v) => matches(v.record.scope, expected) && v.record.queue === queue,
    "Queue snapshot belongs to another queue or scope.",
  );
function precedes(a: string, b: string) {
  const left = new TextEncoder().encode(a),
    right = new TextEncoder().encode(b);
  for (let i = 0; i < Math.min(left.length, right.length); i++)
    if (left[i] !== right[i]) return left[i]! < right[i]!;
  return left.length < right.length;
}
function validPage(keys: string[], after: string | null, next: string | null) {
  return (
    keys.every((key, i) => {
      const previous = i ? keys[i - 1] : after;
      return !previous || precedes(previous, key);
    }) &&
    (next === null || next === keys.at(-1))
  );
}
export function policiesFor(
  expected: QueueScope,
  limit: number,
  after: string | null,
) {
  return s.refine(
    s.object({ items: s.array(snapshot, limit), next: s.nullable(s.string) }),
    (p) =>
      p.items.every((i) => matches(i.record.scope, expected)) &&
      validPage(
        p.items.map((i) => i.record.queue),
        after,
        p.next,
      ),
    "Unexpected queue policy page.",
  );
}
export function holdsFor(
  expected: QueueScope,
  queue: string,
  limit: number,
  includeReleased: boolean,
  after: string | null,
) {
  return s.refine(
    s.object({ items: s.array(hold, limit), next: s.nullable(s.string) }),
    (p) =>
      p.items.every(
        (i) =>
          matches(i.scope, expected) &&
          i.queue === queue &&
          (includeReleased || i.state !== "released"),
      ) &&
      validPage(
        p.items.map((i) => i.attempt_id),
        after,
        p.next,
      ),
    "Unexpected queue holds page.",
  );
}

const reason = s.refine(
  s.string,
  (v) => v.trim().length > 0 && validText(v, 1024),
  "Enter a reason of up to 1,024 bytes without control characters.",
);
export const put = s.object({
  queue: queueName,
  request_id: queueName,
  expected_revision: s.nullable(revision),
  policy,
  reason,
});
export type QueuePut = s.Decoded<typeof put>;
export const releaseHold = s.object({
  queue: queueName,
  task_id: queueName,
  attempt_id: queueName,
  expected_revision: revision,
  request_id: queueName,
  reason,
});
export type QueueRelease = s.Decoded<typeof releaseHold>;
export function putReceiptFor(
  raw: unknown,
  expected: QueueScope,
  cmd: QueuePut,
) {
  const value = s.object({ record, already_accepted: s.boolean })(raw);
  assert(
    matches(value.record.scope, expected) &&
      value.record.queue === cmd.queue &&
      BigInt(value.record.revision) ===
        BigInt(cmd.expected_revision ?? "0") + 1n &&
      JSON.stringify(value.record.policy) === JSON.stringify(cmd.policy),
    "Receipt does not match the submitted queue policy.",
  );
  return value;
}
export function releaseReceiptFor(
  raw: unknown,
  expected: QueueScope,
  cmd: QueueRelease,
) {
  const value = s.object({ hold, already_accepted: s.boolean })(raw);
  const h = value.hold;
  assert(
    matches(h.scope, expected) &&
      h.queue === cmd.queue &&
      h.task_id === cmd.task_id &&
      h.attempt_id === cmd.attempt_id &&
      h.state === "released" &&
      h.release_kind === "operator" &&
      BigInt(h.revision) === BigInt(cmd.expected_revision) + 1n,
    "Receipt does not match the selected queue hold.",
  );
  return value;
}
