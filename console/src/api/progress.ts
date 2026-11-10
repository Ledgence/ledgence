// SPDX-License-Identifier: MIT
import { ContractError } from "./codecs";
import { request } from "./client";
import { parseUserJson } from "./json";
import * as s from "./schema";
import { taskState, workflowState, terminal } from "./resources";

export const progressPageMaximumBytes = 256 * 1024;
export const progressWindowLimit = 500;
const queryMaximumBytes = 8192;
const cursorMaximumBytes = 4096;
function text(value: unknown): string {
  const result = s.id(value);
  if (
    new TextEncoder().encode(result).length > 128 ||
    [...result].some((character) => {
      const point = character.codePointAt(0)!;
      return (
        /\p{Cc}/u.test(character) ||
        (point >= 0xd800 && point <= 0xdfff) ||
        (point >= 0xfdd0 && point <= 0xfdef) ||
        (point & 0xfffe) === 0xfffe
      );
    })
  )
    throw new ContractError("Progress identifier exceeds its limit.");
  return result;
}
export const progressTarget = s.object({
  kind: s.enumeration("task", "workflow"),
  id: text,
});
export type ProgressTarget = s.Decoded<typeof progressTarget>;
const progressScope = s.object({ tenant_id: text, namespace: text });
type ProgressScope = s.Decoded<typeof progressScope>;
const event = s.object({
  sequence: s.decimal,
  at: s.timestamp,
  reason: text,
  activation_id: s.nullable(text),
  attempt_id: s.nullable(text),
});
export type ProgressEvent = s.Decoded<typeof event>;
const page = s.object({
  scope: progressScope,
  target: progressTarget,
  state: s.string,
  terminal: s.boolean,
  high_watermark: s.decimal,
  events: s.array(event),
  next_cursor: s.string,
  has_more: s.boolean,
});
export type ProgressPage = s.Decoded<typeof page>;
export type ProgressRequest = {
  target: ProgressTarget;
  cursor: string | null;
  limit: number;
  wait_ms: number;
};
const taskReasons = new Set([
  "admission_deferred",
  "admission_stopped",
  "submitted",
  "claimed",
  "dispatch_authorized",
  "cancel_requested",
  "cancelled",
  "report_accepted",
  "cleanup_confirmed",
  "succeeded",
  "failed",
  "retry_scheduled",
  "lease_expired",
]);
const sameTarget = (a: ProgressTarget, b: ProgressTarget) =>
  a.kind === b.kind && a.id === b.id;
const sameScope = (a: ProgressScope, b: ProgressScope) =>
  a.tenant_id === b.tenant_id && a.namespace === b.namespace;

/** Cursors remain opaque to callers; validation prevents accepting another feed. */
function cursor(value: string) {
  if (value.length > cursorMaximumBytes || !/^(?:[0-9a-f]{2})+$/.test(value))
    throw new ContractError("Invalid progress cursor.");
  const bytes = Uint8Array.from(value.match(/../g)!, (pair) =>
    parseInt(pair, 16),
  );
  let decoded: unknown;
  try {
    decoded = parseUserJson(
      new TextDecoder("utf-8", { fatal: true }).decode(bytes),
      cursorMaximumBytes / 2,
    );
  } catch {
    throw new ContractError("Invalid progress cursor.");
  }
  const result = s.object({
    version: s.integer(1, 1),
    scope: progressScope,
    target: progressTarget,
    after: s.decimal,
  })(decoded);
  const canonical = Array.from(
    new TextEncoder().encode(JSON.stringify(result)),
    (byte) => byte.toString(16).padStart(2, "0"),
  ).join("");
  if (canonical !== value)
    throw new ContractError("Noncanonical progress cursor.");
  return result;
}
export function decodeProgress(
  raw: unknown,
  query: ProgressRequest,
): ProgressPage {
  const result = page(raw);
  const previous = query.cursor === null ? null : cursor(query.cursor);
  if (
    !sameTarget(result.target, query.target) ||
    (previous &&
      (!sameTarget(previous.target, result.target) ||
        !sameScope(previous.scope, result.scope)))
  )
    throw new ContractError("Progress belongs to another execution.");
  const state =
    result.target.kind === "task"
      ? taskState(result.state)
      : workflowState(result.state);
  if (terminal(state) !== result.terminal || result.events.length > query.limit)
    throw new ContractError("Inconsistent progress snapshot.");
  let position = BigInt(previous?.after ?? "0");
  const watermark = BigInt(result.high_watermark);
  if (position > watermark)
    throw new ContractError("Progress precedes its cursor.");
  for (const item of result.events) {
    if (
      BigInt(item.sequence) !== position + 1n ||
      BigInt(item.sequence) > watermark
    )
      throw new ContractError("Progress contains a gap or invalid order.");
    if (
      result.target.kind === "task"
        ? item.activation_id !== null || !taskReasons.has(item.reason)
        : item.attempt_id !== null
    )
      throw new ContractError("Invalid progress event identity.");
    position = BigInt(item.sequence);
  }
  const next = cursor(result.next_cursor);
  if (
    !sameTarget(next.target, result.target) ||
    !sameScope(next.scope, result.scope) ||
    BigInt(next.after) !== position ||
    result.has_more !== position < watermark ||
    (!result.events.length && result.has_more)
  )
    throw new ContractError("Inconsistent progress continuation.");
  return result;
}
export async function readProgress(
  instanceId: string,
  query: ProgressRequest,
  signal: AbortSignal,
): Promise<ProgressPage> {
  const { value } = await request(
    "/v1/console/executions/progress",
    (raw) => decodeProgress(raw, query),
    signal,
    instanceId,
    { body: JSON.stringify(query), maximumBytes: queryMaximumBytes },
    progressPageMaximumBytes,
  );
  return value;
}

export type ProgressWindow = {
  page: ProgressPage | null;
  events: readonly ProgressEvent[];
  omitted: boolean;
};
export const emptyProgressWindow = (): ProgressWindow => ({
  page: null,
  events: [],
  omitted: false,
});

/** Bounded accumulation; already accepted immutable event identities are not duplicated. */
export function appendProgress(
  current: ProgressWindow,
  next: ProgressPage,
): ProgressWindow {
  const before = current.page;
  if (
    before &&
    (!sameScope(before.scope, next.scope) ||
      !sameTarget(before.target, next.target) ||
      BigInt(next.high_watermark) < BigInt(before.high_watermark) ||
      (before.terminal && next.state !== before.state))
  )
    throw new ContractError("Progress regressed or changed execution.");
  const through = BigInt(before ? cursor(before.next_cursor).after : "0");
  const retained = new Map(current.events.map((item) => [item.sequence, item]));
  const fresh: ProgressEvent[] = [];
  let expected = through + 1n;
  for (const item of next.events) {
    if (BigInt(item.sequence) <= through) {
      const saved = retained.get(item.sequence);
      if (saved && JSON.stringify(saved) !== JSON.stringify(item))
        throw new ContractError("A committed progress event changed.");
    } else {
      if (BigInt(item.sequence) !== expected)
        throw new ContractError("Progress skipped an unobserved event.");
      fresh.push(item);
      expected++;
    }
  }
  // A fully replayed page cannot replace a newer accepted cursor or state.
  if (before && BigInt(cursor(next.next_cursor).after) < through)
    return current;
  const all = [...current.events, ...fresh];
  return {
    page: next,
    events: all.slice(-progressWindowLimit),
    omitted: current.omitted || all.length > progressWindowLimit,
  };
}
