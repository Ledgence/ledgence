import type {
  ProgressPage,
  ProgressRequest,
  ProgressTarget,
} from "../src/api/progress";
export const progressScope = { tenant_id: "acme", namespace: "demo" };
export const taskTarget: ProgressTarget = { kind: "task", id: "task_progress" };
export function progressCursor(
  after: string,
  target = taskTarget,
  scope = progressScope,
): string {
  return Array.from(
    new TextEncoder().encode(
      JSON.stringify({ version: 1, scope, target, after }),
    ),
    (byte) => byte.toString(16).padStart(2, "0"),
  ).join("");
}
export function cursorPosition(value: string | null): bigint {
  if (value === null) return 0n;
  const bytes = Uint8Array.from(value.match(/../g)!, (pair) =>
    parseInt(pair, 16),
  );
  return BigInt(JSON.parse(new TextDecoder().decode(bytes)).after);
}
export function progressQuery(
  after = "0",
  target = taskTarget,
): ProgressRequest {
  return {
    target,
    cursor: after === "0" ? null : progressCursor(after, target),
    limit: 100,
    wait_ms: 0,
  };
}
export function progressPage(
  query: ProgressRequest,
  reasons = ["submitted"],
  state = "queued",
  more = 0,
): ProgressPage {
  const start = cursorPosition(query.cursor);
  const end = start + BigInt(reasons.length);
  return {
    scope: progressScope,
    target: query.target,
    state,
    terminal: ["succeeded", "failed", "cancelled"].includes(state),
    high_watermark: String(end + BigInt(more)),
    events: reasons.map((reason, index) => ({
      sequence: String(start + BigInt(index + 1)),
      at: 1790409600000 + index,
      reason,
      activation_id: null,
      attempt_id: null,
    })),
    next_cursor: progressCursor(String(end), query.target),
    has_more: more > 0,
  };
}
