// SPDX-License-Identifier: MIT
// Calendar controls use UTC. The server accepts [from, until) timestamps.
export const executionFilterKeys = [
  "kind",
  "state",
  "program_id",
  "version",
  "queue",
  "correlation_key",
  "execution_id",
  "submitted_from",
  "submitted_until",
] as const;
export const maximumTimestamp = 253402300799999;
const dayMs = 86400000;
const taskStates = ["queued", "active", "succeeded", "failed", "cancelled"];
const workflowStates = [
  "running",
  "waiting",
  "failing",
  "cancelling",
  "succeeded",
  "failed",
  "cancelled",
];

export function historyStates(kind: string): readonly string[] {
  return kind === "task"
    ? taskStates
    : kind === "workflow"
      ? workflowStates
      : [...taskStates.slice(0, 2), ...workflowStates];
}
export function compatibleHistoryState(kind: string, state: string): string {
  return historyStates(kind).includes(state) ? state : "";
}
function timestamp(value: string | null): number | null {
  if (value === null || !/^\d+$/.test(value)) return null;
  const number = Number(value);
  return Number.isSafeInteger(number) &&
    number >= 0 &&
    number <= maximumTimestamp
    ? number
    : null;
}
export function historyDateValue(
  params: URLSearchParams,
  key: "submitted_from" | "submitted_until",
): string {
  const number = timestamp(params.get(key));
  if (number === null || (key === "submitted_until" && number === 0)) return "";
  // An exclusive midnight belongs to the previous inclusive calendar day.
  return new Date(number - (key === "submitted_until" ? 1 : 0))
    .toISOString()
    .slice(0, 10);
}
export function hasExactHistoryDates(params: URLSearchParams): boolean {
  return ["submitted_from", "submitted_until"].some((key) => {
    const number = timestamp(params.get(key));
    return number !== null && number % dayMs !== 0;
  });
}
function utcDate(value: string): number | null {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) return null;
  const number = Date.parse(`${value}T00:00:00.000Z`);
  if (!Number.isSafeInteger(number) || number < 0 || number > maximumTimestamp)
    return null;
  // Date.parse can normalize an invalid date such as February 30.
  return new Date(number).toISOString().slice(0, 10) === value ? number : null;
}
export type HistoryFilterResult =
  { ok: true; params: URLSearchParams } | { ok: false; message: string };
export function applyHistoryFilters(
  previous: URLSearchParams,
  form: FormData,
): HistoryFilterResult {
  const next = new URLSearchParams();
  if (previous.has("limit")) next.set("limit", previous.get("limit")!);
  const kind = String(form.get("kind") ?? "");
  if (kind === "task" || kind === "workflow") next.set("kind", kind);
  const state = compatibleHistoryState(kind, String(form.get("state") ?? ""));
  if (state) next.set("state", state);
  for (const key of ["program_id", "version", "queue", "execution_id"]) {
    const value = String(form.get(key) ?? "");
    if (value) next.set(key, value);
  }
  if (form.get("correlation_enabled"))
    next.set("correlation_key", String(form.get("correlation_key") ?? ""));
  if (form.get("include_children")) next.set("include_children", "true");
  for (const key of ["submitted_from", "submitted_until"] as const) {
    const value = String(form.get(key) ?? "");
    if (!value) continue;
    const start = utcDate(value);
    if (start === null)
      return {
        ok: false,
        message: "Choose valid UTC dates on or after January 1, 1970.",
      };
    // Keep timestamp-precise deep links when only another filter changes.
    const original = timestamp(previous.get(key));
    const at =
      original !== null && value === historyDateValue(previous, key)
        ? original
        : start + (key === "submitted_until" ? dayMs : 0);
    if (at > maximumTimestamp)
      return {
        ok: false,
        message: "The latest supported through date is December 30, 9999.",
      };
    next.set(key, String(at));
  }
  if (
    next.has("submitted_from") &&
    next.has("submitted_until") &&
    Number(next.get("submitted_from")) >= Number(next.get("submitted_until"))
  ) {
    return {
      ok: false,
      message: "Submitted through must be on or after submitted from.",
    };
  }
  // New query: cursors and their predecessors never survive filter changes.
  return { ok: true, params: next };
}
export function historyElapsed(
  item: { state: string; submitted_at: number; terminal_at: number | null },
  observedAt: number,
): number | null {
  const terminal = ["succeeded", "failed", "cancelled"].includes(item.state);
  const end = item.terminal_at ?? (terminal ? null : observedAt);
  return end === null || end < item.submitted_at
    ? null
    : end - item.submitted_at;
}
export function formatHistoryElapsed(ms: number): string {
  const seconds = Math.floor(ms / 1000);
  if (ms < 1000) return `${ms} ms`;
  if (seconds < 60) return `${seconds}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${seconds % 60}s`;
  if (seconds < 86400)
    return `${Math.floor(seconds / 3600)}h ${Math.floor((seconds % 3600) / 60)}m`;
  return `${Math.floor(seconds / 86400)}d ${Math.floor((seconds % 86400) / 3600)}h`;
}
