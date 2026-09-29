// SPDX-License-Identifier: MIT
import { describe, expect, it } from "vitest";
import {
  applyHistoryFilters,
  compatibleHistoryState,
  formatHistoryElapsed,
  hasExactHistoryDates,
  historyDateValue,
  historyElapsed,
  historyStates,
} from "../../src/features/execution-history-filters";
function apply(fields: Record<string, string>, previous = "") {
  const form = new FormData();
  for (const [key, value] of Object.entries(fields)) form.set(key, value);
  return applyHistoryFilters(new URLSearchParams(previous), form);
}
function successful(fields: Record<string, string>, previous = "") {
  const result = apply(fields, previous);
  if (!result.ok) throw new Error(result.message);
  return result.params;
}
describe("inclusive UTC execution history dates", () => {
  it.each([
    ["2026-01-31", "2026-02-01"],
    ["2024-02-29", "2024-03-01"],
    ["2026-12-31", "2027-01-01"],
    ["2026-03-08", "2026-03-09"],
    ["2026-11-01", "2026-11-02"],
    ["1970-01-01", "1970-01-02"],
  ])(
    "includes every instant of %s using an exclusive %s boundary",
    (day, nextDay) => {
      const params = successful({ submitted_from: day, submitted_until: day });
      expect(params.get("submitted_from")).toBe(
        String(Date.parse(`${day}T00:00:00.000Z`)),
      );
      expect(params.get("submitted_until")).toBe(
        String(Date.parse(`${nextDay}T00:00:00.000Z`)),
      );
      expect(historyDateValue(params, "submitted_from")).toBe(day);
      expect(historyDateValue(params, "submitted_until")).toBe(day);
    },
  );
  it("supports an open-ended range and rejects inverted dates before querying", () => {
    expect(
      successful({ submitted_from: "2026-09-29" }).has("submitted_until"),
    ).toBe(false);
    expect(
      successful({ submitted_until: "2026-09-29" }).has("submitted_from"),
    ).toBe(false);
    expect(
      apply({ submitted_from: "2026-09-30", submitted_until: "2026-09-29" }),
    ).toEqual({
      ok: false,
      message: "Submitted through must be on or after submitted from.",
    });
  });
  it.each(["2026-02-30", "1969-12-31", "2026-13-01", "nonsense", "2026-2-01"])(
    "rejects invalid or unsupported date %s",
    (value) => {
      expect(apply({ submitted_from: value }).ok).toBe(false);
    },
  );
  it("does not overflow the server timestamp range", () => {
    expect(apply({ submitted_until: "9999-12-31" }).ok).toBe(false);
    expect(
      successful({ submitted_until: "9999-12-30" }).get("submitted_until"),
    ).toBe("253402214400000");
  });
  it("keeps exact deep-linked timestamps when another filter changes", () => {
    const from = String(Date.parse("2026-09-28T14:03:01.017Z"));
    const until = String(Date.parse("2026-09-29T06:55:00.020Z"));
    const previous = new URLSearchParams({
      submitted_from: from,
      submitted_until: until,
      cursor: "opaque_cursor",
    });
    expect(hasExactHistoryDates(previous)).toBe(true);
    const params = successful(
      {
        submitted_from: "2026-09-28",
        submitted_until: "2026-09-29",
        state: "waiting",
        kind: "workflow",
      },
      previous.toString(),
    );
    expect(params.get("submitted_from")).toBe(from);
    expect(params.get("submitted_until")).toBe(until);
    expect(params.get("state")).toBe("waiting");
    expect(params.has("cursor")).toBe(false);
  });
  it("replaces an edited exact boundary with UTC midnight", () => {
    const params = successful(
      { submitted_from: "2026-09-29" },
      `submitted_from=${Date.parse("2026-09-28T14:03:01Z")}`,
    );
    expect(params.get("submitted_from")).toBe(
      String(Date.parse("2026-09-29T00:00:00Z")),
    );
  });
  it.each([
    "",
    "-1",
    "Infinity",
    "1.5",
    "253402300800000",
    "1000000000000000000000000",
  ])("does not render invalid incoming timestamp %s", (value) => {
    expect(
      historyDateValue(
        new URLSearchParams({ submitted_from: value }),
        "submitted_from",
      ),
    ).toBe("");
  });
});
describe("execution query semantics", () => {
  it("preserves all server states and clears only incompatible ones", () => {
    expect(historyStates("")).toEqual([
      "queued",
      "active",
      "running",
      "waiting",
      "failing",
      "cancelling",
      "succeeded",
      "failed",
      "cancelled",
    ]);
    expect(compatibleHistoryState("workflow", "active")).toBe("");
    expect(compatibleHistoryState("task", "waiting")).toBe("");
    expect(compatibleHistoryState("workflow", "failing")).toBe("failing");
    expect(compatibleHistoryState("task", "failed")).toBe("failed");
    expect(compatibleHistoryState("", "cancelling")).toBe("cancelling");
  });
  it("resets cursors, preserves row limit and exact opaque filters", () => {
    const params = successful(
      {
        kind: "task",
        state: "waiting",
        program_id: "program / one",
        version: "01.0+opaque",
        queue: "queue/one",
        execution_id: "e?opaque",
        correlation_enabled: "on",
        correlation_key: "",
        include_children: "on",
      },
      "limit=25&cursor=opaque&previous=older&tab=Overview",
    );
    expect(Object.fromEntries(params)).toEqual({
      limit: "25",
      kind: "task",
      program_id: "program / one",
      version: "01.0+opaque",
      queue: "queue/one",
      execution_id: "e?opaque",
      correlation_key: "",
      include_children: "true",
    });
  });
  it("does not silently enable the empty correlation filter", () => {
    expect(successful({ correlation_key: "" }).has("correlation_key")).toBe(
      false,
    );
    expect(
      successful({ correlation_key: "", correlation_enabled: "on" }).get(
        "correlation_key",
      ),
    ).toBe("");
  });
});
describe("elapsed observation, including waiting time", () => {
  it("uses the observation for live work and the terminal timestamp for completed work", () => {
    expect(
      historyElapsed(
        { state: "waiting", submitted_at: 1000, terminal_at: null },
        301000,
      ),
    ).toBe(300000);
    expect(
      historyElapsed(
        { state: "succeeded", submitted_at: 1000, terminal_at: 2000 },
        301000,
      ),
    ).toBe(1000);
  });
  it("does not turn absent or inconsistent terminal timing into zero", () => {
    expect(
      historyElapsed(
        { state: "failed", submitted_at: 1000, terminal_at: null },
        301000,
      ),
    ).toBe(null);
    expect(
      historyElapsed(
        { state: "queued", submitted_at: 1000, terminal_at: null },
        999,
      ),
    ).toBe(null);
    expect(
      historyElapsed(
        { state: "succeeded", submitted_at: 1000, terminal_at: 1000 },
        2000,
      ),
    ).toBe(0);
    expect(formatHistoryElapsed(0)).toBe("0 ms");
  });
  it("formats long elapsed time without pretending it is execution CPU time", () => {
    expect(formatHistoryElapsed(1500)).toBe("1s");
    expect(formatHistoryElapsed(301000)).toBe("5m 1s");
    expect(formatHistoryElapsed(9000000)).toBe("2h 30m");
    expect(formatHistoryElapsed(180000000)).toBe("2d 2h");
  });
});
