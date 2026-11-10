import { afterEach, describe, expect, it, vi } from "vitest";
import {
  appendProgress,
  decodeProgress,
  emptyProgressWindow,
} from "../../src/api/progress";
import { ProgressFeed } from "../../src/api/progress-feed";
import { ApiError } from "../../src/api/errors";
import { requiredCapabilities } from "../../src/api/access-policy";
import {
  cursorPosition,
  progressCursor,
  progressPage,
  progressQuery,
  taskTarget,
} from "../progress-fixture";
import type { ProgressPage, ProgressRequest } from "../../src/api/progress";

afterEach(() => vi.useRealTimers());
describe("durable progress wire", () => {
  it("keeps the full u64 range lossless and binds the consumed position", () => {
    const query = progressQuery("18446744073709551614");
    const page = progressPage(query, ["cleanup_confirmed"], "succeeded");
    expect(decodeProgress(page, query).events[0]?.sequence).toBe(
      "18446744073709551615",
    );
    expect(() =>
      decodeProgress(
        { ...page, high_watermark: "18446744073709551616" },
        query,
      ),
    ).toThrow();
  });
  it("rejects gaps, wrong scope, changed kind, malformed cursors and terminal contradictions", () => {
    const query = progressQuery();
    const page = progressPage(query);
    for (const invalid of [
      { ...page, events: [{ ...page.events[0], sequence: "2" }] },
      { ...page, target: { kind: "workflow", id: taskTarget.id } },
      { ...page, terminal: true },
      { ...page, events: [{ ...page.events[0], attempt_id: "bad\u0080" }] },
      { ...page, events: [{ ...page.events[0], attempt_id: "bad\ud800" }] },
      { ...page, next_cursor: progressCursor("2") },
      { ...page, next_cursor: page.next_cursor.toUpperCase() },
      {
        ...page,
        events: [{ ...page.events[0], activation_id: "activation_other" }],
      },
      { ...page, events: [{ ...page.events[0], reason: "unknown" }] },
      { ...page, events: [] },
      { ...page, has_more: true },
    ])
      expect(() => decodeProgress(invalid, query)).toThrow();
    const resumed = progressQuery("1");
    const empty = progressPage(resumed, []);
    expect(() =>
      decodeProgress(
        { ...empty, scope: { tenant_id: "other", namespace: "demo" } },
        resumed,
      ),
    ).toThrow();
  });
  it("accepts only metadata-read permission for the Console POST", () => {
    expect(
      requiredCapabilities("/v1/console/executions/progress", true),
    ).toEqual(["metadata_read"]);
  });
  it("deduplicates replay, rejects changed facts and bounds the view without losing its cursor", () => {
    let window = emptyProgressWindow();
    let query = progressQuery();
    for (let batch = 0; batch < 6; batch++) {
      const page = decodeProgress(
        progressPage(query, Array(100).fill("claimed")),
        query,
      );
      window = appendProgress(window, page);
      query = { ...query, cursor: page.next_cursor };
    }
    expect(window.events).toHaveLength(500);
    expect(window.events[0]?.sequence).toBe("101");
    expect(window.omitted).toBe(true);
    expect(cursorPosition(window.page!.next_cursor)).toBe(600n);
    const replayed = window.page!;
    expect(appendProgress(window, replayed).events).toHaveLength(500);
    expect(() =>
      appendProgress(window, {
        ...replayed,
        events: replayed.events.map((event) => ({
          ...event,
          at: event.at + 1,
        })),
      }),
    ).toThrow();
    expect(() =>
      appendProgress(window, { ...replayed, high_watermark: "599" }),
    ).toThrow();
    expect(() =>
      appendProgress(window, {
        ...replayed,
        target: { ...taskTarget, id: "different" },
      }),
    ).toThrow();
  });
});

describe("bounded observer lifecycle", () => {
  it("resumes one request at a time and cancels the pending observation", async () => {
    vi.useFakeTimers();
    const calls: { query: ProgressRequest; signal: AbortSignal }[] = [];
    let resolve: ((page: ProgressPage) => void) | undefined;
    const feed = new ProgressFeed(taskTarget, async (query, signal) => {
      calls.push({ query, signal });
      if (calls.length === 1) return progressPage(query);
      return new Promise((done) => {
        resolve = done;
      });
    });
    feed.start();
    await vi.advanceTimersByTimeAsync(250);
    expect(calls).toHaveLength(2);
    expect(cursorPosition(calls[1]!.query.cursor)).toBe(1n);
    expect(calls[1]!.query.wait_ms).toBe(5000);
    await vi.advanceTimersByTimeAsync(10000);
    expect(calls).toHaveLength(2);
    feed.stop();
    expect(calls[1]!.signal.aborted).toBe(true);
    resolve!(progressPage(calls[1]!.query, ["claimed"]));
    await vi.advanceTimersByTimeAsync(0);
    expect(feed.getSnapshot().events).toHaveLength(1);
    expect(feed.getSnapshot().phase).toBe("paused");
    feed.start();
    expect(cursorPosition(calls[2]!.query.cursor)).toBe(1n);
    feed.stop();
  });
  it("throttles immediate empty replies and does not poll a terminal snapshot", async () => {
    vi.useFakeTimers();
    let calls = 0;
    const feed = new ProgressFeed(taskTarget, async (query) => {
      calls++;
      return progressPage(query, [], calls === 4 ? "succeeded" : "queued");
    });
    feed.start();
    await vi.advanceTimersByTimeAsync(749);
    expect(calls).toBe(3);
    await vi.advanceTimersByTimeAsync(1);
    expect(calls).toBe(4);
    expect(feed.getSnapshot().phase).toBe("complete");
    await vi.advanceTimersByTimeAsync(30000);
    expect(calls).toBe(4);
    feed.stop();
  });
  it("stops automatic catch-up after five pages and explicitly resumes at the accepted cursor", async () => {
    vi.useFakeTimers();
    const seen: bigint[] = [];
    const feed = new ProgressFeed(taskTarget, async (query) => {
      seen.push(cursorPosition(query.cursor));
      return progressPage(
        query,
        Array(100).fill("claimed"),
        "active",
        10000 - Number(cursorPosition(query.cursor)),
      );
    });
    feed.start();
    await vi.advanceTimersByTimeAsync(0);
    expect(feed.getSnapshot().phase).toBe("limited");
    expect(seen).toEqual([0n, 100n, 200n, 300n, 400n]);
    feed.start(true);
    await vi.advanceTimersByTimeAsync(0);
    expect(seen[5]).toBe(500n);
    expect(feed.getSnapshot().events).toHaveLength(500);
    feed.stop();
  });
  it("retries transient failures only twice, preserving the cursor for a manual retry", async () => {
    vi.useFakeTimers();
    const cursors: bigint[] = [];
    let fail = true;
    const feed = new ProgressFeed(taskTarget, async (query) => {
      cursors.push(cursorPosition(query.cursor));
      if (cursors.length === 1) return progressPage(query);
      if (fail) throw new ApiError("Disconnected");
      return progressPage(query, ["succeeded"], "succeeded");
    });
    feed.start();
    await vi.advanceTimersByTimeAsync(3500);
    expect(feed.getSnapshot().phase).toBe("error");
    expect(cursors).toEqual([0n, 1n, 1n, 1n]);
    await vi.advanceTimersByTimeAsync(60000);
    expect(cursors).toHaveLength(4);
    fail = false;
    feed.start(true);
    await vi.advanceTimersByTimeAsync(0);
    expect(cursors.at(-1)).toBe(1n);
    expect(feed.getSnapshot().events.map((event) => event.sequence)).toEqual([
      "1",
      "2",
    ]);
    expect(feed.getSnapshot().phase).toBe("complete");
    feed.stop();
  });
  it("does not reset or retry a retired execution or a malformed page", async () => {
    for (const error of [
      new ApiError("Retired", 404),
      new Error("Malformed"),
    ]) {
      const read = vi.fn(async () => {
        throw error;
      });
      const feed = new ProgressFeed(taskTarget, read);
      feed.start();
      await Promise.resolve();
      await Promise.resolve();
      expect(feed.getSnapshot().phase).toBe("error");
      feed.start();
      expect(read).toHaveBeenCalledOnce();
      feed.stop();
    }
  });
});
