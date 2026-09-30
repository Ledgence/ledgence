// SPDX-License-Identifier: MIT
import { afterEach, expect, it, vi } from "vitest";
import { InfiniteQueryObserver, type QueryClient } from "@tanstack/react-query";
import raw from "../../../crates/ledgence-orchestration-api/tests/fixtures/console-v4.json?raw";
import { decodeConfig } from "../../src/api/codecs";
import { executionPage } from "../../src/api/explorer";
import { parseUserJson } from "../../src/api/json";
import { createQueryClient } from "../../src/app/query-client";
import {
  executionHistoryOptions,
  executionHistoryRows,
} from "../../src/features/use-execution-history";

const fixture = parseUserJson(raw, 2 * 1024 * 1024) as Record<string, unknown>;
const config = decodeConfig(fixture.config);
const canonical = executionPage(fixture.execution_history);
const origin = "http://console.test";
const clients: QueryClient[] = [];
const subscriptions: (() => void)[] = [];
function client() {
  const value = createQueryClient();
  value.setDefaultOptions({
    ...value.getDefaultOptions(),
    queries: { ...value.getDefaultOptions().queries, retry: false },
  });
  clients.push(value);
  return value;
}
function page(
  index: number,
  next_cursor: string | null,
  observed_at = 2000000000000,
) {
  return { items: [canonical.items[index]!], next_cursor, observed_at };
}
function reply(value: unknown, instance = config.instance_id) {
  return new Response(JSON.stringify(value), {
    headers: {
      "Content-Type": "application/json",
      "Ledgence-Console-Contract": "4",
      "Ledgence-Instance-Id": instance,
      "Request-Id": "req-history",
    },
  });
}
function observe(
  value: QueryClient,
  parameters: Record<string, string | number | null | undefined> = {
    limit: 50,
  },
) {
  const observer = new InfiniteQueryObserver(
    value,
    executionHistoryOptions(config, parameters, true, value, origin),
  );
  const unsubscribe = observer.subscribe(() => undefined);
  subscriptions.push(unsubscribe);
  return { observer, unsubscribe };
}
afterEach(() => {
  for (const unsubscribe of subscriptions.splice(0)) unsubscribe();
  for (const value of clients.splice(0)) value.clear();
  vi.unstubAllGlobals();
});

it("keeps accumulated pages through invalidation and remount, while allowing another page", async () => {
  const fetch = vi
    .fn()
    .mockResolvedValueOnce(reply(page(0, "cursor-a")))
    .mockResolvedValueOnce(reply(page(1, "cursor-b")))
    .mockResolvedValueOnce(reply(page(2, null)));
  vi.stubGlobal("fetch", fetch);
  const value = client();
  const first = observe(value);
  await vi.waitFor(() =>
    expect(first.observer.getCurrentResult().isSuccess).toBe(true),
  );
  await first.observer.fetchNextPage();
  expect(first.observer.getCurrentResult().data?.pages).toHaveLength(2);
  expect(first.observer.getCurrentResult().isEnabled).toBe(false);
  await value.invalidateQueries();
  expect(fetch).toHaveBeenCalledTimes(2);
  first.unsubscribe();
  const restored = observe(value);
  expect(restored.observer.getCurrentResult().data?.pages).toHaveLength(2);
  expect(restored.observer.getCurrentResult().isFetching).toBe(false);
  await restored.observer.fetchNextPage();
  expect(fetch).toHaveBeenCalledTimes(3);
  expect(fetch.mock.calls[2]?.[0]).toContain("cursor=cursor-b");
  expect(restored.observer.getCurrentResult().data?.pages).toHaveLength(3);
  expect(restored.observer.getCurrentResult().hasNextPage).toBe(false);
});
it("retains the first page after next-page failure and only retries on request", async () => {
  const fetch = vi
    .fn()
    .mockResolvedValueOnce(reply(page(0, "cursor-a")))
    .mockResolvedValueOnce(new Response("", { status: 503 }))
    .mockResolvedValueOnce(reply(page(1, null)));
  vi.stubGlobal("fetch", fetch);
  const value = client();
  const { observer } = observe(value);
  await vi.waitFor(() =>
    expect(observer.getCurrentResult().isSuccess).toBe(true),
  );
  await observer.fetchNextPage();
  expect(observer.getCurrentResult().isFetchNextPageError).toBe(true);
  expect(observer.getCurrentResult().data?.pages).toHaveLength(1);
  expect(observer.getCurrentResult().isEnabled).toBe(false);
  await value.invalidateQueries();
  expect(fetch).toHaveBeenCalledTimes(2);
  await observer.fetchNextPage();
  expect(observer.getCurrentResult().data?.pages).toHaveLength(2);
  expect(observer.getCurrentResult().isFetchNextPageError).toBe(false);
});
it.each(["cursor-b", "cursor-a"])(
  "rejects a cyclic continuation %s before caching the page",
  async (next) => {
    const fetch = vi
      .fn()
      .mockResolvedValueOnce(reply(page(0, "cursor-a")))
      .mockResolvedValueOnce(reply(page(1, "cursor-b")))
      .mockResolvedValueOnce(reply(page(2, next)));
    vi.stubGlobal("fetch", fetch);
    const value = client();
    const { observer } = observe(value);
    await vi.waitFor(() =>
      expect(observer.getCurrentResult().isSuccess).toBe(true),
    );
    await observer.fetchNextPage();
    await observer.fetchNextPage();
    expect(observer.getCurrentResult().error).toMatchObject({
      name: "ContractError",
      requestId: "req-history",
      message: "The server returned a non-advancing execution cursor.",
    });
    expect(observer.getCurrentResult().data?.pages).toHaveLength(2);
    await value.invalidateQueries();
    expect(fetch).toHaveBeenCalledTimes(3);
  },
);
it("keeps a legacy cursor as an initial anchor without pretending it is the live first page", async () => {
  const fetch = vi.fn().mockResolvedValueOnce(reply(page(0, null)));
  vi.stubGlobal("fetch", fetch);
  const value = client();
  const { observer } = observe(value, { limit: 50, cursor: "legacy-anchor" });
  await vi.waitFor(() =>
    expect(observer.getCurrentResult().isSuccess).toBe(true),
  );
  expect(fetch.mock.calls[0]?.[0]).toContain("cursor=legacy-anchor");
  expect(observer.getCurrentResult().data?.pageParams).toEqual([
    "legacy-anchor",
  ]);
  expect(observer.getCurrentResult().isEnabled).toBe(false);
});
it("rejects cross-instance replies without an automatic fatal-error loop", async () => {
  const fetch = vi
    .fn()
    .mockResolvedValueOnce(reply(page(0, null), "other-instance"));
  vi.stubGlobal("fetch", fetch);
  const value = client();
  const { observer } = observe(value);
  await vi.waitFor(() =>
    expect(observer.getCurrentResult().isError).toBe(true),
  );
  expect(observer.getCurrentResult().error).toMatchObject({
    name: "ContractError",
    requestId: "req-history",
  });
  expect(observer.getCurrentResult().isEnabled).toBe(false);
  await value.invalidateQueries();
  expect(fetch).toHaveBeenCalledTimes(1);
});
it("normalizes filters, isolates instance/anchor keys, and preserves row observation times", () => {
  const value = client();
  const options = executionHistoryOptions(
    config,
    { state: "running", limit: 50 },
    true,
    value,
    origin,
  );
  expect(options.queryKey).toEqual(
    executionHistoryOptions(
      config,
      { limit: 50, state: "running", cursor: null },
      true,
      value,
      origin,
    ).queryKey,
  );
  expect(options.queryKey).not.toEqual(
    executionHistoryOptions(
      config,
      { limit: 50, state: "running", cursor: "anchor" },
      true,
      value,
      origin,
    ).queryKey,
  );
  expect(options.queryKey).not.toEqual(
    executionHistoryOptions(
      { ...config, instance_id: "other" },
      { state: "running", limit: 50 },
      true,
      value,
      origin,
    ).queryKey,
  );
  const older = page(0, "cursor-a", 2000000000000);
  const newer = page(1, null, 2000000005000);
  newer.items.push(older.items[0]!);
  const result = executionHistoryRows({
    pages: [older, newer],
    pageParams: [null, "cursor-a"],
  });
  expect(result.rows.map(({ item }) => item.id)).toEqual([
    canonical.items[0]!.id,
    canonical.items[1]!.id,
  ]);
  expect(result.rows.map(({ observedAt }) => observedAt)).toEqual([
    2000000000000, 2000000005000,
  ]);
  expect(result.observedAt).toBe(2000000000000);
});

it("restores even a single cached page without refetching it on mount", async () => {
  const fetch = vi.fn().mockResolvedValueOnce(reply(page(0, "cursor-a")));
  vi.stubGlobal("fetch", fetch);
  const value = client();
  const first = observe(value);
  await vi.waitFor(() =>
    expect(first.observer.getCurrentResult().isSuccess).toBe(true),
  );
  first.unsubscribe();
  const restored = observe(value);
  expect(restored.observer.getCurrentResult().data?.pages).toHaveLength(1);
  expect(restored.observer.getCurrentResult().isFetching).toBe(false);
  expect(fetch).toHaveBeenCalledTimes(1);
});
it("follows an advancing continuation even when its page contains no rows", async () => {
  const fetch = vi
    .fn()
    .mockResolvedValueOnce(
      reply({ items: [], next_cursor: "cursor-a", observed_at: 2000000000000 }),
    )
    .mockResolvedValueOnce(reply(page(0, null)));
  vi.stubGlobal("fetch", fetch);
  const value = client();
  const { observer } = observe(value);
  await vi.waitFor(() =>
    expect(observer.getCurrentResult().isSuccess).toBe(true),
  );
  expect(observer.getCurrentResult().hasNextPage).toBe(true);
  await observer.fetchNextPage();
  expect(observer.getCurrentResult().data?.pages).toHaveLength(2);
  expect(
    executionHistoryRows(observer.getCurrentResult().data).rows,
  ).toHaveLength(1);
});
