import { afterEach, expect, it, vi } from "vitest";
import { render } from "vitest-browser-react";
import {
  focusManager,
  onlineManager,
  QueryClientProvider,
  type QueryClient,
} from "@tanstack/react-query";
import fixtureSource from "../../../crates/ledgence-orchestration-api/tests/fixtures/console-v5.json?raw";
import { parseUserJson, stringifyUserJson } from "../../src/api/json";
import { decodeConfig } from "../../src/api/codecs";
import { object, string } from "../../src/api/schema";
import { useResource } from "../../src/api/hooks";
import { useTerminalRefresh } from "../../src/api/terminal-refresh";
import { InstanceContext } from "../../src/app/instance";
import { createQueryClient } from "../../src/app/query-client";
const fixture = parseUserJson(fixtureSource, 2 * 1024 * 1024);
if (!fixture || typeof fixture !== "object" || !("config" in fixture))
  throw new Error("Missing config");
const config = decodeConfig(fixture.config);
const clients: QueryClient[] = [];
afterEach(() => {
  for (const client of clients) client.clear();
  clients.length = 0;
  vi.restoreAllMocks();
  focusManager.setFocused(undefined);
  onlineManager.setOnline(true);
});
const decode = object({ id: string });
function Probe({
  active,
  id,
  cursor = null,
}: {
  active: boolean;
  id: string;
  cursor?: string | null;
}) {
  const query = useResource("tasks/attempts", { task_id: id, cursor }, decode);
  useTerminalRefresh(active, [id, cursor], query, !cursor);
  return (
    <>
      <p>{query.data?.id ?? "Loading"}</p>
      {query.error && <p role="alert">Unavailable</p>}
    </>
  );
}
function wrap(refetchOnWindowFocus = false) {
  const client = createQueryClient();
  client.setDefaultOptions({
    ...client.getDefaultOptions(),
    queries: {
      ...client.getDefaultOptions().queries,
      refetchOnWindowFocus,
      retryDelay: 1,
    },
  });
  clients.push(client);
  return (active: boolean, id = "first", cursor: string | null = null) => (
    <QueryClientProvider client={client}>
      <InstanceContext.Provider value={config}>
        <Probe active={active} id={id} cursor={cursor} />
      </InstanceContext.Provider>
    </QueryClientProvider>
  );
}
function response(value: unknown, status = 200) {
  return new Response(stringifyUserJson(value), {
    status,
    headers: {
      "Content-Type": "application/json",
      "Ledgence-Console-Contract": "5",
      "Ledgence-Instance-Id": config.instance_id,
    },
  });
}
for (const condition of ["hidden", "offline"] as const) {
  it(`defers its final observation while ${condition} and reads once after recovery`, async () => {
    let reads = 0;
    vi.spyOn(globalThis, "fetch").mockImplementation(async () =>
      response({ id: `read-${++reads}` }),
    );
    const tree = wrap(condition === "hidden");
    const view = await render(tree(true));
    await expect
      .element(view.getByText("read-1", { exact: true }))
      .toBeVisible();
    if (condition === "hidden") focusManager.setFocused(false);
    else onlineManager.setOnline(false);
    await view.rerender(tree(false));
    await new Promise((resolve) => setTimeout(resolve, 100));
    expect(reads).toBe(1);
    if (condition === "hidden") focusManager.setFocused(true);
    else onlineManager.setOnline(true);
    await expect
      .element(view.getByText("read-2", { exact: true }))
      .toBeVisible();
    await new Promise((resolve) => setTimeout(resolve, 100));
    expect(reads).toBe(2);
  });
}
for (const status of [404, 200]) {
  it(`does not restart a permanently failed read (${status === 404 ? "not found" : "contract error"})`, async () => {
    const fetcher = vi
      .spyOn(globalThis, "fetch")
      .mockImplementation(async () => response({}, status));
    const tree = wrap();
    const view = await render(tree(true));
    await expect.element(view.getByRole("alert")).toBeVisible();
    await view.rerender(tree(false));
    await new Promise((resolve) => setTimeout(resolve, 100));
    expect(fetcher).toHaveBeenCalledOnce();
  });
}
for (const changed of ["identity", "cursor"] as const) {
  it(`discards a deferred final refresh when the query ${changed} changes`, async () => {
    const reads: string[] = [];
    vi.spyOn(globalThis, "fetch").mockImplementation(async (input) => {
      const url = new URL(String(input), location.origin);
      const id = `${url.searchParams.get("task_id")}:${url.searchParams.get("cursor") ?? "live"}`;
      reads.push(id);
      return response({ id });
    });
    const tree = wrap();
    const view = await render(tree(true));
    await expect
      .element(view.getByText("first:live", { exact: true }))
      .toBeVisible();
    focusManager.setFocused(false);
    await view.rerender(tree(false));
    const id = changed === "identity" ? "second" : "first";
    const cursor = changed === "cursor" ? "older" : null;
    await view.rerender(tree(false, id, cursor));
    await expect
      .element(view.getByText(`${id}:${cursor ?? "live"}`, { exact: true }))
      .toBeVisible();
    focusManager.setFocused(true);
    await new Promise((resolve) => setTimeout(resolve, 100));
    expect(reads).toEqual(["first:live", `${id}:${cursor ?? "live"}`]);
  });
}
