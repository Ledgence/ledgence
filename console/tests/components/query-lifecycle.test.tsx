import { afterEach, it, expect, vi } from "vitest";
import { render } from "vitest-browser-react";
import { QueryClientProvider, focusManager } from "@tanstack/react-query";
import raw from "../../../crates/ledgence-orchestration-api/tests/fixtures/console-v3.json?raw";
import { parseUserJson } from "../../src/api/json";
import { decodeConfig } from "../../src/api/codecs";
import { object, string } from "../../src/api/schema";
import { useResource } from "../../src/api/hooks";
import { InstanceContext } from "../../src/app/instance";
import { createQueryClient } from "../../src/app/query-client";
const all = parseUserJson(raw, 2 * 1024 * 1024);
if (!all || typeof all !== "object" || !("config" in all))
  throw new Error("Missing Rust config.");
const config = decodeConfig(all.config);
const decoder = object({ id: string });
function reply(id: string) {
  return new Response(JSON.stringify({ id }), {
    headers: {
      "Content-Type": "application/json",
      "Ledgence-Console-Contract": "3",
      "Ledgence-Instance-Id": config.instance_id,
    },
  });
}
function Probe({
  queue,
  interval = false,
}: {
  queue: string;
  interval?: number | false;
}) {
  const q = useResource("tasks", { queue }, decoder, { interval });
  return (
    <>
      <p>{q.data?.id ?? "Loading"}</p>
      {q.error && <p role="alert">Unavailable</p>}
      <button onClick={() => void q.refetch()}>Refresh</button>
    </>
  );
}
afterEach(() => {
  vi.restoreAllMocks();
  focusManager.setFocused(undefined);
});
it("stops periodic and focus refetch after a permanent error", async () => {
  const client = createQueryClient();
  const fetcher = vi
    .spyOn(globalThis, "fetch")
    .mockResolvedValue(new Response("", { status: 404 }));
  const view = await render(
    <QueryClientProvider client={client}>
      <InstanceContext.Provider value={config}>
        <Probe queue="exact" interval={30} />
      </InstanceContext.Provider>
    </QueryClientProvider>,
  );
  await expect.element(view.getByRole("alert")).toBeVisible();
  await new Promise((resolve) => setTimeout(resolve, 150));
  focusManager.setFocused(false);
  focusManager.setFocused(true);
  await new Promise((resolve) => setTimeout(resolve, 100));
  expect(fetcher).toHaveBeenCalledOnce();
  client.clear();
});
it("aborts a replaced filter and ignores its late response", async () => {
  const client = createQueryClient();
  let resolveOld: ((response: Response) => void) | undefined;
  let oldSignal: AbortSignal | null | undefined;
  vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
    if (String(input).endsWith("queue=old")) {
      oldSignal = init?.signal;
      return new Promise<Response>((resolve) => {
        resolveOld = resolve;
      });
    }
    return reply("new filter");
  });
  const wrap = (queue: string) => (
    <QueryClientProvider client={client}>
      <InstanceContext.Provider value={config}>
        <Probe queue={queue} />
      </InstanceContext.Provider>
    </QueryClientProvider>
  );
  const view = await render(wrap("old"));
  await expect.poll(() => !!resolveOld).toBe(true);
  await view.rerender(wrap("new"));
  await expect
    .element(view.getByText("new filter", { exact: true }))
    .toBeVisible();
  expect(oldSignal?.aborted).toBe(true);
  resolveOld?.(reply("obsolete filter"));
  await new Promise((resolve) => setTimeout(resolve, 50));
  await expect
    .element(view.getByText("new filter", { exact: true }))
    .toBeVisible();
  await expect
    .element(view.getByText("obsolete filter", { exact: true }))
    .not.toBeInTheDocument();
  client.clear();
});
it("pauses interval reads while hidden and revalidates on focus", async () => {
  const client = createQueryClient();
  const fetcher = vi
    .spyOn(globalThis, "fetch")
    .mockImplementation(async () => reply("ready"));
  const view = await render(
    <QueryClientProvider client={client}>
      <InstanceContext.Provider value={config}>
        <Probe queue="visible" interval={50} />
      </InstanceContext.Provider>
    </QueryClientProvider>,
  );
  await expect.element(view.getByText("ready", { exact: true })).toBeVisible();
  focusManager.setFocused(false);
  const count = fetcher.mock.calls.length;
  await new Promise((resolve) => setTimeout(resolve, 180));
  expect(fetcher).toHaveBeenCalledTimes(count);
  focusManager.setFocused(true);
  await expect.poll(() => fetcher.mock.calls.length).toBeGreaterThan(count);
  client.clear();
});
