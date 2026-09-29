import { afterEach, it, expect, vi } from "vitest";
import { render } from "vitest-browser-react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import raw from "../../../crates/ledgence-orchestration-api/tests/fixtures/console-v3.json?raw";
import { parseUserJson } from "../../src/api/json";
import { decodeConfig } from "../../src/api/codecs";
import { useCommand, freezeCommand } from "../../src/api/commands";
import { object, string, type Decoder } from "../../src/api/schema";
import { InstanceContext } from "../../src/app/instance";
const all = parseUserJson(raw, 2 * 1024 * 1024);
if (!all || typeof all !== "object" || !("config" in all))
  throw new Error("Rust config missing.");
const config = decodeConfig(all.config);
afterEach(() => vi.restoreAllMocks());
function Probe({
  resource,
  decode,
  onSuccess,
}: {
  resource: string;
  decode: Decoder<{ id: string }>;
  onSuccess: (value: { id: string }) => void;
}) {
  const op = useCommand(resource, decode, onSuccess);
  return (
    <>
      <button
        onClick={() => {
          op.send(
            freezeCommand({ idempotency_key: "stable", data: 1 }, "stable"),
          );
          op.send(
            freezeCommand({ idempotency_key: "wrong", data: 2 }, "wrong"),
          );
        }}
      >
        Send twice
      </button>
      <button onClick={op.retry}>Retry</button>
      {op.mutation.error && <p role="alert">Unknown result</p>}
      {op.mutation.data && <p>Success {op.mutation.data.id}</p>}
    </>
  );
}
it("freezes destination, instance, decoder, callback and bytes across rerenders and guards double send", async () => {
  const client = new QueryClient({
    defaultOptions: { mutations: { retry: false } },
  });
  const calls: { path: string; body: unknown }[] = [];
  vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
    calls.push({ path: String(input), body: init?.body });
    return calls.length === 1
      ? new Response("", { status: 503 })
      : new Response('{"id":"accepted"}', {
          headers: {
            "Content-Type": "application/json",
            "Ledgence-Console-Contract": "3",
            "Ledgence-Instance-Id": config.instance_id,
          },
        });
  });
  const first = vi.fn();
  const later = vi.fn();
  const decode = object({ id: string });
  const wrap = (
    resource: string,
    decoder: Decoder<{ id: string }>,
    callback: (v: { id: string }) => void,
    instance = config,
  ) => (
    <QueryClientProvider client={client}>
      <InstanceContext.Provider value={instance}>
        <Probe resource={resource} decode={decoder} onSuccess={callback} />
      </InstanceContext.Provider>
    </QueryClientProvider>
  );
  const view = await render(wrap("tasks", decode, first));
  await view.getByRole("button", { name: "Send twice" }).click();
  await expect.element(view.getByRole("alert")).toBeVisible();
  expect(calls).toHaveLength(1);
  await view.rerender(
    wrap(
      "workflows",
      () => {
        throw new Error("Changed decoder must not run");
      },
      later,
      { ...config, instance_id: "changed-instance" },
    ),
  );
  await view.getByRole("button", { name: "Retry" }).click();
  await expect.element(view.getByText("Success accepted")).toBeVisible();
  expect(calls).toEqual([
    {
      path: "/v1/console/tasks",
      body: '{"idempotency_key":"stable","data":1}',
    },
    {
      path: "/v1/console/tasks",
      body: '{"idempotency_key":"stable","data":1}',
    },
  ]);
  expect(first).toHaveBeenCalledOnce();
  expect(later).not.toHaveBeenCalled();
  client.clear();
});
