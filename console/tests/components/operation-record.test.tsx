// SPDX-License-Identifier: MIT
import { afterEach, expect, it, vi } from "vitest";
import { render } from "vitest-browser-react";
import { MemoryRouter } from "react-router";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import source from "../../../crates/ledgence-orchestration-api/tests/fixtures/console-v13.json?raw";
import { decodeConfig } from "../../src/api/codecs";
import { parseUserJson, stringifyUserJson } from "../../src/api/json";
import type { ExplorerNode } from "../../src/api/explorer";
import { InstanceContext } from "../../src/app/instance";
import { NodeInspector } from "../../src/features/explorer/inspector";
import { fixtureNodes } from "../explorer-fixture";

const config = decodeConfig(JSON.parse(source).config);
const local = fixtureNodes().find((node) => node.kind === "local")!;
const node = {
  ...local,
  callable: "tool:billing:quote",
  observation: {
    attempt_id: "att_2",
    started_at: 1200,
    elapsed_us: "30",
    state: "replayed" as const,
  },
};
const step = {
  workflow_id: "wf/1",
  activation_id: node.activation_id,
  step_key: node.key,
  attempt_id: node.accepting_attempt_id,
  callable: node.callable,
  accepted_at: node.accepted_at,
};
const detail = {
  step,
  kind: "tool",
  version: "2",
  arguments: parseUserJson('{"amount":18446744073709551615}', 128),
  result: null,
};
const clients: QueryClient[] = [];
afterEach(() => {
  clients.splice(0).forEach((client) => client.clear());
  vi.restoreAllMocks();
});
async function inspect(
  read: () => [number, unknown],
  selected: Extract<ExplorerNode, { kind: "local" }> = node,
) {
  const fetch = vi
    .spyOn(globalThis, "fetch")
    .mockImplementation(async (input) => {
      const url = new URL(String(input), location.origin);
      expect(url.pathname).toBe("/v1/console/workflows/local-steps/inspect");
      expect(Object.fromEntries(url.searchParams)).toEqual({
        workflow_id: step.workflow_id,
        activation_id: step.activation_id,
        step_key: step.step_key,
      });
      const [status, value] = read();
      return new Response(stringifyUserJson(value), {
        status,
        headers: {
          "Content-Type": "application/json",
          "Ledgence-Console-Contract": "13",
          "Ledgence-Instance-Id": config.instance_id,
        },
      });
    });
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false, refetchOnWindowFocus: false } },
  });
  clients.push(client);
  const view = await render(
    <QueryClientProvider client={client}>
      <InstanceContext.Provider value={config}>
        <MemoryRouter>
          <NodeInspector
            node={selected}
            workflowId={step.workflow_id}
            relations={[]}
            select={() => {}}
          />
        </MemoryRouter>
      </InstanceContext.Provider>
    </QueryClientProvider>,
  );
  return { view, fetch, client };
}
it("reads only an expanded journal record and distinguishes its accepting attempt from a replay observation", async () => {
  let retired = false;
  const { view, fetch, client } = await inspect(() =>
    retired ? [404, {}] : [200, detail],
  );
  expect(fetch).not.toHaveBeenCalled();
  await view.getByText("Inspect durable operation").click();
  await expect
    .element(view.getByLabelText("Effective arguments"))
    .toHaveTextContent("18446744073709551615");
  await expect
    .element(view.getByLabelText("Accepted result"))
    .toHaveTextContent("null");
  await expect.element(view.getByText("att_1", { exact: true })).toBeVisible();
  await expect
    .element(
      view.getByText("This attempt was observed reusing an accepted result."),
    )
    .toBeVisible();
  await expect.element(view.getByText("att_2", { exact: true })).toBeVisible();
  retired = true;
  await client.invalidateQueries();
  await expect
    .element(view.getByRole("alert"))
    .toHaveTextContent("not found or retired");
  await expect
    .element(view.getByLabelText("Effective arguments"))
    .not.toBeInTheDocument();
});
it("rejects another operation's journal and hides its payload", async () => {
  const { view } = await inspect(() => [
    200,
    { ...detail, step: { ...step, workflow_id: "another" } },
  ]);
  await view.getByText("Inspect durable operation").click();
  await expect
    .element(view.getByRole("alert"))
    .toHaveTextContent("does not match");
  await expect
    .element(view.getByLabelText("Effective arguments"))
    .not.toBeInTheDocument();
});
it("keeps observed-only attempts independent from durable records", async () => {
  const { view, fetch } = await inspect(() => [404, {}], {
    ...node,
    accepted_at: null,
    accepting_attempt_id: null,
    observation: { ...node.observation, state: "failed" },
  });
  await expect
    .element(view.getByText("No accepted result is recorded for this step."))
    .toBeVisible();
  await expect
    .element(view.getByText("Inspect durable operation"))
    .not.toBeInTheDocument();
  expect(fetch).not.toHaveBeenCalled();
});

it("shows the recorded input for a local step without inventing default arguments", async () => {
  const { view } = await inspect(() => [
    200,
    { ...detail, kind: "local", version: null, arguments: {}, result: "us" },
  ]);
  await view.getByText("Inspect durable operation").click();
  await expect
    .element(view.getByLabelText("Recorded input"))
    .toHaveTextContent("{}");
  await expect
    .element(view.getByLabelText("Effective arguments"))
    .not.toBeInTheDocument();
});
