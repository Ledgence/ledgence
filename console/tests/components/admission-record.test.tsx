// SPDX-License-Identifier: MIT
import { afterEach, expect, it, vi } from "vitest";
import { render } from "vitest-browser-react";
import { MemoryRouter } from "react-router";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import raw from "../../../crates/ledgence-orchestration-api/tests/fixtures/console-v13.json?raw";
import { decodeConfig } from "../../src/api/codecs";
import { parseUserJson, stringifyUserJson } from "../../src/api/json";
import { workflowExplorer } from "../../src/api/explorer";
import { admissionSnapshot } from "../../src/api/admissions";
import { InstanceContext } from "../../src/app/instance";
import { NodeInspector } from "../../src/features/explorer/inspector";
const fixture = JSON.parse(raw);
const config = decodeConfig(fixture.config);
const cases = (parseUserJson(raw, 2 * 1024 * 1024) as Record<string, unknown>)
  .admission as Record<string, unknown>;
const node = workflowExplorer(cases.accepted).page.items.find(
  (v) => v.kind === "admission_operation",
)!;
const clients: QueryClient[] = [];
afterEach(() => {
  clients.splice(0).forEach((v) => v.clear());
  vi.restoreAllMocks();
});
async function setup(foreign = false, governed = false) {
  const provider = cases.provider as Record<string, unknown>;
  const responseValue = governed ? provider.completed_pending : cases.snapshot;
  const value = admissionSnapshot(responseValue);
  const selectedNode = governed
    ? workflowExplorer(provider.graph).page.items.find(
        (v) => v.kind === "admission_wait",
      )!
    : node;
  if (foreign) value.finish!.source.owner.attempt_id = "foreign";
  const fetch = vi
    .spyOn(globalThis, "fetch")
    .mockImplementation(async (input, options) => {
      expect(new URL(String(input), location.origin).pathname).toBe(
        "/v1/console/admissions/inspect",
      );
      expect(options?.method).toBe("POST");
      expect(JSON.parse(String(options?.body))).toEqual({
        workflow_id: "wf_admission",
        origin_activation_id: "act_wf_admission_0",
        key: "summary:0",
      });
      return new Response(stringifyUserJson(governed ? responseValue : value), {
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
            node={selectedNode}
            workflowId="wf_admission"
            relations={[]}
            select={() => {}}
          />
        </MemoryRouter>
      </InstanceContext.Provider>
    </QueryClientProvider>,
  );
  return { view, fetch };
}
it("loads sensitive admission arguments only when expanded and uses the original result source", async () => {
  const { view, fetch } = await setup();
  expect(fetch).not.toHaveBeenCalled();
  await view.getByText("Inspect accepted operation").click();
  await expect
    .element(view.getByLabelText("Effective arguments"))
    .toHaveTextContent("18446744073709551615");
  await expect
    .element(view.getByLabelText("Accepted result"))
    .toHaveTextContent("accepted");
  await expect
    .element(view.getByText("att_model_1", { exact: true }))
    .toBeVisible();
  await view.getByText("Inspect accepted operation").click();
  await expect
    .element(view.getByLabelText("Effective arguments"))
    .not.toBeInTheDocument();
});
it("hides result payload when inspection returns foreign attempt provenance", async () => {
  const { view } = await setup(true);
  await view.getByText("Inspect accepted operation").click();
  await expect.element(view.getByRole("alert")).toBeVisible();
  await expect
    .element(view.getByLabelText("Effective arguments"))
    .not.toBeInTheDocument();
  await expect
    .element(view.getByLabelText("Accepted result"))
    .not.toBeInTheDocument();
});

it("inspects provider call settlement without rendering a successful callback or business outcome", async () => {
  const { view, fetch } = await setup(false, true);
  expect(fetch).not.toHaveBeenCalled();
  await view.getByText("Inspect admission record").click();
  await expect
    .element(view.getByLabelText("Approved arguments"))
    .toHaveTextContent("sales");
  await expect
    .element(view.getByLabelText("Provider ticket"))
    .toHaveTextContent("export:0");
  await expect
    .element(
      view.getByText("Call completion describes capacity settlement.", {
        exact: false,
      }),
    )
    .toBeVisible();
  await expect
    .element(view.getByLabelText("Accepted result"))
    .not.toBeInTheDocument();
});
