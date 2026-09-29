// SPDX-License-Identifier: MIT
import { afterEach, expect, it, vi } from "vitest";
import { render } from "vitest-browser-react";
import { MemoryRouter, Route, Routes } from "react-router";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import raw from "../../../crates/ledgence-orchestration-api/tests/fixtures/console-v2.json?raw";
import { parseUserJson, stringifyUserJson } from "../../src/api/json";
import { decodeConfig } from "../../src/api/codecs";
import { workflowDetail } from "../../src/api/resources";
import { explorerNode } from "../../src/api/explorer";
import { InstanceContext } from "../../src/app/instance";
import { NavigationMemory } from "../../src/app/navigation";
import { WorkflowDetailPage } from "../../src/features/workflows";
import { AttemptResources } from "../../src/features/execution-resources";
import { fixtureNodes, phase } from "../explorer-fixture";
import "../../src/styles/global.css";

const fixture = parseUserJson(raw, 2 * 1024 * 1024);
function field(key: string) {
  if (!fixture || typeof fixture !== "object") throw Error("fixture");
  return Reflect.get(fixture, key);
}
const config = decodeConfig(field("config"));
config.polling.waiting_workflow_ms = 60000;
config.polling.active_task_ms = 20;
const clients: QueryClient[] = [];
afterEach(() => {
  for (const client of clients.splice(0)) client.clear();
  vi.restoreAllMocks();
  localStorage.removeItem("ledgence-explorer-view");
});
function response(value: unknown) {
  return new Response(stringifyUserJson(value), {
    headers: {
      "Content-Type": "application/json",
      "Ledgence-Console-Contract": "2",
      "Ledgence-Instance-Id": config.instance_id,
    },
  });
}
function detail(id: string) {
  const value = workflowDetail(field("workflow_detail"));
  value.summary.workflow.workflow_id = id;
  value.summary.workflow.state = "succeeded";
  value.summary.workflow.terminal_at = 1400;
  value.summary.workflow.parent_workflow_id =
    id === "wf_review" ? "wf_invoice_1042" : null;
  value.summary.controller.program.id =
    id === "wf_review" ? "independent-review" : "change-review";
  return value;
}
function routes(
  options: { next?: string; nodes?: ReturnType<typeof fixtureNodes> } = {},
) {
  return vi.spyOn(globalThis, "fetch").mockImplementation(async (input) => {
    const url = new URL(String(input), location.origin);
    const id =
      url.searchParams.get("workflow_id") ??
      url.searchParams.get("id") ??
      "wf_invoice_1042";
    if (url.pathname.endsWith("/workflows/inspect"))
      return response(detail(id));
    if (url.pathname.endsWith("/workflows/explorer"))
      return response({
        workflow: detail(id),
        page: {
          items:
            id === "wf_review"
              ? [phase("review_phase", "review")]
              : (options.nodes ?? fixtureNodes()),
          observed_at: detail(id).observed_at,
          next_cursor: options.next ?? null,
        },
        evidence: "retained_records_only",
      });
    if (url.pathname.endsWith("/executions/ancestry")) {
      const identity = (value: string) => ({
        execution: { kind: "workflow", id: value },
        program: detail(value).summary.controller.program,
        availability: "available",
      });
      return response({
        execution: { kind: "workflow", id },
        path:
          id === "wf_review"
            ? [identity("wf_invoice_1042"), identity(id)]
            : [identity(id)],
        observed_at: 1500,
      });
    }
    throw Error(`Unexpected request ${url.pathname}`);
  });
}
async function mount(url = "/workflows/wf_invoice_1042?view=graph") {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false, refetchOnWindowFocus: false } },
  });
  clients.push(client);
  const view = await render(
    <QueryClientProvider client={client}>
      <InstanceContext.Provider value={config}>
        <MemoryRouter initialEntries={[url]}>
          <NavigationMemory />
          <main id="main-content" tabIndex={-1}>
            <Routes>
              <Route
                path="/workflows/:workflowId"
                element={<WorkflowDetailPage />}
              />
            </Routes>
          </main>
        </MemoryRouter>
      </InstanceContext.Provider>
    </QueryClientProvider>,
  );
  return { view, client };
}
it("synchronizes Graph and Timeline selection with one evidence inspector", async () => {
  routes();
  const { view } = await mount();
  await view
    .getByRole("button", {
      name: "tests:0 · Local step · accepted",
      exact: true,
    })
    .click();
  const inspector = view.getByRole("complementary", {
    name: "Selected work details",
  });
  await expect
    .element(inspector.getByRole("heading", { name: "tests:0" }))
    .toBeVisible();
  await expect
    .element(inspector.getByText("Durable result accepted", { exact: true }))
    .toBeVisible();
  await expect
    .element(inspector.getByRole("link", { name: "Open execution" }))
    .not.toBeInTheDocument();
  await view.getByRole("button", { name: "Timeline", exact: true }).click();
  await expect
    .element(inspector.getByRole("heading", { name: "tests:0" }))
    .toBeVisible();
  await expect
    .element(view.getByRole("button", { name: /tests:0.*Local step/ }))
    .toHaveAttribute("aria-pressed", "true");
});
it("drills into a subworkflow and restores parent view and selection with Up", async () => {
  routes();
  const { view } = await mount();
  await view
    .getByRole("button", { name: "Collapse phase collect", exact: true })
    .click();
  await view
    .getByRole("button", {
      name: "review:0 · Subworkflow · succeeded",
      exact: true,
    })
    .click();
  await view.getByRole("button", { name: "Timeline", exact: true }).click();
  await view.getByRole("link", { name: "Open execution" }).click();
  await expect
    .element(view.getByRole("link", { name: "Up to parent" }))
    .toBeVisible();
  await view.getByRole("link", { name: "Up to parent" }).click();
  await expect
    .element(view.getByRole("button", { name: "Timeline", exact: true }))
    .toHaveAttribute("aria-pressed", "true");
  await expect
    .element(
      view
        .getByRole("complementary")
        .getByRole("heading", { name: "review:0" }),
    )
    .toBeVisible();
  await view.getByRole("button", { name: "Graph", exact: true }).click();
  await expect
    .element(
      view.getByRole("button", { name: "Expand phase collect", exact: true }),
    )
    .toHaveAttribute("aria-expanded", "false");
});
it("distinguishes unloaded records and an unavailable selection from expiry", async () => {
  routes({ next: "next-page" });
  const { view } = await mount(
    "/workflows/wf_invoice_1042?view=graph&node=missing",
  );
  await expect
    .element(view.getByText("Partial view.", { exact: false }))
    .toBeVisible();
  await expect
    .element(
      view.getByRole("heading", { name: "Selection unavailable on this page" }),
    )
    .toBeVisible();
  await expect
    .element(
      view.getByText("Its absence does not prove it expired.", {
        exact: false,
      }),
    )
    .toBeVisible();
});
it("refreshes new local work without relying on parent workflow revision", async () => {
  let nodes: ReturnType<typeof fixtureNodes> = fixtureNodes().filter(
    (node) => node.kind !== "local",
  );
  const options = {
    get nodes() {
      return nodes;
    },
  };
  routes(options);
  const { view } = await mount();
  await expect
    .element(
      view.getByRole("button", { name: "review:0 · Subworkflow · succeeded" }),
    )
    .toBeVisible();
  nodes = fixtureNodes();
  await view.getByRole("button", { name: "Refresh", exact: true }).click();
  await expect
    .element(
      view.getByRole("button", { name: "tests:0 · Local step · accepted" }),
    )
    .toBeVisible();
});
it("does not poll missing measurements for terminal or legacy attempts", async () => {
  let reads = 0;
  vi.spyOn(globalThis, "fetch").mockImplementation(async () => {
    reads++;
    return response({
      attempt_id: "att_legacy",
      task_id: "task_legacy",
      observations: null,
      observed_at: 2000,
    });
  });
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false, refetchOnWindowFocus: false } },
  });
  clients.push(client);
  const view = await render(
    <QueryClientProvider client={client}>
      <InstanceContext.Provider value={config}>
        <AttemptResources attemptId="att_legacy" />
      </InstanceContext.Provider>
    </QueryClientProvider>,
  );
  await expect
    .element(view.getByText("Missing values are not zero.", { exact: false }))
    .toBeVisible();
  await new Promise((resolve) => setTimeout(resolve, 100));
  expect(reads).toBe(1);
});

it("anchors a selected timeline row when earlier evidence arrives, unless following activity", async () => {
  const local = fixtureNodes().find((node) => node.kind === "local")!;
  let nodes: ReturnType<typeof fixtureNodes> = [
    phase(),
    ...Array.from({ length: 40 }, (_, index) =>
      explorerNode({
        ...local,
        id: `local:${index}`,
        key: `step:${index}`,
        accepted_at: 1010 + index * 10,
      }),
    ),
  ];
  routes({
    get nodes() {
      return nodes;
    },
  });
  const { view } = await mount(
    "/workflows/wf_invoice_1042?view=timeline&node=local:24",
  );
  await expect
    .poll(() => !!document.querySelector(".timeline-viewport"))
    .toBe(true);
  const viewport =
    document.querySelector<HTMLDivElement>(".timeline-viewport")!;
  viewport.scrollTop = 22 * 86;
  viewport.dispatchEvent(new Event("scroll", { bubbles: true }));
  const selectedRow = () =>
    document.querySelector<HTMLElement>('.timeline-row[aria-pressed="true"]')!;
  await expect.poll(() => selectedRow()?.textContent).toContain("step:24");
  const before =
    selectedRow().getBoundingClientRect().top -
    viewport.getBoundingClientRect().top;
  const scrollBefore = viewport.scrollTop;
  nodes = [
    ...nodes,
    explorerNode({
      ...local,
      id: "local:earlier",
      key: "earlier:0",
      accepted_at: 900,
    }),
  ];
  await view.getByRole("button", { name: "Refresh", exact: true }).click();
  await expect.poll(() => viewport.scrollTop).toBe(scrollBefore + 86);
  expect(
    selectedRow().getBoundingClientRect().top -
      viewport.getBoundingClientRect().top,
  ).toBeCloseTo(before, 1);
  await view
    .getByRole("checkbox", { name: "Follow activity", exact: true })
    .click();
  await expect
    .poll(() => viewport.scrollTop)
    .toBe(viewport.scrollHeight - viewport.clientHeight);
  const heightBefore = viewport.scrollHeight;
  nodes = [
    ...nodes,
    explorerNode({
      ...local,
      id: "local:latest",
      key: "latest:0",
      accepted_at: 5000,
    }),
  ];
  await view.getByRole("button", { name: "Refresh", exact: true }).click();
  await expect.poll(() => viewport.scrollHeight).toBe(heightBefore + 86);
  await expect
    .poll(
      () => viewport.scrollHeight - viewport.clientHeight - viewport.scrollTop,
    )
    .toBe(0);
});

it("shows an unavailable child's unknown end explicitly without stretching a completed timeline", async () => {
  const nodes = fixtureNodes();
  const child = nodes.find((node) => node.kind === "child")!;
  if (child.kind !== "child") throw Error("fixture");
  child.state = null;
  child.availability = "unavailable";
  child.terminal_at = null;
  routes({ nodes });
  const { view } = await mount("/workflows/wf_invoice_1042?view=timeline");
  await expect
    .element(view.getByText("End not recorded", { exact: true }))
    .toBeVisible();
  expect(
    document
      .querySelector(".execution-timeline")
      ?.getAttribute("data-timeline-end"),
  ).toBe("1100");
});
