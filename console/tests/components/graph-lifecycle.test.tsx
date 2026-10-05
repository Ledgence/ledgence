// SPDX-License-Identifier: MIT
import { afterEach, expect, it, vi } from "vitest";
import { useState } from "react";
import { page } from "vitest/browser";
import { render } from "vitest-browser-react";
import {
  createMemoryRouter,
  RouterProvider,
  useSearchParams,
} from "react-router";
import { GraphCanvas } from "../../src/features/explorer/graph";
import type { GraphPresentation } from "../../src/features/explorer/layout";
import { entrypoint } from "../explorer-fixture";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import raw from "../../../crates/ledgence-orchestration-api/tests/fixtures/console-v5.json?raw";
import { decodeConfig } from "../../src/api/codecs";
import { parseUserJson, stringifyUserJson } from "../../src/api/json";
import { workflowExplorer } from "../../src/api/explorer";
import { InstanceContext } from "../../src/app/instance";
import { WorkflowExplorer } from "../../src/features/workflow-explorer";
import "../../src/styles/global.css";
import "../../src/styles/explorer.css";

const routers: ReturnType<typeof createMemoryRouter>[] = [];
const clients: QueryClient[] = [];
afterEach(async () => {
  for (const router of routers.splice(0)) router.dispose();
  for (const client of clients.splice(0)) client.clear();
  vi.restoreAllMocks();
  await page.viewport(1280, 900);
});
// Synthetic graph growth isolates presentation stability from backend semantics.
function GraphHost() {
  const [params, setParams] = useSearchParams();
  const [ids, setIds] = useState(["a"]);
  const [presentation, save] = useState<GraphPresentation>();
  const nodes = ids.map((id) => ({ ...entrypoint(id, id), id }));
  const edges = ids.slice(1).map((id) => ({
    id: `a-${id}`,
    from: "a",
    to: id,
    relation: "registers" as const,
    kind: "registers" as const,
    style: "parent" as const,
    evidenceIds: ["a", id],
  }));
  return (
    <>
      <button onClick={() => setIds(["a", "z"])}>First update</button>
      <button onClick={() => setIds(["a", "b", "z"])}>Second update</button>
      <div style={{ width: 1000, height: 600 }}>
        <GraphCanvas
          scope="growth-test"
          nodes={nodes}
          edges={edges}
          selectedId={params.get("node")}
          select={(id) => setParams({ node: id })}
          presentation={presentation}
          save={save}
        />
      </div>
      <output data-testid="saved-presentation">
        {JSON.stringify(presentation)}
      </output>
    </>
  );
}
async function mount() {
  const router = createMemoryRouter([{ path: "/", element: <GraphHost /> }]);
  routers.push(router);
  const view = await render(<RouterProvider router={router} />);
  await expect.poll(() => !!document.querySelector('[data-id="a"]')).toBe(true);
  return { view, router };
}
function nodePosition(id: string) {
  const node = document.querySelector<HTMLElement>(
    `.react-flow__node[data-id="${id}"]`,
  );
  return {
    x: node?.getAttribute("data-position-x"),
    y: node?.getAttribute("data-position-y"),
  };
}
it("persists new positions before a second topology update without moving earlier cards", async () => {
  const { view } = await mount();
  const initial = nodePosition("a");
  await view.getByRole("button", { name: "First update", exact: true }).click();
  await expect.poll(() => nodePosition("z").x).not.toBeUndefined();
  const first = nodePosition("z");
  await expect
    .poll(() => document.querySelectorAll(".react-flow__edge").length)
    .toBe(1);
  await expect
    .element(view.getByTestId("saved-presentation"))
    .toHaveTextContent('"z":');
  await view
    .getByRole("button", { name: "Second update", exact: true })
    .click();
  await expect.poll(() => nodePosition("b").x).not.toBeUndefined();
  expect(nodePosition("z")).toEqual(first);
  expect(nodePosition("a")).toEqual(initial);
  await expect
    .poll(() => document.querySelectorAll(".react-flow__edge").length)
    .toBe(2);
});
it("creates one history entry for one graph card click", async () => {
  const { view, router } = await mount();
  await view
    .getByRole("button", { name: "a · Entrypoint · succeeded", exact: true })
    .click();
  expect(router.state.location.search).toBe("?node=a");
  await router.navigate(-1);
  expect(router.state.location.search).toBe("");
  await expect
    .element(
      view.getByRole("button", {
        name: "a · Entrypoint · succeeded",
        exact: true,
      }),
    )
    .toHaveAttribute("aria-pressed", "false");
});

it("restores the saved camera when Back changes selection within the same workflow", async () => {
  const fixture = parseUserJson(raw, 2 * 1024 * 1024);
  if (!fixture || typeof fixture !== "object")
    throw Error("Missing Rust fixture.");
  const config = decodeConfig(Reflect.get(fixture, "config"));
  const explorer = Reflect.get(fixture, "explorer");
  const workflowId =
    workflowExplorer(explorer).workflow.summary.workflow.workflow_id;
  vi.spyOn(globalThis, "fetch").mockImplementation(
    async () =>
      new Response(stringifyUserJson(explorer), {
        headers: {
          "Content-Type": "application/json",
          "Ledgence-Console-Contract": "5",
          "Ledgence-Instance-Id": config.instance_id,
        },
      }),
  );
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false, refetchOnWindowFocus: false } },
  });
  clients.push(client);
  const router = createMemoryRouter([
    {
      path: "/",
      element: (
        <main id="main-content">
          <WorkflowExplorer
            workflowId={workflowId}
            active={false}
            view="graph"
          />
        </main>
      ),
    },
  ]);
  routers.push(router);
  const view = await render(
    <QueryClientProvider client={client}>
      <InstanceContext.Provider value={config}>
        <RouterProvider router={router} />
      </InstanceContext.Provider>
    </QueryClientProvider>,
  );
  const transform = () =>
    document.querySelector<HTMLElement>(".react-flow__viewport")?.style
      .transform;
  await expect
    .poll(() => document.querySelectorAll(".react-flow__node").length)
    .toBeGreaterThan(0);
  const initial = transform();
  await view.getByRole("button", { name: "Zoom in", exact: true }).click();
  await expect.poll(transform).not.toBe(initial);
  const beforeSelection = transform();
  document.querySelector<HTMLElement>(".react-flow__node")!.click();
  await expect.poll(() => router.state.location.search).toContain("node=");
  const canvas = document.querySelector(".react-flow");
  await view.getByRole("button", { name: "Zoom in", exact: true }).click();
  await expect.poll(transform).not.toBe(beforeSelection);
  await router.navigate(-1);
  expect(router.state.location.search).toBe("");
  await expect.poll(transform).toBe(beforeSelection);
  expect(document.querySelector(".react-flow")).toBe(canvas);
});

function VisibilityHost({ workflowId }: { workflowId: string }) {
  const [params, setParams] = useSearchParams();
  const visible = params.get("tab") !== "General";
  function show(tab: string) {
    const next = new URLSearchParams(params);
    next.set("tab", tab);
    setParams(next);
  }
  return (
    <main id="main-content">
      <button onClick={() => show("General")}>Show General</button>
      <button onClick={() => show("Graph")}>Show Graph</button>
      <label>
        Keep typing
        <input />
      </label>
      <div hidden={!visible}>
        <WorkflowExplorer
          workflowId={workflowId}
          active={false}
          visible={visible}
          view={params.get("tab") === "Trace" ? "trace" : "graph"}
        />
      </div>
    </main>
  );
}
async function mountVisibility(query = "tab=Trace", withLocal = false) {
  const fixture = parseUserJson(raw, 2 * 1024 * 1024);
  if (!fixture || typeof fixture !== "object")
    throw Error("Missing Rust fixture.");
  const config = decodeConfig(Reflect.get(fixture, "config"));
  const explorer = workflowExplorer(Reflect.get(fixture, "explorer"));
  if (withLocal)
    explorer.page.items.push({
      kind: "local",
      relations: [],
      id: "local:resource",
      activation_id: "act_resource",
      revision: "1",
      entrypoint: "start",
      key: "local-resource",
      callable: "tools:inspect",
      accepted_at: 1300,
      accepting_attempt_id: "attempt_resource",
      observation: {
        attempt_id: "attempt_resource",
        started_at: 1200,
        elapsed_us: "100000",
        state: "returned",
      },
    });
  const counts = { resources: 0 };
  vi.spyOn(globalThis, "fetch").mockImplementation(async (input) => {
    const url = new URL(String(input), location.origin);
    const resources = url.pathname.endsWith("/attempts/observations");
    if (resources) counts.resources++;
    return new Response(
      stringifyUserJson(
        resources
          ? {
              attempt_id: "attempt_resource",
              task_id: "act_resource",
              observations: null,
              observed_at: 2000,
            }
          : explorer,
      ),
      {
        headers: {
          "Content-Type": "application/json",
          "Ledgence-Console-Contract": "5",
          "Ledgence-Instance-Id": config.instance_id,
        },
      },
    );
  });
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false, refetchOnWindowFocus: false } },
  });
  clients.push(client);
  const router = createMemoryRouter(
    [
      {
        path: "/",
        element: (
          <VisibilityHost
            workflowId={explorer.workflow.summary.workflow.workflow_id}
          />
        ),
      },
    ],
    {
      initialEntries: [
        {
          pathname: "/",
          search: `?${query}`,
          key: `visibility-${Math.random()}`,
        },
      ],
    },
  );
  routers.push(router);
  const view = await render(
    <QueryClientProvider client={client}>
      <InstanceContext.Provider value={config}>
        <RouterProvider router={router} />
      </InstanceContext.Provider>
    </QueryClientProvider>,
  );
  return { view, router, client, counts };
}
it("reveals mobile details only after an explicit selection and returns focus to the canvas on close", async () => {
  await page.viewport(390, 844);
  const { view, router, client } = await mountVisibility();
  const rows = view.getByRole("list", { name: "Recorded work" });
  await rows.getByRole("button").first().click();
  const details = view.getByRole("complementary", {
    name: "Selected work details",
  });
  await expect.element(details).toHaveFocus();
  expect(details.element().getBoundingClientRect().bottom).toBeLessThanOrEqual(
    window.innerHeight + 1,
  );
  const input = view.getByRole("textbox", { name: "Keep typing" });
  input.element().focus({ preventScroll: true });
  await client.refetchQueries({ type: "active" });
  await expect.element(input).toHaveFocus();
  await view
    .getByRole("button", { name: "Close details", exact: true })
    .click();
  await expect
    .poll(() => document.activeElement?.classList.contains("timeline-row"))
    .toBe(true);
  const row = document.activeElement!;
  expect(row.getBoundingClientRect().top).toBeGreaterThanOrEqual(0);
  expect(row.getBoundingClientRect().bottom).toBeLessThanOrEqual(
    window.innerHeight + 1,
  );
  input.element().focus({ preventScroll: true });
  await router.navigate(-1);
  await expect.element(details).toBeInTheDocument();
  await expect.element(input).toHaveFocus();
});
it("does not initialize a hidden graph when General follows Trace", async () => {
  const { view } = await mountVisibility();
  await expect
    .element(view.getByRole("list", { name: "Recorded work" }))
    .toBeVisible();
  await view.getByRole("button", { name: "Show General" }).click();
  expect(document.querySelector(".react-flow")).toBeNull();
  await view.getByRole("button", { name: "Show Graph" }).click();
  await expect
    .poll(() => document.querySelectorAll(".react-flow__node").length)
    .toBeGreaterThan(0);
  const transform = document.querySelector<HTMLElement>(
    ".react-flow__viewport",
  )!.style.transform;
  const zoom = Number(transform.match(/scale\(([^)]+)\)/)?.[1]);
  expect(zoom).toBeGreaterThan(0);
});
it("unmounts expanded inspector resources while General is visible", async () => {
  const { view, client, counts } = await mountVisibility(
    "tab=Trace&node=local%3Aresource",
    true,
  );
  await view
    .getByText("Process resources for this attempt", { exact: true })
    .click();
  await expect.poll(() => counts.resources).toBe(1);
  await expect
    .element(view.getByRole("heading", { name: "Attempt resources" }))
    .toBeVisible();
  await view.getByRole("button", { name: "Show General" }).click();
  await expect
    .poll(() => document.querySelector(".explorer-inspector"))
    .toBeNull();
  await expect
    .poll(() =>
      client
        .getQueryCache()
        .getAll()
        .filter((query) =>
          query.queryKey.some(
            (value) =>
              typeof value === "string" &&
              value.includes("attempts/observations"),
          ),
        )
        .every((query) => query.getObserversCount() === 0),
    )
    .toBe(true);
  const readsAfterUnmount = counts.resources;
  await client.refetchQueries({ type: "active" });
  expect(counts.resources).toBe(readsAfterUnmount);
});
