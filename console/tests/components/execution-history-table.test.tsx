// SPDX-License-Identifier: MIT
import { afterEach, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-react";
import {
  createMemoryRouter,
  RouterProvider,
  useLocation,
  Outlet,
} from "react-router";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import {
  consoleContractVersion,
  type ConsoleConfig,
} from "../../src/api/contracts";
import type { Execution } from "../../src/api/explorer";
import { InstanceContext } from "../../src/app/instance";
import { AppShell } from "../../src/components/app-shell";
import { BackLink } from "../../src/components/resource-ui";
import { ExecutionsPage } from "../../src/features/execution-history";
import "../../src/styles/global.css";

// Synthetic presentation data tests table queries/navigation, not orchestration.
// Canonical wire compatibility is checked by the Rust fixture contract suite.
const config: ConsoleConfig = {
  contract_version: consoleContractVersion,
  server_version: "test",
  instance_id: "history-ui-test",
  instance_name: "Local workspace",
  capabilities: {
    executions: true,
    workflows: true,
    programs: true,
    workers: true,
  },
  suggested_queues: [],
  limits: {
    default_page_size: 50,
    max_page_size: 100,
    metadata_max_bytes: 2097152,
    submission_max_bytes: 2097152,
    input_max_bytes: 2097152,
    max_visible_workflow_nodes: 100,
    max_detailed_worker_slots: 100,
  },
  polling: {
    lists_ms: 60000,
    active_task_ms: 60000,
    waiting_workflow_ms: 60000,
    workers_ms: 60000,
    catalog_stale_ms: 60000,
    worker_fresh_ms: 60000,
    worker_recent_ms: 60000,
  },
};
const descriptor = {
  program: { id: "release-pipeline", version: "1.0.0" },
  digest: "sha256:history-demo",
  size: "512",
};
const submittedAt = Date.parse("2026-09-29T12:30:00Z");
const records: Execution[] = [
  {
    kind: "workflow",
    id: "wf_release",
    descriptor,
    queue: "work",
    state: "waiting",
    submitted_at: submittedAt,
    terminal_at: null,
    correlation_key: "Release readiness",
    parent_workflow_id: null,
    root_workflow_id: "wf_release",
  },
  {
    kind: "task",
    id: "task_notes",
    descriptor,
    queue: "work",
    state: "succeeded",
    submitted_at: submittedAt + 60000,
    terminal_at: submittedAt + 63000,
    correlation_key: "Prepare release notes",
    parent_workflow_id: null,
    root_workflow_id: null,
  },
];
const clients: QueryClient[] = [];
const routers: ReturnType<typeof createMemoryRouter>[] = [];
afterEach(async () => {
  for (const client of clients.splice(0)) client.clear();
  for (const router of routers.splice(0)) router.dispose();
  vi.restoreAllMocks();
  await page.viewport(1280, 900);
});
function Detail() {
  const location = useLocation();
  return (
    <>
      <h1>Execution detail</h1>
      <p>{location.pathname}</p>
      <BackLink to="/executions">Back to executions</BackLink>
    </>
  );
}
async function mount(query = "", continuation?: typeof records) {
  const paths: URL[] = [];
  vi.spyOn(globalThis, "fetch").mockImplementation(async (input) => {
    const url = new URL(String(input), location.origin);
    paths.push(url);
    return new Response(
      JSON.stringify({
        items: url.searchParams.has("cursor") ? (continuation ?? []) : records,
        next_cursor:
          continuation && !url.searchParams.has("cursor") ? "page-2" : null,
        observed_at: submittedAt + 300000,
      }),
      {
        headers: {
          "Content-Type": "application/json",
          "Ledgence-Console-Contract": String(consoleContractVersion),
          "Ledgence-Instance-Id": config.instance_id,
        },
      },
    );
  });
  const router = createMemoryRouter(
    [
      {
        element: (
          <AppShell instanceName={config.instance_name}>
            <Outlet />
          </AppShell>
        ),
        children: [
          { path: "/executions", element: <ExecutionsPage /> },
          { path: "/workflows/:id", element: <Detail /> },
          { path: "/executions/:id", element: <Detail /> },
        ],
      },
    ],
    { initialEntries: [`/executions${query ? `?${query}` : ""}`] },
  );
  routers.push(router);
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false, refetchOnWindowFocus: false } },
  });
  clients.push(client);
  const view = await render(
    <QueryClientProvider client={client}>
      <InstanceContext.Provider value={config}>
        <RouterProvider router={router} />
      </InstanceContext.Provider>
    </QueryClientProvider>,
  );
  return { view, router, paths };
}
it("appends execution pages automatically and preserves filtered list context across a detail link", async () => {
  const query = "kind=workflow&state=waiting&limit=25";
  const { view, router, paths } = await mount(query, [
    {
      ...records[0]!,
      id: "wf_publish",
      root_workflow_id: "wf_publish",
      correlation_key: "Publish deployment",
    },
  ]);
  const listNavigationKey = router.state.location.key;
  await expect
    .element(view.getByRole("link", { name: "Release readiness", exact: true }))
    .toBeVisible();
  // Let the real IntersectionObserver see the end of the current table.
  view.getByRole("table").element().scrollIntoView({ block: "end" });
  await expect
    .element(
      view.getByText("3 executions loaded · End of list", { exact: true }),
    )
    .toBeVisible();
  expect(paths.map((url) => url.pathname)).toEqual([
    "/v1/console/executions",
    "/v1/console/executions",
  ]);
  for (const url of paths) {
    expect(url.searchParams.get("kind")).toBe("workflow");
    expect(url.searchParams.get("state")).toBe("waiting");
    expect(url.searchParams.get("limit")).toBe("25");
    expect(url.searchParams.get("include_children")).toBe("false");
  }
  expect(paths[0]?.searchParams.get("cursor")).toBeNull();
  expect(paths[1]?.searchParams.get("cursor")).toBe("page-2");
  expect(router.state.location.search).toBe(`?${query}`);
  expect(router.state.location.key).toBe(listNavigationKey);
  await view
    .getByRole("link", { name: "Publish deployment", exact: true })
    .click();
  await expect
    .element(view.getByRole("heading", { name: "Execution detail" }))
    .toBeVisible();
  expect(router.state.location.pathname).toBe("/workflows/wf_publish");
  expect(router.state.location.state).toEqual({
    returnTo: `/executions?${query}`,
    returnNavigationKey: listNavigationKey,
  });
  await expect
    .element(view.getByRole("link", { name: "Back to executions" }))
    .toHaveAttribute("href", `/executions?${query}`);
  await view.getByRole("link", { name: "Back to executions" }).click();
  await expect
    .element(view.getByRole("combobox", { name: "Status", exact: true }))
    .toHaveValue("waiting");
  await expect
    .element(view.getByRole("combobox", { name: "Type", exact: true }))
    .toHaveValue("workflow");
  await expect
    .element(view.getByRole("link", { name: "Release readiness", exact: true }))
    .toBeVisible();
  await expect
    .element(
      view.getByRole("link", { name: "Publish deployment", exact: true }),
    )
    .toBeVisible();
  await expect
    .element(
      view.getByText("3 executions loaded · End of list", { exact: true }),
    )
    .toBeVisible();
  expect(paths).toHaveLength(2);
  await expect
    .element(view.getByRole("button", { name: /^(Next|Previous|Refresh)$/ }))
    .not.toBeInTheDocument();
  await expect
    .element(view.getByRole("combobox", { name: "Rows", exact: true }))
    .not.toBeInTheDocument();
});

it("shows observed elapsed time and makes task/workflow destinations real links", async () => {
  const { view } = await mount();
  await expect
    .element(view.getByRole("link", { name: "Release readiness", exact: true }))
    .toHaveAttribute("href", "/workflows/wf_release");
  await expect
    .element(
      view.getByRole("link", { name: "Prepare release notes", exact: true }),
    )
    .toHaveAttribute("href", "/executions/task_notes");
  await expect
    .element(view.getByRole("columnheader", { name: "Scope", exact: true }))
    .not.toBeInTheDocument();
  await expect
    .element(view.getByRole("cell", { name: "5m 0s", exact: true }))
    .toBeVisible();
  await expect
    .element(view.getByRole("cell", { name: "3s", exact: true }))
    .toBeVisible();
});
it("includes children for exact program searches while keeping scope explicit", async () => {
  const { view, paths } = await mount("program_id=release-pipeline");
  await expect
    .element(view.getByRole("link", { name: "Release readiness", exact: true }))
    .toBeVisible();
  expect(paths[0]?.searchParams.get("include_children")).toBe("true");
  const scope = view.getByRole("columnheader", { name: "Scope", exact: true });
  scope.element().scrollIntoView({ block: "nearest", inline: "nearest" });
  await expect.element(scope).toBeVisible();
});
it("contains mobile table scrolling and keeps the calendar fields usable at 390px", async () => {
  await page.viewport(390, 844);
  const { view } = await mount();
  await expect
    .element(view.getByRole("link", { name: "Release readiness", exact: true }))
    .toBeVisible();
  const scroll = view
    .getByRole("region", { name: "Execution history" })
    .element();
  expect(scroll.scrollWidth).toBeGreaterThan(scroll.clientWidth);
  expect(document.documentElement.scrollWidth).toBeLessThanOrEqual(
    window.innerWidth + 1,
  );
  const from = view.getByLabelText("Submitted from · UTC").element();
  expect(from.getBoundingClientRect().height).toBeGreaterThanOrEqual(44);
  expect(from.getBoundingClientRect().right).toBeLessThanOrEqual(
    window.innerWidth,
  );
  expect(getComputedStyle(view.getByRole("table").element()).display).toBe(
    "table",
  );
  const kind = view
    .getByRole("combobox", { name: "Type", exact: true })
    .element();
  expect(kind.getBoundingClientRect().height).toBeGreaterThanOrEqual(44);
  expect(
    view.getByRole("table").element().getBoundingClientRect().width,
  ).toBeGreaterThanOrEqual(900);
  expect(
    view
      .getByRole("columnheader", { name: "Execution", exact: true })
      .element()
      .getBoundingClientRect().width,
  ).toBeGreaterThanOrEqual(205);
  await page.screenshot();
});
