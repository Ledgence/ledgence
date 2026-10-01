// SPDX-License-Identifier: MIT
import { afterEach, expect, it, vi } from "vitest";
import { cleanup, render } from "vitest-browser-react";
import { createMemoryRouter, RouterProvider, Outlet } from "react-router";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import raw from "../../../crates/ledgence-orchestration-api/tests/fixtures/console-v4.json?raw";
import {
  NavigationHistoryProvider,
  NavigationMemory,
} from "../../src/app/navigation";
import { positions, recentPaths } from "../../src/app/navigation-state";
import { DetailPanels } from "../../src/features/detail-panels";
import { ExecutionContext } from "../../src/components/execution-context";
import { InstanceContext } from "../../src/app/instance";
import { decodeConfig } from "../../src/api/codecs";
import { ancestry } from "../../src/api/explorer";
import { parseUserJson, stringifyUserJson } from "../../src/api/json";

const routers: ReturnType<typeof createMemoryRouter>[] = [];
const clients: QueryClient[] = [];
afterEach(async () => {
  await cleanup();
  for (const router of routers.splice(0)) router.dispose();
  for (const client of clients.splice(0)) client.clear();
  positions.clear();
  recentPaths.clear();
  vi.restoreAllMocks();
  window.scrollTo(0, 0);
});

async function mountRestoration(focus: string | null = null) {
  const desiredY = window.innerHeight * 2 + 150;
  positions.set("saved-list", {
    x: 0,
    y: desiredY,
    focus,
    containers: {},
    url: "/executions",
  });
  const router = createMemoryRouter(
    [
      {
        path: "/executions",
        element: (
          <>
            <NavigationMemory />
            <main id="main-content" tabIndex={-1}>
              <a href="#row" data-focus-key="saved-row">
                Earlier execution
              </a>
            </main>
          </>
        ),
      },
    ],
    {
      initialEntries: [
        {
          pathname: "/executions",
          state: { restoreNavigationKey: "saved-list" },
        },
      ],
    },
  );
  routers.push(router);
  await render(<RouterProvider router={router} />);
  const content = document.getElementById("main-content")!;
  function appendRows() {
    const rows = document.createElement("div");
    rows.style.height = `${window.innerHeight * 4}px`;
    rows.textContent = "Delayed cached execution rows";
    content.append(rows);
  }
  return { desiredY, appendRows };
}

for (const focus of [null, "saved-row"]) {
  it(`restores clamped scroll after cached rows mount ${focus ? "without changing the remembered focus" : "without a remembered focus"}`, async () => {
    const { desiredY, appendRows } = await mountRestoration(focus);
    expect(window.scrollY).toBeLessThan(desiredY);
    appendRows();
    await expect.poll(() => window.scrollY).toBeCloseTo(desiredY, 0);
    if (focus)
      expect((document.activeElement as HTMLElement).dataset.focusKey).toBe(
        focus,
      );
    // Further appends must not pull the user back after restoration completed.
    window.scrollTo(0, 20);
    appendRows();
    await new Promise<void>((resolve) =>
      requestAnimationFrame(() => resolve()),
    );
    expect(window.scrollY).toBe(20);
  });
}

for (const interaction of ["wheel", "touchstart"]) {
  it(`abandons delayed restoration on user ${interaction}`, async () => {
    const { desiredY, appendRows } = await mountRestoration();
    expect(window.scrollY).toBeLessThan(desiredY);
    document.dispatchEvent(new Event(interaction));
    appendRows();
    await new Promise<void>((resolve) =>
      requestAnimationFrame(() => resolve()),
    );
    expect(window.scrollY).toBeLessThan(desiredY);
  });
}

const fixture = parseUserJson(raw, 2 * 1024 * 1024);
function field(name: string) {
  if (!fixture || typeof fixture !== "object" || !(name in fixture))
    throw Error(`Missing Rust fixture ${name}`);
  return Reflect.get(fixture, name);
}
const config = decodeConfig(field("config"));
const lineage = ancestry(field("ancestry"));

for (const returnNavigationKey of ["saved-list", undefined, 123, ""]) {
  it(`returns from the execution breadcrumb with only a valid saved list key: ${String(returnNavigationKey)}`, async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(stringifyUserJson(lineage), {
        headers: {
          "Content-Type": "application/json",
          "Ledgence-Console-Contract": "4",
          "Ledgence-Instance-Id": config.instance_id,
        },
      }),
    );
    const client = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });
    clients.push(client);
    const router = createMemoryRouter(
      [
        {
          path: "*",
          element: (
            <ExecutionContext
              kind={lineage.execution.kind}
              id={lineage.execution.id}
            />
          ),
        },
      ],
      {
        initialEntries: [
          {
            pathname: "/workflows/current",
            state: {
              returnTo: "/executions?state=failed",
              returnNavigationKey,
            },
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
    await view.getByRole("link", { name: "Executions", exact: true }).click();
    expect(router.state.location.pathname + router.state.location.search).toBe(
      "/executions?state=failed",
    );
    expect(router.state.location.state).toEqual(
      returnNavigationKey === "saved-list"
        ? { restoreNavigationKey: "saved-list" }
        : null,
    );
  });
}

for (const kind of ["task", "workflow"] as const) {
  it(`uses a safe list fallback after canonicalizing a directly opened ${kind}`, async () => {
    const client = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });
    clients.push(client);
    vi.spyOn(globalThis, "fetch").mockRejectedValue(
      new Error("Ancestry unavailable"),
    );
    const path = kind === "task" ? "/executions/direct" : "/workflows/direct";
    const router = createMemoryRouter(
      [
        {
          element: (
            <NavigationHistoryProvider>
              <Outlet />
            </NavigationHistoryProvider>
          ),
          children: [
            { path: "/executions", element: <h1>Execution list</h1> },
            {
              path,
              element: (
                <>
                  <ExecutionContext kind={kind} id="direct" />
                  <DetailPanels kind={kind}>
                    {() => <p>Detail contents</p>}
                  </DetailPanels>
                </>
              ),
            },
          ],
        },
      ],
      { initialEntries: [path] },
    );
    routers.push(router);
    const view = await render(
      <QueryClientProvider client={client}>
        <InstanceContext.Provider value={config}>
          <RouterProvider router={router} />
        </InstanceContext.Provider>
      </QueryClientProvider>,
    );
    await expect.poll(() => router.state.location.search).toContain("tab=");
    expect(router.state.location.key).not.toBe("default");
    await view.getByRole("button", { name: "Back", exact: true }).click();
    await expect
      .element(view.getByRole("heading", { name: "Execution list" }))
      .toBeVisible();
    // A real in-app push creates a predecessor; canonical replacement preserves it.
    await router.navigate(path);
    await expect.poll(() => router.state.location.search).toContain("tab=");
    await view.getByRole("button", { name: "Back", exact: true }).click();
    await expect
      .element(view.getByRole("heading", { name: "Execution list" }))
      .toBeVisible();
    // Forward/Back continue to use the recorded stack instead of key heuristics.
    await router.navigate(1);
    await expect
      .element(view.getByText("Detail contents", { exact: true }))
      .toBeVisible();
    await view.getByRole("button", { name: "Back", exact: true }).click();
    await expect
      .element(view.getByRole("heading", { name: "Execution list" }))
      .toBeVisible();
  });
}

it.each([
  [
    "/executions?kind=workflow&state=failed",
    "/executions?kind=workflow&state=failed",
  ],
  ["/workflows?state=failed", "/workflows?state=failed"],
  ["https://example.invalid/executions", "/executions"],
  ["//example.invalid/executions", "/executions"],
  ["/executions/another-task", "/executions"],
  ["/programs", "/executions"],
  [null, "/executions"],
])(
  "restores only a validated saved list destination after reload: %s",
  async (returnTo, expected) => {
    const client = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });
    clients.push(client);
    vi.spyOn(globalThis, "fetch").mockRejectedValue(
      new Error("Ancestry unavailable"),
    );
    const router = createMemoryRouter(
      [
        {
          element: (
            <NavigationHistoryProvider>
              <Outlet />
            </NavigationHistoryProvider>
          ),
          children: [
            { path: "/executions", element: <h1>Execution list</h1> },
            { path: "/workflows", element: <h1>Execution list</h1> },
            {
              path: "/executions/reloaded",
              element: (
                <>
                  <ExecutionContext kind="task" id="reloaded" />
                  <DetailPanels kind="task">
                    {() => <p>Reloaded detail</p>}
                  </DetailPanels>
                </>
              ),
            },
          ],
        },
      ],
      {
        initialEntries: [
          {
            pathname: "/executions/reloaded",
            key: "persisted-detail-key",
            state: { returnTo, returnNavigationKey: "saved-filtered-list" },
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
    await expect.poll(() => router.state.location.search).toContain("tab=");
    await view.getByRole("button", { name: "Back", exact: true }).click();
    await expect
      .element(view.getByRole("heading", { name: "Execution list" }))
      .toBeVisible();
    expect(router.state.location.pathname + router.state.location.search).toBe(
      expected,
    );
    expect(router.state.location.state).toEqual(
      returnTo === expected
        ? { restoreNavigationKey: "saved-filtered-list" }
        : null,
    );
  },
);
