// SPDX-License-Identifier: MIT
import { afterEach, expect, it, vi } from "vitest";
import { cleanup, render } from "vitest-browser-react";
import { createMemoryRouter, RouterProvider } from "react-router";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import raw from "../../../crates/ledgence-orchestration-api/tests/fixtures/console-v4.json?raw";
import { NavigationMemory } from "../../src/app/navigation";
import { positions, recentPaths } from "../../src/app/navigation-state";
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
