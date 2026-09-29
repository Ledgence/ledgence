// SPDX-License-Identifier: MIT
import { afterEach, expect, it } from "vitest";
import { render } from "vitest-browser-react";
import { createMemoryRouter, RouterProvider } from "react-router";
import raw from "../../../crates/ledgence-orchestration-api/tests/fixtures/console-v3.json?raw";
import { parseUserJson } from "../../src/api/json";
import { workflowExplorer } from "../../src/api/explorer";
import {
  readExplorerState,
  saveExplorerState,
} from "../../src/app/navigation-state";
import { NodeInspector } from "../../src/features/explorer/inspector";

const routers: ReturnType<typeof createMemoryRouter>[] = [];
afterEach(() => {
  for (const router of routers.splice(0)) router.dispose();
});

it("keeps the Explorer page, selected wait and camera restoration when opening wait actions", async () => {
  const fixture = parseUserJson(raw, 2 * 1024 * 1024);
  if (!fixture || typeof fixture !== "object")
    throw new Error("Missing Rust fixture");
  const cases = Reflect.get(fixture, "explorer_cases");
  if (!cases || typeof cases !== "object")
    throw new Error("Missing Rust Explorer cases");
  const explorer = workflowExplorer(Reflect.get(cases, "external_waits"));
  const node = explorer.page.items.find(
    (item) => item.kind === "external_wait",
  )!;
  const workflowId = explorer.workflow.summary.workflow.workflow_id;
  const path = `/workflows/${encodeURIComponent(workflowId)}`;
  const cursor = "page-2+/opaque==";
  const params = new URLSearchParams({
    tab: "Graph",
    explorer_cursor: cursor,
    explorer_limit: "25",
    node: node.id,
    cursor: "unrelated-panel-cursor",
  });
  const originalKey = "inspector-explorer-page-two";
  const returnTo = "/executions?kind=workflow&state=waiting";
  const presentation = {
    graph: {
      scope: `${workflowId}:${cursor}`,
      positions: { [node.id]: { x: 410, y: 280 } },
      viewport: { x: -120, y: -240, zoom: 0.8 },
    },
    start: "",
    end: "",
    search: "approval",
    follow: false,
  };
  saveExplorerState(originalKey, presentation);
  const router = createMemoryRouter(
    [
      {
        path: "*",
        element: <NodeInspector node={node} workflowId={workflowId} />,
      },
    ],
    {
      initialEntries: [
        {
          pathname: path,
          search: `?${params}`,
          key: originalKey,
          state: { returnTo },
        },
      ],
    },
  );
  routers.push(router);
  const view = await render(<RouterProvider router={router} />);
  await view
    .getByRole("link", { name: "Inspect waits and available actions" })
    .click();

  const destination = router.state.location;
  const query = new URLSearchParams(destination.search);
  expect(destination.pathname).toBe(path);
  expect(query.get("tab")).toBe("General");
  expect(query.get("section")).toBe("waits");
  expect(query.get("explorer_cursor")).toBe(cursor);
  expect(query.get("explorer_limit")).toBe("25");
  expect(query.get("node")).toBe(node.id);
  expect(query.has("cursor")).toBe(false);
  expect(destination.state).toEqual({
    returnTo,
    restoreNavigationKey: originalKey,
  });
  expect(readExplorerState(destination.key, destination.state)).toEqual(
    presentation,
  );
});
