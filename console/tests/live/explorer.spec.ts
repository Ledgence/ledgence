// SPDX-License-Identifier: MIT
// Real PostgreSQL/orchestrator/worker evidence; no mocked requests.
import { expect, test, type APIRequestContext } from "@playwright/test";
import { executionPage, workflowExplorer } from "../../src/api/explorer";
import { parseUserJson } from "../../src/api/json";
import {
  evidenceEdges,
  graphEdges,
  recordedRelations,
} from "../../src/features/explorer-model";
async function get<T>(
  request: APIRequestContext,
  path: string,
  decode: (value: unknown) => T,
) {
  const response = await request.get(`/v1/console/${path}`);
  expect(response.ok()).toBe(true);
  return decode(parseUserJson(await response.text(), 2 * 1024 * 1024));
}
test("real fork graph distinguishes parent locals, explores review, and restores selection", async ({
  page,
  request,
}, info) => {
  const executions = await get(
    request,
    "executions?correlation_key=explorer-native-fork&limit=25",
    executionPage,
  );
  const root = executions.items.find(
    (item) => item.kind === "workflow" && !item.parent_workflow_id,
  );
  if (!root) throw Error("The native explorer fork/join seed is required.");
  const evidence = await get(
    request,
    `workflows/explorer?workflow_id=${encodeURIComponent(root.id)}&limit=100`,
    workflowExplorer,
  );
  const local = evidence.page.items.find(
    (node) => node.kind === "local" && node.key === "tests:0",
  );
  const review = evidence.page.items.find(
    (node) => node.kind === "child" && node.key === "review:0",
  );
  const fork = evidence.page.items.find((node) => node.kind === "fork");
  if (!local || review?.kind !== "child" || fork?.kind !== "fork")
    throw Error("Native local/fork/subworkflow evidence is missing.");
  expect(fork.branch_keys).toEqual(["review:0"]);
  expect(local.activation_id).toBe(fork.activation_id);
  const localInvocation = local.relations.find(
    (relation) =>
      relation.kind === "invokes" && relation.target.kind === "local",
  );
  expect(localInvocation).toBeDefined();
  expect(localInvocation?.source).toEqual({
    kind: "entrypoint",
    activation_id: local.activation_id,
  });
  expect(localInvocation?.target).toEqual({
    kind: "local",
    activation_id: local.activation_id,
    key: "tests:0",
  });
  // A real one-record API page preserves the relation even without its source.
  let cursor: string | null = null;
  let checkedLocalPage = false;
  for (let index = 0; index < evidence.page.items.length; index++) {
    const query = new URLSearchParams({ workflow_id: root.id, limit: "1" });
    if (cursor) query.set("cursor", cursor);
    const partial = await get(
      request,
      `workflows/explorer?${query}`,
      workflowExplorer,
    );
    expect(partial.page.items).toHaveLength(1);
    if (partial.page.items[0]?.id === local.id) {
      expect(partial.page.items[0].relations).toContainEqual(localInvocation);
      expect(recordedRelations(partial.page.items)).toContainEqual(
        expect.objectContaining({ source: null, evidenceIds: [local.id] }),
      );
      expect(evidenceEdges(partial.page.items)).toEqual([]);
      checkedLocalPage = true;
      break;
    }
    cursor = partial.page.next_cursor;
    if (!cursor) break;
  }
  expect(checkedLocalPage).toBe(true);
  const errors: string[] = [];
  page.on("pageerror", (error) => errors.push(error.message));
  await page.setViewportSize({ width: 1440, height: 1050 });
  await page.goto(
    `/console/workflows/${encodeURIComponent(root.id)}?view=graph&explorer_limit=100`,
  );
  await expect(page.locator(".graph-node")).toHaveCount(
    evidence.page.items.length,
  );
  await expect
    .poll(async () =>
      (
        await page
          .locator("[data-edge-id]")
          .evaluateAll((edges) =>
            edges.map((edge) => edge.getAttribute("data-edge-id")),
          )
      ).sort(),
    )
    .toEqual(
      graphEdges(evidenceEdges(evidence.page.items))
        .map((edge) => edge.id)
        .sort(),
    );
  expect(
    await page
      .locator("[data-edge-id]")
      .evaluateAll((edges) =>
        edges.map((edge) => edge.getAttribute("data-edge-id")),
      ),
  ).toContain(localInvocation!.id);
  const geometry = await page.locator(".workflow-flow").evaluate((graph) => {
    const cards = Array.from(
      graph.querySelectorAll<HTMLElement>(".graph-node"),
    ).map((card) => ({
      id: card.dataset.focusKey?.slice(5),
      x: Number(card.dataset.positionX),
      y: Number(card.dataset.positionY),
      width: Number.parseFloat(card.style.width),
      height: Number.parseFloat(card.style.height),
    }));
    const intersections: string[] = [];
    for (const edge of graph.querySelectorAll<SVGGElement>(
      "g[data-edge-from]",
    )) {
      const points = Array.from(
        (edge.querySelector("path")?.getAttribute("d") ?? "").matchAll(
          /[ML](-?[\d.]+),(-?[\d.]+)/g,
        ),
        (match) => ({ x: Number(match[1]), y: Number(match[2]) }),
      );
      for (let index = 1; index < points.length; index++) {
        const a = points[index - 1]!;
        const b = points[index]!;
        for (const card of cards) {
          if (
            card.id === edge.dataset.edgeFrom ||
            card.id === edge.dataset.edgeTo
          )
            continue;
          const crosses =
            a.x === b.x
              ? a.x > card.x &&
                a.x < card.x + card.width &&
                Math.max(a.y, b.y) > card.y &&
                Math.min(a.y, b.y) < card.y + card.height
              : a.y > card.y &&
                a.y < card.y + card.height &&
                Math.max(a.x, b.x) > card.x &&
                Math.min(a.x, b.x) < card.x + card.width;
          if (crosses)
            intersections.push(
              `${edge.dataset.edgeFrom}→${edge.dataset.edgeTo} crosses ${card.id}`,
            );
        }
      }
    }
    const canvas = graph.parentElement!;
    return { intersections, fits: canvas.scrollWidth <= canvas.clientWidth };
  });
  expect(geometry.intersections).toEqual([]);
  expect(geometry.fits).toBe(true);
  await page
    .getByRole("button", {
      name: "tests:0 · Local step · accepted",
      exact: true,
    })
    .click();
  await expect(
    page
      .getByRole("complementary", { name: "Selected work details" })
      .getByRole("heading", { name: "tests:0", exact: true }),
  ).toBeVisible();
  await expect(
    page
      .getByRole("complementary", { name: "Selected work details" })
      .getByText("Callable observation", { exact: true }),
  ).toBeVisible();
  // The canvas pans independently of document scrolling. Bring the complete
  // loaded graph into view after the inspector changes its available width.
  await page.getByRole("button", { name: "Fit", exact: true }).click();
  await page
    .getByRole("button", {
      name: "review:0 · Subworkflow · succeeded",
      exact: true,
    })
    .click();
  await page.getByRole("button", { name: "Trace", exact: true }).click();
  // The browser must bound this completed, short run by execution evidence,
  // even though the page is observed much later during the browser suite.
  const renderedEnd = Number(
    await page.locator(".execution-timeline").getAttribute("data-timeline-end"),
  );
  const renderedStart = Number(
    await page
      .locator(".execution-timeline")
      .getAttribute("data-timeline-start"),
  );
  if (root.terminal_at === null)
    throw Error(
      "The native workflow must be terminal before browser inspection.",
    );
  expect(renderedEnd).toBeLessThanOrEqual(root.terminal_at);
  expect(renderedStart).toBeGreaterThanOrEqual(root.submitted_at);
  expect(renderedEnd - renderedStart).toBeLessThanOrEqual(
    root.terminal_at - root.submitted_at,
  );
  await expect(page.locator(".timeline-row").first()).toContainText(
    "Entrypoint",
  );
  await expect(
    page
      .getByRole("complementary", { name: "Selected work details" })
      .getByRole("heading", { name: "review:0", exact: true }),
  ).toBeVisible();
  const parentUrl = page.url();
  await page.getByRole("link", { name: "Open workflow", exact: true }).click();
  await expect(page).toHaveURL(
    new RegExp(encodeURIComponent(review.execution.id)),
  );
  await expect(
    page.getByRole("link", { name: "Up to parent", exact: true }),
  ).toBeVisible();
  await page.getByRole("button", { name: "Back", exact: true }).click();
  await expect(page).toHaveURL(parentUrl);
  await expect(
    page.getByRole("button", { name: "Trace", exact: true }),
  ).toHaveAttribute("aria-current", "page");
  await expect(
    page
      .getByRole("complementary", { name: "Selected work details" })
      .getByRole("heading", { name: "review:0", exact: true }),
  ).toBeVisible();
  // Up also works from a fresh deep link, without a prior parent visit.
  await page.goto(
    `/console/workflows/${encodeURIComponent(review.execution.id)}?view=graph`,
  );
  await page.getByRole("link", { name: "Up to parent", exact: true }).click();
  await expect(page).toHaveURL(new RegExp(encodeURIComponent(root.id)));
  for (const width of [375, 1440])
    for (const theme of ["light", "dark"]) {
      await page.setViewportSize({ width, height: 1050 });
      await page
        .getByRole("combobox", { name: "Appearance" })
        .selectOption(theme);
      await page
        .getByRole("button", {
          name: width === 375 ? "Trace" : "Graph",
          exact: true,
        })
        .click();
      expect(
        await page.evaluate(
          () => document.documentElement.scrollWidth <= innerWidth,
        ),
      ).toBe(true);
      await page.screenshot({
        path: info.outputPath(`explorer-${width}-${theme}.png`),
        fullPage: false,
        animations: "disabled",
      });
    }
  expect(errors).toEqual([]);
  await expect(page.getByRole("alert")).toHaveCount(0);
});

test("real four-branch release navigates one workflow level through review and publication", async ({
  page,
  request,
}, info) => {
  const executions = await get(
    request,
    "executions?correlation_key=explorer-native-fork4&limit=25",
    executionPage,
  );
  const root = executions.items.find(
    (item) => item.kind === "workflow" && !item.parent_workflow_id,
  );
  if (!root)
    throw Error("The real four-branch release acceptance seed is required.");
  const evidence = await get(
    request,
    `workflows/explorer?workflow_id=${encodeURIComponent(root.id)}&limit=100`,
    workflowExplorer,
  );
  const nodes = evidence.page.items;
  expect(evidence.page.next_cursor).toBeNull();
  const fork = nodes.find((node) => node.kind === "fork");
  if (fork?.kind !== "fork") throw Error("Recorded fork is required.");
  expect(new Set(fork.branch_keys)).toEqual(
    new Set(["security:0", "tests:0", "dependencies:0", "docs:0"]),
  );
  expect(
    nodes
      .filter((node) => node.kind === "entrypoint")
      .map((node) => node.entrypoint),
  ).toEqual([
    "start",
    "after_prepare",
    "publish_draft",
    "review",
    "publish_report",
    "finish",
  ]);
  const preparation = nodes.find(
    (node) => node.kind === "child" && node.key === "prepare:0",
  );
  if (preparation?.kind !== "child")
    throw Error("Independent parent task is required.");
  expect(preparation.fork_key).toBeNull();
  expect(
    evidenceEdges(nodes).some(
      (edge) => edge.from === fork.id && edge.to === preparation.id,
    ),
  ).toBe(false);
  const errors: string[] = [];
  page.on("pageerror", (error) => errors.push(error.message));
  await page.setViewportSize({ width: 1440, height: 1050 });
  await page.goto(
    `/console/workflows/${encodeURIComponent(root.id)}?tab=Graph&explorer_limit=100`,
  );
  await expect(page.locator(".react-flow__node")).toHaveCount(nodes.length);
  await expect
    .poll(async () =>
      (
        await page
          .locator("[data-edge-id]")
          .evaluateAll((edges) =>
            edges.map((edge) => edge.getAttribute("data-edge-id")),
          )
      ).sort(),
    )
    .toEqual(
      graphEdges(evidenceEdges(nodes))
        .map((edge) => edge.id)
        .sort(),
    );
  await expect(page.locator("[data-obstructed]")).toHaveCount(0);
  expect(await page.locator(".graph-phase-area").count()).toBe(0);
  await page
    .getByRole("button", { name: "Expand to full screen", exact: true })
    .click();
  await page.getByRole("button", { name: "Fit", exact: true }).click();
  await page.screenshot({
    path: info.outputPath("real-release-fork4-fullscreen.png"),
    animations: "disabled",
  });
  await page.keyboard.press("Escape");
  for (const branch of nodes.filter(
    (node) => node.kind === "child" && node.fork_key === fork.key,
  )) {
    if (branch.kind !== "child") continue;
    const card = page.getByRole("button", {
      name: `${branch.key} · Subworkflow · succeeded`,
      exact: true,
    });
    await card.focus();
    await card.press("Enter");
    await page
      .getByRole("link", { name: "Open workflow", exact: true })
      .click();
    await expect(page).toHaveURL(
      new RegExp(encodeURIComponent(branch.execution.id)),
    );
    const child = await get(
      request,
      `workflows/explorer?workflow_id=${encodeURIComponent(branch.execution.id)}&limit=100`,
      workflowExplorer,
    );
    await expect(page.locator(".react-flow__node")).toHaveCount(
      child.page.items.length,
    );
    expect(
      child.page.items.every(
        (node) => !nodes.some((parent) => parent.id === node.id),
      ),
    ).toBe(true);
    await page.getByRole("link", { name: "Up to parent", exact: true }).click();
    await expect(page).toHaveURL(new RegExp(encodeURIComponent(root.id)));
    await expect(page.locator(".react-flow__node")).toHaveCount(nodes.length);
  }
  await page.getByRole("button", { name: "Trace", exact: true }).click();
  await expect(page.locator(".execution-timeline")).toBeVisible();
  await page.getByRole("button", { name: "General", exact: true }).click();
  await page.getByRole("button", { name: "Output", exact: true }).click();
  await expect(
    page.getByRole("heading", { name: "Workflow succeeded", exact: true }),
  ).toBeVisible();
  await expect(
    page.locator("pre").filter({ hasText: "report-2026.09" }),
  ).toBeVisible();
  expect(errors).toEqual([]);
});
