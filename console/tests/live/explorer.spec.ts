// SPDX-License-Identifier: MIT
// Real PostgreSQL/orchestrator/worker evidence; no mocked requests.
import { expect, test, type APIRequestContext } from "@playwright/test";
import { executionPage, workflowExplorer } from "../../src/api/explorer";
import { parseUserJson } from "../../src/api/json";
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
  const errors: string[] = [];
  page.on("pageerror", (error) => errors.push(error.message));
  await page.setViewportSize({ width: 1440, height: 1050 });
  await page.goto(
    `/console/workflows/${encodeURIComponent(root.id)}?view=graph&explorer_limit=100`,
  );
  await expect(page.locator(".graph-node")).toHaveCount(
    evidence.page.items.length,
  );
  const geometry = await page.locator(".semantic-graph").evaluate((graph) => {
    const cards = Array.from(
      graph.querySelectorAll<HTMLElement>(".graph-node"),
    ).map((card) => ({
      id: card.dataset.focusKey?.slice(5),
      x: Number.parseFloat(card.style.left),
      y: Number.parseFloat(card.style.top),
      width: Number.parseFloat(card.style.width),
      height: Number.parseFloat(card.style.height),
    }));
    const intersections: string[] = [];
    for (const edge of graph.querySelectorAll<SVGPathElement>(
      "path[data-edge-from]",
    )) {
      const points = Array.from(
        (edge.getAttribute("d") ?? "").matchAll(/[ML]([\d.]+),([\d.]+)/g),
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
  await page
    .getByRole("button", {
      name: "review:0 · Subworkflow · succeeded",
      exact: true,
    })
    .click();
  await page.getByRole("button", { name: "Timeline", exact: true }).click();
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
    "Controller phase",
  );
  await expect(
    page
      .getByRole("complementary", { name: "Selected work details" })
      .getByRole("heading", { name: "review:0", exact: true }),
  ).toBeVisible();
  const parentUrl = page.url();
  await page.getByRole("link", { name: "Open execution", exact: true }).click();
  await expect(page).toHaveURL(
    new RegExp(encodeURIComponent(review.execution.id)),
  );
  await expect(
    page.getByRole("link", { name: "Up to parent", exact: true }),
  ).toBeVisible();
  await page.getByRole("button", { name: "Back", exact: true }).click();
  await expect(page).toHaveURL(parentUrl);
  await expect(
    page.getByRole("button", { name: "Timeline", exact: true }),
  ).toHaveAttribute("aria-pressed", "true");
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
          name: width === 375 ? "Timeline" : "Graph",
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
