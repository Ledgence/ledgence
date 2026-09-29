// SPDX-License-Identifier: MIT
import { readFileSync } from "node:fs";
import { expect, test, type Page, type TestInfo } from "@playwright/test";
import { parseUserJson, stringifyUserJson } from "../../src/api/json";
import { decodeConfig } from "../../src/api/codecs";
import { ancestry, workflowExplorer } from "../../src/api/explorer";

const browserLogs = new WeakMap<Page, string[]>();
test.beforeEach(async ({ page }) => {
  const logs: string[] = [];
  browserLogs.set(page, logs);
  page.on("console", (message) =>
    logs.push(`${message.type()}: ${message.text()}`),
  );
  page.on("pageerror", (error) => logs.push(`pageerror: ${error.message}`));
});
test.afterEach(async ({ page }, testInfo) => {
  if (testInfo.status === testInfo.expectedStatus) return;
  const geometry = await page.evaluate(() => ({
    viewport: document.querySelector<HTMLElement>(".react-flow__viewport")
      ?.style.transform,
    edges: document.querySelectorAll(".react-flow__edge").length,
    nodes: [...document.querySelectorAll<HTMLElement>(".react-flow__node")].map(
      (node) => ({
        id: node.dataset.id,
        position: [node.dataset.positionX, node.dataset.positionY],
        rect: node.getBoundingClientRect().toJSON(),
        offset: [node.offsetWidth, node.offsetHeight],
        visibility: getComputedStyle(node).visibility,
        handles: [
          ...node.querySelectorAll<HTMLElement>(".react-flow__handle"),
        ].map((handle) => ({
          html: handle.outerHTML,
          rect: handle.getBoundingClientRect().toJSON(),
          offset: [handle.offsetWidth, handle.offsetHeight],
        })),
      }),
    ),
  }));
  await testInfo.attach("graph-diagnostics", {
    body: JSON.stringify({ logs: browserLogs.get(page), geometry }, null, 2),
    contentType: "application/json",
  });
  await page.screenshot({
    path: testInfo.outputPath("graph-failure.png"),
    fullPage: true,
  });
});

const raw = readFileSync(
  new URL(
    "../../../crates/ledgence-orchestration-api/tests/fixtures/console-v4.json",
    import.meta.url,
  ),
  "utf8",
);
const fixture = parseUserJson(raw, 2 * 1024 * 1024);
function field(name: string): unknown {
  if (!fixture || typeof fixture !== "object" || !(name in fixture))
    throw Error(`Missing Rust fixture ${name}`);
  return Reflect.get(fixture, name);
}
function branch() {
  const cases = field("explorer_cases");
  if (!cases || typeof cases !== "object")
    throw Error("Missing Rust explorer cases");
  return workflowExplorer(Reflect.get(cases, "branch_security"));
}
const parent = workflowExplorer(field("explorer"));
const parentId = parent.workflow.summary.workflow.workflow_id;
const branchId = branch().workflow.summary.workflow.workflow_id;
const parentUrl = `/console/workflows/${encodeURIComponent(parentId)}`;
const config = decodeConfig(field("config"));
const headers = {
  "Content-Type": "application/json",
  "Ledgence-Console-Contract": "4",
  "Ledgence-Instance-Id": config.instance_id,
};

async function mount(
  page: Page,
  delayedObservation = false,
  initialVisible = Infinity,
) {
  const explorerReads: string[] = [];
  let settled = !delayedObservation;
  let visibleNodes = initialVisible;
  await page.route("**/v1/console/**", async (route) => {
    const url = new URL(route.request().url());
    const path = url.pathname.replace("/v1/console/", "");
    const id =
      url.searchParams.get("workflow_id") ??
      url.searchParams.get("id") ??
      parentId;
    let body: unknown;
    if (path === "config")
      body = {
        ...config,
        polling: { ...config.polling, waiting_workflow_ms: 60000 },
      };
    else if (path === "workflows/explorer" || path === "workflows/inspect") {
      if (id !== parentId && id !== branchId)
        throw Error(`Unexpected workflow scope ${id}`);
      const snapshot =
        id === branchId ? branch() : workflowExplorer(field("explorer"));
      // This controlled delayed child observation exercises UI refresh only.
      // The canonical Rust fixture supplies every identity and relationship;
      // this mutation is not evidence of real orchestration behavior.
      if (!settled && id === parentId) {
        const docs = snapshot.page.items.find(
          (node) => node.kind === "child" && node.key === "docs:0",
        );
        if (!docs || docs.kind !== "child")
          throw Error("Canonical docs branch missing");
        docs.state = "running";
        docs.terminal_at = null;
      }
      if (path === "workflows/explorer") {
        // Reveal retained records in controlled updates to test presentation
        // stability. Every revealed node is from the canonical Rust fixture.
        if (id === parentId)
          snapshot.page.items = snapshot.page.items.slice(0, visibleNodes);
        explorerReads.push(id);
        body = snapshot;
      } else body = snapshot.workflow;
    } else if (path === "executions/ancestry") {
      const value = ancestry(field("ancestry"));
      body =
        id === branchId
          ? value
          : {
              ...value,
              execution: value.path[0]!.execution,
              path: value.path.slice(0, 1),
            };
    } else throw Error(`Unexpected Console read ${path}`);
    await route.fulfill({
      status: 200,
      headers,
      body: stringifyUserJson(body),
    });
  });
  return {
    explorerReads,
    settle: () => {
      settled = true;
    },
    reveal: (count: number) => {
      visibleNodes = count;
    },
  };
}
async function openGraph(page: Page) {
  await page.goto(`${parentUrl}?tab=Graph`);
  await expect(page.locator(".react-flow__node")).toHaveCount(20);
  await expect(page.locator("[data-edge-id]")).toHaveCount(27);
  await expect(
    page.getByRole("button", {
      name: "start · Entrypoint · succeeded",
      exact: true,
    }),
  ).toBeVisible();
}
function positions(page: Page) {
  return page.locator(".react-flow__node").evaluateAll((nodes) =>
    Object.fromEntries(
      nodes.map((node) => [
        node.getAttribute("data-id"),
        {
          x: node.getAttribute("data-position-x"),
          y: node.getAttribute("data-position-y"),
        },
      ]),
    ),
  );
}
function viewport(page: Page) {
  return page
    .locator(".react-flow__viewport")
    .evaluate((node) => (node as HTMLElement).style.transform);
}
async function shot(page: Page, testInfo: TestInfo, name: string) {
  const path = testInfo.outputPath(`${name}.png`);
  await page.screenshot({ path, fullPage: true, animations: "disabled" });
  await testInfo.attach(name, { path, contentType: "image/png" });
}

for (const theme of ["light", "dark"] as const) {
  test(`canonical fork4 graph at desktop in ${theme}`, async ({
    page,
  }, testInfo) => {
    await mount(page);
    await page.setViewportSize({ width: 1440, height: 1080 });
    await openGraph(page);
    await page
      .getByRole("combobox", { name: "Appearance", exact: true })
      .selectOption(theme);
    await expect(page.locator(".react-flow__node-work.selected")).toHaveCount(
      0,
    );
    await expect(page.locator('[data-obstructed="true"]')).toHaveCount(0);
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth,
      ),
    ).toBe(true);
    await shot(page, testInfo, `fork4-desktop-${theme}`);
    await page
      .getByRole("button", { name: "Expand to full screen", exact: true })
      .click();
    await page.getByRole("button", { name: "Fit", exact: true }).click();
    await expect
      .poll(() =>
        page.locator(".react-flow__node").evaluateAll((nodes) => {
          const tools = document
            .querySelector(".graph-tools")!
            .getBoundingClientRect();
          return nodes.every((node) => {
            const box = node.getBoundingClientRect();
            return (
              box.right <= tools.left ||
              box.left >= tools.right ||
              box.bottom <= tools.top ||
              box.top >= tools.bottom
            );
          });
        }),
      )
      .toBe(true);
    await shot(page, testInfo, `fork4-fullscreen-${theme}`);
    await page.keyboard.press("Escape");
    await expect(
      page.getByRole("button", { name: "Expand to full screen", exact: true }),
    ).toBeFocused();
  });
}

test("canonical fork4 offers a bounded mobile graph and a readable Trace alternative", async ({
  page,
}, testInfo) => {
  await mount(page);
  await page.setViewportSize({ width: 390, height: 844 });
  await openGraph(page);
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= innerWidth,
    ),
  ).toBe(true);
  await expect
    .poll(async () => {
      const graph = await page.locator(".workflow-flow").boundingBox();
      const start = await page
        .getByRole("button", {
          name: "start · Entrypoint · succeeded",
          exact: true,
        })
        .boundingBox();
      return (
        !!graph &&
        !!start &&
        start.x >= graph.x &&
        start.x + start.width <= graph.x + graph.width &&
        start.y >= graph.y &&
        start.y + start.height <= graph.y + graph.height
      );
    })
    .toBe(true);
  await shot(page, testInfo, "fork4-mobile-graph");
  await page.getByRole("button", { name: "Trace", exact: true }).click();
  await expect(
    page.getByLabel("Trace work rows", { exact: true }),
  ).toBeVisible();
  await expect(page.locator(".timeline-row")).not.toHaveCount(0);
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= innerWidth,
    ),
  ).toBe(true);
  await shot(page, testInfo, "fork4-mobile-trace");
});

test("mobile Trace to General to Graph initializes a readable canvas on first opening", async ({
  page,
}) => {
  await mount(page);
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto(parentUrl);
  await expect(
    page.getByRole("button", { name: "Trace", exact: true }),
  ).toHaveAttribute("aria-current", "page");
  await expect(
    page.getByLabel("Trace work rows", { exact: true }),
  ).toBeVisible();
  await expect(page.locator(".react-flow__node")).toHaveCount(0);
  await page.getByRole("button", { name: "General", exact: true }).click();
  await expect(
    page.getByRole("button", { name: "General", exact: true }),
  ).toHaveAttribute("aria-current", "page");
  await expect(
    page.getByRole("button", { name: "Workflow details", exact: true }),
  ).toBeVisible();
  await expect(page.locator(".react-flow__node")).toHaveCount(0);
  await page.getByRole("button", { name: "Graph", exact: true }).click();
  await expect(page.locator(".react-flow__node")).toHaveCount(20);
  await expect(page.locator("[data-edge-id]")).toHaveCount(27);
  await expect
    .poll(() =>
      page.locator(".react-flow__viewport").evaluate((node) => {
        const matrix = new DOMMatrixReadOnly(getComputedStyle(node).transform);
        return matrix.a >= 0.65 && matrix.d >= 0.65;
      }),
    )
    .toBe(true);
  const graph = await page.locator(".workflow-flow").boundingBox();
  const start = await page
    .getByRole("button", {
      name: "start · Entrypoint · succeeded",
      exact: true,
    })
    .boundingBox();
  if (!graph || !start) throw Error("First mobile graph has no visible bounds");
  expect(start.x).toBeGreaterThanOrEqual(graph.x);
  expect(start.y).toBeGreaterThanOrEqual(graph.y);
  expect(start.x + start.width).toBeLessThanOrEqual(graph.x + graph.width);
  expect(start.y + start.height).toBeLessThanOrEqual(graph.y + graph.height);
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= innerWidth,
    ),
  ).toBe(true);
});

test("dragging and panning survive observation refresh, view switches and fullscreen", async ({
  page,
}) => {
  const source = await mount(page, true);
  await page.setViewportSize({ width: 1440, height: 1080 });
  await openGraph(page);
  const initial = await positions(page);
  const start = page.getByRole("button", {
    name: "start · Entrypoint · succeeded",
    exact: true,
  });
  const box = await start.boundingBox();
  if (!box) throw Error("Start entrypoint has no bounds");
  await page.mouse.move(box.x + box.width / 2, box.y + box.height / 2);
  await page.mouse.down();
  await page.mouse.move(
    box.x + box.width / 2 + 72,
    box.y + box.height / 2 + 30,
    { steps: 12 },
  );
  await page.mouse.up();
  await expect.poll(() => positions(page)).not.toEqual(initial);
  const pane = await page.locator(".workflow-flow").evaluate((element) => {
    const box = element.getBoundingClientRect();
    for (const x of [0.05, 0.95, 0.1, 0.9])
      for (const y of [0.4, 0.6, 0.25]) {
        const point = { x: box.x + box.width * x, y: box.y + box.height * y };
        if (
          document
            .elementFromPoint(point.x, point.y)
            ?.classList.contains("react-flow__pane")
        )
          return point;
      }
    return null;
  });
  if (!pane) throw Error("No blank graph region available to pan");
  const cameraBeforePan = await viewport(page);
  await page.mouse.move(pane.x, pane.y);
  await page.mouse.down();
  await page.mouse.move(pane.x + 35, pane.y - 30, { steps: 8 });
  await page.mouse.up();
  await expect.poll(() => viewport(page)).not.toBe(cameraBeforePan);
  const placed = await positions(page);
  const camera = await viewport(page);
  source.settle();
  const reads = source.explorerReads.length;
  await page.getByRole("button", { name: "Refresh", exact: true }).click();
  await expect.poll(() => source.explorerReads.length).toBeGreaterThan(reads);
  await expect(
    page.getByRole("button", {
      name: "docs:0 · Subworkflow · succeeded",
      exact: true,
    }),
  ).toHaveCount(1);
  await expect(page.locator("[data-edge-id]")).toHaveCount(27);
  expect(await positions(page)).toEqual(placed);
  expect(await viewport(page)).toBe(camera);
  await page.getByRole("button", { name: "Trace", exact: true }).click();
  await page.getByRole("button", { name: "General", exact: true }).click();
  await page.getByRole("button", { name: "Graph", exact: true }).click();
  await expect(page.locator(".react-flow__node")).toHaveCount(20);
  await expect(page.locator("[data-edge-id]")).toHaveCount(27);
  expect(await positions(page)).toEqual(placed);
  expect(await viewport(page)).toBe(camera);
  await page
    .getByRole("button", { name: "Expand to full screen", exact: true })
    .click();
  expect(await viewport(page)).toBe(camera);
  await page.keyboard.press("Escape");
  await expect(page.locator("[data-edge-id]")).toHaveCount(27);
  expect(await positions(page)).toEqual(placed);
  expect(await viewport(page)).toBe(camera);
  await expect(
    page.getByRole("button", { name: "Expand to full screen", exact: true }),
  ).toBeFocused();
});

test("opening a subworkflow shows only that level and Up restores the parent canvas", async ({
  page,
}) => {
  const source = await mount(page);
  await page.setViewportSize({ width: 1440, height: 1080 });
  await openGraph(page);
  const security = page.getByRole("button", {
    name: "security:0 · Subworkflow · succeeded",
    exact: true,
  });
  await security.focus();
  await security.press("Enter");
  await expect(
    page.getByRole("link", { name: "Open workflow", exact: true }),
  ).toBeVisible();
  const placed = await positions(page);
  const camera = await viewport(page);
  await page.getByRole("link", { name: "Open workflow", exact: true }).click();
  await expect(page).toHaveURL(new RegExp(`/workflows/${branchId}`));
  await expect(page.locator(".react-flow__node")).toHaveCount(2);
  await expect(
    page.getByRole("button", {
      name: "security · Entrypoint · succeeded",
      exact: true,
    }),
  ).toHaveCount(1);
  await expect(
    page.getByRole("button", {
      name: "check:0 · Local step · accepted",
      exact: true,
    }),
  ).toHaveCount(1);
  await expect(
    page.getByRole("button", {
      name: "start · Entrypoint · succeeded",
      exact: true,
    }),
  ).toHaveCount(0);
  expect(source.explorerReads).toContain(branchId);
  expect(new Set(source.explorerReads)).toEqual(new Set([parentId, branchId]));
  await page.getByRole("link", { name: "Up to parent", exact: true }).click();
  await expect(page.locator(".react-flow__node")).toHaveCount(20);
  await expect(page.locator("[data-edge-id]")).toHaveCount(27);
  expect(await positions(page)).toEqual(placed);
  expect(await viewport(page)).toBe(camera);
  await expect(security).toHaveAttribute("aria-pressed", "true");
});

test("browser Back from full screen to General releases focus and scroll locks", async ({
  page,
}) => {
  await mount(page);
  await page.setViewportSize({ width: 1440, height: 1080 });
  await page.goto(`${parentUrl}?tab=General`);
  await expect(
    page.getByRole("button", { name: "General", exact: true }),
  ).toHaveAttribute("aria-current", "page");
  await page.getByRole("button", { name: "Graph", exact: true }).click();
  await expect(page.locator(".react-flow__node")).toHaveCount(20);
  const overflow = await page.evaluate(() => document.body.style.overflow);
  await page
    .getByRole("button", { name: "Expand to full screen", exact: true })
    .click();
  await expect(
    page.getByRole("dialog", { name: "Workflow execution", exact: true }),
  ).toBeVisible();
  await expect
    .poll(() => page.evaluate(() => document.body.style.overflow))
    .toBe("hidden");
  await page.goBack();
  await expect(
    page.getByRole("button", { name: "General", exact: true }),
  ).toHaveAttribute("aria-current", "page");
  await expect(
    page.getByRole("dialog", { name: "Workflow execution", exact: true }),
  ).toHaveCount(0);
  await expect
    .poll(() => page.evaluate(() => document.body.style.overflow))
    .toBe(overflow);
  await expect(page.locator("[inert]")).toHaveCount(0);
  const details = page.getByRole("button", {
    name: "Workflow details",
    exact: true,
  });
  await details.focus();
  await expect(details).toBeFocused();
  await details.press("Enter");
  await expect(details).toHaveAttribute("aria-expanded", "false");
});

test("two record-growth updates preserve manual and newly assigned positions", async ({
  page,
}) => {
  const source = await mount(page, false, 8);
  await page.setViewportSize({ width: 1440, height: 1080 });
  await page.goto(`${parentUrl}?tab=Graph`);
  await expect(page.locator(".react-flow__node")).toHaveCount(8);
  const original = await positions(page);
  const start = page.getByRole("button", {
    name: "start · Entrypoint · succeeded",
    exact: true,
  });
  const box = await start.boundingBox();
  if (!box) throw Error("Start entrypoint is not visible");
  await page.mouse.move(box.x + box.width / 2, box.y + box.height / 2);
  await page.mouse.down();
  await page.mouse.move(
    box.x + box.width / 2 + 60,
    box.y + box.height / 2 + 25,
    { steps: 10 },
  );
  await page.mouse.up();
  await expect.poll(() => positions(page)).not.toEqual(original);
  let placed = await positions(page);
  const camera = await viewport(page);
  for (const count of [12, 20]) {
    source.reveal(count);
    await page.getByRole("button", { name: "Refresh", exact: true }).click();
    await expect(page.locator(".react-flow__node")).toHaveCount(count);
    const expanded = await positions(page);
    for (const [id, point] of Object.entries(placed))
      expect(expanded[id]).toEqual(point);
    expect(await viewport(page)).toBe(camera);
    placed = expanded;
  }
  await expect(page.locator("[data-edge-id]")).toHaveCount(27);
});
