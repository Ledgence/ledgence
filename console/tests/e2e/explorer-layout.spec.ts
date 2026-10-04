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
    "../../../crates/ledgence-orchestration-api/tests/fixtures/console-v5.json",
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
function explorerCase(name: string) {
  const cases = field("explorer_cases");
  if (!cases || typeof cases !== "object")
    throw Error("Missing Rust explorer cases");
  return workflowExplorer(Reflect.get(cases, name));
}
function branch() {
  return explorerCase("branch_security");
}
const parent = workflowExplorer(field("explorer"));
const parentId = parent.workflow.summary.workflow.workflow_id;
const branchId = branch().workflow.summary.workflow.workflow_id;
const parentUrl = `/console/workflows/${encodeURIComponent(parentId)}`;
const config = decodeConfig(field("config"));
const headers = {
  "Content-Type": "application/json",
  "Ledgence-Console-Contract": "5",
  "Ledgence-Instance-Id": config.instance_id,
};

async function mount(
  page: Page,
  delayedObservation = false,
  initialVisible = Infinity,
  rootCase?: string,
) {
  const root = () =>
    rootCase ? explorerCase(rootCase) : workflowExplorer(field("explorer"));
  const rootId = root().workflow.summary.workflow.workflow_id;
  const explorerReads: string[] = [];
  const liveObservations =
    delayedObservation || Number.isFinite(initialVisible);
  let settled = !delayedObservation;
  let visibleNodes = initialVisible;
  await page.route("**/v1/console/**", async (route) => {
    const url = new URL(route.request().url());
    const path = url.pathname.replace("/v1/console/", "");
    const id =
      url.searchParams.get("workflow_id") ??
      url.searchParams.get("id") ??
      rootId;
    let body: unknown;
    if (path === "config")
      body = {
        ...config,
        polling: {
          ...config.polling,
          waiting_workflow_ms: liveObservations ? 1000 : 60000,
        },
      };
    else if (path === "workflows/explorer" || path === "workflows/inspect") {
      if (id !== rootId && id !== branchId)
        throw Error(`Unexpected workflow scope ${id}`);
      const snapshot = id === branchId ? branch() : root();
      // Keep controlled observations live so updates arrive through the real
      // server-configured polling path, without a manual Refresh control.
      if (id === rootId && liveObservations) {
        snapshot.workflow.summary.workflow.state = "running";
        snapshot.workflow.summary.workflow.terminal_at = null;
      }
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
              execution: { kind: "workflow", id: rootId },
              path: [
                {
                  ...value.path[0]!,
                  execution: { kind: "workflow", id: rootId },
                  program: root().workflow.summary.controller.program,
                },
              ],
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
async function expectNoManualWorkflowControls(page: Page) {
  await expect(
    page.getByRole("button", { name: "Refresh", exact: true }),
  ).toHaveCount(0);
  await expect(
    page.getByRole("combobox", { name: "Rows", exact: true }),
  ).toHaveCount(0);
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

// These product dimensions are deliberately independent of graphNodeSize so a
// uniform-card regression cannot make both the implementation and test pass.
const nodeDimensions = {
  entrypoint: [232, 64],
  child: [224, 100],
  local: [212, 84],
  fork: [200, 48],
  child_wait: [200, 48],
  external_wait: [224, 64],
} as const;

async function expectReadableGeometry(
  page: Page,
  snapshot: ReturnType<typeof workflowExplorer>,
) {
  const nodes = await page
    .locator(".react-flow__node")
    .evaluateAll((elements) =>
      elements.map((element) => {
        const node = element as HTMLElement;
        const card = node.querySelector<HTMLElement>(".work-card")!;
        const cardBox = card.getBoundingClientRect();
        return {
          id: node.dataset.id!,
          position: {
            x: Number(node.dataset.positionX),
            y: Number(node.dataset.positionY),
          },
          size: [node.offsetWidth, node.offsetHeight],
          cardSize: [card.offsetWidth, card.offsetHeight],
          clippedLabels: [
            ...card.querySelectorAll<HTMLElement>(
              "strong, .work-card-kind, .work-card-count, .work-card-status, .work-card-duration",
            ),
          ]
            .filter((label) => {
              const box = label.getBoundingClientRect();
              return (
                label.scrollWidth > label.clientWidth + 1 ||
                label.scrollHeight > label.clientHeight + 1 ||
                box.left < cardBox.left - 1 ||
                box.right > cardBox.right + 1 ||
                box.top < cardBox.top - 1 ||
                box.bottom > cardBox.bottom + 1
              );
            })
            .map((label) => label.textContent),
        };
      }),
    );
  expect(nodes).toHaveLength(snapshot.page.items.length);
  for (const node of nodes) {
    const record = snapshot.page.items.find((item) => item.id === node.id)!;
    expect(node.size, `${record.kind} wrapper size`).toEqual(
      nodeDimensions[record.kind],
    );
    expect(node.cardSize, `${record.kind} card size`).toEqual(
      nodeDimensions[record.kind],
    );
    expect(node.clippedLabels, `Clipped labels in ${node.id}`).toEqual([]);
    expect(
      Number.isFinite(node.position.x) && Number.isFinite(node.position.y),
    ).toBe(true);
  }
  for (let index = 0; index < nodes.length; index++) {
    const a = nodes[index]!;
    for (const b of nodes.slice(index + 1)) {
      const overlap =
        a.position.x < b.position.x + b.size[0]! &&
        a.position.x + a.size[0]! > b.position.x &&
        a.position.y < b.position.y + b.size[1]! &&
        a.position.y + a.size[1]! > b.position.y;
      expect(overlap, `Overlapping cards ${a.id} and ${b.id}`).toBe(false);
    }
  }
}

async function expectCompleteConnectors(page: Page, ids: string[]) {
  await expect(page.locator("[data-edge-id]")).toHaveCount(ids.length);
  const connections = await page
    .locator("[data-edge-id]")
    .evaluateAll((edges) => {
      const nodes = new Map(
        [...document.querySelectorAll<HTMLElement>(".react-flow__node")].map(
          (node) => [node.dataset.id, node],
        ),
      );
      return edges.map((edge) => {
        const from = nodes.get(edge.getAttribute("data-edge-from")!)!;
        const to = nodes.get(edge.getAttribute("data-edge-to")!)!;
        const path = edge.querySelector<SVGPathElement>(
          ".react-flow__edge-path",
        )!;
        const length = path.getTotalLength();
        const start = path.getPointAtLength(0);
        const end = path.getPointAtLength(length);
        const expectedStart = {
          x: Number(from.dataset.positionX) + from.offsetWidth / 2,
          y: Number(from.dataset.positionY) + from.offsetHeight,
        };
        const expectedEnd = {
          x: Number(to.dataset.positionX) + to.offsetWidth / 2,
          y: Number(to.dataset.positionY),
        };
        return {
          id: edge.getAttribute("data-edge-id"),
          connected:
            length > 0 &&
            Boolean(path.getAttribute("marker-end")) &&
            Math.abs(start.x - expectedStart.x) < 1 &&
            Math.abs(start.y - expectedStart.y) < 1 &&
            Math.abs(end.x - expectedEnd.x) < 1 &&
            Math.abs(end.y - expectedEnd.y) < 1,
        };
      });
    });
  expect(connections.map((edge) => edge.id).sort()).toEqual(ids);
  expect(connections.filter((edge) => !edge.connected)).toEqual([]);
  await expect(page.locator('[data-obstructed="true"]')).toHaveCount(0);
}

for (const scenario of [
  { label: "fork, join and child", fixture: undefined, edges: 27 },
  { label: "local step", fixture: "branch_security", edges: 1 },
  { label: "external waits", fixture: "external_waits", edges: 7 },
] as const) {
  test(`differentiated ${scenario.label} cards remain readable and connected after selection and Reorganize`, async ({
    page,
  }) => {
    await mount(page, false, Infinity, scenario.fixture);
    await page.setViewportSize({ width: 1440, height: 1080 });
    const snapshot = scenario.fixture ? explorerCase(scenario.fixture) : parent;
    const workflowId = snapshot.workflow.summary.workflow.workflow_id;
    await page.goto(
      `/console/workflows/${encodeURIComponent(workflowId)}?tab=Graph`,
    );
    await expect(page.locator(".react-flow__node")).toHaveCount(
      snapshot.page.items.length,
    );
    await expect(page.locator("[data-edge-id]")).toHaveCount(scenario.edges);
    // Check the cards near their natural size, rather than hiding clipping in
    // a Fit view that scales a long workflow down to a thumbnail.
    const zoom = () =>
      page
        .locator(".react-flow__viewport")
        .evaluate(
          (node) => new DOMMatrixReadOnly(getComputedStyle(node).transform).a,
        );
    for (let step = 0; step < 3 && (await zoom()) < 0.99; step++)
      await page.getByRole("button", { name: "Zoom in", exact: true }).click();
    expect(await zoom()).toBeGreaterThanOrEqual(0.99);
    expect(await zoom()).toBeLessThanOrEqual(1.21);
    await expectReadableGeometry(page, snapshot);
    const automaticPositions = await positions(page);
    const edgeIds = await page
      .locator("[data-edge-id]")
      .evaluateAll((edges) =>
        edges.map((edge) => edge.getAttribute("data-edge-id")!).sort(),
      );
    await expectCompleteConnectors(page, edgeIds);
    const selected = page
      .locator(".react-flow__node:has(.work-card-entrypoint)")
      .first();
    await selected.focus();
    await selected.press("Enter");
    await expect(selected).toHaveAttribute("aria-pressed", "true");
    await expectCompleteConnectors(page, edgeIds);
    await expectReadableGeometry(page, snapshot);
    await selected.press("ArrowRight");
    await expect.poll(() => positions(page)).not.toEqual(automaticPositions);
    await expectCompleteConnectors(page, edgeIds);
    await page.getByRole("button", { name: "Reorganize", exact: true }).click();
    await expect.poll(() => positions(page)).toEqual(automaticPositions);
    await expectCompleteConnectors(page, edgeIds);
    await expectReadableGeometry(page, snapshot);
  });
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

test("Fit reveals later work clipped by the readable initial camera in the ordinary canvas", async ({
  page,
}) => {
  await mount(page);
  await page.setViewportSize({ width: 1440, height: 1050 });
  await openGraph(page);
  const finish = page.getByRole("button", {
    name: "finish · Entrypoint · succeeded",
    exact: true,
  });
  await expect(finish).not.toBeInViewport();
  await page.getByRole("button", { name: "Fit", exact: true }).click();
  // Match the native check's tolerance for transformed-card subpixel rounding.
  await expect(finish).toBeInViewport({ ratio: 0.999 });
  await finish.click();
  await expect(
    page
      .getByRole("complementary", { name: "Selected work details" })
      .getByRole("heading", { name: "finish", exact: true }),
  ).toBeVisible();
});

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
  await expectNoManualWorkflowControls(page);
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
  await expectNoManualWorkflowControls(page);
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
    await expect(page.locator(".react-flow__node")).toHaveCount(count);
    const expanded = await positions(page);
    for (const [id, point] of Object.entries(placed))
      expect(expanded[id]).toEqual(point);
    expect(await viewport(page)).toBe(camera);
    placed = expanded;
  }
  await expect(page.locator("[data-edge-id]")).toHaveCount(27);
});
