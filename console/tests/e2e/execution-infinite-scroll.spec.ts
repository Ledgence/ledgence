// SPDX-License-Identifier: MIT
import { readFileSync } from "node:fs";
import { expect, test, type Page } from "@playwright/test";
import { decodeConfig } from "../../src/api/codecs";
import { executionPage, type Execution } from "../../src/api/explorer";
import { parseUserJson, stringifyUserJson } from "../../src/api/json";
import { observedTask } from "../../src/api/resources";

const raw = readFileSync(
  new URL(
    "../../../crates/ledgence-orchestration-api/tests/fixtures/console-v4.json",
    import.meta.url,
  ),
  "utf8",
);
const parsed = parseUserJson(raw, 2 * 1024 * 1024);
function fixture(name: string): unknown {
  if (!parsed || typeof parsed !== "object" || !(name in parsed))
    throw Error(`Missing Rust fixture ${name}`);
  return Reflect.get(parsed, name);
}
const canonicalConfig = decodeConfig(fixture("config"));
const canonicalPage = executionPage(fixture("execution_history"));
const observedAt = canonicalPage.observed_at;
const headers = {
  "Content-Type": "application/json",
  "Ledgence-Console-Contract": "4",
  "Ledgence-Instance-Id": canonicalConfig.instance_id,
};

// Synthetic list metadata exercises pagination only. Canonical Rust descriptors
// and strict C4 decoding keep these browser fixtures compatible with the wire.
function records(count: number, filtered = false): Execution[] {
  return Array.from({ length: count }, (_, index) => {
    const kind = filtered || index % 2 === 0 ? "workflow" : "task";
    const canonical = canonicalPage.items.find((item) => item.kind === kind)!;
    return {
      ...canonical,
      id: `${kind}_${filtered ? "filtered" : "history"}_${String(index).padStart(3, "0")}`,
      correlation_key: `${filtered ? "Filtered" : "History"} run ${String(index).padStart(3, "0")}`,
      state: filtered ? "failed" : "succeeded",
      submitted_at: observedAt - (index + 2) * 1000,
      terminal_at: observedAt - (index + 2) * 1000 + 500,
      parent_workflow_id: null,
      root_workflow_id: null,
    };
  });
}
function result(items: Execution[], next_cursor: string | null) {
  return executionPage({ items, next_cursor, observed_at: observedAt });
}

async function mount(
  page: Page,
  options: {
    batch?: number;
    failNext?: boolean;
    holdCursor?: string;
    repeatCursor?: boolean;
  } = {},
) {
  const batch = options.batch ?? 25;
  const all = records(batch * 2 + 6);
  const filtered = records(batch + 6, true);
  const requests: {
    cursor: string | null;
    kind: string | null;
    state: string | null;
    limit: string | null;
  }[] = [];
  let failNext = options.failNext ?? false;
  let held = false;
  let release!: () => void;
  const heldResponse = new Promise<void>((resolve) => {
    release = resolve;
  });
  let completed!: () => void;
  const heldComplete = new Promise<void>((resolve) => {
    completed = resolve;
  });
  await page.route("**/v1/console/**", async (route) => {
    const url = new URL(route.request().url());
    const resource = url.pathname.replace("/v1/console/", "");
    let body: unknown;
    if (resource === "config")
      body = {
        ...canonicalConfig,
        limits: { ...canonicalConfig.limits, default_page_size: batch },
        polling: { ...canonicalConfig.polling, lists_ms: 60000 },
      };
    else if (resource === "executions") {
      const cursor = url.searchParams.get("cursor");
      const kind = url.searchParams.get("kind");
      const state = url.searchParams.get("state");
      requests.push({
        cursor,
        kind,
        state,
        limit: url.searchParams.get("limit"),
      });
      if (kind === "workflow" && state === "failed") {
        if (cursor === null)
          body = result(filtered.slice(0, batch), "filtered-next");
        else if (cursor === "filtered-next")
          body = result(filtered.slice(batch), null);
        else throw Error(`Old cursor leaked into filtered query: ${cursor}`);
      } else {
        if (cursor && cursor === options.holdCursor && !held) {
          held = true;
          await heldResponse;
        }
        if (cursor && failNext) {
          await route.fulfill({
            status: 500,
            headers,
            body: "Controlled later-page failure",
          });
          return;
        }
        if (cursor === null)
          body = result(
            all.slice(0, batch),
            options.repeatCursor ? "loop" : "after-first",
          );
        else if (cursor === "after-first" || cursor === "loop") {
          // A boundary record appears in both pages. It must remain one row.
          body = result(
            all.slice(batch - 1, batch * 2 - 1),
            options.repeatCursor ? "loop" : "after-second",
          );
        } else if (cursor === "after-second")
          body = result(all.slice(batch * 2 - 1), null);
        else throw Error(`Unexpected execution cursor ${cursor}`);
      }
    } else if (resource === "tasks/status") {
      const current = observedTask(fixture("task_status"));
      const id = url.searchParams.get("task_id");
      const item = all.find((record) => record.id === id);
      if (!item || item.kind !== "task") throw Error(`Unexpected task ${id}`);
      body = {
        ...current,
        observed_at: observedAt,
        task: {
          ...current.task,
          task_id: item.id,
          correlation_key: item.correlation_key,
          state: "succeeded",
          queue: item.queue,
          submitted_at: item.submitted_at,
          terminal_at: item.terminal_at,
          available_at: item.submitted_at,
        },
      };
    } else if (resource === "tasks/attempts")
      body = { items: [], next_cursor: null, observed_at: observedAt };
    else if (resource === "executions/ancestry") {
      const execution = {
        kind: url.searchParams.get("kind"),
        id: url.searchParams.get("id"),
      };
      body = {
        execution,
        path: [{ execution, program: null, availability: "available" }],
        observed_at: observedAt,
      };
    } else throw Error(`Unexpected Console read ${resource}`);
    try {
      await route.fulfill({
        status: 200,
        headers,
        body: stringifyUserJson(body),
      });
    } finally {
      if (held && url.searchParams.get("cursor") === options.holdCursor)
        completed();
    }
  });
  return {
    all,
    filtered,
    requests,
    allowNext: () => {
      failNext = false;
    },
    releaseOld: async () => {
      release();
      await heldComplete;
    },
  };
}
const rows = (page: Page) => page.locator(".history-table tbody tr");
const ids = (page: Page) =>
  page.locator(".history-table .execution-id").allTextContents();
async function nearBottom(page: Page) {
  await page.evaluate(() =>
    window.scrollTo(0, document.documentElement.scrollHeight),
  );
}
async function expectNoPaginationControls(page: Page) {
  for (const name of ["Refresh", "Previous", "Next"])
    await expect(page.getByRole("button", { name, exact: true })).toHaveCount(
      0,
    );
  await expect(
    page.getByRole("combobox", { name: "Rows", exact: true }),
  ).toHaveCount(0);
  await expect(page.locator(".history-table-panel .page-controls")).toHaveCount(
    0,
  );
}
async function exerciseBottomAgain(page: Page) {
  // Give intersection callbacks two rendering turns after entering and leaving
  // the bottom; this detects request loops without advancing list polling.
  for (const top of [0, Number.MAX_SAFE_INTEGER, 0, Number.MAX_SAFE_INTEGER]) {
    await page.evaluate(async (value) => {
      window.scrollTo(0, value);
      await new Promise<void>((resolve) =>
        requestAnimationFrame(() => requestAnimationFrame(() => resolve())),
      );
    }, top);
  }
}

for (const width of [1440, 390]) {
  test(`scroll appends unique execution rows and stops at the end at ${width}px`, async ({
    page,
  }, testInfo) => {
    const batch = width === 1440 ? 25 : 50;
    const server = await mount(page, { batch });
    await page.setViewportSize({ width, height: 850 });
    await page.goto("/console/executions");
    await expect(rows(page)).toHaveCount(batch);
    // Development StrictMode may abort the initial request and replace it.
    // Each completed list still has one cursor chain, without duplicate pages.
    expect(server.requests.length).toBeGreaterThan(0);
    expect(
      server.requests.every(
        (request) =>
          request.cursor === null &&
          request.kind === null &&
          request.state === null &&
          request.limit === String(batch),
      ),
    ).toBe(true);
    const initialReads = server.requests.length;
    await expectNoPaginationControls(page);
    await nearBottom(page);
    await expect(rows(page)).toHaveCount(batch * 2 - 1);
    expect(await ids(page)).toEqual(
      server.all.slice(0, batch * 2 - 1).map((row) => row.id),
    );
    await nearBottom(page);
    await expect(rows(page)).toHaveCount(server.all.length);
    expect(await ids(page)).toEqual(server.all.map((row) => row.id));
    await exerciseBottomAgain(page);
    expect(
      server.requests.filter((request) => request.cursor === null),
    ).toHaveLength(initialReads);
    expect(
      server.requests.map((request) => request.cursor).filter(Boolean),
    ).toEqual(["after-first", "after-second"]);
    await expect(page).toHaveURL(/\/console\/executions$/);
    await expectNoPaginationControls(page);
    await page.screenshot({
      path: testInfo.outputPath(`execution-history-${width}.png`),
      animations: "disabled",
    });
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth,
      ),
    ).toBe(true);
  });
}

test("a later-page failure retains loaded rows and Retry loading resumes the chain", async ({
  page,
}) => {
  const server = await mount(page, { failNext: true });
  await page.setViewportSize({ width: 1440, height: 850 });
  await page.goto("/console/executions");
  await expect(rows(page)).toHaveCount(25);
  await nearBottom(page);
  const retry = page.getByRole("button", {
    name: "Retry loading",
    exact: true,
  });
  await expect(retry).toBeVisible();
  expect(await ids(page)).toEqual(server.all.slice(0, 25).map((row) => row.id));
  const failures = server.requests.length;
  await exerciseBottomAgain(page);
  expect(server.requests).toHaveLength(failures);
  server.allowNext();
  await retry.click();
  await expect(rows(page)).toHaveCount(49);
  await expect(retry).toHaveCount(0);
  expect(server.requests.at(-1)?.cursor).toBe("after-first");
  expect(await ids(page)).toEqual(server.all.slice(0, 49).map((row) => row.id));
});

test("filters reset the cursor chain and a late old response cannot mix rows", async ({
  page,
}) => {
  const server = await mount(page, { holdCursor: "after-second" });
  await page.setViewportSize({ width: 1440, height: 850 });
  await page.goto("/console/executions");
  await expect(rows(page)).toHaveCount(25);
  await nearBottom(page);
  await expect(rows(page)).toHaveCount(49);
  await nearBottom(page);
  await expect.poll(() => server.requests.at(-1)?.cursor).toBe("after-second");
  await page
    .getByRole("combobox", { name: "Type", exact: true })
    .selectOption("workflow");
  await page
    .getByRole("combobox", { name: "Status", exact: true })
    .selectOption("failed");
  await page
    .getByRole("button", { name: "Apply filters", exact: true })
    .click();
  await expect(rows(page)).toHaveCount(25);
  expect(await ids(page)).toEqual(
    server.filtered.slice(0, 25).map((row) => row.id),
  );
  expect(server.requests.at(-1)).toMatchObject({
    kind: "workflow",
    state: "failed",
    cursor: null,
  });
  await server.releaseOld();
  await nearBottom(page);
  await expect(rows(page)).toHaveCount(server.filtered.length);
  expect(await ids(page)).toEqual(server.filtered.map((row) => row.id));
  expect(
    server.requests
      .filter((request) => request.kind === "workflow")
      .map((request) => request.cursor),
  ).toEqual([null, "filtered-next"]);
});

for (const returnVia of ["browser Back", "Executions breadcrumb"]) {
  test(`opening an execution then ${returnVia} restores loaded rows, focus and scroll`, async ({
    page,
  }) => {
    const server = await mount(page);
    await page.setViewportSize({ width: 1440, height: 850 });
    await page.goto("/console/executions");
    await expect(rows(page)).toHaveCount(25);
    await nearBottom(page);
    await expect(rows(page)).toHaveCount(49);
    const link = page.getByRole("link", {
      name: "History run 035",
      exact: true,
    });
    await link.evaluate((element) =>
      window.scrollTo(
        0,
        element.getBoundingClientRect().top + scrollY - innerHeight / 2,
      ),
    );
    await expect(link).toBeInViewport();
    const before = await page.evaluate(() => scrollY);
    expect(before).toBeGreaterThan(1000);
    const listIds = await ids(page);
    const loadedRequests = [...server.requests];
    await link.focus();
    await link.press("Enter");
    await expect(
      page.getByRole("heading", { name: "History run 035", exact: true }),
    ).toBeVisible();
    if (returnVia === "browser Back") await page.goBack();
    else
      await page
        .getByRole("navigation", { name: "Execution ancestry" })
        .getByRole("link", { name: "Executions", exact: true })
        .click();
    await expect(rows(page)).toHaveCount(49);
    expect(await ids(page)).toEqual(listIds);
    await expect
      .poll(async () => Math.abs((await page.evaluate(() => scrollY)) - before))
      .toBeLessThan(3);
    await expect(link).toBeFocused();
    expect(server.requests).toEqual(loadedRequests);
  });
}

test("a repeated next cursor stops automatic requests and keeps the loaded list usable", async ({
  page,
}) => {
  const server = await mount(page, { repeatCursor: true });
  await page.setViewportSize({ width: 1440, height: 850 });
  await page.goto("/console/executions");
  await expect(rows(page)).toHaveCount(25);
  await nearBottom(page);
  await expect(page.getByRole("alert")).toContainText(
    "Unable to load more executions",
  );
  await expect(rows(page)).toHaveCount(25);
  expect(await ids(page)).toEqual(server.all.slice(0, 25).map((row) => row.id));
  await exerciseBottomAgain(page);
  expect(
    server.requests.map((request) => request.cursor).filter(Boolean),
  ).toEqual(["loop"]);
  await expectNoPaginationControls(page);
});
