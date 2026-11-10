import {
  decodeProgress,
  progressPageMaximumBytes,
  type ProgressRequest,
} from "../../src/api/progress";
// Real server/DB/worker tests. No request routing or fixture responses are allowed here.
import {
  expect,
  test,
  type APIRequestContext,
  type Page,
  type Request,
  type Response,
} from "@playwright/test";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";
import { parseUserJson, stringifyUserJson } from "../../src/api/json";
import * as dto from "../../src/api/resources";
import { decodeConfig } from "../../src/api/codecs";
import { consoleContractVersion } from "../../src/api/contracts";
import { approval as decodeApproval } from "../../src/api/approvals";
import { executionPage } from "../../src/api/explorer";
import { nativeExecutionSeeds } from "./seeded-executions";
import {
  keyboardDialog,
  reducedMotionDialog,
  touchNavigation,
  doubledLayout,
} from "../interaction-checks";
async function get<T>(
  request: APIRequestContext,
  path: string,
  decode: (value: unknown) => T,
): Promise<T> {
  const response = await request.get(`/v1/console/${path}`);
  expect(response.ok()).toBe(true);
  return decode(parseUserJson(await response.text(), 10 * 1024 * 1024));
}
async function seededInvoice(request: APIRequestContext) {
  const { taskId } = await nativeExecutionSeeds();
  const task = await get(
    request,
    `tasks/inspect?task_id=${encodeURIComponent(taskId)}`,
    dto.taskDetail,
  );
  expect(task).toMatchObject({
    task_id: taskId,
    idempotency_key: "lossless-and-warm",
    state: "succeeded",
    descriptor: { program: { id: "invoice", version: "1.0.0" } },
  });
  return task;
}
async function ready(page: Page, title: string) {
  await expect(
    page.getByRole("heading", { name: title, exact: true }).first(),
  ).toBeVisible();
  await expect(page.locator(".loading-state:visible")).toHaveCount(0);
  await expect(page.getByRole("alert")).toHaveCount(0);
}
function noRuntimeErrors(page: Page) {
  const errors: string[] = [];
  page.on("pageerror", (error) => errors.push(error.message));
  page.on("console", (message) => {
    if (
      message.type() === "error" &&
      /Content Security Policy|Refused to|violates/.test(message.text())
    )
      errors.push(message.text());
  });
  return errors;
}
test("Rust-served four views support light/dark and mobile without overflow", async ({
  page,
  request,
}, info) => {
  const config = await get(request, "config", decodeConfig);
  expect(config.instance_id).toBe("console-acceptance");
  const errors = noRuntimeErrors(page);
  for (const [route, title] of [
    ["executions", "Executions"],
    ["workflows", "Executions"],
    ["programs", "Programs"],
    ["workers", "Workers"],
  ]) {
    for (const width of [320, 375, 768, 1280]) {
      await page.setViewportSize({ width, height: 900 });
      await page.goto(`/console/${route}`);
      await ready(page, title ?? "");
      await page
        .getByRole("combobox", { name: "Appearance" })
        .selectOption(width === 1280 ? "dark" : "light");
      expect(
        await page.evaluate(
          () => document.documentElement.scrollWidth <= window.innerWidth,
        ),
      ).toBe(true);
      if (width === 1280 || width === 375)
        await page.screenshot({
          path: info.outputPath(`${route}-${width}.png`),
          fullPage: false,
          animations: "disabled",
        });
    }
  }
  expect(errors).toEqual([]);
});
test("real attempts, workflow records, and catalog dialogs load under production CSP", async ({
  page,
  request,
}, info) => {
  const errors = noRuntimeErrors(page);
  const tasks = await get(request, "tasks?state=failed&limit=25", dto.taskPage);
  const task = tasks.items[0];
  expect(task).toBeDefined();
  if (!task) throw new Error("Seeded failed execution required.");
  await page.goto(
    `/console/executions/${encodeURIComponent(task.task.task_id)}?tab=Result`,
  );
  await ready(page, task.task.correlation_key || "Task execution");
  await expect(
    page.getByRole("heading", { name: "Execution failed" }),
  ).toBeVisible();
  await page.getByRole("button", { name: "Input", exact: true }).click();
  await ready(page, task.task.correlation_key || "Task execution");
  await page.getByRole("button", { name: "Trace", exact: true }).click();
  await page.getByRole("button", { name: "Lifecycle", exact: true }).click();
  await ready(page, task.task.correlation_key || "Task execution");
  await page.getByRole("button", { name: "Attempts", exact: true }).click();
  await page.getByRole("button", { name: /^att_/ }).first().click();
  await expect(
    page.getByRole("heading", { name: "Attempt details" }),
  ).toBeVisible();
  await expect(page.getByRole("alert")).toHaveCount(0);
  const { workflowId } = await nativeExecutionSeeds();
  const { summary: wf } = await get(
    request,
    `workflows/inspect?workflow_id=${encodeURIComponent(workflowId)}`,
    dto.workflowDetail,
  );
  expect(wf).toMatchObject({
    workflow: {
      workflow_id: workflowId,
      parent_workflow_id: null,
      state: "succeeded",
      correlation_key: "same-business-correlation",
    },
    controller: { program: { id: "owned-controller", version: "1.0.0" } },
  });
  await page.goto(
    `/console/workflows/${encodeURIComponent(wf.workflow.workflow_id)}?tab=Recorded+work`,
  );
  await ready(page, wf.workflow.correlation_key || wf.controller.program.id);
  await expect(
    page.getByRole("heading", { name: "Recorded work", exact: true }),
  ).toBeVisible();
  await page.screenshot({
    path: info.outputPath("recorded-work.png"),
    fullPage: false,
    animations: "disabled",
  });
  for (const tab of [
    "Waits and events",
    "Lifecycle",
    "Output",
    "Workflow details",
  ]) {
    await page.getByRole("button", { name: tab, exact: true }).click();
    await ready(page, wf.workflow.correlation_key || wf.controller.program.id);
  }
  await page.goto("/console/agents/invoice/versions/1.0.0");
  await ready(page, "invoice");
  await page.goto("/console/agents");
  await ready(page, "Programs");
  await page
    .getByRole("button", { name: "Register program", exact: true })
    .click();
  await expect(page.getByRole("dialog")).toBeVisible();
  await page.getByRole("button", { name: "Close dialog" }).click();
  await expect(
    page.getByRole("button", { name: "Register program", exact: true }),
  ).toBeFocused();
  const workers = await get(request, "workers", dto.workerPage);
  const worker = workers.items.find((w) => w.detail_state === "available");
  if (!worker) throw new Error("Seeded detailed worker required.");
  await page.goto(
    `/console/workers/${encodeURIComponent(worker.worker_session_id)}`,
  );
  await ready(page, worker.display_name ?? "Worker session");
  await page
    .getByRole("button", { name: /Inspect process slot/ })
    .first()
    .click();
  await expect(page.getByRole("dialog")).toBeVisible();
  await page.screenshot({
    path: info.outputPath("worker-slot.png"),
    fullPage: false,
    animations: "disabled",
  });
  await page.getByRole("button", { name: "Close dialog" }).click();
  expect(errors).toEqual([]);
});
test("Run again submits a distinct real task and preserves lossless numeric input", async ({
  page,
  request,
}) => {
  const original = await seededInvoice(request);
  await page.goto(
    `/console/executions/${encodeURIComponent(original.task_id)}`,
  );
  await ready(page, original.input.correlation_key || "Task execution");
  await page.getByRole("link", { name: "Run again", exact: true }).click();
  await ready(page, "Run again");
  const input = page.getByRole("textbox", { name: "JSON input", exact: true });
  const old = parseUserJson(await input.inputValue(), 1024 * 1024);
  if (!old || typeof old !== "object" || Array.isArray(old))
    throw new Error("Expected acceptance program input.");
  Reflect.set(old, "mode", "success");
  Reflect.set(
    old,
    "value",
    parseUserJson('{"large":9007199254740993,"float":1.0,"zero":-0.0}', 1000),
  );
  await input.fill(stringifyUserJson(old, 2));
  await page.getByRole("button", { name: "Verify reference" }).click();
  await expect(page.getByText("Registered task package")).toBeVisible();
  await page
    .getByRole("button", { name: "Submit execution", exact: true })
    .click();
  await expect(page).toHaveURL(/\/console\/executions\/task_/);
  const taskId = decodeURIComponent(
    new URL(page.url()).pathname.split("/").at(-1) ?? "",
  );
  expect(taskId).not.toBe(original.task_id);
  await page.getByRole("button", { name: "General", exact: true }).click();
  await page.getByRole("button", { name: "Input", exact: true }).click();
  await expect(page.locator('pre[aria-label="Input"]')).toContainText(
    "9007199254740993",
  );
  await page.getByRole("button", { name: "Output", exact: true }).click();
  await expect(
    page.getByRole("heading", { name: "Execution succeeded" }),
  ).toBeVisible({ timeout: 30000 });
  const result = await get(
    request,
    `tasks/result?task_id=${encodeURIComponent(taskId)}`,
    dto.taskResult,
  );
  expect(result.outcome?.kind).toBe("succeeded");
  if (result.outcome?.kind === "succeeded") {
    const output = stringifyUserJson(result.outcome.output);
    expect(output).toContain("9007199254740993");
    expect(output).toContain("1.0");
    expect(output).toContain("-0.0");
  }
  const inspected = await get(
    request,
    `tasks/inspect?task_id=${encodeURIComponent(taskId)}`,
    dto.taskDetail,
  );
  expect(stringifyUserJson(inspected.input.data)).toContain("9007199254740993");
  expect(stringifyUserJson(inspected.input.data)).toContain("-0.0");
  await expect(page.getByRole("alert")).toHaveCount(0);
});

test("real durable approval audit renders its effective action and resumed activation", async ({
  page,
  request,
}, info) => {
  const executions = await get(
    request,
    "executions?correlation_key=console-durable-approval&limit=25",
    executionPage,
  );
  const workflow = executions.items.find((item) => item.kind === "workflow");
  if (!workflow) throw new Error("Seeded durable approval workflow required.");
  const response = await request.post("/v1/console/approvals/inspect", {
    data: { workflow_id: workflow.id, key: "refund" },
  });
  expect(response.ok()).toBe(true);
  expect(response.headers()["ledgence-console-contract"]).toBe(
    String(consoleContractVersion),
  );
  const approval = decodeApproval(
    parseUserJson(await response.text(), 96 * 1024),
  );
  expect(approval.status).toBe("approved");
  expect(approval.resumed_activation_id).not.toBeNull();
  const errors = noRuntimeErrors(page);
  await page.goto("/console/approvals");
  await page
    .getByRole("combobox", { name: "Status", exact: true })
    .selectOption("approved");
  await page.getByLabel("Program ID", { exact: true }).fill("durable-approval");
  const cutoff = page.getByLabel("Due before (UTC)", { exact: true });
  // datetime-local normalizes a fractional second such as ".860" to ".86".
  // Playwright fill requires the shortest HTML value after that normalization.
  function inputInstant(milliseconds: number) {
    return new Date(milliseconds)
      .toISOString()
      .slice(0, -1)
      .replace(/0+$/u, "")
      .replace(/\.$/u, "")
      .replace(/(T\d{2}:\d{2}):00$/u, "$1");
  }
  await cutoff.fill(inputInstant(approval.deadline));
  await page.getByRole("button", { name: "Apply filters" }).click();
  await expect(page).toHaveURL(new RegExp("due_before=" + approval.deadline));
  await expect(
    page.getByText("No approval requests match this view."),
  ).toBeVisible();
  await cutoff.fill(inputInstant(approval.deadline + 1));
  await page.getByRole("button", { name: "Apply filters" }).click();
  const row = page.getByRole("row").filter({ hasText: workflow.id });
  await row
    .getByRole("link", { name: "program:simulate_refund", exact: true })
    .click();
  const card = page.getByRole("article", { name: "Approval refund" });
  await expect(card).toBeVisible();
  await expect(
    card.getByText("program:simulate_refund", { exact: true }),
  ).toBeVisible();
  await expect(
    card.locator('pre[aria-label="Effective action arguments"]'),
  ).toContainText('"amount": 50');
  await expect(
    card.locator('pre[aria-label="Effective action arguments"]'),
  ).toContainText('"currency": "USD"');
  await card.locator("summary").click();
  await expect(
    card.locator('pre[aria-label="Original proposed arguments"]'),
  ).toContainText('"amount": 100');
  await expect(
    card.getByText("console-acceptance-reviewer", { exact: true }),
  ).toBeVisible();
  await expect(
    card.getByText("Reviewed effective amount 50", { exact: true }),
  ).toBeVisible();
  await expect(
    card.getByRole("link", { name: approval.resumed_activation_id! }),
  ).toHaveAttribute(
    "href",
    `/console/executions/${encodeURIComponent(approval.resumed_activation_id!)}`,
  );
  await expect(
    card.getByRole("button", { name: "Approve", exact: true }),
  ).toHaveCount(0);
  await expect(page.getByRole("alert")).toHaveCount(0);
  await page.screenshot({
    path: info.outputPath("approval-audit.png"),
    fullPage: false,
    animations: "disabled",
  });
  await page.getByRole("link", { name: workflow.id, exact: true }).click();
  const inspector = page.getByRole("complementary", {
    name: "Selected work details",
  });
  await expect(
    inspector.getByText("refund", { exact: true }).first(),
  ).toBeVisible();
  await page.goBack();
  await page.getByRole("link", { name: "Back to approvals" }).click();
  await expect(
    page.getByRole("combobox", { name: "Status", exact: true }),
  ).toHaveValue("approved");
  await expect(
    page.getByRole("row").filter({ hasText: workflow.id }),
  ).toBeVisible();
  expect(errors).toEqual([]);
});

test("WebKit remains usable through ten minutes of real polling and reconnect", async ({
  page,
  request,
  context,
  browserName,
}, info) => {
  test.skip(
    browserName !== "webkit",
    "The prolonged compatibility gate targets WebKit.",
  );
  test.setTimeout(720000);
  const config = await get(request, "config", decodeConfig);
  expect(config.instance_id).toBe("console-acceptance");
  const errors = noRuntimeErrors(page);
  let workerResponses = 0;
  page.on("response", (response) => {
    if (
      new URL(response.url()).pathname === "/v1/console/workers" &&
      response.ok()
    )
      workerResponses++;
  });
  await page.goto("/console/workers");
  await ready(page, "Workers");
  const started = Date.now();
  let reconnected = false;
  let rounds = 0;
  try {
    while (Date.now() - started < 600000) {
      if (!reconnected && Date.now() - started >= 120000) {
        const beforeOffline = workerResponses;
        await context.setOffline(true);
        // Real browser connectivity changes; no intercepted or mocked response.
        await page.waitForTimeout(30000);
        await expect(
          page.getByRole("heading", { name: "Workers", exact: true }),
        ).toBeVisible();
        await expect(page.locator("main")).not.toBeEmpty();
        await context.setOffline(false);
        await expect
          .poll(() => workerResponses, { timeout: 30000 })
          .toBeGreaterThan(beforeOffline);
        await page
          .getByRole("link", { name: "Executions", exact: true })
          .click();
        await ready(page, "Executions");
        await page.getByRole("link", { name: "Workers", exact: true }).click();
        await ready(page, "Workers");
        reconnected = true;
      }
      const beforePolling = workerResponses;
      await page.waitForTimeout(20000);
      await expect
        .poll(() => workerResponses, { timeout: 10000 })
        .toBeGreaterThan(beforePolling);
      await ready(page, "Workers");
      expect(errors).toEqual([]);
      rounds++;
      if (rounds % 3 === 0) {
        for (const title of [
          "Executions",
          "Programs",
          "Approvals",
          "Workers",
        ]) {
          await page.getByRole("link", { name: title, exact: true }).click();
          await ready(page, title);
        }
      }
    }
  } finally {
    await context.setOffline(false);
  }
  expect(reconnected).toBe(true);
  expect(workerResponses).toBeGreaterThan(6);
  expect(Date.now() - started).toBeGreaterThanOrEqual(600000);
  expect(errors).toEqual([]);
  await page.screenshot({
    path: info.outputPath("webkit-ten-minute-soak.png"),
    animations: "disabled",
  });
  await info.attach("soak-observations", {
    body: JSON.stringify({
      elapsed_ms: Date.now() - started,
      worker_responses: workerResponses,
      navigation_rounds: rounds,
      reconnected,
    }),
    contentType: "application/json",
  });
});

test("real interface supports keyboard, reduced motion, touch and 200 percent layout scaling", async ({
  page,
  browser,
  baseURL,
}, info) => {
  const errors = noRuntimeErrors(page);
  await page.goto("/console/agents");
  await keyboardDialog(page);
  await reducedMotionDialog(page);
  await doubledLayout(page);
  await page.screenshot({
    path: info.outputPath("layout-scale-200-percent-640px.png"),
    animations: "disabled",
  });
  if (!baseURL) throw new Error("The test server base URL is required.");
  const touch = await browser.newContext({
    baseURL,
    hasTouch: true,
    viewport: { width: 375, height: 812 },
  });
  try {
    const touchPage = await touch.newPage();
    const touchErrors = noRuntimeErrors(touchPage);
    await touchPage.goto("/console/agents");
    await touchNavigation(touchPage);
    expect(touchErrors).toEqual([]);
  } finally {
    await touch.close();
  }
  expect(errors).toEqual([]);
});

for (const kind of ["task", "workflow"] as const) {
  test(`real ${kind} lifecycle feed matches committed pages and rebuilds after reload`, async ({
    page,
    request,
  }, info) => {
    const config = await get(request, "config", decodeConfig);
    expect(config.capabilities.execution_progress).toBe(true);
    const tasks =
      kind === "task"
        ? await get(request, "tasks?state=failed&limit=25", dto.taskPage)
        : null;
    const workflows =
      kind === "workflow"
        ? await get(
            request,
            "workflows?state=succeeded&limit=25",
            dto.workflowPage,
          )
        : null;
    const id =
      tasks?.items[0]?.task.task_id ??
      workflows?.items[0]?.workflow.workflow_id;
    if (!id) throw new Error("A seeded terminal execution is required.");
    const query = {
      target: { kind, id },
      cursor: null,
      limit: 100,
      wait_ms: 0,
    };
    const response = await request.post("/v1/console/executions/progress", {
      data: query,
    });
    expect(response.ok()).toBe(true);
    const expected = decodeProgress(
      parseUserJson(await response.text(), progressPageMaximumBytes),
      query,
    );
    expect(expected.events.length).toBeGreaterThan(0);
    const route =
      kind === "task"
        ? `/console/executions/${encodeURIComponent(id)}?tab=Trace&section=history`
        : `/console/workflows/${encodeURIComponent(id)}?tab=General&section=history`;
    const observed: (string | null)[] = [];
    page.on("request", (request) => {
      if (new URL(request.url()).pathname === "/v1/console/executions/progress")
        observed.push(request.postDataJSON().cursor);
    });
    await page.setViewportSize({ width: 390, height: 900 });
    await page.goto(route);
    const feed = page.getByRole("region", { name: "Live lifecycle history" });
    await expect(feed.locator(".progress-events li")).toHaveCount(
      expected.events.length,
    );
    for (const event of expected.events)
      await expect(
        feed.getByText(`#${event.sequence}`, { exact: true }),
      ).toBeVisible();
    await expect(feed.getByRole("status")).toHaveText(
      "Caught up · execution finished",
    );
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth,
      ),
    ).toBe(true);
    await page.screenshot({
      path: info.outputPath(`${kind}-live-history-mobile.png`),
      fullPage: true,
    });
    await page.reload();
    await expect(feed.locator(".progress-events li")).toHaveCount(
      expected.events.length,
    );
    expect(observed.filter((cursor) => cursor === null)).toHaveLength(2);
    await feed.getByRole("button", { name: "Check for updates" }).click();
    await expect.poll(() => observed.at(-1)).toBe(expected.next_cursor);
    await expect(feed.locator(".progress-events li")).toHaveCount(
      expected.events.length,
    );
    await expect(page.getByRole("alert")).toHaveCount(0);
  });
}

test("real active task wakes a pending lifecycle wait and stops at terminal with its cursor retained", async ({
  page,
  request,
}, info) => {
  const config = await get(request, "config", decodeConfig);
  expect(config.capabilities.execution_progress).toBe(true);
  const original = await seededInvoice(request);
  const directory = await mkdtemp(join(tmpdir(), "ledgence-progress-"));
  const marker = join(directory, "invocations.jsonl");
  const gate = join(directory, "gate");
  let taskId: string | undefined;
  const errors = noRuntimeErrors(page);
  const exchanges: {
    request: Request;
    query: ProgressRequest;
    started: number;
    response?: Response;
    failed?: boolean;
  }[] = [];
  page.on("request", (outgoing) => {
    if (new URL(outgoing.url()).pathname === "/v1/console/executions/progress")
      exchanges.push({
        request: outgoing,
        query: outgoing.postDataJSON(),
        started: Date.now(),
      });
  });
  page.on("response", (response) => {
    const exchange = exchanges.find(
      (item) => item.request === response.request(),
    );
    if (exchange) exchange.response = response;
  });
  page.on("requestfailed", (failed) => {
    const exchange = exchanges.find((item) => item.request === failed);
    if (exchange) exchange.failed = true;
  });
  try {
    // This fixture program runs on the actual local worker and waits for a file.
    // A unique directory makes the task independent of both browser projects.
    const submitted = await request.post("/v1/console/tasks", {
      data: {
        idempotency_key: `live-progress-${basename(directory)}`,
        input: {
          program: original.descriptor.program,
          queue: original.input.queue,
          data: { mode: "gate", marker, gate },
          retry_policy: { max_attempts: 1, retry_delay_ms: 0 },
          attempt_timeout_ms: 60000,
        },
      },
    });
    expect(submitted.ok()).toBe(true);
    const task = dto.taskDetail(
      parseUserJson(await submitted.text(), 1024 * 1024),
    );
    taskId = task.task_id;
    await expect
      .poll(
        async () => {
          try {
            return (await readFile(marker, "utf8")).includes(task.task_id);
          } catch {
            return false;
          }
        },
        { timeout: 20000 },
      )
      .toBe(true);
    const active = await get(
      request,
      `tasks/status?task_id=${encodeURIComponent(taskId)}`,
      dto.observedTask,
    );
    expect(active.task.state).toBe("active");
    const initialQuery: ProgressRequest = {
      target: { kind: "task", id: taskId },
      cursor: null,
      limit: 100,
      wait_ms: 0,
    };
    const initialResponse = await request.post(
      "/v1/console/executions/progress",
      { data: initialQuery },
    );
    expect(initialResponse.ok()).toBe(true);
    const initial = decodeProgress(
      parseUserJson(await initialResponse.text(), progressPageMaximumBytes),
      initialQuery,
    );
    expect(initial.terminal).toBe(false);
    expect(
      initial.events.some((event) => event.reason === "dispatch_authorized"),
    ).toBe(true);
    await page.goto(
      `/console/executions/${encodeURIComponent(taskId)}?tab=Trace&section=history`,
    );
    const feed = page.getByRole("region", { name: "Live lifecycle history" });
    await expect(feed.locator(".progress-events li")).toHaveCount(
      initial.events.length,
    );
    const pendingWait = () =>
      exchanges.find(
        (item) =>
          item.query.wait_ms === 5000 &&
          item.query.cursor === initial.next_cursor &&
          !item.response &&
          !item.failed,
      );
    await expect.poll(pendingWait).toBeDefined();
    const waiting = pendingWait()!;
    // Prove the real request remains pending before the mutation. Observing a
    // request alone would also pass for an incorrectly immediate empty reply.
    await page.waitForTimeout(250);
    expect(waiting.response).toBeUndefined();
    expect(waiting.failed).toBeUndefined();
    const waitedBeforeCancellation = Date.now() - waiting.started;
    const cancelled = await request.post("/v1/console/tasks/cancel", {
      data: { task_id: taskId },
    });
    expect(cancelled.ok()).toBe(true);
    dto.cancelTaskReceipt(parseUserJson(await cancelled.text(), 1024 * 1024));
    await expect.poll(() => waiting.response).toBeDefined();
    const resumedResponse = waiting.response!;
    expect(resumedResponse.ok()).toBe(true);
    const resumed = decodeProgress(
      parseUserJson(await resumedResponse.text(), progressPageMaximumBytes),
      waiting.query,
    );
    expect(resumed.events.length).toBeGreaterThan(0);
    expect(BigInt(resumed.events[0]!.sequence)).toBeGreaterThan(
      BigInt(initial.high_watermark),
    );
    expect(
      resumed.events.some((event) =>
        ["cancel_requested", "cancelled"].includes(event.reason),
      ),
    ).toBe(true);
    await expect(feed.getByRole("status")).toHaveText(
      "Caught up · execution finished",
      { timeout: 20000 },
    );
    const terminalExchange = exchanges.at(-1)!;
    expect(terminalExchange.response?.ok()).toBe(true);
    const terminal = decodeProgress(
      parseUserJson(
        await terminalExchange.response!.text(),
        progressPageMaximumBytes,
      ),
      terminalExchange.query,
    );
    expect(terminal.state).toBe("cancelled");
    expect(terminal.terminal).toBe(true);
    const finishedCount = exchanges.length;
    await page.waitForTimeout(600);
    expect(exchanges).toHaveLength(finishedCount);
    expect(exchanges.every((item) => !!item.response && !item.failed)).toBe(
      true,
    );
    const beforeRefresh = await feed.locator(".progress-events li").count();
    await feed.getByRole("button", { name: "Check for updates" }).click();
    await expect.poll(() => exchanges.length).toBe(finishedCount + 1);
    const refresh = exchanges.at(-1)!;
    expect(refresh.query.cursor).toBe(terminal.next_cursor);
    await expect(feed.getByRole("status")).toHaveText(
      "Caught up · execution finished",
    );
    const pages = await Promise.all(
      exchanges.map(async (item) => {
        expect(item.response?.ok()).toBe(true);
        return decodeProgress(
          parseUserJson(await item.response!.text(), progressPageMaximumBytes),
          item.query,
        );
      }),
    );
    const sequences = pages.flatMap((item) =>
      item.events.map((event) => event.sequence),
    );
    expect(new Set(sequences).size).toBe(sequences.length);
    await expect(feed.locator(".progress-events li")).toHaveCount(
      sequences.length,
    );
    expect(sequences.length).toBeGreaterThan(initial.events.length);
    expect(sequences.length).toBeGreaterThanOrEqual(beforeRefresh);
    for (const sequence of sequences)
      await expect(
        feed.getByText(`#${sequence}`, { exact: true }),
      ).toBeVisible();
    await expect(page.getByRole("alert")).toHaveCount(0);
    expect(errors).toEqual([]);
    await info.attach("active-progress-evidence", {
      body: JSON.stringify({
        task_id: taskId,
        initial_events: initial.events,
        wait_ms: waiting.query.wait_ms,
        pending_before_cancel_ms: waitedBeforeCancellation,
        wake_events: resumed.events,
        terminal_cursor: terminal.next_cursor,
        refresh_cursor: refresh.query.cursor,
        sequences,
        stopped_request_count: finishedCount,
      }),
      contentType: "application/json",
    });
  } finally {
    try {
      // Also release the gate if an assertion fails before cancellation succeeds.
      await writeFile(gate, "released\n");
      const cleanupTaskId = taskId;
      if (cleanupTaskId) {
        const cleanup = await request.post("/v1/console/tasks/cancel", {
          data: { task_id: cleanupTaskId },
        });
        expect(cleanup.ok()).toBe(true);
        await expect
          .poll(
            async () => {
              const status = await get(
                request,
                `tasks/status?task_id=${encodeURIComponent(cleanupTaskId)}`,
                dto.observedTask,
              );
              return dto.terminal(status.task.state);
            },
            { timeout: 15000 },
          )
          .toBe(true);
      }
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  }
});
