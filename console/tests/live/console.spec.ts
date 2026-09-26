// Real server/DB/worker tests. No request routing or fixture responses are allowed here.
import {
  expect,
  test,
  type APIRequestContext,
  type Page,
} from "@playwright/test";
import { parseUserJson, stringifyUserJson } from "../../src/api/json";
import * as dto from "../../src/api/resources";
import { decodeConfig } from "../../src/api/codecs";
async function get<T>(
  request: APIRequestContext,
  path: string,
  decode: (value: unknown) => T,
): Promise<T> {
  const response = await request.get(`/v1/console/${path}`);
  expect(response.ok()).toBe(true);
  return decode(parseUserJson(await response.text(), 10 * 1024 * 1024));
}
async function ready(page: Page, title: string) {
  await expect(
    page.getByRole("heading", { name: title, exact: true }).first(),
  ).toBeVisible();
  await expect(page.locator(".loading-state")).toHaveCount(0);
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
    ["workflows", "Workflows"],
    ["agents", "Agents"],
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
  await ready(page, "Execution");
  await expect(
    page.getByRole("heading", { name: "Execution failed" }),
  ).toBeVisible();
  for (const tab of ["Input", "Attempts", "History"]) {
    await page.getByRole("button", { name: tab, exact: true }).click();
    await ready(page, "Execution");
  }
  await page.getByRole("button", { name: "Attempts", exact: true }).click();
  await page.getByRole("button", { name: /^att_/ }).first().click();
  await expect(
    page.getByRole("heading", { name: "Attempt details" }),
  ).toBeVisible();
  await expect(page.getByRole("alert")).toHaveCount(0);
  const workflows = await get(
    request,
    "workflows?state=succeeded&limit=25",
    dto.workflowPage,
  );
  const wf = workflows.items.find(
    (w) => w.controller.program.id === "owned-controller",
  );
  if (!wf) throw new Error("Seeded owned workflow required.");
  await page.goto(
    `/console/workflows/${encodeURIComponent(wf.workflow.workflow_id)}`,
  );
  await ready(page, "Workflow");
  await expect(
    page.getByRole("heading", { name: "Recorded work", exact: true }),
  ).toBeVisible();
  await page.screenshot({
    path: info.outputPath("recorded-work.png"),
    fullPage: false,
    animations: "disabled",
  });
  for (const tab of ["Waits", "History", "Result", "Context"]) {
    await page.getByRole("button", { name: tab, exact: true }).click();
    await ready(page, "Workflow");
  }
  await page.goto("/console/agents/invoice/versions/1.0.0");
  await ready(page, "invoice");
  await page.goto("/console/agents");
  await ready(page, "Agents");
  await page
    .getByRole("button", { name: "Register agent", exact: true })
    .click();
  await expect(page.getByRole("dialog")).toBeVisible();
  await page.getByRole("button", { name: "Close dialog" }).click();
  await expect(
    page.getByRole("button", { name: "Register agent", exact: true }),
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
  const tasks = await get(
    request,
    "tasks?state=succeeded&limit=100",
    dto.taskPage,
  );
  const original = tasks.items.find(
    (t) => t.descriptor.program.id === "invoice",
  );
  if (!original)
    throw new Error("Seeded successful invoice execution required.");
  await page.goto(
    `/console/executions/${encodeURIComponent(original.task.task_id)}`,
  );
  await ready(page, "Execution");
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
  expect(taskId).not.toBe(original.task.task_id);
  await page.getByRole("button", { name: "Input", exact: true }).click();
  await expect(page.getByLabel("Input", { exact: true })).toContainText(
    "9007199254740993",
  );
  await page.getByRole("button", { name: "Result", exact: true }).click();
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
        for (const title of ["Executions", "Workflows", "Agents", "Workers"]) {
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
