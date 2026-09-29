import { readFileSync } from "node:fs";
import { expect, test, type Page } from "@playwright/test";
import { parseUserJson, stringifyUserJson } from "../../src/api/json";
import * as dto from "../../src/api/resources";
import {
  keyboardDialog,
  reducedMotionDialog,
  touchNavigation,
  doubledLayout,
} from "../interaction-checks";
const raw = readFileSync(
  new URL(
    "../../../crates/ledgence-orchestration-api/tests/fixtures/console-v2.json",
    import.meta.url,
  ),
  "utf8",
);
const parsed = parseUserJson(raw, 2 * 1024 * 1024);
function fixture(name: string): unknown {
  if (!parsed || typeof parsed !== "object" || !(name in parsed))
    throw new Error(`Missing fixture ${name}`);
  return Reflect.get(parsed, name);
}
const headers = {
  "Content-Type": "application/json",
  "Ledgence-Console-Contract": "2",
  "Ledgence-Instance-Id": "instance_demo",
  "Request-Id": "request-contract-test",
};
async function mount(page: Page, result = "pending_result") {
  const requests: string[] = [];
  const paths: Record<string, string> = {
    config: "config",
    tasks: "tasks",
    "tasks/status": "task_status",
    "tasks/inspect": "task_detail",
    "tasks/result": result,
    "tasks/attempts": "attempts",
    "attempts/inspect": "attempt_detail",
    workflows: "workflows",
    "workflows/inspect": "workflow_detail",
    "workflows/activations": "activations",
    "workflows/children": "children",
    "workflows/waits": "waits",
    "workflows/local-steps": "local_steps",
    "workflows/history": "workflow_history",
    programs: "programs",
    "programs/inspect": "program_detail",
    "programs/versions": "program_versions",
    workers: "workers",
    "workers/inspect": "worker_detail",
  };
  await page.route("**/v1/console/**", async (route) => {
    const name = new URL(route.request().url()).pathname.replace(
      "/v1/console/",
      "",
    );
    requests.push(name);
    if (name === "executions/ancestry") {
      const url = new URL(route.request().url());
      const kind = url.searchParams.get("kind");
      const id = url.searchParams.get("id");
      await route.fulfill({
        status: 200,
        headers,
        body: stringifyUserJson({
          execution: { kind, id },
          path: [
            {
              execution: { kind, id },
              program: null,
              availability: "available",
            },
          ],
          observed_at: 1790409600000,
        }),
      });
      return;
    }
    if (name === "executions") {
      const page = dto.taskPage(fixture("tasks"));
      await route.fulfill({
        status: 200,
        headers,
        body: stringifyUserJson({
          ...page,
          items: page.items.map(({ task, descriptor }) => ({
            kind: "task",
            id: task.task_id,
            descriptor,
            queue: task.queue,
            state: task.state,
            submitted_at: task.submitted_at,
            terminal_at: task.terminal_at,
            correlation_key: task.correlation_key,
            parent_workflow_id: task.workflow_id,
            root_workflow_id: null,
          })),
        }),
      });
      return;
    }
    if (name === "programs/catalog") {
      const page = dto.programPage(fixture("programs"));
      await route.fulfill({
        status: 200,
        headers,
        body: stringifyUserJson({
          ...page,
          items: page.items.map((program) => ({
            program,
            kinds: [program.metadata.kind ?? "unspecified"],
          })),
        }),
      });
      return;
    }
    const key = paths[name];
    if (!key) throw new Error(`Unexpected request ${name}`);
    await route.fulfill({
      status: 200,
      headers,
      body: stringifyUserJson(fixture(key)),
    });
  });
  return requests;
}
test("execution metadata loads without eager input or result calls", async ({
  page,
}) => {
  const requests = await mount(page);
  await page.goto("/console/executions");
  await page.getByRole("link", { name: "INV-1042", exact: true }).click();
  await expect(
    page.getByRole("heading", { name: "INV-1042", exact: true }),
  ).toBeVisible();
  expect(requests).not.toContain("tasks/inspect");
  expect(requests).not.toContain("tasks/result");
  await page.getByRole("button", { name: "Input", exact: true }).click();
  await expect(page.getByLabel("Input", { exact: true })).toContainText(
    "9007199254740993",
  );
});
test("pending and successful null remain visually distinct", async ({
  page,
}) => {
  await mount(page, "null_result");
  await page.goto("/console/executions/task_invoice_1042?tab=Result");
  await expect(
    page.getByRole("heading", { name: "Execution succeeded" }),
  ).toBeVisible();
  await expect(page.getByLabel("Result", { exact: true })).toHaveText("null");
  await expect(page.getByText("Result pending.", { exact: false })).toHaveCount(
    0,
  );
});
test("lost submit response is retried with identical bytes and identity", async ({
  page,
}) => {
  await mount(page);
  const bodies: string[] = [];
  await page.route("**/v1/console/tasks", async (route) => {
    if (route.request().method() !== "POST") {
      await route.fallback();
      return;
    }
    bodies.push(route.request().postData() ?? "");
    if (bodies.length === 1) {
      await route.abort("connectionfailed");
      return;
    }
    await route.fulfill({
      status: 200,
      headers,
      body: stringifyUserJson(fixture("task_detail")),
    });
  });
  await page.goto(
    "/console/executions/new?program=invoice-issuer&version=release-a",
  );
  await page.getByRole("button", { name: "Verify reference" }).click();
  await expect(page.getByText("Registered task package")).toBeVisible();
  await page
    .getByRole("textbox", { name: "JSON input", exact: true })
    .fill('{"integer":9007199254740993,"float":1.0,"negative_zero":-0.0}');
  await page.getByRole("button", { name: "Submit execution" }).click();
  await expect(
    page.getByText("The operation may have been accepted.", { exact: false }),
  ).toBeVisible();
  await expect(
    page.getByRole("textbox", { name: "JSON input", exact: true }),
  ).toBeDisabled();
  expect(bodies).toHaveLength(1);
  await page.getByRole("button", { name: "Try again", exact: true }).click();
  await expect(
    page.getByRole("heading", { name: "INV-1042", exact: true }),
  ).toBeVisible();
  expect(bodies).toHaveLength(2);
  expect(bodies[1]).toBe(bodies[0]);
  expect(bodies[0]).toContain("9007199254740993");
  expect(bodies[0]).toContain("1.0");
  expect(bodies[0]).toContain("-0.0");
});
test("workflow relationships use recorded creation IDs", async ({ page }) => {
  await mount(page);
  await page.goto("/console/workflows/wf_invoice_1042?tab=Recorded+work");
  await expect(
    page.getByRole("heading", { name: "Recorded work", exact: true }),
  ).toBeVisible();
  await expect(
    page.getByRole("link", { name: "task_child_1", exact: true }),
  ).toBeVisible();
  await expect(page.locator(".activation-group")).toContainText(
    "task_controller_1",
  );
  await expect(page.locator(".activation-group")).toContainText("task_child_1");
});
for (const width of [320, 375, 768, 1280])
  for (const theme of ["light", "dark"]) {
    test(`execution table reflows at ${width}px in ${theme}`, async ({
      page,
    }) => {
      await mount(page);
      await page.setViewportSize({ width, height: 900 });
      await page.goto("/console/executions");
      await page
        .getByRole("combobox", { name: "Appearance" })
        .selectOption(theme);
      await expect(
        page.getByRole("link", { name: "INV-1042", exact: true }),
      ).toBeVisible();
      expect(
        await page.evaluate(
          () => document.documentElement.scrollWidth <= window.innerWidth,
        ),
      ).toBe(true);
    });
  }

test("status tabs keep exact filters and reset pagination", async ({
  page,
}) => {
  await mount(page);
  const paths: string[] = [];
  page.on("request", (req) => {
    if (new URL(req.url()).pathname === "/v1/console/executions")
      paths.push(req.url());
  });
  await page.goto(
    "/console/executions?queue=billing&correlation_key=&cursor=prior",
  );
  await expect(
    page.getByRole("link", { name: "INV-1042", exact: true }),
  ).toBeVisible();
  await page
    .getByRole("combobox", { name: "Status", exact: true })
    .selectOption("failed");
  await page
    .getByRole("button", { name: "Apply filters", exact: true })
    .click();
  await expect(page).toHaveURL(/state=failed/);
  const address = new URL(page.url());
  expect(address.searchParams.get("queue")).toBe("billing");
  expect(address.searchParams.get("correlation_key")).toBe("");
  expect(address.searchParams.has("cursor")).toBe(false);
  await expect(
    page.getByRole("combobox", { name: "Status", exact: true }),
  ).toHaveValue("failed");
  await page
    .getByRole("button", { name: "Apply filters", exact: true })
    .click();
  await expect(page).toHaveURL(/state=failed/);
  expect(paths.length).toBeGreaterThan(1);
});

test("catalog capability is optional and long correlation keys round-trip", async ({
  page,
}) => {
  const requests = await mount(page);
  const config = fixture("config");
  if (!config || typeof config !== "object") throw new Error("Expected config");
  const capabilities = Reflect.get(config, "capabilities");
  const modified = {
    ...config,
    capabilities: { ...capabilities, programs: false },
  };
  await page.route("**/v1/console/config", (route) =>
    route.fulfill({ status: 200, headers, body: stringifyUserJson(modified) }),
  );
  let sent = "";
  await page.route("**/v1/console/tasks", async (route) => {
    sent = route.request().postData() ?? "";
    await route.fulfill({
      status: 200,
      headers,
      body: stringifyUserJson(fixture("task_detail")),
    });
  });
  await page.goto(
    "/console/executions/new?program=invoice-issuer&version=release-a",
  );
  await expect(
    page.getByRole("button", { name: "Verify reference" }),
  ).toHaveCount(0);
  await expect(
    page.getByRole("combobox", { name: "Execution kind" }),
  ).toBeVisible();
  await page
    .getByRole("textbox", { name: "Correlation key", exact: true })
    .fill("c".repeat(512));
  await page
    .getByRole("checkbox", { name: "Include correlation, including empty" })
    .check();
  await page
    .getByRole("button", { name: "Submit execution", exact: true })
    .click();
  await expect(
    page.getByRole("heading", { name: "INV-1042", exact: true }),
  ).toBeVisible();
  expect(sent).toContain("c".repeat(512));
  expect(requests).not.toContain("programs/inspect");
});

test("a failed catalog recheck cannot submit stale verification", async ({
  page,
}) => {
  await mount(page);
  let checks = 0;
  await page.route("**/v1/console/programs/inspect?**", async (route) => {
    checks++;
    await route.fulfill(
      checks === 1
        ? {
            status: 200,
            headers,
            body: stringifyUserJson(fixture("program_detail")),
          }
        : { status: 400, headers, body: "{}" },
    );
  });
  await page.goto(
    "/console/executions/new?program=invoice-issuer&version=release-a",
  );
  const verify = page.getByRole("button", { name: "Verify reference" });
  const submit = page.getByRole("button", {
    name: "Submit execution",
    exact: true,
  });
  await verify.click();
  await expect(submit).toBeEnabled();
  await verify.click();
  await expect(page.getByRole("alert")).toBeVisible();
  await expect(submit).toBeDisabled();
  await expect(page.getByText("Registered task package")).toHaveCount(0);
});

test("correlation validation counts UTF-8 bytes before submission", async ({
  page,
}) => {
  await mount(page);
  let submissions = 0;
  page.on("request", (request) => {
    if (request.method() === "POST") submissions++;
  });
  await page.goto(
    "/console/executions/new?program=invoice-issuer&version=release-a",
  );
  await page.getByRole("button", { name: "Verify reference" }).click();
  await expect(page.getByText("Registered task package")).toBeVisible();
  await page
    .getByRole("textbox", { name: "Correlation key", exact: true })
    .fill("é".repeat(257));
  await page
    .getByRole("checkbox", { name: "Include correlation, including empty" })
    .check();
  await page
    .getByRole("button", { name: "Submit execution", exact: true })
    .click();
  await expect(page.getByRole("alert")).toHaveText(
    "Correlation key must be at most 512 UTF-8 bytes.",
  );
  expect(submissions).toBe(0);
});

test("workflow status labels remain intact at desktop width", async ({
  page,
}) => {
  await mount(page);
  const workflows = dto.workflowPage(fixture("workflows"));
  const first = workflows.items[0];
  if (!first) throw new Error("Workflow fixture required");
  workflows.items = ["cancelled", "succeeded"].map((state, index) => ({
    ...first,
    workflow: {
      ...first.workflow,
      workflow_id: `wf_layout_${index}`,
      state: state as "cancelled" | "succeeded",
    },
  }));
  await page.route("**/v1/console/executions?**", (route) =>
    route.fulfill({
      status: 200,
      headers,
      body: stringifyUserJson({
        ...workflows,
        items: workflows.items.map(({ workflow, controller, queue }) => ({
          kind: "workflow",
          id: workflow.workflow_id,
          descriptor: controller,
          queue,
          state: workflow.state,
          submitted_at: workflow.submitted_at,
          terminal_at: workflow.terminal_at,
          correlation_key: workflow.correlation_key,
          parent_workflow_id: workflow.parent_workflow_id,
          root_workflow_id: workflow.root_workflow_id,
        })),
      }),
    }),
  );
  await page.setViewportSize({ width: 1280, height: 900 });
  await page.goto("/console/workflows");
  const labels = page.locator('td[data-label="Status"] .status');
  await expect(labels).toHaveCount(2);
  for (const label of await labels.all()) {
    expect(
      await label.evaluate((element) => element.getBoundingClientRect().height),
    ).toBeLessThan(25);
  }
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= innerWidth,
    ),
  ).toBe(true);
});

test("keyboard Tab traverses the modal and Escape restores the trigger", async ({
  page,
}) => {
  await mount(page);
  await page.goto("/console/agents");
  await keyboardDialog(page);
});

test("reduced motion preserves navigation and dialog interaction", async ({
  page,
}) => {
  await mount(page);
  await page.goto("/console/agents");
  await reducedMotionDialog(page);
});

test("touch input activates navigation and a modal at 375px", async ({
  browser,
  baseURL,
}) => {
  if (!baseURL) throw new Error("The test server base URL is required.");
  const context = await browser.newContext({
    baseURL,
    hasTouch: true,
    viewport: { width: 375, height: 812 },
  });
  try {
    const page = await context.newPage();
    await mount(page);
    await page.goto("/console/agents");
    await touchNavigation(page);
  } finally {
    await context.close();
  }
});

test("200 percent CSS layout scaling preserves controls and bounded content", async ({
  page,
}) => {
  await mount(page);
  await doubledLayout(page);
});

test("accepted cancellation stays pending until an observed terminal state", async ({
  page,
}) => {
  await mount(page);
  const observed = dto.observedTask(fixture("task_status"));
  observed.task.state = "active";
  observed.task.terminal_at = null;
  observed.task.cancel_requested_at = null;
  let accepted = false;
  let terminal = false;
  let reads = 0;
  await page.route("**/v1/console/tasks/status?**", async (route) => {
    reads++;
    await route.fulfill({
      status: 200,
      headers,
      body: stringifyUserJson({
        ...observed,
        task: {
          ...observed.task,
          state: terminal ? "cancelled" : "active",
          cancel_requested_at: accepted ? observed.observed_at : null,
          terminal_at: terminal ? observed.observed_at : null,
        },
      }),
    });
  });
  await page.route("**/v1/console/tasks/cancel", async (route) => {
    accepted = true;
    await route.fulfill({
      status: 200,
      headers,
      body: stringifyUserJson({
        task_id: observed.task.task_id,
        state: "active",
        observed_at: observed.observed_at,
      }),
    });
  });
  await page.goto(`/console/executions/${observed.task.task_id}`);
  await page
    .getByRole("button", { name: "Cancel execution", exact: true })
    .click();
  await page
    .getByRole("button", { name: "Request cancellation", exact: true })
    .click();
  await expect(page.getByRole("dialog")).toHaveCount(0);
  await expect(
    page.getByText("Cancellation requested; awaiting final state", {
      exact: true,
    }),
  ).toBeVisible();
  await expect(page.locator(".summary-line .status")).toHaveText("active");
  await expect(
    page.getByRole("heading", { name: "Execution cancelled" }),
  ).toHaveCount(0);
  const pendingRead = reads;
  terminal = true;
  // Refocus triggers the real query lifecycle; the backend response controls the state.
  await page.evaluate(() => window.dispatchEvent(new Event("online")));
  await expect
    .poll(() => reads, { timeout: 15000 })
    .toBeGreaterThan(pendingRead);
  await expect(page.locator(".summary-line .status")).toHaveText("cancelled");
  await expect(
    page.getByText("Cancellation requested; awaiting final state", {
      exact: true,
    }),
  ).toHaveCount(0);
  await expect(
    page.getByRole("button", { name: "Cancel execution", exact: true }),
  ).toHaveCount(0);
});
