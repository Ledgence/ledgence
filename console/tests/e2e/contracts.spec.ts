import { readFileSync } from "node:fs";
import { expect, test, type Page } from "@playwright/test";
import { parseUserJson, stringifyUserJson } from "../../src/api/json";
import * as dto from "../../src/api/resources";
import { decodeConfig } from "../../src/api/codecs";
import {
  keyboardDialog,
  reducedMotionDialog,
  touchNavigation,
  doubledLayout,
} from "../interaction-checks";
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
    throw new Error(`Missing fixture ${name}`);
  return Reflect.get(parsed, name);
}
const headers = {
  "Content-Type": "application/json",
  "Ledgence-Console-Contract": "4",
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
for (const kind of ["task", "workflow"] as const) {
  test(`header Back leaves a directly opened ${kind} after canonical URL replacement`, async ({
    page,
  }) => {
    await mount(page);
    const task = dto.observedTask(fixture("task_status")).task;
    const workflow = dto.workflowDetail(fixture("workflow_detail")).summary
      .workflow;
    const path =
      kind === "task"
        ? `/console/executions/${task.task_id}`
        : `/console/workflows/${workflow.workflow_id}?tab=Overview`;
    await page.goto(path);
    await expect(page).toHaveURL(/tab=(Trace|General)&section=/);
    await page
      .locator(".shell-header")
      .getByRole("button", { name: "Back", exact: true })
      .click();
    await expect(page).toHaveURL(/\/console\/executions$/);
    await expect(
      page.getByRole("heading", { name: "Executions", exact: true }),
    ).toBeVisible();
  });
}

test("header Back preserves the filtered list after reloading a detail", async ({
  page,
}) => {
  await mount(page);
  const list = "/console/executions?kind=task&state=active&limit=25";
  await page.goto(list);
  await page.locator(".execution-name").first().click();
  await expect(page).toHaveURL(
    /\/executions\/[^?]+\?tab=Trace&section=attempts/,
  );
  await page.reload();
  await page
    .locator(".shell-header")
    .getByRole("button", { name: "Back", exact: true })
    .click();
  await expect(page).toHaveURL(new URL(list, page.url()).href);
  await expect(
    page.getByRole("combobox", { name: "Status", exact: true }),
  ).toHaveValue("active");
  await expect(
    page.getByRole("combobox", { name: "Type", exact: true }),
  ).toHaveValue("task");
});

for (const width of [320, 390, 1280]) {
  for (const kind of ["task", "workflow"] as const) {
    test(`unified execution header keeps ${kind} identity and controls usable at ${width}px`, async ({
      page,
    }, testInfo) => {
      await mount(page);
      await page.setViewportSize({ width, height: 900 });
      const task = dto.observedTask(fixture("task_status")).task;
      const workflow = dto.workflowDetail(fixture("workflow_detail")).summary
        .workflow;
      const id = kind === "task" ? task.task_id : workflow.workflow_id;
      const title = (
        kind === "task" ? task.correlation_key : workflow.correlation_key
      )!;
      const instanceName = decodeConfig(fixture("config")).instance_name;
      await page.goto(
        `/console/${kind === "task" ? "executions" : "workflows"}/${id}?tab=General`,
      );
      const header = page.locator(".shell-header");
      const heading = header.getByRole("heading", {
        level: 1,
        name: title,
        exact: true,
      });
      const ancestry = header.getByRole("navigation", {
        name: "Execution ancestry",
      });
      await expect(heading).toBeVisible();
      await expect(page.getByRole("heading", { level: 1 })).toHaveCount(1);
      await expect(header.locator(".instance-header-name")).toHaveText(
        instanceName,
      );
      await expect(
        ancestry.getByRole("link", { name: "Executions", exact: true }),
      ).toBeVisible();
      await expect(
        header.getByRole("button", { name: "Back", exact: true }),
      ).toBeVisible();
      await expect(
        header.getByRole("combobox", { name: "Appearance" }),
      ).toBeVisible();
      await expect(
        page.getByRole("button", { name: "General", exact: true }),
      ).toHaveAttribute("aria-current", "page");
      await expect(
        page.getByRole("button", { name: "Trace", exact: true }),
      ).toBeVisible();
      await expect(
        page.getByRole("button", {
          name: kind === "task" ? "Copy task ID" : "Copy workflow ID",
          exact: true,
        }),
      ).toBeVisible();
      const status = kind === "task" ? task.state : workflow.state;
      await expect(page.locator(".execution-summary .status")).toHaveText(
        status,
      );
      if (kind === "task") {
        await expect(
          page.getByRole("link", { name: "Run again", exact: true }),
        ).toHaveAttribute("href", `/console/executions/new?source_task=${id}`);
        await expect(
          page.getByRole("button", { name: "Graph", exact: true }),
        ).toHaveCount(0);
      } else {
        await expect(
          page.getByRole("button", { name: "Graph", exact: true }),
        ).toBeVisible();
        const cancel = page.getByRole("button", {
          name: "Cancel workflow",
          exact: true,
        });
        await cancel.focus();
        await cancel.press("Enter");
        await expect(page.getByRole("dialog")).toBeVisible();
        await page.keyboard.press("Escape");
        await expect(
          page.getByRole("button", { name: "Cancel workflow", exact: true }),
        ).toBeFocused();
      }
      expect(
        await page.evaluate(() => document.documentElement.scrollWidth),
      ).toBeLessThanOrEqual(width);
      const headerBox = (await header.boundingBox())!;
      const titleBox = (await heading.boundingBox())!;
      expect(titleBox.x).toBeGreaterThanOrEqual(headerBox.x);
      expect(titleBox.x + titleBox.width).toBeLessThanOrEqual(
        headerBox.x + headerBox.width,
      );
      expect(titleBox.width).toBeGreaterThan(0);
      expect(
        await heading.evaluate(
          (element) => element.scrollWidth <= element.clientWidth,
        ),
      ).toBe(true);
      expect(
        await heading.evaluate((element) =>
          parseFloat(getComputedStyle(element).fontSize),
        ),
      ).toBeGreaterThanOrEqual(15);
      if (width >= 768) {
        const instanceBox = (await header
          .locator(".instance-header-name")
          .boundingBox())!;
        const ancestryBox = (await ancestry.boundingBox())!;
        expect(
          Math.abs(
            instanceBox.y +
              instanceBox.height / 2 -
              titleBox.y -
              titleBox.height / 2,
          ),
        ).toBeLessThanOrEqual(2);
        expect(instanceBox.x + instanceBox.width).toBeLessThan(ancestryBox.x);
        expect(headerBox.height).toBeLessThanOrEqual(80);
      } else {
        expect(headerBox.height).toBeLessThanOrEqual(120);
        expect(
          (await header
            .getByRole("button", { name: "Back", exact: true })
            .boundingBox())!.height,
        ).toBeGreaterThanOrEqual(44);
      }
      if (
        testInfo.project.name === "chromium" &&
        (width === 390 || width === 1280)
      )
        await page.screenshot({
          path: testInfo.outputPath(`header-${kind}-${width}.png`),
          animations: "disabled",
        });
      await ancestry
        .getByRole("link", { name: "Executions", exact: true })
        .click();
      await expect(
        page.getByRole("heading", {
          level: 1,
          name: "Executions",
          exact: true,
        }),
      ).toBeVisible();
      await expect(header.getByRole("heading", { level: 1 })).toHaveCount(0);
    });
  }
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
  await page.getByRole("button", { name: "General", exact: true }).click();
  await page.getByRole("button", { name: "Input", exact: true }).click();
  await expect(page.locator('pre[aria-label="Input"]')).toContainText(
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
    page.getByRole("button", { name: "Recorded work", exact: true }),
  ).toHaveAttribute("aria-expanded", "true");
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

test("status filters keep exact filters and reset pagination", async ({
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
  const labels = page.getByRole("table").getByRole("cell").locator(".status");
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
  await expect(page.locator(".execution-summary .status")).toHaveText("active");
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
  await expect(page.locator(".execution-summary .status")).toHaveText(
    "cancelled",
  );
  await expect(
    page.getByText("Cancellation requested; awaiting final state", {
      exact: true,
    }),
  ).toHaveCount(0);
  await expect(
    page.getByRole("button", { name: "Cancel execution", exact: true }),
  ).toHaveCount(0);
});

for (const width of [320, 390, 1280]) {
  test(`General keeps payloads readable and bounded at ${width}px`, async ({
    page,
  }, testInfo) => {
    const requests = await mount(page);
    await page.setViewportSize({ width, height: 900 });
    await page.goto("/console/executions/task_invoice_1042?tab=Input");
    await expect(
      page.getByRole("button", { name: "General", exact: true }),
    ).toHaveAttribute("aria-current", "page");
    await expect(
      page.getByRole("button", { name: "Input", exact: true }),
    ).toHaveAttribute("aria-expanded", "true");
    await expect(page.locator('pre[aria-label="Input"]')).toContainText(
      "9007199254740993",
    );
    await expect(
      page.getByRole("button", { name: "Graph", exact: true }),
    ).toHaveCount(0);
    expect(requests).not.toContain("tasks/result");
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= window.innerWidth,
      ),
    ).toBe(true);
    if (width === 390 && testInfo.project.name === "chromium")
      await page.screenshot({
        path: testInfo.outputPath("general-mobile.png"),
        fullPage: true,
      });
    await page.getByRole("button", { name: "Input", exact: true }).click();
    await expect(page.locator('pre[aria-label="Input"]')).toHaveCount(0);
    await page.getByRole("button", { name: "Trace", exact: true }).click();
    await expect(
      page.getByRole("button", { name: "att_invoice_1", exact: true }),
    ).toBeVisible();
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= window.innerWidth,
      ),
    ).toBe(true);
  });
}

test("date and type filters send inclusive UTC days and prevent inverted queries", async ({
  page,
}) => {
  await mount(page);
  const reads: URL[] = [];
  page.on("request", (request) => {
    const url = new URL(request.url());
    if (url.pathname === "/v1/console/executions") reads.push(url);
  });
  await page.goto("/console/executions");
  await expect(
    page.getByRole("link", { name: "INV-1042", exact: true }),
  ).toBeVisible();
  await page
    .getByLabel("Submitted from · UTC", { exact: true })
    .fill("2026-09-28");
  await page
    .getByLabel("Submitted through · UTC", { exact: true })
    .fill("2026-09-29");
  await page
    .getByRole("combobox", { name: "Type", exact: true })
    .selectOption("workflow");
  await page
    .getByRole("combobox", { name: "Status", exact: true })
    .selectOption("waiting");
  await page
    .getByRole("button", { name: "Apply filters", exact: true })
    .click();
  await expect
    .poll(() => reads.at(-1)?.searchParams.get("state"))
    .toBe("waiting");
  expect(reads.at(-1)?.searchParams.get("submitted_from")).toBe(
    String(Date.UTC(2026, 8, 28)),
  );
  expect(reads.at(-1)?.searchParams.get("submitted_until")).toBe(
    String(Date.UTC(2026, 8, 30)),
  );
  expect(reads.at(-1)?.searchParams.get("kind")).toBe("workflow");
  await page
    .getByRole("combobox", { name: "Type", exact: true })
    .selectOption("task");
  await expect(
    page.getByRole("combobox", { name: "Status", exact: true }),
  ).toHaveValue("");
  const before = reads.length;
  await page
    .getByLabel("Submitted from · UTC", { exact: true })
    .fill("2026-10-01");
  await page
    .getByRole("button", { name: "Apply filters", exact: true })
    .click();
  await expect(page.getByRole("alert")).toContainText("Submitted through");
  expect(reads).toHaveLength(before);
});
