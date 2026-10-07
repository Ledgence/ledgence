import { afterEach, expect, it, vi } from "vitest";
import { render } from "vitest-browser-react";
import { MemoryRouter, Route, Routes, useLocation } from "react-router";
import { QueryClientProvider, type QueryClient } from "@tanstack/react-query";
import fixtureSource from "../../../crates/ledgence-orchestration-api/tests/fixtures/console-v5.json?raw";
import { parseUserJson, stringifyUserJson } from "../../src/api/json";
import { decodeConfig } from "../../src/api/codecs";
import * as dto from "../../src/api/resources";
import { InstanceContext } from "../../src/app/instance";
import { createQueryClient } from "../../src/app/query-client";
import { ExecutionDetailPage } from "../../src/features/executions";
import { WorkflowDetailPage } from "../../src/features/workflows";

const fixture = parseUserJson(fixtureSource, 2 * 1024 * 1024);
function field(key: string): unknown {
  if (!fixture || typeof fixture !== "object" || !(key in fixture))
    throw new Error(`Missing Rust fixture: ${key}`);
  return Reflect.get(fixture, key);
}
const config = decodeConfig(field("config"));
// Tests drive the parent observation explicitly; periodic reads must not mask a
// missing final refresh or extend the final request's bounded retry policy.
config.polling.active_task_ms = 60000;
config.polling.waiting_workflow_ms = 60000;
const clients: QueryClient[] = [];
afterEach(() => {
  for (const client of clients) client.clear();
  clients.length = 0;
  vi.restoreAllMocks();
});
function response(value: unknown, status = 200) {
  return new Response(stringifyUserJson(value), {
    status,
    headers: {
      "Content-Type": "application/json",
      "Ledgence-Console-Contract": "5",
      "Ledgence-Instance-Id": config.instance_id,
    },
  });
}
async function mount(route: string, returnTo?: string) {
  const address = new URL(route, "http://console.test");
  let currentRoute = route;
  function LocationProbe() {
    const current = useLocation();
    currentRoute = current.pathname + current.search;
    return null;
  }
  const client = createQueryClient();
  client.setDefaultOptions({
    ...client.getDefaultOptions(),
    queries: {
      ...client.getDefaultOptions().queries,
      refetchOnWindowFocus: false,
      retryDelay: 1,
    },
  });
  clients.push(client);
  const view = await render(
    <QueryClientProvider client={client}>
      <InstanceContext.Provider value={config}>
        <MemoryRouter
          initialEntries={[
            {
              pathname: address.pathname,
              search: address.search,
              state: returnTo ? { returnTo } : null,
            },
          ]}
        >
          <LocationProbe />
          <Routes>
            <Route
              path="/executions/:taskId"
              element={<ExecutionDetailPage />}
            />
            <Route
              path="/workflows/:workflowId"
              element={<WorkflowDetailPage />}
            />
          </Routes>
        </MemoryRouter>
      </InstanceContext.Provider>
    </QueryClientProvider>,
  );
  return { view, client, route: () => currentRoute };
}
function refreshParent(client: QueryClient, resource: string) {
  return client.invalidateQueries({
    predicate: (query) =>
      query.queryKey.some(
        (key) =>
          typeof key === "string" && key.startsWith(`/v1/console/${resource}?`),
      ),
  });
}
function taskStatus(terminal: boolean) {
  const value = dto.observedTask(field("task_status"));
  value.task.state = terminal ? "succeeded" : "active";
  value.task.attempt_count = 1;
  value.task.current_attempt_id = terminal ? null : "att_invoice_1";
  value.task.latest_attempt_id = "att_invoice_1";
  value.task.terminal_at = terminal ? value.observed_at : null;
  return value;
}
function attempts(terminal: boolean) {
  const value = dto.attemptPage(field("attempts"));
  const attempt = value.items[0];
  if (!attempt) throw new Error("Missing attempt fixture");
  attempt.state = terminal ? "succeeded" : "active";
  attempt.finished_at = terminal ? value.observed_at : null;
  attempt.quiescence = terminal ? "confirmed" : "unconfirmed";
  return value;
}

it("refreshes attempts after parent completion and retries the final read", async () => {
  let terminal = false;
  let attemptReads = 0;
  vi.spyOn(globalThis, "fetch").mockImplementation(async (input) => {
    const path = new URL(String(input), location.origin).pathname;
    if (path.endsWith("/tasks/status")) return response(taskStatus(terminal));
    if (path.endsWith("/tasks/attempts")) {
      attemptReads++;
      return attemptReads === 2
        ? response({}, 503)
        : response(attempts(terminal));
    }
    throw new Error(`Unexpected request ${path}`);
  });
  const { view, client } = await mount(
    "/executions/task_invoice_1042?tab=Attempts",
  );
  await expect
    .element(view.getByRole("button", { name: "att_invoice_1" }))
    .toBeVisible();
  terminal = true;
  await refreshParent(client, "tasks/status");
  await expect.poll(() => attemptReads, { timeout: 1000 }).toBe(3);
  await expect
    .element(view.getByRole("cell", { name: "succeeded", exact: true }))
    .toBeVisible();
  await new Promise((resolve) => setTimeout(resolve, 100));
  expect(attemptReads).toBe(3);
});

it("waits for an in-flight initial observation before making the final read", async () => {
  let terminal = false;
  let attemptReads = 0;
  let release: ((reply: Response) => void) | undefined;
  vi.spyOn(globalThis, "fetch").mockImplementation(async (input) => {
    const path = new URL(String(input), location.origin).pathname;
    if (path.endsWith("/tasks/status")) return response(taskStatus(terminal));
    if (path.endsWith("/tasks/attempts")) {
      if (++attemptReads === 1)
        return new Promise<Response>((resolve) => {
          release = resolve;
        });
      return response(attempts(terminal));
    }
    throw new Error(`Unexpected request ${path}`);
  });
  const { view, client } = await mount(
    "/executions/task_invoice_1042?tab=Attempts",
  );
  await expect.poll(() => !!release).toBe(true);
  terminal = true;
  await refreshParent(client, "tasks/status");
  release?.(response(attempts(false)));
  await expect.poll(() => attemptReads, { timeout: 1000 }).toBe(2);
  await expect
    .element(view.getByRole("cell", { name: "succeeded", exact: true }))
    .toBeVisible();
});

for (const [status, totalReads] of [
  [404, 2],
  [503, 4],
] as const) {
  it(`stops the final attempt read after bounded HTTP ${status} handling`, async () => {
    let terminal = false;
    let attemptReads = 0;
    vi.spyOn(globalThis, "fetch").mockImplementation(async (input) => {
      const path = new URL(String(input), location.origin).pathname;
      if (path.endsWith("/tasks/status")) return response(taskStatus(terminal));
      if (path.endsWith("/tasks/attempts")) {
        attemptReads++;
        return terminal ? response({}, status) : response(attempts(false));
      }
      throw new Error(`Unexpected request ${path}`);
    });
    const { view, client } = await mount(
      "/executions/task_invoice_1042?tab=Attempts",
    );
    await expect
      .element(view.getByRole("button", { name: "att_invoice_1" }))
      .toBeVisible();
    terminal = true;
    await refreshParent(client, "tasks/status");
    await expect.poll(() => attemptReads, { timeout: 1000 }).toBe(totalReads);
    await expect
      .element(view.getByRole("alert"))
      .toHaveTextContent(`HTTP ${status}`);
    await new Promise((resolve) => setTimeout(resolve, 100));
    expect(attemptReads).toBe(totalReads);
  });
}

it("refreshes recorded activations and children when the workflow completes", async () => {
  let terminal = false;
  let activationReads = 0;
  let childReads = 0;
  vi.spyOn(globalThis, "fetch").mockImplementation(async (input) => {
    const path = new URL(String(input), location.origin).pathname;
    if (path.endsWith("/workflows/inspect")) {
      const detail = dto.workflowDetail(field("workflow_detail"));
      detail.summary.workflow.state = terminal ? "succeeded" : "waiting";
      detail.summary.workflow.terminal_at = terminal
        ? detail.observed_at
        : null;
      if (terminal) detail.child_wait = null;
      return response(detail);
    }
    if (path.endsWith("/workflows/activations")) {
      activationReads++;
      return response(field("activations"));
    }
    if (path.endsWith("/workflows/children")) {
      childReads++;
      const children = dto.childPage(field("children"));
      const child = children.items[0];
      if (!child) throw new Error("Missing child fixture");
      child.task_state = terminal ? "succeeded" : "active";
      child.consumed = terminal;
      return response(children);
    }
    throw new Error(`Unexpected request ${path}`);
  });
  const { view, client } = await mount(
    "/workflows/wf_invoice_1042?tab=Recorded+work",
  );
  await expect
    .element(view.getByText("Result not consumed", { exact: true }))
    .toBeVisible();
  await view.getByRole("checkbox", { name: "Show completed children" }).click();
  terminal = true;
  await refreshParent(client, "workflows/inspect");
  await expect
    .poll(() => [activationReads, childReads], { timeout: 1000 })
    .toEqual([2, 2]);
  await expect
    .element(view.getByText("Result consumed", { exact: true }))
    .toBeVisible();
});

it("refreshes waits once after workflow completion", async () => {
  let terminal = false;
  let waitReads = 0;
  vi.spyOn(globalThis, "fetch").mockImplementation(async (input) => {
    const path = new URL(String(input), location.origin).pathname;
    if (path.endsWith("/workflows/inspect")) {
      const detail = dto.workflowDetail(field("workflow_detail"));
      detail.summary.workflow.state = terminal ? "succeeded" : "waiting";
      detail.summary.workflow.terminal_at = terminal
        ? detail.observed_at
        : null;
      detail.child_wait = null;
      detail.external_wait_key = terminal ? null : "review-event";
      return response(detail);
    }
    if (path.endsWith("/workflows/waits")) {
      waitReads++;
      const waits = dto.workflowWaits(field("waits"));
      waits.child_wait = null;
      waits.page.items = [
        {
          workflow_id: "wf_invoice_1042",
          wait_key: "review-event",
          activation_id: "task_controller_1",
          kind: "event",
          deadline: null,
          registered_at: waits.page.observed_at,
          closed_at: terminal ? waits.page.observed_at : null,
        },
      ];
      return response(waits);
    }
    throw new Error(`Unexpected request ${path}`);
  });
  const { view, client } = await mount("/workflows/wf_invoice_1042?tab=Waits");
  await expect.element(view.getByText("open", { exact: true })).toBeVisible();
  terminal = true;
  await refreshParent(client, "workflows/inspect");
  await expect.poll(() => waitReads, { timeout: 1000 }).toBe(2);
  await expect.element(view.getByText("closed", { exact: true })).toBeVisible();
});

it("keeps the filtered execution return link when selecting an attempt", async () => {
  vi.spyOn(globalThis, "fetch").mockImplementation(async (input) => {
    const path = new URL(String(input), location.origin).pathname;
    if (path.endsWith("/tasks/status")) return response(taskStatus(false));
    if (path.endsWith("/tasks/attempts")) return response(field("attempts"));
    if (path.endsWith("/attempts/inspect"))
      return response(field("attempt_detail"));
    throw new Error(`Unexpected request ${path}`);
  });
  const returnTo = "/executions?queue=billing&cursor=prior-page";
  const { view } = await mount(
    "/executions/task_invoice_1042?tab=Attempts",
    returnTo,
  );
  await view
    .getByRole("button", { name: "att_invoice_1", exact: true })
    .click();
  await expect
    .element(
      view.getByRole("heading", { name: "Attempt details", exact: true }),
    )
    .toBeVisible();
  await expect
    .element(view.getByRole("link", { name: "Executions", exact: true }))
    .toHaveAttribute("href", returnTo);
});

for (const explicit of [false, true]) {
  it(`refreshes ${explicit ? "selected" : "default"} local checkpoints after the workflow becomes terminal`, async () => {
    let terminal = false;
    let currentActivation = "task_controller_1";
    let reads = 0;
    vi.spyOn(globalThis, "fetch").mockImplementation(async (input) => {
      const path = new URL(String(input), location.origin).pathname;
      if (path.endsWith("/workflows/inspect")) {
        const detail = dto.workflowDetail(field("workflow_detail"));
        detail.summary.workflow.state = terminal ? "succeeded" : "running";
        detail.summary.workflow.activation_id = terminal
          ? null
          : currentActivation;
        detail.summary.workflow.terminal_at = terminal
          ? detail.observed_at
          : null;
        detail.child_wait = null;
        return response(detail);
      }
      if (path.endsWith("/workflows/local-steps")) {
        reads++;
        const steps = dto.localStepPage(field("local_steps"));
        if (!terminal) steps.items = [];
        return response(steps);
      }
      throw new Error(`Unexpected request ${path}`);
    });
    const { view, client } = await mount(
      "/workflows/wf_invoice_1042?tab=Local+steps" +
        (explicit ? "&activation=task_controller_1" : ""),
    );
    await expect.poll(() => reads).toBe(1);
    currentActivation = "task_controller_2";
    await refreshParent(client, "workflows/inspect");
    await expect
      .element(
        view.getByRole("textbox", { name: "Activation ID", exact: true }),
      )
      .toHaveValue("task_controller_1");
    expect(reads).toBe(1);
    terminal = true;
    await refreshParent(client, "workflows/inspect");
    await expect.poll(() => reads, { timeout: 1000 }).toBe(2);
    const step = dto.localStepPage(field("local_steps")).items[0];
    if (!step) throw new Error("Missing local checkpoint fixture");
    await expect
      .element(view.getByRole("cell", { name: step.step_key, exact: true }))
      .toBeVisible();
    await new Promise((resolve) => setTimeout(resolve, 100));
    expect(reads).toBe(2);
  });
}

it("keeps the activation in a copied checkpoint page URL after completion", async () => {
  let terminal = false;
  const requests: URL[] = [];
  vi.spyOn(globalThis, "fetch").mockImplementation(async (input) => {
    const url = new URL(String(input), location.origin);
    if (url.pathname.endsWith("/workflows/inspect")) {
      const detail = dto.workflowDetail(field("workflow_detail"));
      detail.summary.workflow.state = terminal ? "succeeded" : "running";
      detail.summary.workflow.activation_id = terminal
        ? null
        : "task_controller_1";
      detail.summary.workflow.terminal_at = terminal
        ? detail.observed_at
        : null;
      detail.child_wait = null;
      return response(detail);
    }
    if (url.pathname.endsWith("/workflows/local-steps")) {
      requests.push(url);
      const steps = dto.localStepPage(field("local_steps"));
      steps.next_cursor = url.searchParams.has("cursor")
        ? null
        : "checkpoint-page-2";
      return response(steps);
    }
    throw new Error(`Unexpected request ${url.pathname}`);
  });
  const first = await mount("/workflows/wf_invoice_1042?tab=Local+steps");
  await first.view.getByRole("button", { name: "Next", exact: true }).click();
  await expect.poll(() => requests.length).toBe(2);
  const saved = first.route();
  const params = new URL(saved, "http://console.test").searchParams;
  expect(params.get("activation")).toBe("task_controller_1");
  expect(params.get("cursor")).toBe("checkpoint-page-2");
  await first.view.unmount();
  terminal = true;
  const restored = await mount(saved);
  await expect.poll(() => requests.length).toBe(3);
  expect(requests.at(-1)?.searchParams.get("activation_id")).toBe(
    "task_controller_1",
  );
  expect(requests.at(-1)?.searchParams.get("cursor")).toBe("checkpoint-page-2");
  await expect
    .element(
      restored.view.getByRole("textbox", {
        name: "Activation ID",
        exact: true,
      }),
    )
    .toHaveValue("task_controller_1");
});

it("reloads checkpoints when Load steps selects the same activation", async () => {
  let reads = 0;
  vi.spyOn(globalThis, "fetch").mockImplementation(async (input) => {
    const path = new URL(String(input), location.origin).pathname;
    if (path.endsWith("/workflows/inspect"))
      return response(field("workflow_detail"));
    if (path.endsWith("/workflows/local-steps")) {
      const steps = dto.localStepPage(field("local_steps"));
      if (++reads === 1) steps.items = [];
      return response(steps);
    }
    throw new Error(`Unexpected request ${path}`);
  });
  const { view } = await mount(
    "/workflows/wf_invoice_1042?tab=Local+steps&activation=task_controller_1",
  );
  await expect.poll(() => reads).toBe(1);
  await view.getByRole("button", { name: "Load steps", exact: true }).click();
  await expect.poll(() => reads, { timeout: 1000 }).toBe(2);
  const step = dto.localStepPage(field("local_steps")).items[0];
  if (!step) throw new Error("Missing local checkpoint fixture");
  await expect
    .element(view.getByRole("cell", { name: step.step_key, exact: true }))
    .toBeVisible();
});

it("keeps the filtered workflow return link when choosing local-step activation", async () => {
  vi.spyOn(globalThis, "fetch").mockImplementation(async (input) => {
    const path = new URL(String(input), location.origin).pathname;
    if (path.endsWith("/workflows/inspect"))
      return response(field("workflow_detail"));
    if (path.endsWith("/workflows/local-steps"))
      return response(field("local_steps"));
    throw new Error(`Unexpected request ${path}`);
  });
  const returnTo = "/workflows?state=waiting&cursor=prior-page";
  const { view } = await mount(
    "/workflows/wf_invoice_1042?tab=Local+steps",
    returnTo,
  );
  await view
    .getByRole("textbox", { name: "Activation ID", exact: true })
    .fill("task_controller_1");
  await view.getByRole("button", { name: "Load steps", exact: true }).click();
  await expect
    .element(view.getByRole("link", { name: "Executions", exact: true }))
    .toHaveAttribute("href", returnTo);
});

it("uses task Trace by default and requests payloads only when their General section opens", async () => {
  const reads: string[] = [];
  vi.spyOn(globalThis, "fetch").mockImplementation(async (input) => {
    const path = new URL(String(input), location.origin).pathname;
    reads.push(path);
    if (path.endsWith("/tasks/status")) return response(taskStatus(false));
    if (path.endsWith("/tasks/attempts")) return response(field("attempts"));
    if (path.endsWith("/tasks/inspect")) return response(field("task_detail"));
    if (path.endsWith("/tasks/result"))
      return response(field("pending_result"));
    return response({}, 404);
  });
  const { view } = await mount("/executions/task_invoice_1042");
  await expect
    .element(view.getByRole("button", { name: "Trace", exact: true }))
    .toHaveAttribute("aria-current", "page");
  await expect
    .element(view.getByRole("button", { name: "att_invoice_1", exact: true }))
    .toBeVisible();
  await expect
    .element(view.getByRole("button", { name: "Graph", exact: true }))
    .not.toBeInTheDocument();
  expect(
    reads.some(
      (path) =>
        path.endsWith("/tasks/inspect") || path.endsWith("/tasks/result"),
    ),
  ).toBe(false);
  await view.getByRole("button", { name: "General", exact: true }).click();
  await view.getByRole("button", { name: "Input", exact: true }).click();
  await expect
    .poll(() => reads.filter((path) => path.endsWith("/tasks/inspect")).length)
    .toBe(1);
  expect(reads.some((path) => path.endsWith("/tasks/result"))).toBe(false);
  await view.getByRole("button", { name: "Output", exact: true }).click();
  await expect
    .poll(() => reads.filter((path) => path.endsWith("/tasks/result")).length)
    .toBe(1);
});

for (const kind of ["task", "workflow"] as const) {
  it(`refreshes the visible ${kind} lifecycle after the parent becomes terminal`, async () => {
    let terminal = false;
    let reads = 0;
    vi.spyOn(globalThis, "fetch").mockImplementation(async (input) => {
      const path = new URL(String(input), location.origin).pathname;
      if (path.endsWith("/tasks/status")) return response(taskStatus(terminal));
      if (path.endsWith("/workflows/inspect")) {
        const detail = dto.workflowDetail(field("workflow_detail"));
        detail.summary.workflow.state = terminal ? "succeeded" : "waiting";
        detail.summary.workflow.terminal_at = terminal
          ? detail.observed_at
          : null;
        if (terminal) detail.child_wait = null;
        return response(detail);
      }
      if (
        path.endsWith(`/${kind === "task" ? "tasks" : "workflows"}/history`)
      ) {
        reads++;
        // Synthetic observation tests polling only; canonical wire contracts
        // are tested separately against Rust fixtures.
        return response({
          items: terminal
            ? [
                {
                  sequence: "1",
                  at: 0,
                  reason: "completed",
                  ...(kind === "task"
                    ? { task_id: "task_invoice_1042", attempt_id: null }
                    : { workflow_id: "wf_invoice_1042", activation_id: null }),
                },
              ]
            : [],
          observed_at: 0,
          next_cursor: null,
        });
      }
      return response({}, 404);
    });
    const route =
      kind === "task"
        ? "/executions/task_invoice_1042?tab=Trace&section=history"
        : "/workflows/wf_invoice_1042?tab=General&section=history";
    const { view, client } = await mount(route);
    await expect.poll(() => reads).toBe(1);
    terminal = true;
    await refreshParent(
      client,
      kind === "task" ? "tasks/status" : "workflows/inspect",
    );
    await expect
      .element(view.getByText("completed", { exact: true }))
      .toBeVisible();
    expect(reads).toBe(2);
  });
}
