import { afterEach, expect, it, vi } from "vitest";
import { render } from "vitest-browser-react";
import { createMemoryRouter, RouterProvider } from "react-router";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import fixtureSource from "../../../crates/ledgence-orchestration-api/tests/fixtures/console-v4.json?raw";
import { apiPath } from "../../src/api/client";
import { decodeConfig } from "../../src/api/codecs";
import { parseUserJson, stringifyUserJson } from "../../src/api/json";
import * as dto from "../../src/api/resources";
import { InstanceContext } from "../../src/app/instance";
import {
  AgentDetailPage,
  ProgramVersionPage,
} from "../../src/features/catalog";
import { ExecutionDetailPage } from "../../src/features/executions";
import { WorkflowDetailPage } from "../../src/features/workflows";

const fixture = parseUserJson(fixtureSource, 2 * 1024 * 1024);
function field(key: string): unknown {
  if (!fixture || typeof fixture !== "object" || !(key in fixture))
    throw new Error(`Missing Rust fixture: ${key}`);
  return Reflect.get(fixture, key);
}
const config = decodeConfig(field("config"));
config.polling.active_task_ms = 60000;
config.polling.waiting_workflow_ms = 60000;
const clients: QueryClient[] = [];
const routers: ReturnType<typeof createMemoryRouter>[] = [];
afterEach(() => {
  for (const router of routers) router.dispose();
  for (const client of clients) client.clear();
  routers.length = 0;
  clients.length = 0;
  vi.restoreAllMocks();
});
function response(value: unknown, status = 200) {
  return new Response(stringifyUserJson(value), {
    status,
    headers: {
      "Content-Type": "application/json",
      "Ledgence-Console-Contract": "4",
      "Ledgence-Instance-Id": config.instance_id,
    },
  });
}
function queryKey(path: string) {
  return [location.origin, config.contract_version, config.instance_id, path];
}
async function mount(
  route: string,
  reads: Map<string, unknown>,
  postResponse?: () => Promise<Response>,
) {
  const posts: { path: string; body: string }[] = [];
  vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
    const url = new URL(String(input), location.origin);
    if (init?.method === "POST") {
      posts.push({ path: url.pathname, body: String(init.body) });
      if (postResponse) return postResponse();
      return response({}, 503);
    }
    const path = `${url.pathname}${url.search}`;
    if (!reads.has(path)) throw new Error(`Unexpected read: ${path}`);
    return response(reads.get(path));
  });
  const client = new QueryClient({
    defaultOptions: {
      queries: { retry: false, refetchOnWindowFocus: false },
      mutations: { retry: false },
    },
  });
  clients.push(client);
  // Both routes are cached so navigating cannot hide the bug by unmounting
  // their command controls behind an intermediate loading state.
  for (const [path, value] of reads) client.setQueryData(queryKey(path), value);
  const router = createMemoryRouter(
    [
      { path: "/agents/:programId", element: <AgentDetailPage /> },
      {
        path: "/agents/:programId/versions/:version",
        element: <ProgramVersionPage />,
      },
      { path: "/executions/:taskId", element: <ExecutionDetailPage /> },
      { path: "/workflows/:workflowId", element: <WorkflowDetailPage /> },
    ],
    { initialEntries: [route] },
  );
  routers.push(router);
  const view = await render(
    <QueryClientProvider client={client}>
      <InstanceContext.Provider value={config}>
        <RouterProvider router={router} />
      </InstanceContext.Provider>
    </QueryClientProvider>,
  );
  return { view, client, router, posts };
}
function workflowReads(ids: string[], tab: "Waits" | "Context") {
  const reads = new Map<string, unknown>();
  for (const id of ids) {
    const detail = dto.workflowDetail(field("workflow_detail"));
    detail.summary.workflow.workflow_id = id;
    detail.summary.workflow.state = "waiting";
    detail.child_wait = null;
    detail.external_wait_key = "approval:1";
    reads.set(apiPath("workflows/inspect", { workflow_id: id }), detail);
    if (tab === "Waits") {
      reads.set(
        apiPath("workflows/waits", {
          workflow_id: id,
          limit: config.limits.default_page_size,
        }),
        dto.workflowWaits({
          page: {
            items: [
              {
                workflow_id: id,
                wait_key: "approval:1",
                activation_id: "task_activation",
                kind: "event",
                deadline: null,
                registered_at: 0,
                closed_at: null,
              },
            ],
            next_cursor: null,
            observed_at: 0,
          },
          child_wait: null,
          revision: "1",
        }),
      );
    }
  }
  return reads;
}

it("keeps an uncertain event on the same workflow but starts fresh on another cached workflow", async () => {
  const { view, client, router, posts } = await mount(
    "/workflows/first?tab=Waits",
    workflowReads(["first", "second"], "Waits"),
  );
  const open = () =>
    view.getByRole("button", { name: "Send event", exact: true }).click();
  const close = () =>
    view.getByRole("button", { name: "Close dialog", exact: true }).click();
  const event = (id: string) =>
    stringifyUserJson({
      specversion: "1.0",
      id,
      source: "urn:test:approval",
      type: "approved",
      data: {},
    });
  await open();
  await view
    .getByRole("textbox", { name: "Complete CloudEvent JSON" })
    .fill(event("review-first"));
  await view
    .getByRole("dialog")
    .getByRole("button", { name: "Send event", exact: true })
    .click();
  await expect
    .element(view.getByText("The server returned HTTP 503.", { exact: true }))
    .toBeVisible();
  await expect
    .element(
      view.getByText("first | approval:1 | urn:test:approval | review-first", {
        exact: true,
      }),
    )
    .toBeVisible();
  await close();
  await client.refetchQueries({
    queryKey: queryKey(apiPath("workflows/inspect", { workflow_id: "first" })),
  });
  await client.refetchQueries({
    queryKey: queryKey(
      apiPath("workflows/waits", {
        workflow_id: "first",
        limit: config.limits.default_page_size,
      }),
    ),
  });
  await open();
  await expect
    .element(view.getByRole("textbox", { name: "Complete CloudEvent JSON" }))
    .toBeDisabled();
  await view.getByRole("button", { name: "Try again", exact: true }).click();
  await expect.poll(() => posts.length).toBe(2);
  expect(posts[1]).toEqual(posts[0]);
  await expect
    .element(view.getByText("The server returned HTTP 503.", { exact: true }))
    .toBeVisible();
  await close();
  await router.navigate("/workflows/second?tab=Waits");
  await open();
  const input = view.getByRole("textbox", { name: "Complete CloudEvent JSON" });
  await expect.element(input).toBeEnabled();
  await expect.element(input).toHaveValue("");
  await expect
    .element(view.getByRole("button", { name: "Try again", exact: true }))
    .not.toBeInTheDocument();
  await input.fill(event("review-second"));
  await view
    .getByRole("dialog")
    .getByRole("button", { name: "Send event", exact: true })
    .click();
  await expect.poll(() => posts.length).toBe(3);
  expect(parseUserJson(posts[2]!.body, 4096)).toEqual({
    workflow_id: "second",
    key: "approval:1",
    event: parseUserJson(event("review-second"), 4096),
  });
});

it("does not apply a late event receipt to the next workflow's open draft", async () => {
  let complete: ((value: Response) => void) | undefined;
  const { view, client, router, posts } = await mount(
    "/workflows/first?tab=Waits",
    workflowReads(["first", "second"], "Waits"),
    () =>
      new Promise<Response>((resolve) => {
        complete = resolve;
      }),
  );
  await view.getByRole("button", { name: "Send event", exact: true }).click();
  await view.getByRole("textbox", { name: "Complete CloudEvent JSON" }).fill(
    stringifyUserJson({
      specversion: "1.0",
      id: "review-first",
      source: "urn:test:approval",
      type: "approved",
      data: {},
    }),
  );
  await view
    .getByRole("dialog")
    .getByRole("button", { name: "Send event", exact: true })
    .click();
  await expect.poll(() => posts.length).toBe(1);
  await router.navigate("/workflows/second?tab=Waits");
  await expect.element(view.getByRole("dialog")).not.toBeInTheDocument();
  await view.getByRole("button", { name: "Send event", exact: true }).click();
  const input = view.getByRole("textbox", { name: "Complete CloudEvent JSON" });
  await expect.element(input).toHaveValue("");
  await input.fill('{"draft":"second workflow"}');
  complete?.(
    response({
      workflow_id: "first",
      key: "approval:1",
      event_id: "review-first",
      event_source: "urn:test:approval",
      accepted_at: 0,
      already_accepted: false,
    }),
  );
  await expect
    .poll(() => client.getMutationCache().getAll()[0]?.state.status)
    .toBe("success");
  await expect.element(view.getByRole("dialog")).toBeVisible();
  await expect.element(input).toBeEnabled();
  await expect.element(input).toHaveValue('{"draft":"second workflow"}');
  await expect
    .element(view.getByRole("heading", { name: "Event accepted", exact: true }))
    .not.toBeInTheDocument();
  expect(posts).toHaveLength(1);
});

for (const kind of ["task", "workflow"] as const) {
  it(`binds uncertain ${kind} cancellation to its resource across cached route changes`, async () => {
    const route = (id: string) =>
      kind === "task" ? `/executions/${id}` : `/workflows/${id}?tab=Context`;
    const resource = kind === "task" ? "tasks/status" : "workflows/inspect";
    const parameters = (id: string) =>
      kind === "task" ? { task_id: id } : { workflow_id: id };
    const reads =
      kind === "workflow"
        ? workflowReads(["first", "second"], "Context")
        : new Map<string, unknown>();
    if (kind === "task")
      for (const id of ["first", "second"]) {
        const status = dto.observedTask(field("task_status"));
        status.task.task_id = id;
        status.task.state = "active";
        reads.set(apiPath(resource, parameters(id)), status);
      }
    const { view, client, router, posts } = await mount(route("first"), reads);
    const open = () =>
      view
        .getByRole("button", {
          name: kind === "task" ? "Cancel execution" : "Cancel workflow",
          exact: true,
        })
        .click();
    const close = () =>
      view.getByRole("button", { name: "Close dialog", exact: true }).click();
    await open();
    await view
      .getByRole("button", { name: "Request cancellation", exact: true })
      .click();
    await expect
      .element(view.getByText("The server returned HTTP 503.", { exact: true }))
      .toBeVisible();
    await close();
    await client.refetchQueries({
      queryKey: queryKey(apiPath(resource, parameters("first"))),
    });
    await open();
    await view.getByRole("button", { name: "Try again", exact: true }).click();
    await expect.poll(() => posts.length).toBe(2);
    expect(posts[1]).toEqual(posts[0]);
    await expect
      .element(view.getByText("The server returned HTTP 503.", { exact: true }))
      .toBeVisible();
    await close();
    await router.navigate(route("second"));
    await open();
    await expect
      .element(view.getByRole("button", { name: "Try again", exact: true }))
      .not.toBeInTheDocument();
    await view
      .getByRole("button", { name: "Request cancellation", exact: true })
      .click();
    await expect.poll(() => posts.length).toBe(3);
    expect(posts[2]?.path).toBe(
      apiPath(`${kind === "task" ? "tasks" : "workflows"}/cancel`),
    );
    expect(parseUserJson(posts[2]!.body, 4096)).toEqual(parameters("second"));
  });
}

for (const target of ["program", "version", "program-and-version"] as const) {
  it(`starts registration from the current ${target} route without carrying another reference's command`, async () => {
    const first = { id: "first", version: "1.0.0" };
    const second = {
      id: target === "version" ? "first" : "second",
      version: "2.0.0",
    };
    const route = (program: typeof first) =>
      target === "program"
        ? `/agents/${program.id}`
        : `/agents/${program.id}/versions/${program.version}`;
    const path = (program: typeof first) =>
      target === "program"
        ? apiPath("programs/versions", {
            program_id: program.id,
            limit: config.limits.default_page_size,
          })
        : apiPath("programs/inspect", {
            program_id: program.id,
            version: program.version,
          });
    const reads = new Map<string, unknown>();
    for (const program of [first, second]) {
      const detail = dto.programDetail(field("program_detail"));
      detail.version.descriptor.program = program;
      detail.version.manifest.program = program;
      reads.set(
        path(program),
        target === "program"
          ? { items: [], next_cursor: null, observed_at: 0 }
          : detail,
      );
    }
    const { view, client, router, posts } = await mount(route(first), reads);
    const open = () =>
      view
        .getByRole("button", {
          name:
            target === "program"
              ? "Register program"
              : "Register / update metadata",
          exact: true,
        })
        .click();
    const close = () =>
      view.getByRole("button", { name: "Close dialog", exact: true }).click();
    await open();
    await expect
      .element(view.getByRole("textbox", { name: "Program ID", exact: true }))
      .toHaveValue(first.id);
    if (target === "program")
      await view
        .getByRole("textbox", { name: "Exact version", exact: true })
        .fill(first.version);
    await view
      .getByRole("button", { name: "Register reference", exact: true })
      .click();
    await expect
      .element(view.getByText("The server returned HTTP 503.", { exact: true }))
      .toBeVisible();
    await close();
    await client.refetchQueries({ queryKey: queryKey(path(first)) });
    await open();
    await view.getByRole("button", { name: "Try again", exact: true }).click();
    await expect.poll(() => posts.length).toBe(2);
    expect(posts[1]).toEqual(posts[0]);
    await expect
      .element(view.getByText("The server returned HTTP 503.", { exact: true }))
      .toBeVisible();
    await close();
    await router.navigate(route(second));
    await open();
    await expect
      .element(view.getByRole("textbox", { name: "Program ID", exact: true }))
      .toHaveValue(second.id);
    const version = view.getByRole("textbox", {
      name: "Exact version",
      exact: true,
    });
    await expect.element(version).toBeEnabled();
    await expect
      .element(version)
      .toHaveValue(target === "program" ? "" : second.version);
    if (target === "program") await version.fill(second.version);
    await view
      .getByRole("button", { name: "Register reference", exact: true })
      .click();
    await expect.poll(() => posts.length).toBe(3);
    expect(posts[2]?.path).toBe(apiPath("programs/register"));
    expect(parseUserJson(posts[2]!.body, 4096)).toMatchObject({
      program: second,
    });
  });
}
