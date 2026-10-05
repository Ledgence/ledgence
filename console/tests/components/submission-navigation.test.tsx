// SPDX-License-Identifier: MIT
import { afterEach, expect, it, vi } from "vitest";
import { render } from "vitest-browser-react";
import { createMemoryRouter, RouterProvider } from "react-router";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import source from "../../../crates/ledgence-orchestration-api/tests/fixtures/console-v5.json?raw";
import { decodeConfig } from "../../src/api/codecs";
import { parseUserJson, stringifyUserJson } from "../../src/api/json";
import { observedWorkflow, taskDetail } from "../../src/api/resources";
import { InstanceContext } from "../../src/app/instance";
import { NewExecutionPage } from "../../src/features/new-execution";

const fixture = parseUserJson(source, 2 * 1024 * 1024);
function field(key: string): unknown {
  if (!fixture || typeof fixture !== "object" || !(key in fixture))
    throw new Error(`Missing Rust fixture: ${key}`);
  return Reflect.get(fixture, key);
}
const config = decodeConfig(field("config"));
config.capabilities.programs = false;
const clients: QueryClient[] = [];
const routers: ReturnType<typeof createMemoryRouter>[] = [];
afterEach(() => {
  for (const router of routers.splice(0)) router.dispose();
  for (const client of clients.splice(0)) client.clear();
  vi.restoreAllMocks();
});

for (const kind of ["task", "workflow"] as const) {
  for (const destination of [
    "current",
    "other-page",
    "another-draft",
  ] as const) {
    it(`handles a late ${kind} submission when viewing ${destination}`, async () => {
      let complete: ((response: Response) => void) | undefined;
      const requests: RequestInit[] = [];
      vi.spyOn(globalThis, "fetch").mockImplementation((input, init) => {
        expect(String(input)).toBe(
          `/v1/console/${kind === "task" ? "tasks" : "workflows"}`,
        );
        expect(init?.method).toBe("POST");
        if (init) requests.push(init);
        return new Promise<Response>((resolve) => {
          complete = resolve;
        });
      });
      const client = new QueryClient({
        defaultOptions: {
          queries: { retry: false },
          mutations: { retry: false },
        },
      });
      clients.push(client);
      const invalidate = vi.spyOn(client, "invalidateQueries");
      const router = createMemoryRouter(
        [
          { path: "/executions/new", element: <NewExecutionPage /> },
          { path: "*", element: <p>Another page</p> },
        ],
        {
          initialEntries: [
            "/executions/new?program=invoice-issuer&version=release-a",
          ],
        },
      );
      routers.push(router);
      const view = await render(
        <QueryClientProvider client={client}>
          <InstanceContext.Provider value={config}>
            <RouterProvider router={router} />
          </InstanceContext.Provider>
        </QueryClientProvider>,
      );
      if (kind === "workflow")
        await view
          .getByRole("combobox", { name: "Execution kind" })
          .selectOptions("workflow");
      await view
        .getByRole("button", {
          name: kind === "task" ? "Submit execution" : "Start workflow",
          exact: true,
        })
        .click();
      await expect.poll(() => requests.length).toBe(1);
      if (destination === "other-page") {
        await router.navigate("/other");
        await expect
          .element(view.getByText("Another page", { exact: true }))
          .toBeVisible();
      }
      if (destination === "another-draft") {
        await router.navigate("/executions/new?program=second&version=1");
        await view
          .getByRole("textbox", { name: "JSON input", exact: true })
          .fill('{"new":"draft"}');
      }
      const reply =
        kind === "task"
          ? taskDetail(field("task_detail"))
          : observedWorkflow(field("workflow_status"));
      complete?.(
        new Response(stringifyUserJson(reply), {
          headers: {
            "Content-Type": "application/json",
            "Ledgence-Console-Contract": "5",
            "Ledgence-Instance-Id": config.instance_id,
          },
        }),
      );
      await expect
        .poll(() => client.getMutationCache().getAll()[0]?.state.status)
        .toBe("success");
      expect(invalidate).toHaveBeenCalledOnce();
      expect(requests).toHaveLength(1);
      expect(requests[0]?.signal?.aborted).toBe(false);
      if (destination === "current") {
        expect(router.state.location.pathname).toBe(
          kind === "task"
            ? `/executions/${taskDetail(field("task_detail")).task_id}`
            : `/workflows/${observedWorkflow(field("workflow_status")).workflow.workflow_id}`,
        );
      } else if (destination === "other-page") {
        expect(router.state.location.pathname).toBe("/other");
      } else {
        expect(
          router.state.location.pathname + router.state.location.search,
        ).toBe("/executions/new?program=second&version=1");
        await expect
          .element(
            view.getByRole("textbox", { name: "Program ID", exact: true }),
          )
          .toHaveValue("second");
        await expect
          .element(
            view.getByRole("textbox", { name: "JSON input", exact: true }),
          )
          .toHaveValue('{"new":"draft"}');
      }
    });
  }
}
