// SPDX-License-Identifier: MIT
import { afterEach, expect, it, vi } from "vitest";
import { render } from "vitest-browser-react";
import { createMemoryRouter, RouterProvider } from "react-router";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import source from "../../../crates/ledgence-orchestration-api/tests/fixtures/console-v16.json?raw";
import { decodeConfig } from "../../src/api/codecs";
import { InstanceContext } from "../../src/app/instance";
import { accessController } from "../../src/app/access-state";
import { ArtifactsPanel } from "../../src/features/artifacts";
import "../../src/styles/global.css";
const fixture = JSON.parse(source);
const config = decodeConfig(fixture.config);
const clients: QueryClient[] = [];
const routers: ReturnType<typeof createMemoryRouter>[] = [];
function identity(capabilities?: ("metadata_read" | "sensitive_read")[]) {
  const epoch = accessController.begin("test", true);
  accessController.accept(
    capabilities
      ? {
          mode: "credentials",
          actor_key: "actor-test",
          issuer: "issuer",
          subject: "reader",
          class: "operator",
          instance_id: config.instance_id,
          scope: { tenant_id: "acme", namespace: "billing" },
          capabilities,
          expires_at_ms: Date.now() + 60000,
        }
      : { mode: "operator_trusted" },
    epoch,
    config.instance_id,
  );
}
afterEach(() => {
  clients.splice(0).forEach((client) => client.clear());
  routers.splice(0).forEach((router) => router.dispose());
  vi.restoreAllMocks();
  identity();
});
function response(value: unknown) {
  return new Response(JSON.stringify(value), {
    headers: {
      "Content-Type": "application/json",
      "Ledgence-Console-Contract": "16",
      "Ledgence-Instance-Id": config.instance_id,
    },
  });
}
async function mount() {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  clients.push(client);
  const router = createMemoryRouter(
    [
      {
        path: "/detail",
        element: (
          <ArtifactsPanel
            target={{ kind: "task", id: "task_invoice_1042" }}
            active={false}
          />
        ),
      },
      { path: "/else", element: <p>Another view</p> },
    ],
    { initialEntries: ["/detail?tab=General&section=artifacts"] },
  );
  routers.push(router);
  const view = await render(
    <QueryClientProvider client={client}>
      <InstanceContext.Provider value={config}>
        <RouterProvider router={router} />
      </InstanceContext.Provider>
    </QueryClientProvider>,
  );
  return { view, router };
}
it("separates metadata access from downloads and explains canonical-root provenance", async () => {
  identity(["metadata_read"]);
  const fetch = vi
    .spyOn(globalThis, "fetch")
    .mockResolvedValue(response(fixture.artifact_page));
  const { view } = await mount();
  await expect
    .element(view.getByText("invoice-report.txt", { exact: true }))
    .toBeVisible();
  await expect
    .element(view.getByText("Sensitive-read access required"))
    .toBeVisible();
  await expect
    .element(view.getByRole("button", { name: "Download invoice-report.txt" }))
    .not.toBeInTheDocument();
  expect(JSON.parse(String(fetch.mock.calls[0]?.[1]?.body))).toEqual({
    target: { kind: "task", id: "task_invoice_1042" },
    limit: config.limits.default_page_size,
    cursor: null,
  });
  await view.getByText("File details", { exact: true }).click();
  await expect
    .element(view.getByRole("link", { name: "Workflow: wf_invoice_1042" }))
    .toBeVisible();
  expect(document.querySelector("iframe, img, object, embed")).toBeNull();
});
it("pages explicitly and displays an honest empty state", async () => {
  identity();
  const requests: unknown[] = [];
  vi.spyOn(globalThis, "fetch").mockImplementation(async (_url, init) => {
    const body = JSON.parse(String(init?.body));
    requests.push(body);
    return response(
      body.cursor
        ? { items: [], next_cursor: null }
        : { ...fixture.artifact_page, next_cursor: "next-page" },
    );
  });
  const { view } = await mount();
  await expect
    .element(view.getByText("invoice-report.txt", { exact: true }))
    .toBeVisible();
  await view.getByRole("button", { name: "Next", exact: true }).click();
  await expect
    .element(view.getByText("No artifacts yet.", { exact: false }))
    .toBeVisible();
  expect(requests.at(-1)).toMatchObject({ cursor: "next-page" });
  await view.getByRole("button", { name: "Previous", exact: true }).click();
  await expect
    .element(view.getByText("invoice-report.txt", { exact: true }))
    .toBeVisible();
});
it("does not fetch file contents before a click and cancels on navigation", async () => {
  identity();
  let binarySignal: AbortSignal | null | undefined;
  const paths: string[] = [];
  vi.spyOn(globalThis, "fetch").mockImplementation(async (url, init) => {
    paths.push(String(url));
    if (String(url).endsWith("/list")) return response(fixture.artifact_page);
    binarySignal = init?.signal;
    return new Promise<Response>((_resolve, reject) => {
      init?.signal?.addEventListener(
        "abort",
        () => reject(init.signal?.reason),
        { once: true },
      );
    });
  });
  const { view, router } = await mount();
  await expect
    .element(view.getByRole("button", { name: "Download invoice-report.txt" }))
    .toBeVisible();
  expect(paths).toEqual(["/v1/console/artifacts/list"]);
  await view
    .getByRole("button", { name: "Download invoice-report.txt" })
    .click();
  await expect.poll(() => binarySignal !== undefined).toBe(true);
  // The route can reuse the same panel and artifact IDs (for example while
  // moving among descendants). Navigation still cancels its binary transfer.
  await router.navigate("/detail?tab=General&section=artifacts&changed=1");
  await expect.poll(() => binarySignal?.aborted).toBe(true);
  await router.navigate("/else");
  await expect.element(view.getByText("Another view")).toBeVisible();
  await expect.poll(() => binarySignal?.aborted).toBe(true);
});
it("denies metadata locally without issuing a request", async () => {
  identity(["sensitive_read"]);
  const fetch = vi.spyOn(globalThis, "fetch");
  const { view } = await mount();
  await expect
    .element(
      view.getByText(
        "Artifact metadata is not available with your current access.",
      ),
    )
    .toBeVisible();
  expect(fetch).not.toHaveBeenCalled();
});

it("admits only one browser download and does not queue a second row", async () => {
  identity();
  const other = structuredClone(fixture.artifact_manifest);
  other.reference.id = "art_second";
  other.filename = "second.txt";
  let requests = 0;
  vi.spyOn(globalThis, "fetch").mockImplementation(async (url, init) => {
    if (String(url).endsWith("/list"))
      return response({
        items: [fixture.artifact_manifest, other],
        next_cursor: null,
      });
    requests += 1;
    return new Promise<Response>((_resolve, reject) => {
      init?.signal?.addEventListener(
        "abort",
        () => reject(init.signal?.reason),
        { once: true },
      );
    });
  });
  const { view } = await mount();
  const first = view.getByRole("button", {
    name: "Download invoice-report.txt",
  });
  const second = view.getByRole("button", { name: "Download second.txt" });
  await expect.element(first).toBeVisible();
  await first.click();
  await expect.element(second).toBeDisabled();
  // A disabled native control ignores clicks, and there is no queued transfer.
  second.element().dispatchEvent(new MouseEvent("click", { bubbles: true }));
  expect(requests).toBe(1);
  await view.getByRole("button", { name: "Cancel download" }).click();
  await expect.element(second).toBeEnabled();
  expect(requests).toBe(1);
  await second.click();
  await expect.poll(() => requests).toBe(2);
  await view.getByRole("button", { name: "Cancel download" }).click();
});
