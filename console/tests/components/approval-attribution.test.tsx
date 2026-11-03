// SPDX-License-Identifier: MIT
import { afterEach, expect, it, vi } from "vitest";
import { render } from "vitest-browser-react";
import { MemoryRouter } from "react-router";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import source from "../../../crates/ledgence-orchestration-api/tests/fixtures/console-v7.json?raw";
import { parseUserJson } from "../../src/api/json";
import { decodeConfig } from "../../src/api/codecs";
import { accessController } from "../../src/app/access-state";
import { InstanceContext } from "../../src/app/instance";
import { ApprovalAttribution } from "../../src/features/approval-attribution";
import { ApprovalCard } from "../../src/features/approvals";
import { acceptedApproval, acceptanceEvent } from "../approval-audit-fixture";
const raw = parseUserJson(source, 2 * 1024 * 1024) as Record<string, unknown>;
const config = decodeConfig(raw.config);
const clients: QueryClient[] = [];
afterEach(() => {
  clients.splice(0).forEach((client) => client.clear());
  accessController.begin("discard", true);
  vi.restoreAllMocks();
});
async function mount(
  options: {
    items?: unknown[];
    failure?: number;
    supported?: boolean;
    list?: boolean;
  } = {},
) {
  accessController.accept(
    {
      mode: "credentials",
      actor_key: "current-actor",
      issuer: "current-issuer",
      subject: "Current reader",
      class: "operator",
      instance_id: config.instance_id,
      scope: { tenant_id: "acme", namespace: "demo" },
      capabilities: ["metadata_read", "sensitive_read", "approval_decide"],
      expires_at_ms: Date.now() + 60000,
      operator_audit: options.supported ?? true,
    },
    accessController.begin("start", true),
    config.instance_id,
  );
  let failure = options.failure;
  const fetch = vi
    .spyOn(globalThis, "fetch")
    .mockImplementation(async (url, init) => {
      expect(String(url)).toBe("/v1/console/audit/events");
      expect(init?.method).toBe("POST");
      expect(JSON.parse(String(init?.body)).filter.revision).toBe(
        acceptedApproval().revision,
      );
      return new Response(
        JSON.stringify({
          items: options.items ?? [acceptanceEvent(config.instance_id)],
          next_cursor: null,
        }),
        {
          status: failure ?? 200,
          headers: {
            "Content-Type": "application/json",
            "Ledgence-Console-Contract": "7",
            "Ledgence-Instance-Id": config.instance_id,
          },
        },
      );
    });
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  });
  clients.push(client);
  const view = await render(
    <QueryClientProvider client={client}>
      <InstanceContext.Provider value={config}>
        <MemoryRouter>
          {options.list ? (
            Array.from({ length: 10 }, (_, i) => (
              <ApprovalCard
                key={i}
                approval={{ ...acceptedApproval(), key: `approval-${i}` }}
              />
            ))
          ) : (
            <ApprovalAttribution approval={acceptedApproval()} />
          )}
        </MemoryRouter>
      </InstanceContext.Provider>
    </QueryClientProvider>,
  );
  return {
    view,
    client,
    fetch,
    recover() {
      failure = undefined;
    },
  };
}
it("shows original actor distinctly and caches immutable evidence through ordinary invalidation", async () => {
  const { view, client, fetch } = await mount();
  await expect
    .element(view.getByText("Original operator", { exact: true }))
    .toBeVisible();
  await expect
    .element(view.getByText("Current reader", { exact: true }))
    .not.toBeInTheDocument();
  await expect
    .element(view.getByText(/reviewer label is caller supplied/))
    .toBeVisible();
  await client.invalidateQueries();
  expect(fetch).toHaveBeenCalledOnce();
});
it("only a successful empty exact lookup reports no verified actor", async () => {
  const { view } = await mount({ items: [] });
  await expect
    .element(view.getByText("No verified actor recorded."))
    .toBeVisible();
});
it("unavailable audit requires manual retry and never implies historical absence", async () => {
  const { view, client, fetch, recover } = await mount({ failure: 503 });
  await expect
    .element(view.getByText(/Verified attribution is unavailable/))
    .toBeVisible();
  await expect
    .element(view.getByText("No verified actor recorded."))
    .not.toBeInTheDocument();
  await client.invalidateQueries();
  expect(fetch).toHaveBeenCalledOnce();
  recover();
  await view.getByRole("button", { name: "Retry attribution" }).click();
  await expect
    .element(view.getByText("Original operator", { exact: true }))
    .toBeVisible();
  expect(fetch).toHaveBeenCalledTimes(2);
});
it("an explicitly auth-only instance does not pretend its audit lookup was empty", async () => {
  const { view, fetch } = await mount({ supported: false });
  await expect
    .element(view.getByText(/unavailable with this instance or credential/))
    .toBeVisible();
  expect(fetch).not.toHaveBeenCalled();
});
it("workflow approval pages avoid ten eager acceptance queries", async () => {
  const { view, fetch } = await mount({ list: true });
  await expect
    .element(
      view.getByRole("link", { name: "View recorded attribution" }).first(),
    )
    .toBeVisible();
  expect(fetch).not.toHaveBeenCalled();
});
