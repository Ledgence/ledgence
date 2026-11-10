// SPDX-License-Identifier: MIT
import { afterEach, expect, it, vi } from "vitest";
import { render } from "vitest-browser-react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import raw from "../../../crates/ledgence-orchestration-api/tests/fixtures/console-v16.json?raw";
import ledger from "../../../crates/ledgence-orchestration-api/tests/fixtures/task-admission-v1.json";
import { decodeConfig } from "../../src/api/codecs";
import { InstanceContext } from "../../src/app/instance";
import { TaskAdmissionPanel } from "../../src/features/task-admission";

const clients: QueryClient[] = [];
afterEach(() => {
  clients.splice(0).forEach((client) => client.clear());
  vi.restoreAllMocks();
});
it("shows retained occupancy for the selected immutable task generation", async () => {
  const config = {
    ...decodeConfig(JSON.parse(raw).config),
    instance_id: "instance",
  };
  vi.spyOn(globalThis, "fetch").mockImplementation(async (url, options) => {
    expect(new URL(String(url), location.origin).pathname).toBe(
      "/v1/console/task-admissions/inspect",
    );
    expect(JSON.parse(String(options?.body))).toEqual({
      task_id: "task",
      generation: 1,
    });
    return new Response(JSON.stringify(ledger.snapshots.unknown), {
      headers: {
        "Content-Type": "application/json",
        "Ledgence-Console-Contract": String(config.contract_version),
        "Ledgence-Instance-Id": config.instance_id,
      },
    });
  });
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  clients.push(client);
  const view = await render(
    <QueryClientProvider client={client}>
      <InstanceContext.Provider value={config}>
        <TaskAdmissionPanel
          taskId="task"
          resource="model.requests"
          current={1}
        />
      </InstanceContext.Provider>
    </QueryClientProvider>,
  );
  await expect
    .element(view.getByText("unknown occupied", { exact: true }))
    .toBeVisible();
  await expect
    .element(view.getByText("The invocation may have run.", { exact: false }))
    .toBeVisible();
  await expect
    .element(view.getByText(ledger.source.source_id, { exact: true }))
    .toBeVisible();
});
