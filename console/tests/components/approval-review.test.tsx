// SPDX-License-Identifier: MIT
import { afterEach, expect, it, vi } from "vitest";
import { render } from "vitest-browser-react";
import { MemoryRouter } from "react-router";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import source from "../../../crates/ledgence-orchestration-api/tests/fixtures/console-v7.json?raw";
import { decodeConfig } from "../../src/api/codecs";
import { parseUserJson, stringifyUserJson } from "../../src/api/json";
import { workflowDetail } from "../../src/api/resources";
import { InstanceContext } from "../../src/app/instance";
import { ApprovalReviewPage } from "../../src/features/approval-inbox";
import { approvalFixture } from "../approval-fixture";

const fixtures = parseUserJson(source, 2 * 1024 * 1024);
if (
  !fixtures ||
  typeof fixtures !== "object" ||
  !("config" in fixtures) ||
  !("workflow_detail" in fixtures)
)
  throw new Error("Missing Console fixtures");
const config = decodeConfig(fixtures.config);
const status = workflowDetail(fixtures.workflow_detail).summary.workflow;
config.polling.waiting_workflow_ms = 30;
const clients: QueryClient[] = [];
afterEach(() => {
  clients.splice(0).forEach((x) => x.clear());
  vi.restoreAllMocks();
});
function reply(value: unknown, status = 200) {
  return new Response(stringifyUserJson(value), {
    status,
    headers: {
      "Content-Type": "application/json",
      "Ledgence-Console-Contract": "7",
      "Ledgence-Instance-Id": config.instance_id,
    },
  });
}
async function review(
  read: () => Response,
  parent?: () => Response,
  decide?: (body: string) => Response,
) {
  vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
    const url = new URL(String(input), location.origin);
    if (url.pathname.endsWith("/approvals/inspect")) return read();
    if (url.pathname.endsWith("/workflows/status") && parent) return parent();
    if (url.pathname.endsWith("/approvals/decide") && decide)
      return decide(String(init?.body));
    throw Error("Unexpected route " + url.pathname);
  });
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false, refetchOnWindowFocus: false } },
  });
  clients.push(client);
  const { workflow_id, key } = approvalFixture();
  return render(
    <QueryClientProvider client={client}>
      <InstanceContext.Provider value={config}>
        <MemoryRouter
          initialEntries={[
            "/approvals/review?" +
              new URLSearchParams({ workflow: workflow_id, key }),
          ]}
        >
          <ApprovalReviewPage />
        </MemoryRouter>
      </InstanceContext.Provider>
    </QueryClientProvider>,
  );
}
const quiet = () => new Promise((resolve) => setTimeout(resolve, 130));
it.each(["approved", "rejected", "expired"] as const)(
  "observes a %s request's subsequently committed resume, then stops polling",
  async (result) => {
    const snapshot = approvalFixture(result);
    let reads = 0,
      statusReads = 0,
      committed = false;
    const view = await review(
      () => {
        reads++;
        return reply({
          ...snapshot,
          resumed_activation_id: committed ? "next-activation" : null,
        });
      },
      () => {
        statusReads++;
        return reply({
          workflow: {
            ...status,
            workflow_id: snapshot.workflow_id,
            state: "running",
            terminal_at: null,
          },
          observed_at: 1000 + statusReads,
        });
      },
    );
    await expect.poll(() => reads).toBeGreaterThanOrEqual(2);
    committed = true;
    await expect
      .element(view.getByRole("link", { name: "next-activation" }))
      .toBeVisible();
    const final = [reads, statusReads];
    await quiet();
    expect([reads, statusReads]).toEqual(final);
  },
);
it("takes one final approval observation after parent cancellation and stops when no resume exists", async () => {
  const snapshot = {
    ...approvalFixture("approved"),
    resumed_activation_id: null,
  };
  let reads = 0,
    statusReads = 0,
    closed = false,
    afterClose = 0;
  await review(
    () => {
      reads++;
      if (closed) afterClose++;
      return reply(snapshot);
    },
    () => {
      statusReads++;
      if (statusReads >= 2) closed = true;
      return reply({
        workflow: {
          ...status,
          workflow_id: snapshot.workflow_id,
          state: closed ? "cancelled" : "cancelling",
          terminal_at: closed ? 2000 : null,
        },
        observed_at: 1000 + statusReads,
      });
    },
  );
  await expect.poll(() => afterClose).toBe(1);
  const final = [reads, statusReads];
  await quiet();
  expect([reads, statusReads]).toEqual(final);
});
it("retires a cached pending request without offering new writes", async () => {
  let retired = false;
  const view = await review(() =>
    retired ? reply({}, 404) : reply(approvalFixture()),
  );
  await view.getByRole("button", { name: "Approve", exact: true }).click();
  retired = true;
  await expect
    .element(
      view
        .getByRole("dialog")
        .getByText("This request is no longer available for a decision."),
    )
    .toBeVisible();
  await expect
    .element(view.getByRole("button", { name: "Confirm approval" }))
    .not.toBeInTheDocument();
  await expect
    .element(view.getByRole("button", { name: "Reject", exact: true }))
    .not.toBeInTheDocument();
});
it("retains the exact already-sent command even when inspection later returns 404", async () => {
  let retired = false;
  const writes: string[] = [];
  const view = await review(
    () => (retired ? reply({}, 404) : reply(approvalFixture())),
    undefined,
    (body) => {
      writes.push(body);
      retired = true;
      return reply({}, 503);
    },
  );
  await view.getByRole("button", { name: "Approve", exact: true }).click();
  await view
    .getByRole("textbox", { name: "Reviewer", exact: true })
    .fill("Jamie");
  await view.getByRole("button", { name: "Confirm approval" }).click();
  await expect
    .element(view.getByText("Showing the last successful observation"))
    .toBeVisible();
  await view
    .getByRole("dialog")
    .getByRole("button", { name: "Try again" })
    .click();
  await expect.poll(() => writes.length).toBe(2);
  expect(writes[0]).toBe(writes[1]);
});
