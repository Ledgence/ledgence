// SPDX-License-Identifier: MIT
import { afterEach, expect, it, vi } from "vitest";
import { render } from "vitest-browser-react";
import { createMemoryRouter, RouterProvider, useParams } from "react-router";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import fixtureSource from "../../../crates/ledgence-orchestration-api/tests/fixtures/console-v5.json?raw";
import { decodeConfig } from "../../src/api/codecs";
import { parseUserJson, stringifyUserJson } from "../../src/api/json";
import type { Approval, ApprovalDecision } from "../../src/api/approvals";
import { InstanceContext } from "../../src/app/instance";
import { WorkflowApprovals } from "../../src/features/approvals";
import { approvalFixture } from "../approval-fixture";

const all = parseUserJson(fixtureSource, 2 * 1024 * 1024);
if (!all || typeof all !== "object" || !("config" in all))
  throw new Error("Missing Rust configuration");
const config = decodeConfig(all.config);
config.polling.waiting_workflow_ms = 60000;
const clients: QueryClient[] = [];
const routers: ReturnType<typeof createMemoryRouter>[] = [];
afterEach(() => {
  for (const router of routers.splice(0)) router.dispose();
  for (const client of clients.splice(0)) client.clear();
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
function Page() {
  const { workflowId = "" } = useParams();
  return (
    <WorkflowApprovals key={workflowId} workflowId={workflowId} active={true} />
  );
}
async function mount(
  options: {
    snapshot?: Approval;
    readSnapshot?: () => Approval;
    decide?: (body: ApprovalDecision, count: number) => Response;
    foreignPage?: boolean;
    nextCursor?: string;
  } = {},
) {
  const initial = options.snapshot ?? approvalFixture();
  const posts: string[] = [];
  const reads: {
    workflow_id: string;
    after_key: string | null;
    limit: number;
  }[] = [];
  vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
    expect(init?.method).toBe("POST");
    const path = new URL(String(input), location.origin).pathname;
    const body = String(init?.body);
    if (path.endsWith("/approvals/list")) {
      const read = JSON.parse(body) as (typeof reads)[number];
      reads.push(read);
      return response({
        items: [
          {
            ...(options.readSnapshot?.() ?? initial),
            workflow_id: options.foreignPage ? "foreign" : read.workflow_id,
          },
        ],
        next_cursor: options.nextCursor ?? null,
      });
    }
    if (path.endsWith("/approvals/decide")) {
      posts.push(body);
      const sent = parseUserJson(body, 100000) as ApprovalDecision;
      return options.decide?.(sent, posts.length) ?? response({}, 503);
    }
    throw new Error(`Unexpected API route ${path}`);
  });
  const client = new QueryClient({
    defaultOptions: {
      queries: { retry: false, refetchOnWindowFocus: false },
      mutations: { retry: false },
    },
  });
  clients.push(client);
  const router = createMemoryRouter(
    [{ path: "/workflows/:workflowId", element: <Page /> }],
    {
      initialEntries: [
        `/workflows/${initial.workflow_id}?tab=General&section=approvals`,
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
  return { view, posts, reads, router, client };
}
function decidedApproval(
  sent: ApprovalDecision,
  resumedActivationId: string | null = "activation-resumed",
): Approval {
  return {
    ...approvalFixture(sent.decision === "approve" ? "approved" : "rejected"),
    workflow_id: sent.workflow_id,
    action: sent.action,
    resumed_activation_id: resumedActivationId,
    decision: {
      decision_id: sent.decision_id,
      decision: sent.decision,
      reviewer: sent.reviewer,
      reason: sent.reason,
      decided_at: 5000,
    },
  };
}
function receipt(sent: ApprovalDecision) {
  return response({
    approval: decidedApproval(sent),
    already_accepted: true,
  });
}

it("renders the exact effective and original arguments and leaves expiry to the backend", async () => {
  const { view, reads } = await mount();
  const card = view.getByRole("article", { name: "Approval publish-report" });
  await expect
    .element(card.getByRole("button", { name: "Approve", exact: true }))
    .toBeVisible();
  await expect
    .element(card.getByText("release-3", { exact: true }))
    .toBeVisible();
  // The pre element's label is available through the DOM even without an ARIA role.
  expect(
    card.element().querySelector('pre[aria-label="Effective action arguments"]')
      ?.textContent,
  ).toContain("18446744073709551615");
  await card
    .getByText("Original proposed arguments", { exact: true })
    .first()
    .click();
  expect(card.element().textContent).toContain("reports/draft");
  expect(card.element().textContent).toContain("reports/approved");
  expect(card.element().textContent).toContain("pending");
  expect(reads[0]).toEqual({
    workflow_id: "workflow-review",
    after_key: null,
    limit: 10,
  });
});

it.each(["approved", "rejected", "expired", "cancelled"] as const)(
  "renders %s as a recorded terminal decision without action controls",
  async (status) => {
    const { view } = await mount({ snapshot: approvalFixture(status) });
    await expect
      .element(view.getByRole("article", { name: "Approval publish-report" }))
      .toBeVisible();
    await expect
      .element(view.getByRole("button", { name: "Approve", exact: true }))
      .not.toBeInTheDocument();
    await expect
      .element(view.getByRole("button", { name: "Reject", exact: true }))
      .not.toBeInTheDocument();
    if (status === "approved" || status === "rejected") {
      await expect
        .element(view.getByText("Casey", { exact: true }))
        .toBeVisible();
      await expect
        .element(view.getByText("Reviewed destination", { exact: true }))
        .toBeVisible();
      await expect
        .element(view.getByRole("link", { name: "activation-resumed" }))
        .toHaveAttribute("href", "/executions/activation-resumed");
    }
  },
);

it("freezes a decision and retries identical bytes after a lost response and resource refresh", async () => {
  const { view, posts, client } = await mount({
    decide: (sent, count) => (count === 1 ? response({}, 503) : receipt(sent)),
  });
  await view.getByRole("button", { name: "Approve", exact: true }).click();
  await view
    .getByRole("textbox", { name: "Reviewer", exact: true })
    .fill("Avery");
  await view
    .getByRole("textbox", { name: "Reason (optional)" })
    .fill("Validated effective arguments");
  const confirm = view.getByRole("button", { name: "Confirm approval" });
  const element = confirm.element();
  if (!(element instanceof HTMLButtonElement))
    throw new Error("Expected a decision button");
  element.click();
  element.click();
  await expect.element(view.getByRole("alert")).toBeVisible();
  expect(posts).toHaveLength(1);
  await client.invalidateQueries();
  await expect
    .element(view.getByRole("textbox", { name: "Reviewer", exact: true }))
    .toBeDisabled();
  await expect
    .element(view.getByRole("textbox", { name: "Reason (optional)" }))
    .toBeDisabled();
  await expect
    .element(view.getByRole("button", { name: "Separate this operation" }))
    .not.toBeInTheDocument();
  await view.getByRole("button", { name: "Try again", exact: true }).click();
  await expect
    .element(view.getByText(/This exact decision was already recorded/))
    .toBeVisible();
  expect(posts).toHaveLength(2);
  expect(posts[1]).toBe(posts[0]);
  const sent = JSON.parse(posts[0]!);
  expect(sent).toMatchObject({
    workflow_id: "workflow-review",
    key: "publish-report",
    activation_id: "activation-review",
    revision: "9007199254740993",
    decision: "approve",
    reviewer: "Avery",
    reason: "Validated effective arguments",
  });
  expect(sent).not.toHaveProperty("scope");
  expect(sent.decision_id).toMatch(/^[0-9a-f-]{36}$/);
  expect(posts[0]).toContain("18446744073709551615");
});

it.each([null, "activation-resumed"])(
  "keeps the accepted receipt ahead of stale reads and renders a later resume (receipt resume: %s)",
  async (resumedActivationId) => {
    let snapshot = approvalFixture();
    let accepted: Approval | undefined;
    const { view, client, reads } = await mount({
      readSnapshot: () => snapshot,
      decide: (sent) => {
        accepted = decidedApproval(sent, resumedActivationId);
        return response({ approval: accepted, already_accepted: false });
      },
    });
    await view.getByRole("button", { name: "Approve", exact: true }).click();
    await view
      .getByRole("textbox", { name: "Reviewer", exact: true })
      .fill("Avery");
    await view.getByRole("button", { name: "Confirm approval" }).click();
    await expect
      .element(view.getByText(/Decision recorded\. Current status: approved/))
      .toBeVisible();
    await view
      .getByRole("button", { name: "Close dialog", exact: true })
      .click();
    await client.invalidateQueries();
    expect(reads.length).toBeGreaterThan(1);
    const card = view.getByRole("article", { name: "Approval publish-report" });
    await expect
      .element(card.getByText("approved", { exact: true }))
      .toBeVisible();
    await expect
      .element(card.getByText("Avery", { exact: true }))
      .toBeVisible();
    if (!accepted) throw new Error("Expected an accepted decision receipt");
    snapshot = { ...accepted, resumed_activation_id: null };
    await client.invalidateQueries();
    if (resumedActivationId !== null) {
      await expect
        .element(card.getByRole("link", { name: "activation-resumed" }))
        .toHaveAttribute("href", "/executions/activation-resumed");
    } else {
      await expect
        .element(card.getByRole("link", { name: "activation-resumed" }))
        .not.toBeInTheDocument();
    }
    snapshot = { ...accepted, resumed_activation_id: "activation-resumed" };
    await client.invalidateQueries();
    await expect
      .element(card.getByRole("link", { name: "activation-resumed" }))
      .toHaveAttribute("href", "/executions/activation-resumed");
    await expect
      .element(card.getByRole("button", { name: "Approve", exact: true }))
      .not.toBeInTheDocument();
  },
);

it("records rejection separately and rejects a receipt that changes the reviewed action", async () => {
  const { view, posts } = await mount({
    decide: (sent) => {
      const changed = { ...sent, action: { ...sent.action, version: "other" } };
      return receipt(changed);
    },
  });
  await view.getByRole("button", { name: "Reject", exact: true }).click();
  await view
    .getByRole("textbox", { name: "Reviewer", exact: true })
    .fill("Avery");
  await view.getByRole("button", { name: "Confirm rejection" }).click();
  await expect
    .element(view.getByRole("alert"))
    .toHaveTextContent("does not match this exact decision");
  expect(JSON.parse(posts[0]!).decision).toBe("reject");
  await view.getByRole("button", { name: "Try again", exact: true }).click();
  await expect.poll(() => posts.length).toBe(2);
  expect(posts[1]).toBe(posts[0]);
});

it("isolates an uncertain decision from a different workflow", async () => {
  const { view, router, posts } = await mount();
  await view.getByRole("button", { name: "Approve", exact: true }).click();
  await view
    .getByRole("textbox", { name: "Reviewer", exact: true })
    .fill("Avery");
  await view.getByRole("button", { name: "Confirm approval" }).click();
  await expect.element(view.getByRole("alert")).toBeVisible();
  await view.getByRole("button", { name: "Close dialog", exact: true }).click();
  await router.navigate(
    "/workflows/other-workflow?tab=General&section=approvals",
  );
  await view.getByRole("button", { name: "Reject", exact: true }).click();
  await expect
    .element(view.getByRole("textbox", { name: "Reviewer", exact: true }))
    .toHaveValue("");
  await view
    .getByRole("textbox", { name: "Reviewer", exact: true })
    .fill("Morgan");
  await view.getByRole("button", { name: "Confirm rejection" }).click();
  await expect.poll(() => posts.length).toBe(2);
  expect(JSON.parse(posts[1]!)).toMatchObject({
    workflow_id: "other-workflow",
    decision: "reject",
    reviewer: "Morgan",
  });
  expect(JSON.parse(posts[1]!).decision_id).not.toBe(
    JSON.parse(posts[0]!).decision_id,
  );
});

it("does not expose approval controls for a page belonging to another workflow", async () => {
  const { view } = await mount({ foreignPage: true });
  await expect
    .element(view.getByRole("alert"))
    .toHaveTextContent("does not match this workflow");
  await expect
    .element(view.getByRole("button", { name: "Approve", exact: true }))
    .not.toBeInTheDocument();
});
