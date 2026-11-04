// SPDX-License-Identifier: MIT
import { readFileSync } from "node:fs";
import { test, expect } from "@playwright/test";
import { parseUserJson, stringifyUserJson } from "../../src/api/json";
import { approvalFixture } from "../approval-fixture";
import { workflowDetail } from "../../src/api/resources";

const raw = readFileSync(
  new URL(
    "../../../crates/ledgence-orchestration-api/tests/fixtures/console-v12.json",
    import.meta.url,
  ),
  "utf8",
);
const fixture = parseUserJson(raw, 2 * 1024 * 1024);
function field(name: string) {
  if (!fixture || typeof fixture !== "object" || !(name in fixture))
    throw new Error(`Missing Rust fixture ${name}`);
  return Reflect.get(fixture, name);
}
test("approvals remain readable and keyboard-accessible inside workflow General", async ({
  page,
}) => {
  const approval = approvalFixture();
  const writes: string[] = [];
  await page.route("**/v1/console/**", async (route) => {
    if (new URL(route.request().url()).pathname === "/v1/console/access") {
      await route.fulfill({
        status: 200,
        headers: {
          "Content-Type": "application/json",
          "Ledgence-Console-Contract": "12",
          "Ledgence-Instance-Id": "instance_demo",
        },
        body: JSON.stringify({ mode: "operator_trusted" }),
      });
      return;
    }

    const request = route.request();
    const path = new URL(request.url()).pathname;
    let body: unknown;
    if (path.endsWith("/config")) body = field("config");
    else if (path.endsWith("/workflows/inspect")) {
      const detail = workflowDetail(field("workflow_detail"));
      detail.summary.workflow.workflow_id = approval.workflow_id;
      body = detail;
    } else if (path.endsWith("/executions/ancestry"))
      body = {
        execution: { kind: "workflow", id: approval.workflow_id },
        path: [
          {
            execution: { kind: "workflow", id: approval.workflow_id },
            program: null,
            availability: "available",
          },
        ],
        observed_at: 10000,
      };
    else if (path.endsWith("/approvals/list"))
      body = { items: [approval], next_cursor: null };
    else if (path.endsWith("/approvals/decide")) {
      writes.push(request.postData()!);
      await route.fulfill({ status: 503, body: "Unavailable" });
      return;
    } else throw new Error(`Unexpected route ${path}`);
    await route.fulfill({
      status: 200,
      headers: {
        "Content-Type": "application/json",
        "Ledgence-Console-Contract": "12",
        "Ledgence-Instance-Id": "instance_demo",
      },
      body: stringifyUserJson(body),
    });
  });
  await page.goto(
    `/console/workflows/${approval.workflow_id}?tab=General&section=approvals`,
  );
  const card = page.getByRole("article", { name: "Approval publish-report" });
  await expect(card).toBeVisible();
  const trigger = card.getByRole("button", { name: "Approve", exact: true });
  for (const width of [1280, 640]) {
    await page.setViewportSize({ width, height: 900 });
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth,
      ),
    ).toBe(true);
    await trigger.focus();
    await page.keyboard.press("Enter");
    const dialog = page.getByRole("dialog");
    await expect(dialog).toBeVisible();
    expect(
      await dialog.evaluate((element) =>
        element.contains(document.activeElement),
      ),
    ).toBe(true);
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth,
      ),
    ).toBe(true);
    await page.keyboard.press("Escape");
    await expect(dialog).toHaveCount(0);
    await expect(trigger).toBeFocused();
  }
  await trigger.click();
  await page
    .getByRole("textbox", { name: "Reviewer", exact: true })
    .fill("Avery");
  await page.getByRole("button", { name: "Confirm approval" }).click();
  await expect(page.getByRole("alert")).toBeVisible();
  await page.getByRole("button", { name: "Try again", exact: true }).click();
  await expect.poll(() => writes.length).toBe(2);
  expect(writes[1]).toBe(writes[0]);
  await page.getByRole("button", { name: "Close dialog", exact: true }).click();
  await expect(
    page.getByRole("button", { name: "View submitted decision" }),
  ).toBeFocused();
});

test("the inbox filters across workflows, pages by cursor and reconciles another reviewer's decision", async ({
  page,
}) => {
  const original = approvalFixture();
  const requests: {
    status: string | null;
    program_id: string | null;
    due_before?: number;
    cursor: string | null;
    limit: number;
  }[] = [];
  let decided = false,
    inspected = 0,
    sent = 0;
  await page.route("**/v1/console/**", async (route) => {
    if (new URL(route.request().url()).pathname === "/v1/console/access") {
      await route.fulfill({
        status: 200,
        headers: {
          "Content-Type": "application/json",
          "Ledgence-Console-Contract": "12",
          "Ledgence-Instance-Id": "instance_demo",
        },
        body: JSON.stringify({ mode: "operator_trusted" }),
      });
      return;
    }

    const path = new URL(route.request().url()).pathname;
    let value: unknown;
    if (path.endsWith("/config")) value = field("config");
    else if (path.endsWith("/approvals/inbox")) {
      const input = route.request().postDataJSON() as (typeof requests)[number];
      requests.push(input);
      const row = (i: number) => ({
        workflow_id: "wf-" + String(i).padStart(3, "0"),
        key: "refund",
        activation_id: "act-" + i,
        program_id: "billing",
        program_version: "1",
        action_name: "refund_invoice",
        action_version: "1",
        created_at: 1000,
        deadline: 2000 + i,
        status: decided && i === 25 ? "rejected" : "pending",
      });
      const rows = Array.from({ length: 26 }, (_, i) => row(i)).filter(
        (r) =>
          (!input.status || r.status === input.status) &&
          (!input.program_id || r.program_id === input.program_id) &&
          (input.due_before === undefined || r.deadline < input.due_before),
      );
      const offset = input.cursor === "approved-fixture-cursor" ? 25 : 0;
      value = {
        items: rows.slice(offset, offset + input.limit),
        next_cursor:
          rows.length > offset + input.limit ? "approved-fixture-cursor" : null,
      };
    } else if (path.endsWith("/approvals/inspect")) {
      inspected++;
      expect(route.request().postDataJSON()).toEqual({
        workflow_id: "wf-025",
        key: "refund",
      });
      value = {
        ...approvalFixture(decided ? "rejected" : "pending"),
        workflow_id: "wf-025",
        key: "refund",
        activation_id: "act-25",
        action: { ...original.action, name: "refund_invoice", version: "1" },
      };
    } else if (path.endsWith("/approvals/decide")) {
      sent++;
      decided = true;
      return route.fulfill({ status: 409, body: "" });
    } else return route.fulfill({ status: 404, body: "missing fixture" });
    await route.fulfill({
      status: 200,
      headers: {
        "Content-Type": "application/json",
        "Ledgence-Console-Contract": "12",
        "Ledgence-Instance-Id": "instance_demo",
      },
      body: stringifyUserJson(value),
    });
  });
  await page.goto("/console/approvals?limit=25");
  await expect(
    page
      .getByRole("navigation", { name: "Main navigation" })
      .getByRole("link", { name: "Approvals" }),
  ).toHaveAttribute("aria-current", "page");
  await expect(
    page.getByRole("row").filter({ hasText: "wf-000" }),
  ).toBeVisible();
  expect(requests[0]).toEqual({
    status: "pending",
    program_id: null,
    cursor: null,
    limit: 25,
  });
  expect(inspected).toBe(0);
  await page.getByRole("button", { name: "Next", exact: true }).click();
  await page
    .getByRole("row")
    .filter({ hasText: "wf-025" })
    .getByRole("link", { name: "refund_invoice" })
    .click();
  const card = page.getByRole("article", { name: "Approval refund" });
  await expect(card.locator("pre").first()).toContainText(
    "18446744073709551615",
  );
  await card.getByRole("button", { name: "Approve", exact: true }).click();
  await page
    .getByRole("textbox", { name: "Reviewer", exact: true })
    .fill("Jamie");
  await page.getByRole("button", { name: "Confirm approval" }).click();
  await expect(
    page
      .getByRole("dialog")
      .getByRole("status", { name: "Approval decision status" }),
  ).toContainText("rejected");
  await expect(
    page
      .getByRole("dialog")
      .getByRole("status", { name: "Approval decision status" }),
  ).toContainText("Casey");
  expect(sent).toBe(1);
  await page.keyboard.press("Escape");
  await page.getByRole("link", { name: "Back to approvals" }).click();
  await expect(page).toHaveURL(/cursor=approved-fixture-cursor/);
  await page
    .getByRole("combobox", { name: "Status", exact: true })
    .selectOption("all");
  await page.getByLabel("Program ID", { exact: true }).fill("billing");
  await page.getByRole("button", { name: "Apply filters" }).click();
  await expect(
    page.getByRole("row").filter({ hasText: "wf-025" }),
  ).toContainText("rejected");
  expect(requests.at(-1)).toMatchObject({
    program_id: "billing",
    status: null,
    cursor: null,
  });
  const cutoff = page.getByLabel("Due before (UTC)", { exact: true });
  await cutoff.fill("1970-01-01T00:00:02.025");
  await page.getByRole("button", { name: "Apply filters" }).click();
  await expect(page).toHaveURL(/due_before=2025/);
  await expect(
    page.getByRole("row").filter({ hasText: "wf-024" }),
  ).toBeVisible();
  await expect(page.getByRole("row").filter({ hasText: "wf-025" })).toHaveCount(
    0,
  );
  expect(requests.at(-1)).toMatchObject({ due_before: 2025, cursor: null });
  const count = requests.length;
  await page.goto("/console/approvals?due_before=invalid");
  await expect(page.getByText("Invalid deadline filter")).toBeVisible();
  expect(requests).toHaveLength(count);
  await page.getByRole("button", { name: "Clear deadline filter" }).click();
  await expect(
    page.getByRole("row").filter({ hasText: "wf-000" }),
  ).toBeVisible();
});
