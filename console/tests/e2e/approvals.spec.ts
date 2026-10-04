// SPDX-License-Identifier: MIT
import { readFileSync } from "node:fs";
import { test, expect } from "@playwright/test";
import { parseUserJson, stringifyUserJson } from "../../src/api/json";
import { approvalFixture } from "../approval-fixture";
import { workflowDetail } from "../../src/api/resources";

const raw = readFileSync(
  new URL(
    "../../../crates/ledgence-orchestration-api/tests/fixtures/console-v5.json",
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
        "Ledgence-Console-Contract": "5",
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
