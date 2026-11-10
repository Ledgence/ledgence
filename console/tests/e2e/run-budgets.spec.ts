// SPDX-License-Identifier: MIT
import { readFileSync } from "node:fs";
import { expect, test } from "@playwright/test";
import { parseUserJson, stringifyUserJson } from "../../src/api/json";
const raw = readFileSync(
  new URL(
    "../../../crates/ledgence-orchestration-api/tests/fixtures/console-v15.json",
    import.meta.url,
  ),
  "utf8",
);
const f = JSON.parse(raw);
test("a reader can open a child’s budget, follow the ledger cursor and identify its shared root", async ({
  page,
}) => {
  const wf = f.workflow_detail.summary.workflow.workflow_id;
  const subject = { kind: "workflow", workflow_id: wf };
  const snap = { ...f.run_budget, subject },
    first = {
      ...f.run_budget_entries,
      subject,
      items: f.run_budget_entries.items.map((row: Record<string, unknown>) => ({
        ...row,
        subject,
        approval: {
          scope: snap.scope,
          workflow_id: wf,
          activation_id: "activate/1",
          approval_key: "controlled:call",
        },
      })),
    };
  const calls: string[] = [];
  await page.route("**/v1/console/**", async (route) => {
    const req = route.request(),
      path = new URL(req.url()).pathname.replace("/v1/console/", "");
    const reply = (v: unknown, status = 200) =>
      route.fulfill({
        status,
        contentType: "application/json",
        headers: {
          "Ledgence-Console-Contract": "15",
          "Ledgence-Instance-Id": f.config.instance_id,
        },
        body: stringifyUserJson(v),
      });
    if (path === "config") return reply(f.config);
    if (path === "access")
      return reply({
        mode: "credentials",
        actor_key: "reader",
        issuer: "local",
        class: "operator",
        subject: "reader",
        instance_id: f.config.instance_id,
        scope: snap.scope,
        capabilities: ["metadata_read"],
        expires_at_ms: Date.now() + 600000,
        operator_audit: true,
        native_effect_resolution: true,
        provider_operation: true,
      });
    if (path === "workflows/inspect")
      return reply(
        (parseUserJson(raw, 2 * 1024 * 1024) as Record<string, unknown>)
          .workflow_detail,
      );
    if (path === "budgets/inspect") {
      expect(req.postDataJSON()).toEqual({ subject });
      return reply(snap);
    }
    if (path === "budgets/entries") {
      const { before } = req.postDataJSON();
      calls.push(before);
      return reply(
        before === null ? first : { ...first, items: [], next: null },
      );
    }
    return reply({ code: "not_found" }, 404);
  });
  await page.goto("/console/workflows/" + wf + "?tab=General&section=budget");
  await expect(
    page.getByRole("heading", { name: "Shared run budget" }),
  ).toBeVisible();
  await expect(
    page.getByRole("link", { name: snap.root.workflow_id }),
  ).toHaveAttribute("href", "/console/workflows/" + snap.root.workflow_id);
  await expect(
    page.getByText(/reported · input 120 · output 16/),
  ).toBeVisible();
  const link = page.getByRole("link", { name: "Review approval" });
  await expect(link).toHaveAttribute(
    "href",
    "/console/approvals/review?" +
      new URLSearchParams({
        workflow: wf,
        key: "controlled:call",
        activation: "activate/1",
      }),
  );
  await page.getByRole("button", { name: "Older entries" }).click();
  await expect(page.getByText("No budget entries on this page.")).toBeVisible();
  expect(calls).toContain(first.next);
});

test("workflow-only budget support hides task budgeting and blocks a retained workflow draft", async ({
  page,
}) => {
  const sent: string[] = [];
  await page.route("**/v1/console/**", async (route) => {
    const req = route.request(),
      path = new URL(req.url()).pathname.replace("/v1/console/", "");
    const reply = (v: unknown, status = 200) =>
      route.fulfill({
        status,
        contentType: "application/json",
        headers: {
          "Ledgence-Console-Contract": "15",
          "Ledgence-Instance-Id": f.config.instance_id,
        },
        body: stringifyUserJson(v),
      });
    if (path === "access")
      return reply({
        mode: "credentials",
        actor_key: "operator",
        issuer: "local",
        subject: "operator",
        class: "operator",
        instance_id: f.config.instance_id,
        scope: f.run_budget.scope,
        capabilities: ["execution_submit", "metadata_read"],
        expires_at_ms: Date.now() + 600000,
        operator_audit: true,
        native_effect_resolution: true,
        provider_operation: true,
      });
    if (path === "config")
      return reply({
        ...f.config,
        capabilities: {
          ...f.config.capabilities,
          programs: false,
          run_budgets: true,
          task_run_budgets: false,
        },
      });
    if (req.method() === "POST") sent.push(path);
    return reply({ code: "not_found" }, 404);
  });
  await page.goto("/console/executions/new");
  const kind = page.getByRole("combobox", { name: "Execution kind" });
  await expect(kind).toBeVisible();
  await expect(page.getByLabel("Maximum calls")).toHaveCount(0);
  await kind.selectOption("workflow");
  await page.getByLabel("Maximum calls").fill("1");
  await kind.selectOption("task");
  await expect(page.getByLabel("Maximum calls")).toHaveCount(0);
  await expect(page.getByRole("status")).toContainText(
    "cannot enforce a budget",
  );
  await page.getByLabel("Program ID").fill("p");
  await page.getByLabel("Exact version").fill("1");
  await page.getByRole("button", { name: "Submit execution" }).click();
  await expect(page.getByRole("alert")).toContainText("cannot enforce");
  expect(sent).not.toContain("tasks");
  await page
    .getByRole("button", { name: "Remove budget from submission" })
    .click();
  await kind.selectOption("workflow");
  await expect(page.getByLabel("Maximum calls")).toHaveValue("");
});
