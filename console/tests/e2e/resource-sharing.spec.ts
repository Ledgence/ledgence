// SPDX-License-Identifier: MIT
import { expect, test, type Page } from "@playwright/test";
import { readFileSync } from "node:fs";
const fixture = JSON.parse(
  readFileSync(
    new URL(
      "../../../crates/ledgence-orchestration-api/tests/fixtures/console-v13.json",
      import.meta.url,
    ),
    "utf8",
  ),
);
const weighted = fixture.fair_resource_snapshot;
const name = weighted.resource.name as string;
const page1 = fixture.resource_decision_page;
const cursor = page1.next as string;
const fifo = structuredClone(weighted);
fifo.resource.name = "unlimited";
delete fifo.resource.policy.fair_sharing;
async function wire(
  page: Page,
  capabilities: string[] = ["metadata_read"],
  available = true,
) {
  const calls: { path: string; body: Record<string, unknown> }[] = [];
  await page.route("**/v1/console/**", async (route) => {
    const req = route.request();
    const path = new URL(req.url()).pathname.replace("/v1/console/", "");
    const reply = (data: unknown, status = 200) =>
      route.fulfill({
        status,
        contentType: "application/json",
        headers: {
          "Ledgence-Console-Contract": "13",
          "Ledgence-Instance-Id": fixture.config.instance_id,
        },
        body: JSON.stringify(data),
      });
    if (path === "config")
      return reply({
        ...fixture.config,
        capabilities: {
          ...fixture.config.capabilities,
          resource_fair_sharing: available,
        },
      });
    if (path === "access")
      return reply({
        mode: "credentials",
        actor_key: "reader",
        issuer: "local",
        subject: "reader",
        class: "operator",
        instance_id: fixture.config.instance_id,
        scope: weighted.resource.scope,
        capabilities,
        expires_at_ms: Date.now() + 600000,
        operator_audit: true,
        native_effect_resolution: true,
        provider_operation: true,
      });
    if (!path.startsWith("resources/"))
      return reply({ code: "not_found" }, 404);
    const body = req.postDataJSON();
    calls.push({ path, body });
    expect(req.method()).toBe("POST");
    expect(req.headers()["x-ledgence-actor"]).toBe("reader");
    expect(body).not.toHaveProperty("scope");
    if (path === "resources/list")
      return reply(
        body.after === null
          ? { items: [weighted], next: name }
          : { items: [fifo], next: null },
      );
    if (path === "resources/inspect")
      return reply(body.name === name ? weighted : fifo);
    if (path === "resources/decisions") {
      if (body.name !== name) return reply({ items: [], next: null });
      if (body.before === null) return reply(page1);
      expect(body.before).toBe(cursor);
      return reply({
        items: [
          {
            ...page1.items[0],
            sequence: "2",
            policy_revision: "1",
            eligible_groups: 1,
            reason: "sole_eligible",
            subject: {
              kind: "provider",
              workflow_id: "workflow-2",
              origin_activation_id: "act",
              key: "invoke",
            },
          },
        ],
        next: null,
      });
    }
    return reply({ code: "not_found" }, 404);
  });
  return calls;
}

test("reader can trace weighted offers across revisions and navigate without numeric cursor loss", async ({
  page,
}) => {
  const calls = await wire(page);
  await page.goto("/console/resources");
  await page.getByRole("link", { name, exact: true }).click();
  await expect(page.getByRole("heading", { name, exact: true })).toBeVisible();
  await expect(page.getByText("All unmapped queues")).toBeVisible();
  await expect(
    page.getByText("Weighted turn across 2 eligible groups", { exact: false }),
  ).toBeVisible();
  await expect(page.getByText("#" + cursor, { exact: false })).toBeVisible();
  await expect(
    page.getByRole("link", { name: "task_support" }),
  ).toHaveAttribute("href", "/console/executions/task_support");
  await page.getByRole("link", { name: "Older offers" }).click();
  await expect(page).toHaveURL(new RegExp("before=" + cursor + "$"));
  await expect(
    page.getByText("Only eligible group", { exact: false }),
  ).toBeVisible();
  await expect(page.getByRole("link", { name: "workflow-2" })).toHaveAttribute(
    "href",
    "/console/workflows/workflow-2",
  );
  await page.getByRole("link", { name: "Newest offers" }).click();
  await expect(page.getByText("#" + cursor, { exact: false })).toBeVisible();
  await page.getByRole("link", { name: "All resources" }).click();
  await page.getByRole("link", { name: "Next resources" }).click();
  await page.getByRole("link", { name: "unlimited", exact: true }).click();
  await expect(
    page.getByText(
      "FIFO. The oldest eligible request receives the next offer.",
    ),
  ).toBeVisible();
  await expect(
    page.getByText("No weighted offers on this page."),
  ).toBeVisible();
  expect(
    calls.some((c) => c.path === "resources/list" && c.body.after === name),
  ).toBe(true);
  expect(
    calls.some(
      (c) => c.path === "resources/decisions" && c.body.before === cursor,
    ),
  ).toBe(true);
});
for (const scenario of [
  {
    label: "missing metadata permission",
    capabilities: ["resource_control"],
    available: true,
  },
  {
    label: "unavailable scheduler capability",
    capabilities: ["metadata_read"],
    available: false,
  },
]) {
  test(
    "resource reads stay disabled with " + scenario.label,
    async ({ page }) => {
      const calls = await wire(page, scenario.capabilities, scenario.available);
      await page.goto("/console/resources?name=model.requests");
      await expect(
        page.getByText(
          scenario.available
            ? "Your credential does not permit this operation."
            : "Resource inspection requires authenticated resource admission",
          { exact: false },
        ),
      ).toBeVisible();
      expect(calls).toHaveLength(0);
    },
  );
}
test("invalid decimal cursor cannot trigger a resource read", async ({
  page,
}) => {
  const calls = await wire(page);
  await page.goto("/console/resources?name=model.requests&before=01");
  await expect(
    page.getByRole("heading", { name: "Invalid resource link" }),
  ).toBeVisible();
  expect(calls).toHaveLength(0);
});
