// SPDX-License-Identifier: MIT
import { expect, test, type Page } from "@playwright/test";
import { readFileSync } from "node:fs";
import { parseUserJson } from "../../src/api/json";
import { decodeConfig } from "../../src/api/codecs";
const fixture = JSON.parse(
  readFileSync(
    new URL(
      "../../../crates/ledgence-orchestration-api/tests/fixtures/console-v11.json",
      import.meta.url,
    ),
    "utf8",
  ),
);
const config = decodeConfig(
  parseUserJson(JSON.stringify(fixture.config), 65536),
);
const scope = { tenant_id: "acme", namespace: "billing" };
const initial = { max_concurrency: 5, timeout: null };
const hold = {
  scope,
  queue: "bank",
  task_id: "task_1",
  attempt_id: "att_1",
  worker_session_id: "ws_1",
  consumer_id: 0,
  policy_revision: "1",
  grace_ms: null,
  force_at: null,
  revision: "9007199254740993",
  state: "uncertain",
  acquired_at: 100,
  deadline: 150,
  stop_requested_at: 150,
  released_at: null,
  release_kind: null,
};
async function wire(
  page: Page,
  permissions: string[],
  lost = false,
  losePolicy = false,
  revision = "1",
) {
  const requests: Array<{ route: string; body: string }> = [];
  let record = {
    scope,
    queue: "bank",
    revision,
    policy: initial,
    updated_at: 100,
  };
  let held = hold;
  let firstRelease = "";
  let originalPut: string | undefined;
  await page.route("**/v1/console/**", async (route) => {
    const req = route.request();
    const path = new URL(req.url()).pathname.replace("/v1/console/", "");
    const reply = (value: unknown, status = 200) =>
      route.fulfill({
        status,
        contentType: "application/json",
        headers: {
          "Ledgence-Console-Contract": "11",
          "Ledgence-Instance-Id": config.instance_id,
        },
        body: JSON.stringify(value),
      });
    if (path === "access")
      return reply({
        mode: "credentials",
        actor_key: "actor",
        issuer: "local",
        subject: "operator",
        class: "operator",
        instance_id: config.instance_id,
        scope,
        capabilities: permissions,
        expires_at_ms: Date.now() + 600000,
        operator_audit: true,
        native_effect_resolution: true,
        provider_operation: true,
      });
    if (path === "config") return reply(config);
    const bytes = req.postData() ?? "";
    const body = JSON.parse(bytes || "{}");
    requests.push({ route: path, body: bytes });
    expect(req.headers()["x-ledgence-actor"]).toBe("actor");
    expect(body.scope).toBeUndefined();
    const snapshot = () => ({
      record,
      occupied: held.state === "uncertain" ? "1" : "0",
      uncertain: held.state === "uncertain" ? "1" : "0",
    });
    if (path === "queue-policies/list")
      return reply({ items: [snapshot()], next: null });
    if (path === "queue-policies/inspect") return reply(snapshot());
    if (path === "queue-policies/holds")
      return reply({
        items:
          body.include_released || held.state === "uncertain" ? [held] : [],
        next: null,
      });
    if (path === "queue-policies/put") {
      if (originalPut) {
        if (originalPut === bytes)
          return reply({ record, already_accepted: true });
        return reply({ code: "conflict" }, 409);
      }
      if (body.expected_revision !== record.revision)
        return reply({ code: "conflict" }, 409);

      record = {
        ...record,
        revision: String(BigInt(record.revision) + 1n),
        policy: body.policy,
      };
      if (losePolicy) {
        originalPut = bytes;
        return route.abort("failed");
      }
      return reply({ record, already_accepted: false });
    }
    if (path === "queue-policies/release") {
      expect(body).toMatchObject({
        queue: "bank",
        task_id: "task_1",
        attempt_id: "att_1",
        expected_revision: "9007199254740993",
      });
      if (firstRelease) expect(bytes).toBe(firstRelease);
      // Return real-shaped receipt on the identical retry after an intentionally lost reply.
      const receipt = {
        ...hold,
        state: "released",
        revision: "9007199254740994",
        released_at: 160,
        release_kind: "operator",
      };
      if (!firstRelease && lost) {
        firstRelease = bytes;
        held = { ...hold, state: "released" };
        return route.abort("failed");
      }
      held = { ...hold, state: "released" };
      return reply({ hold: receipt, already_accepted: !!firstRelease });
    }
    throw new Error("Unexpected Console route " + path);
  });
  return requests;
}
test("operator edits capacity and reconciles the exact release after a lost response", async ({
  page,
}) => {
  const requests = await wire(
    page,
    ["metadata_read", "sensitive_read", "resource_control"],
    true,
  );
  await page.goto("/console/queues");
  await page.getByRole("link", { name: "bank", exact: true }).click();
  await page.getByRole("button", { name: "Edit policy" }).click();
  await page.getByLabel("Maximum concurrent attempts").fill("2");
  await page.getByLabel("Request a stop after a timeout").check();
  await page.getByLabel("After requesting a stop").selectOption("force");
  await page.getByLabel("Release grace (ms)").fill("2000");
  await page
    .getByLabel("Reason for this change")
    .fill("Protect the downstream API");
  await page.getByRole("button", { name: "Save policy" }).click();
  expect(requests.some((x) => x.route.endsWith("/put"))).toBe(false);
  await page.getByLabel("I understand that another task").check();
  await page.getByRole("button", { name: "Save policy" }).click();
  await expect(page.getByText("Queue policy saved.")).toBeVisible();
  const saved = requests.find((x) => x.route.endsWith("/put"));
  expect(JSON.parse(saved!.body)).toMatchObject({
    expected_revision: "1",
    policy: {
      max_concurrency: 2,
      timeout: {
        after_ms: 60000,
        release: { mode: "after_grace", grace_ms: 2000 },
      },
    },
  });
  expect(requests.some((x) => x.route.endsWith("/holds"))).toBe(false);
  await page.getByRole("button", { name: "Inspect holds" }).click();
  const desktop = page.viewportSize();
  await page.setViewportSize({ width: 320, height: 700 });
  const table = page.getByRole("region", { name: "Capacity holds table" });
  await table.focus();
  await expect(table).toBeFocused();
  expect(await table.evaluate((el) => el.scrollWidth > el.clientWidth)).toBe(
    true,
  );
  expect(
    await page.evaluate(() => document.documentElement.scrollWidth),
  ).toBeLessThanOrEqual(320);
  const review = page.getByRole("button", { name: "Review release of att_1" });
  await review.click();
  await expect(
    page.getByRole("dialog", { name: "Release uncertain capacity" }),
  ).toBeVisible();
  await page.keyboard.press("Escape");
  await expect(review).toBeFocused();
  if (desktop) await page.setViewportSize(desktop);
  await review.click();
  await page
    .getByLabel("Reason for release")
    .fill("Verified the original attempt stopped");
  await page.getByRole("button", { name: "Release this slot" }).click();
  await expect(
    page.getByRole("button", { name: "Retry exact request" }),
  ).toBeVisible();
  // The authoritative release may finish before the page can receive its receipt.
  // Leave the view and return when the hold is already absent from the active page.
  await page.keyboard.press("Escape");
  await page
    .getByRole("navigation")
    .getByRole("link", { name: "Queues", exact: true })
    .click();
  await page.getByRole("link", { name: "bank", exact: true }).click();
  await page
    .getByRole("button", { name: "Reconcile previous release" })
    .click();
  await page.getByRole("button", { name: "Retry exact request" }).click();
  await expect(page.getByText("Release accepted.")).toBeVisible();
  expect(
    requests.filter((x) => x.route.endsWith("/release")).map((x) => x.body),
  ).toHaveLength(2);
});
test("metadata-only credentials can browse queue limits without disclosing held attempts", async ({
  page,
}) => {
  const requests = await wire(page, ["metadata_read"]);
  await page.goto("/console/queues?queue=bank");
  await expect(
    page.getByRole("heading", { name: "Current policy" }),
  ).toBeVisible();
  await expect(page.getByRole("button", { name: "Edit policy" })).toHaveCount(
    0,
  );
  await expect(
    page.getByText("Inspecting attempts requires sensitive_read."),
  ).toBeVisible();
  expect(
    requests.some(
      (x) =>
        x.route.endsWith("/holds") ||
        x.route.endsWith("/release") ||
        x.route.endsWith("/put"),
    ),
  ).toBe(false);
});

test("lost policy acknowledgement can be reconciled after another read returns the new revision", async ({
  page,
}) => {
  const requests = await wire(
    page,
    ["metadata_read", "resource_control"],
    false,
    true,
  );
  await page.goto("/console/queues?queue=bank");
  await page.getByRole("button", { name: "Edit policy" }).click();
  await page.getByLabel("Concurrency policy").selectOption("paused");
  await page
    .getByLabel("Reason for this change")
    .fill("Pause the downstream system");
  await page.getByRole("button", { name: "Save policy" }).click();
  await expect(
    page.getByRole("button", { name: "Retry exact request" }),
  ).toBeVisible();
  await page
    .getByRole("navigation")
    .getByRole("link", { name: "Queues", exact: true })
    .click();
  await page.getByRole("link", { name: "bank", exact: true }).click();
  await expect(
    page.getByRole("button", { name: "Retry exact request" }),
  ).toBeVisible();
  await page.getByRole("button", { name: "Retry exact request" }).click();
  await expect(page.getByText("Queue policy saved.")).toBeVisible();
  const puts = requests
    .filter((x) => x.route.endsWith("/put"))
    .map((x) => x.body);
  expect(puts).toHaveLength(2);
  expect(puts[0]).toBe(puts[1]);
  expect(JSON.parse(puts[0]!)).toMatchObject({
    expected_revision: "1",
    policy: { max_concurrency: 0, timeout: null },
  });
});

test("a second edit waits for a current snapshot and uses the accepted revision losslessly", async ({
  page,
}) => {
  const requests = await wire(
    page,
    ["metadata_read", "resource_control"],
    false,
    false,
    "9007199254740993",
  );
  await page.goto("/console/queues?queue=bank");
  await page.getByRole("button", { name: "Edit policy" }).click();
  const concurrency = page.getByLabel("Concurrency policy");
  await expect(concurrency).toBeFocused();
  await concurrency.selectOption("paused");
  await page
    .getByLabel("Reason for this change")
    .fill("Pause while upstream drains");
  let allowRefresh!: () => void;
  const gate = new Promise<void>((resolve) => {
    allowRefresh = resolve;
  });
  await page.route("**/v1/console/queue-policies/inspect", async (route) => {
    await gate;
    await route.fallback();
  });
  await page.getByRole("button", { name: "Save policy" }).click();
  const edit = page.getByRole("button", { name: "Edit policy" });
  try {
    await expect(page.getByRole("status")).toContainText(
      "Refreshing the current snapshot",
    );
    await expect(edit).toBeDisabled();
  } finally {
    allowRefresh();
  }
  await edit.click();
  await expect(concurrency).toHaveValue("paused");
  await concurrency.selectOption("unlimited");
  await page.getByLabel("Reason for this change").fill("Reopen the queue");
  await page.getByRole("button", { name: "Save policy" }).click();
  await expect(
    page.getByText("Queue policy saved.", { exact: true }),
  ).toBeVisible();
  expect(
    requests
      .filter((x) => x.route.endsWith("/put"))
      .map((x) => JSON.parse(x.body).expected_revision),
  ).toEqual(["9007199254740993", "9007199254740994"]);
});
