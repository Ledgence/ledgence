// SPDX-License-Identifier: MIT
import { randomUUID } from "node:crypto";
import { expect, test, type Page } from "@playwright/test";
import { login } from "./credential-session";
import { decodeAccess } from "../../src/api/access";
import { inspectCommittedResource } from "./resource-policy-witness";

const putPath = "/v1/console/resources/put";
const putReply = (page: Page) =>
  page.waitForResponse(
    (r) =>
      new URL(r.url()).pathname === putPath && r.request().method() === "POST",
  );
async function save(page: Page, reason: string, status = 200) {
  await page.getByLabel("Reason for this change").fill(reason);
  const reply = putReply(page);
  await page.getByRole("button", { name: "Save policy" }).click();
  const r = await reply;
  expect(r.status()).toBe(status);
  if (status === 200)
    await expect(
      page.getByRole("button", { name: "Edit policy" }),
    ).toBeEnabled();
  return r;
}

test("resource policies replay a lost acknowledgement, fence stale edits and keep source discovery audited", async ({
  page,
  browser,
  baseURL,
}) => {
  if (!baseURL) throw new Error("A disposable Console origin is required.");
  const url = new URL(baseURL);
  if (!["127.0.0.1", "[::1]", "localhost"].includes(url.hostname))
    throw new Error("A disposable loopback instance is required.");
  const sourceName = process.env.LEDGENCE_RESOURCE_SOURCE_NAME,
    task = process.env.LEDGENCE_RESOURCE_SOURCE_TASK;
  if (!sourceName || !task)
    throw new Error("The disposable charged source is required.");
  const name = "pw-resource-" + randomUUID(),
    detail = "/console/resources?name=" + encodeURIComponent(name);
  await login(page.context(), url.origin, "LEDGENCE_QUEUE_OPERATOR_FILE");
  const operatorReply = await page.request.get(
    url.origin + "/v1/console/access",
    {
      headers: {
        Origin: url.origin,
        "X-Ledgence-Console-Origin": url.origin,
      },
      maxRedirects: 0,
      maxRetries: 0,
    },
  );
  expect(operatorReply.status()).toBe(200);
  const operator = decodeAccess(await operatorReply.json());
  if (operator.mode !== "credentials")
    throw new Error("This acceptance requires the credential-bound operator.");
  const authority = { actor: operator.actor_key, scope: operator.scope };
  await page.goto("/console/resources");
  await page.getByLabel("Resource name").fill(name);
  await page.getByRole("button", { name: "Create policy" }).click();
  await page.getByLabel("Maximum occupancy").fill("5");
  await page.getByLabel("Limit start rate").check();
  await page.getByLabel("Starts per period").fill("2");
  await page.getByLabel("Period (ms)").fill("1500");
  await page.getByLabel("Burst", { exact: true }).fill("3");
  await page.getByLabel("Use weighted queue groups").check();
  await page.getByLabel("Group 1 name").fill("support");
  await page.getByLabel("Group 1 weight").fill("3");
  await page.getByLabel("Group 1 queues").fill("urgent");
  await page.getByRole("button", { name: "Add group" }).click();
  await page.getByLabel("Group 2 name").fill("batch");
  await page.getByLabel("Default group").selectOption("batch");
  await page
    .getByLabel("Reason for this change")
    .fill("Qualify browser recovery");
  let original = "";
  await page.route(
    "**" + putPath,
    async (route) => {
      original = route.request().postData() ?? "";
      const actual = await route.fetch({ maxRetries: 0, maxRedirects: 0 });
      expect(actual.status()).toBe(200);
      expect(await actual.json()).toMatchObject({
        already_accepted: false,
        resource: { revision: 1 },
      });
      await route.abort("failed");
    },
    { times: 1 },
  );
  await page.getByRole("button", { name: "Save policy" }).click();
  await expect(
    page.getByRole("button", { name: "Retry exact request" }),
  ).toBeVisible();
  // Router navigation stays in the signed-in session and recovers the frozen command.
  await page.getByRole("link", { name: "All resources" }).click();
  await page.getByRole("link", { name, exact: true }).click();
  // Capture the server's real replay receipt before delivering its unchanged
  // response to the browser. The receipt-only already_accepted flag cannot be
  // recovered from a later policy inspection.
  let acceptReplay!: (value: unknown) => void;
  let rejectReplay!: (error: unknown) => void;
  const capturedReplay = new Promise<unknown>((resolve, reject) => {
    acceptReplay = resolve;
    rejectReplay = reject;
  });
  await page.route(
    "**" + putPath,
    async (route) => {
      try {
        expect(route.request().postData()).toBe(original);
        const actual = await route.fetch({ maxRetries: 0, maxRedirects: 0 });
        expect(actual.status()).toBe(200);
        const receipt: unknown = await actual.json();
        await route.fulfill({ response: actual });
        acceptReplay(receipt);
      } catch (error) {
        rejectReplay(error);
        await route.abort("failed").catch(() => {});
      }
    },
    { times: 1 },
  );
  const [replay] = await Promise.all([
    capturedReplay,
    page.getByRole("button", { name: "Retry exact request" }).click(),
  ]);
  expect(replay).toMatchObject({
    already_accepted: true,
    resource: {
      name,
      scope: authority.scope,
      revision: 1,
      policy: {
        max_occupancy: 5,
        rate: { numerator: 2, period_ms: 1500, burst: 3 },
        fair_sharing: {
          default_group: "batch",
          groups: [
            { name: "support", weight: 3, queues: ["urgent"] },
            { name: "batch", weight: 1, queues: [] },
          ],
        },
      },
    },
  });
  await expect(
    page.getByText("Resource policy saved at revision 1."),
  ).toBeVisible();

  const stale = await page.context().newPage();
  await stale.goto(detail);
  await stale.getByRole("button", { name: "Edit policy" }).click();
  await stale.getByLabel("Maximum occupancy").fill("8");
  await page.getByRole("button", { name: "Edit policy" }).click();
  await page.getByLabel("Maximum occupancy").fill("4");
  const acceptedReply = await save(page, "Reduce maximum occupancy");
  await expect(
    page.getByText("Resource policy saved at revision 2."),
  ).toBeVisible();
  const accepted = await inspectCommittedResource(
    page,
    acceptedReply,
    authority,
    name,
  );
  expect(accepted.resource.revision).toBe(2);
  await save(stale, "Stale limit change", 409);
  // The conflicting write must not have changed the accepted policy.
  const afterConflict = await inspectCommittedResource(
    page,
    acceptedReply,
    authority,
    name,
  );
  expect(afterConflict.resource).toEqual(accepted.resource);
  await expect(
    stale.getByText("The revision changed.", { exact: false }),
  ).toBeVisible();
  await stale.getByRole("button", { name: "Separate this operation" }).click();
  await stale
    .getByRole("button", { name: "Continue with a new operation" })
    .click();
  await stale.getByRole("button", { name: "Edit policy" }).click();
  await expect(stale.getByLabel("Maximum occupancy")).toHaveValue("4");
  await stale.getByLabel("Use weighted queue groups").uncheck();
  await stale.getByLabel("Limit start rate").uncheck();
  const finalReply = await save(stale, "Return to FIFO");
  await expect(
    stale.getByText("Resource policy saved at revision 3."),
  ).toBeVisible();
  const final = await inspectCommittedResource(
    stale,
    finalReply,
    authority,
    name,
  );
  expect(final.resource.revision).toBe(3);
  expect(final.resource.policy.rate).toBeNull();
  expect(final.resource.policy).not.toHaveProperty("fair_sharing");
  await stale.close();

  const sourceURL = "/console/resources?name=" + encodeURIComponent(sourceName);
  await page.goto(sourceURL);
  await page.getByRole("button", { name: "Inspect retained sources" }).click();
  await expect(
    page.getByText("Task " + task + " · generation 1"),
  ).toBeVisible();
  await page.getByRole("button", { name: "Inspect exact record" }).click();
  await expect(page.getByText("Source ID", { exact: false })).toBeVisible();

  const reader = await browser.newContext({ baseURL: url.origin });
  try {
    await login(reader, url.origin, "LEDGENCE_QUEUE_READER_FILE");
    const read = await reader.newPage();
    let sourceReads = 0;
    read.on("request", (r) => {
      if (new URL(r.url()).pathname.endsWith("/resources/sources"))
        sourceReads++;
    });
    const accessReply = read.waitForResponse(
      (r) =>
        new URL(r.url()).pathname === "/v1/console/access" &&
        r.status() === 200,
    );
    await read.goto(sourceURL);
    await expect(
      read.getByText("Inspecting retained sources requires sensitive_read."),
    ).toBeVisible();
    await expect(read.getByRole("button", { name: "Edit policy" })).toHaveCount(
      0,
    );
    await expect(read.getByText("1 / 1", { exact: true })).toBeVisible();
    const access = await (await accessReply).json();
    expect(access.capabilities).toEqual(["metadata_read"]);
    expect(sourceReads).toBe(0);
    for (const [path, body] of [
      [putPath, JSON.parse(original)],
      [
        "/v1/console/resources/sources",
        { name: sourceName, state: null, cursor: null, limit: 5 },
      ],
    ] as const) {
      const denied = await reader.request.post(url.origin + path, {
        data: body,
        headers: {
          Origin: url.origin,
          "X-Ledgence-Console-Origin": url.origin,
          "X-Ledgence-Actor": access.actor_key,
        },
      });
      expect(denied.status()).toBe(403);
    }
  } finally {
    await reader.close();
  }

  const sensitiveReader = await browser.newContext({ baseURL: url.origin });
  try {
    await login(sensitiveReader, url.origin, "LEDGENCE_RESOURCE_READER_FILE");
    const read = await sensitiveReader.newPage();
    const paths: string[] = [];
    read.on("request", (request) =>
      paths.push(new URL(request.url()).pathname),
    );
    const configReply = read.waitForResponse(
      (r) =>
        new URL(r.url()).pathname === "/v1/console/config" &&
        r.status() === 200,
    );
    const accessReply = read.waitForResponse(
      (r) =>
        new URL(r.url()).pathname === "/v1/console/access" &&
        r.status() === 200,
    );
    await read.goto("/console/");
    const config = await (await configReply).json(),
      access = await (await accessReply).json();
    expect(access.capabilities).toEqual(["sensitive_read"]);
    expect(config.suggested_queues).toEqual([]);
    expect(config.instance_name).toBe("Ledgence");
    expect(config.server_version).toBe("undisclosed");
    expect(
      Object.entries(config.capabilities)
        .filter(([, on]) => on)
        .map(([key]) => key)
        .sort(),
    ).toEqual(["resource_sources", "task_admission"]);
    await expect(read).toHaveURL(/\/console\/resources$/);
    await expect(
      read.getByRole("link", { name: "Executions", exact: true }),
    ).toHaveCount(0);
    await expect(
      read.getByRole("link", { name: "Resources", exact: true }),
    ).toBeVisible();
    await read.getByLabel("Resource name").fill(sourceName);
    await read.getByRole("button", { name: "Open resource" }).click();
    await expect(read.getByRole("button", { name: "Edit policy" })).toHaveCount(
      0,
    );
    expect(paths).not.toContain("/v1/console/resources/sources");
    await read
      .getByRole("button", { name: "Inspect retained sources" })
      .click();
    await expect(
      read.getByText("Task " + task + " · generation 1"),
    ).toBeVisible();
    await read.getByRole("button", { name: "Inspect exact record" }).click();
    await expect(read.getByText("Source ID", { exact: false })).toBeVisible();
    expect(paths).not.toContain("/v1/console/resources/list");
    expect(paths).not.toContain("/v1/console/resources/inspect");
    expect(paths).not.toContain("/v1/console/resources/decisions");
    expect(
      paths.filter((p) => p === "/v1/console/resources/sources"),
    ).toHaveLength(1);
    expect(
      paths.filter((p) => p === "/v1/console/task-admissions/inspect"),
    ).toHaveLength(1);
    for (const [operation, body] of [
      ["list", { after: null, limit: 1 }],
      ["inspect", { name: sourceName }],
      ["decisions", { name: sourceName, before: null, limit: 1 }],
    ] as const) {
      const denied = await sensitiveReader.request.post(
        url.origin + "/v1/console/resources/" + operation,
        {
          data: body,
          headers: {
            Origin: url.origin,
            "X-Ledgence-Console-Origin": url.origin,
            "X-Ledgence-Actor": access.actor_key,
          },
        },
      );
      expect(denied.status()).toBe(403);
    }
  } finally {
    await sensitiveReader.close();
  }
});
