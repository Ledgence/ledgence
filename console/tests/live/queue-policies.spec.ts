// SPDX-License-Identifier: MIT
// Real Console, authenticated backend and queue authority. No route interception.
import { randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import { test, expect, type BrowserContext, type Page } from "@playwright/test";

async function login(context: BrowserContext, origin: string, key: string) {
  const file = process.env[key];
  if (!file)
    throw new Error("Provide a disposable local " + key + " credential file.");
  const credential = JSON.parse(readFileSync(file, "utf8")) as {
    server: string;
    token: string;
  };
  if (new URL(credential.server).origin !== origin || !credential.token)
    throw new Error(
      "The test credential must be bound to the disposable server.",
    );
  // Use the real login endpoint outside Playwright instrumentation so raw credentials do not reach test traces.
  const response = await fetch(origin + "/v1/console/access/login", {
    method: "POST",
    redirect: "error",
    headers: {
      "Content-Type": "application/json",
      Origin: origin,
      "X-Ledgence-Console-Origin": origin,
    },
    body: JSON.stringify({ credential: credential.token }),
    signal: AbortSignal.timeout(10000),
  });
  if (!response.ok)
    throw new Error("Local Console login failed with HTTP " + response.status);
  const header = response.headers.get("set-cookie");
  if (!header || !header.toLowerCase().includes("httponly"))
    throw new Error("An HttpOnly Console session cookie was required.");
  const pair = header.slice(0, header.indexOf(";"));
  const eq = pair.indexOf("=");
  if (eq <= 0) throw new Error("Invalid session cookie.");
  await context.addCookies([
    {
      name: pair.slice(0, eq),
      value: pair.slice(eq + 1),
      domain: new URL(origin).hostname,
      path: "/",
      httpOnly: true,
      secure: new URL(origin).protocol === "https:",
      sameSite: "Strict",
    },
  ]);
}
async function sendChange(page: Page) {
  const wire = page.waitForResponse(
    (r) =>
      new URL(r.url()).pathname === "/v1/console/queue-policies/put" &&
      r.request().method() === "POST",
  );
  await page.getByRole("button", { name: "Save policy" }).click();
  const response = await wire;
  expect(response.status()).toBe(200);
  return response.json() as Promise<{
    record: {
      revision: string;
      policy: {
        max_concurrency: number | null;
        timeout: null | {
          after_ms: number;
          release: { mode: string; grace_ms?: number };
        };
      };
    };
  }>;
}

test("operator manages real queue capacity and a metadata reader cannot change it", async ({
  browser,
  page,
  baseURL,
}) => {
  if (!baseURL) throw new Error("Set the disposable Console origin.");
  const url = new URL(baseURL);
  if (!["127.0.0.1", "[::1]", "localhost"].includes(url.hostname))
    throw new Error(
      "This acceptance uses an explicitly disposable loopback instance.",
    );
  const name = "pw-cap-" + randomUUID();
  await login(page.context(), url.origin, "LEDGENCE_QUEUE_OPERATOR_FILE");
  await page.goto("/console/queues");
  await expect(
    page.getByRole("heading", { name: "Queues", exact: true }),
  ).toBeVisible();
  await page.getByLabel("Queue name").fill(name);
  await page.getByRole("button", { name: "Create policy" }).click();
  await expect(
    page.getByText("On first enrollment", { exact: false }),
  ).toBeVisible();
  await page.getByLabel("Maximum concurrent attempts").fill("5");
  await page
    .getByLabel("Reason for this change")
    .fill("Browser qualification of a downstream concurrency limit");
  let receipt = await sendChange(page);
  expect(receipt.record).toMatchObject({
    revision: "1",
    policy: { max_concurrency: 5, timeout: null },
  });
  await expect(
    page.getByRole("heading", { name: "Current policy" }),
  ).toBeVisible();
  await page.getByRole("button", { name: "Edit policy" }).click();
  await expect(
    page.getByText("On first enrollment", { exact: false }),
  ).toHaveCount(0);
  await page.getByLabel("Concurrency policy").selectOption("paused");
  await page.getByLabel("Reason for this change").fill("Pause new work");
  receipt = await sendChange(page);
  expect(receipt.record).toMatchObject({
    revision: "2",
    policy: { max_concurrency: 0 },
  });
  await page.getByRole("button", { name: "Edit policy" }).click();
  await page.getByLabel("Concurrency policy").selectOption("limited");
  await page.getByLabel("Maximum concurrent attempts").fill("2");
  await page.getByLabel("Request a stop after a timeout").check();
  await page.getByLabel("Stop timeout (ms)").fill("5000");
  await page.getByLabel("After requesting a stop").selectOption("force");
  await page.getByLabel("Release grace (ms)").fill("2500");
  await page.getByLabel("I understand that another task").check();
  await page
    .getByLabel("Reason for this change")
    .fill("Bound the time to stop with explicit grace");
  receipt = await sendChange(page);
  expect(receipt.record).toMatchObject({
    revision: "3",
    policy: {
      max_concurrency: 2,
      timeout: {
        after_ms: 5000,
        release: { mode: "after_grace", grace_ms: 2500 },
      },
    },
  });
  await page.getByRole("button", { name: "Inspect holds" }).click();
  await expect(page.getByText("No matching holds.")).toBeVisible();
  const reader = await browser.newContext({ baseURL: url.origin });
  try {
    await login(reader, url.origin, "LEDGENCE_QUEUE_READER_FILE");
    const readPage = await reader.newPage();
    await readPage.goto("/console/queues?queue=" + encodeURIComponent(name));
    await expect(
      readPage.getByRole("heading", { name: "Current policy" }),
    ).toBeVisible();
    await expect(
      readPage.getByText("Inspecting attempts requires sensitive_read."),
    ).toBeVisible();
    await expect(
      readPage.getByRole("button", { name: "Edit policy" }),
    ).toHaveCount(0);
  } finally {
    await reader.close();
  }
  await page.getByRole("button", { name: "Edit policy" }).click();
  await page.getByLabel("Concurrency policy").selectOption("unlimited");
  await page.getByLabel("Request a stop after a timeout").uncheck();
  await page
    .getByLabel("Reason for this change")
    .fill("Finish this disposable qualification");
  receipt = await sendChange(page);
  expect(receipt.record).toMatchObject({
    revision: "4",
    policy: { max_concurrency: null, timeout: null },
  });
});
