// SPDX-License-Identifier: MIT
import { expect, test } from "@playwright/test";
import { login } from "./credential-session";

test("a reader follows a child to the budget owner and an operator submits an isolated budget", async ({
  browser,
  page,
  baseURL,
}) => {
  const child = process.env.LEDGENCE_BUDGET_CHILD,
    root = process.env.LEDGENCE_BUDGET_ROOT,
    queue = process.env.LEDGENCE_BUDGET_QUEUE;
  if (!baseURL || !child || !root || !queue)
    throw new Error(
      "Expected an explicitly prepared disposable budget instance.",
    );
  const url = new URL(baseURL);
  if (!["127.0.0.1", "[::1]", "localhost"].includes(url.hostname))
    throw new Error("Expected a loopback budget server");
  await login(page.context(), url.origin, "LEDGENCE_BUDGET_READER_FILE");
  await page.goto(
    "/console/workflows/" +
      encodeURIComponent(child) +
      "?tab=General&section=budget",
  );
  const panel = page.getByRole("heading", { name: "Shared run budget" });
  await expect(panel).toBeVisible();
  await expect(
    page
      .locator("dl")
      .filter({ has: page.getByText("Budget owner", { exact: true }) })
      .getByRole("link", { name: root, exact: true }),
  ).toHaveAttribute("href", "/console/workflows/" + encodeURIComponent(root));
  await expect(page.getByText("call · denied · calls exhausted")).toBeVisible();
  await expect(page.getByText("1 / 1", { exact: true })).toBeVisible();

  const op = await browser.newContext({ baseURL });
  try {
    await login(op, url.origin, "LEDGENCE_BUDGET_OPERATOR_FILE");
    const editor = await op.newPage();
    editor.setDefaultTimeout(10000);
    await editor.goto("/console/executions/new");
    await editor.getByLabel("Program ID").fill("shared-run-budget");
    await editor.getByLabel("Exact version").fill("1.0.0");
    const verify = editor.getByRole("button", { name: "Verify reference" });
    const intent = editor.getByRole("combobox", { name: "Execution kind" });
    if (await verify.count()) {
      const reply = editor.waitForResponse(
        (r) => new URL(r.url()).pathname === "/v1/console/programs/inspect",
        { timeout: 10000 },
      );
      await verify.click();
      const reference = await reply;
      if (reference.status() === 404) await intent.selectOption("workflow");
      else if (reference.status() !== 200)
        throw new Error(
          "Program verification failed: HTTP " + reference.status(),
        );
      else if (await intent.isVisible()) await intent.selectOption("workflow");
    } else {
      await intent.selectOption("workflow");
    }
    await editor.getByLabel("Queue", { exact: true }).fill(queue);
    await editor.getByLabel("Maximum calls").fill("0");
    await editor.getByLabel("Maximum delegations").fill("0");
    await editor.getByLabel("JSON input").fill(
      JSON.stringify({
        tickets: ["never-start"],
        queue,
        endpoint: process.env.LEDGENCE_BUDGET_PROVIDER_URL,
      }),
    );
    const accepted = editor.waitForResponse(
      (r) =>
        new URL(r.url()).pathname === "/v1/console/workflows" &&
        r.request().method() === "POST",
      { timeout: 15000 },
    );
    await editor
      .getByRole("button", { name: "Start workflow", exact: true })
      .click();
    const receipt = await accepted;
    expect(receipt.status()).toBe(200);
    await expect(editor).toHaveURL(/\/console\/workflows\//);
    const newId = new URL(editor.url()).pathname.split("/").pop();
    await editor.goto(
      "/console/workflows/" + newId + "?tab=General&section=budget",
    );
    await expect(
      editor.getByRole("heading", { name: "Shared run budget" }),
    ).toBeVisible();
    await expect(editor.getByText("0 / 0", { exact: true })).toHaveCount(2);
  } finally {
    await op.close();
  }
});
