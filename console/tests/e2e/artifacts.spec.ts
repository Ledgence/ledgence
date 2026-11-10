// SPDX-License-Identifier: MIT
import { readFileSync } from "node:fs";
import { expect, test } from "@playwright/test";
const fixture = JSON.parse(
  readFileSync(
    new URL(
      "../../../crates/ledgence-orchestration-api/tests/fixtures/console-v16.json",
      import.meta.url,
    ),
    "utf8",
  ),
);
const headers = {
  "Content-Type": "application/json",
  "Ledgence-Console-Contract": "16",
  "Ledgence-Instance-Id": "instance_demo",
};
for (const kind of ["task", "workflow"] as const)
  test(`${kind} artifacts remain explicit verified downloads`, async ({
    page,
  }, testInfo) => {
    const requests: string[] = [];
    await page.route("**/v1/console/**", async (route) => {
      const url = new URL(route.request().url());
      const name = url.pathname.replace("/v1/console/", "");
      requests.push(name);
      if (name === "access")
        return route.fulfill({
          headers,
          body: JSON.stringify({ mode: "operator_trusted" }),
        });
      if (name === "executions/ancestry") {
        const execution = {
          kind: url.searchParams.get("kind"),
          id: url.searchParams.get("id"),
        };
        return route.fulfill({
          headers,
          body: JSON.stringify({
            execution,
            path: [{ execution, program: null, availability: "available" }],
            observed_at: 1790409600000,
          }),
        });
      }
      if (name === "artifacts/download")
        return route.fulfill({
          headers: {
            ...headers,
            "Content-Type": "application/octet-stream",
            "Content-Length": "5",
            "Content-Disposition": "attachment; filename=invoice-report.txt",
            "Ledgence-Artifact-Manifest": Buffer.from(
              JSON.stringify(fixture.artifact_manifest),
            ).toString("base64"),
          },
          body: "hello",
        });
      if (name.endsWith("/inbox"))
        return route.fulfill({
          headers,
          body: JSON.stringify({ items: [], next_cursor: null }),
        });
      const key = (
        {
          config: "config",
          "tasks/status": "task_status",
          "workflows/inspect": "workflow_detail",
          "artifacts/list": "artifact_page",
        } as Record<string, string>
      )[name];
      if (!key) return route.fulfill({ status: 404, headers, body: "{}" });
      return route.fulfill({ headers, body: JSON.stringify(fixture[key]) });
    });
    const path =
      kind === "task"
        ? "executions/task_invoice_1042"
        : "workflows/wf_invoice_1042";
    await page.goto(`${path}?tab=General&section=artifacts`);
    await expect(
      page.getByText("invoice-report.txt", { exact: true }),
    ).toBeVisible();
    expect(
      requests.filter((request) => request === "artifacts/download"),
    ).toHaveLength(0);
    const download = page.waitForEvent("download");
    await page
      .getByRole("button", { name: "Download invoice-report.txt" })
      .click();
    const file = await download;
    expect(file.suggestedFilename()).toBe("invoice-report.txt");
    expect(readFileSync((await file.path())!, "utf8")).toBe("hello");
    await expect(
      page.getByText("Verified and sent to your browser."),
    ).toBeVisible();
    await page.getByText("File details", { exact: true }).click();
    await expect(
      page.getByRole("link", {
        name: "Workflow: wf_invoice_1042",
        exact: true,
      }),
    ).toBeVisible();
    await page.screenshot({
      path: testInfo.outputPath("artifacts.png"),
      fullPage: true,
    });
  });
