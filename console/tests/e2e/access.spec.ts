// SPDX-License-Identifier: MIT
import { createServer, type Server } from "node:http";
import { readFileSync } from "node:fs";
import { test, expect, type Page } from "@playwright/test";
import { parseUserJson, stringifyUserJson } from "../../src/api/json";
import { decodeConfig } from "../../src/api/codecs";
import { approvalFixture } from "../approval-fixture";
import { acceptedApproval, acceptanceEvent } from "../approval-audit-fixture";
import type { Approval, ApprovalDecision } from "../../src/api/approvals";
const raw = parseUserJson(
  readFileSync(
    new URL(
      "../../../crates/ledgence-orchestration-api/tests/fixtures/console-v16.json",
      import.meta.url,
    ),
    "utf8",
  ),
  2 * 1024 * 1024,
);
if (!raw || typeof raw !== "object" || !("config" in raw))
  throw Error("config fixture");
const config = decodeConfig(raw.config);
const approval = approvalFixture();
const reviewUrl = `/console/approvals/review?${new URLSearchParams({ workflow: approval.workflow_id, key: approval.key })}`;
const servers: Server[] = [];
const fixtureErrors: unknown[] = [];
test.afterEach(async () => {
  for (const server of servers.splice(0)) {
    server.closeAllConnections();
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
  expect(fixtureErrors.splice(0)).toEqual([]);
});
async function server(baseURL: string, trusted = false, audited = false) {
  let currentApproval = approval;
  let recordedAcceptance: ReturnType<typeof acceptanceEvent> | undefined;
  let auditStatus = 200;
  let auditItems: unknown[] | undefined;
  let delayAudit: Promise<void> | undefined;
  const auditRequests: unknown[] = [];
  let currentConfig = config;
  const posts: {
    body: string;
    actor: string | undefined;
    cookie: string | undefined;
    accepted: boolean;
  }[] = [];
  let loseFirst = false;
  let denyNext = false;
  let delayAccess: Promise<void> | undefined;
  let allowSubmit = true;
  let delayInspect: Promise<void> | undefined;
  function access(subject: string) {
    subject = subject === "alice-rotated" ? "alice" : subject;
    return {
      operator_audit: audited,
      mode: "credentials",
      actor_key: `actor-${subject}`,
      issuer: "urn:authority:test",
      subject,
      class: "operator",
      instance_id: currentConfig.instance_id,
      scope: { tenant_id: "acme", namespace: "demo" },
      capabilities:
        subject === "reader"
          ? ["metadata_read"]
          : [
              "metadata_read",
              "sensitive_read",
              "approval_decide",
              ...(allowSubmit ? ["execution_submit"] : []),
            ],
      expires_at_ms: Date.now() + 600000,
    };
  }
  let origin = "";
  const wire = createServer((request, response) => {
    void (async () => {
      const url = new URL(request.url!, origin);
      if (!url.pathname.startsWith("/v1/console/")) {
        const asset = await fetch(new URL(request.url!, baseURL));
        response.writeHead(asset.status, {
          "Content-Type":
            asset.headers.get("Content-Type") ?? "application/octet-stream",
          "Cache-Control": "no-store",
        });
        response.end(Buffer.from(await asset.arrayBuffer()));
        return;
      }
      const headers = request.headers;
      const path = url.pathname.replace("/v1/console/", "");
      const chunks: Buffer[] = [];
      for await (const chunk of request) chunks.push(Buffer.from(chunk));
      const body = Buffer.concat(chunks).toString("utf8");
      const credential = /ledgence_access_test=([a-z-]+)/.exec(
        String(headers.cookie ?? ""),
      )?.[1];
      const subject = credential === "alice-rotated" ? "alice" : credential;
      const reply = (value: unknown, status = 200, cookie?: string) => {
        response.writeHead(status, {
          "Content-Type": "application/json",
          "Ledgence-Console-Contract": "16",
          "Ledgence-Instance-Id": currentConfig.instance_id,
          ...(cookie ? { "Set-Cookie": cookie } : {}),
        });
        response.end(stringifyUserJson(value));
      };
      if (
        headers["x-ledgence-console-origin"] !== origin ||
        (request.method === "POST" && headers.origin !== origin)
      )
        return reply({ code: "forbidden" }, 403);
      if (path === "access/login") {
        const value = JSON.parse(body);
        if (
          !["alice", "alice-rotated", "bob", "reader"].includes(
            value.credential,
          )
        )
          return reply({ code: "unauthenticated" }, 401);
        return reply(
          access(value.credential),
          200,
          `ledgence_access_test=${value.credential}; HttpOnly; SameSite=Strict; Path=/`,
        );
      }
      if (!trusted && !subject) return reply({ code: "unauthenticated" }, 401);
      if (path === "access") {
        if (delayAccess) await delayAccess;
        return reply(trusted ? { mode: "operator_trusted" } : access(subject!));
      }
      const accepted =
        trusted || headers["x-ledgence-actor"] === `actor-${subject}`;
      if (path === "approvals/decide")
        posts.push({
          body: body,
          actor:
            typeof headers["x-ledgence-actor"] === "string"
              ? headers["x-ledgence-actor"]
              : undefined,
          cookie: subject,
          accepted,
        });
      if (!accepted) return reply({ code: "forbidden" }, 403);
      if (path === "access/logout")
        return reply(
          { signed_out: true },
          200,
          "ledgence_access_test=; Max-Age=0; HttpOnly; SameSite=Strict; Path=/",
        );
      if (path === "config") return reply(currentConfig);
      if (path === "approvals/inspect") {
        if (subject === "reader") return reply({ code: "forbidden" }, 403);
        if (delayInspect) await delayInspect;
        return reply(currentApproval);
      }
      if (path === "audit/events") {
        auditRequests.push(JSON.parse(body));
        if (delayAudit) await delayAudit;
        if (auditStatus !== 200)
          return reply({ code: "unavailable" }, auditStatus);
        return reply({
          items: auditItems ?? [
            recordedAcceptance ??
              acceptanceEvent(currentConfig.instance_id, currentApproval),
          ],
          next_cursor: null,
        });
      }
      if (path === "approvals/decide") {
        if (denyNext) {
          denyNext = false;
          return reply({ code: "forbidden" }, 403);
        }
        const sent = parseUserJson(body, 1024 * 1024) as ApprovalDecision;
        currentApproval = {
          ...approval,
          status: sent.decision === "approve" ? "approved" : "rejected",
          decision: {
            decision_id: sent.decision_id,
            decision: sent.decision,
            reviewer: sent.reviewer,
            reason: sent.reason,
            decided_at: approval.created_at + 1,
          },
        };
        if (!recordedAcceptance) {
          recordedAcceptance = acceptanceEvent(
            currentConfig.instance_id,
            currentApproval,
          );
          recordedAcceptance.actor.subject =
            subject ?? "operator-trusted-fixture";
          recordedAcceptance.actor.credential_id =
            credential ?? "unverified-fixture";
        }
        if (loseFirst && posts.length === 1)
          return reply({ code: "unavailable" }, 503);
        return reply({
          approval: currentApproval,
          already_accepted: posts.length > 1,
        });
      }
      if (path === "workflows/status") return reply({ code: "not_found" }, 404);
      return reply({ code: "not_found" }, 404);
    })().catch((error) => {
      if (request.aborted || response.destroyed) return;
      fixtureErrors.push(error);
      response.writeHead(500);
      response.end();
    });
  });
  servers.push(wire);
  await new Promise<void>((resolve) => wire.listen(0, "127.0.0.1", resolve));
  const address = wire.address();
  if (!address || typeof address === "string")
    throw Error("HTTP fixture did not bind");
  origin = `http://127.0.0.1:${address.port}`;
  return {
    url: origin,
    posts,
    auditRequests,
    approval(value: Approval) {
      currentApproval = value;
    },
    audit(value: {
      status?: number;
      items?: unknown[];
      delay?: Promise<void>;
    }) {
      auditStatus = value.status ?? 200;
      auditItems = value.items;
      delayAudit = value.delay;
    },
    delayAccess(value: Promise<void> | undefined) {
      delayAccess = value;
    },
    allowSubmit(value: boolean) {
      allowSubmit = value;
    },
    restart(instanceId: string) {
      currentConfig = { ...config, instance_id: instanceId };
      currentApproval = approval;
      recordedAcceptance = undefined;
    },
    loseFirst() {
      loseFirst = true;
    },
    denyNext() {
      denyNext = true;
    },
    delayInspect(value: Promise<void> | undefined) {
      delayInspect = value;
    },
  };
}
async function signIn(page: Page, who = "alice") {
  await page.getByLabel("Instance credential").fill(who);
  await page.getByRole("button", { name: "Sign in", exact: true }).click();
  await expect(
    page.getByText(`Signed in as ${who === "alice-rotated" ? "alice" : who}`, {
      exact: false,
    }),
  ).toBeVisible();
}
async function prepare(page: Page) {
  await page.getByRole("button", { name: "Approve", exact: true }).click();
  await page
    .getByRole("textbox", { name: "Reviewer", exact: true })
    .fill("Avery");
}
test("shared-cookie actor changes cannot submit an old view even without a broadcast", async ({
  context,
  page,
  baseURL,
}) => {
  const backend = await server(baseURL!);
  await page.goto(backend.url + reviewUrl);
  await signIn(page);
  const other = await context.newPage();
  await other.goto(backend.url + "/console/");
  await page.bringToFront();
  await expect(
    page.getByRole("button", { name: "Approve", exact: true }),
  ).toBeVisible();
  await prepare(page);
  // Another same-origin tab changes the shared cookie without announcing it.
  await other.evaluate(async () => {
    await fetch("/v1/console/access/login", {
      method: "POST",
      credentials: "same-origin",
      headers: {
        "Content-Type": "application/json",
        "X-Ledgence-Console-Origin": location.origin,
      },
      body: JSON.stringify({ credential: "bob" }),
    });
  });
  await page.getByRole("button", { name: "Confirm approval" }).click();
  await expect.poll(() => backend.posts.length).toBe(1);
  expect(backend.posts[0]).toMatchObject({
    actor: "actor-alice",
    cookie: "bob",
    accepted: false,
  });
  await expect(page.getByLabel("Instance credential")).toBeVisible();
  await expect(
    page.getByText("Effective arguments", { exact: true }),
  ).toHaveCount(0);
});
test("same-actor reauthentication permits only a deliberate byte-identical retry", async ({
  context,
  page,
  baseURL,
}) => {
  const backend = await server(baseURL!);
  backend.loseFirst();
  await page.goto(backend.url + reviewUrl);
  await signIn(page);
  await prepare(page);
  await page.getByRole("button", { name: "Confirm approval" }).click();
  await expect(page.getByRole("alert")).toBeVisible();
  expect(backend.posts).toHaveLength(1);
  await context.clearCookies();
  await page.evaluate(() => window.dispatchEvent(new Event("focus")));
  await expect(page.getByLabel("Instance credential")).toBeVisible();
  await signIn(page);
  expect(backend.posts).toHaveLength(1);
  await page.getByRole("button", { name: "View submitted decision" }).click();
  await page.getByRole("button", { name: "Try again", exact: true }).click();
  await expect.poll(() => backend.posts.length).toBe(2);
  expect(backend.posts[1]!.body).toBe(backend.posts[0]!.body);
  await expect(
    page.getByText(/This exact decision was already recorded/),
  ).toBeVisible();
});
test("explicit sign-out clears pending decisions in other tabs and does not replay on login", async ({
  context,
  page,
  baseURL,
}) => {
  const backend = await server(baseURL!);
  backend.loseFirst();
  await page.goto(backend.url + reviewUrl);
  await signIn(page);
  const other = await context.newPage();
  await other.goto(backend.url + reviewUrl);
  await expect(
    other.getByRole("button", { name: "Approve", exact: true }),
  ).toBeVisible();
  await page.bringToFront();
  await prepare(page);
  await page.getByRole("button", { name: "Confirm approval" }).click();
  await expect(page.getByRole("alert")).toBeVisible();
  await other.getByRole("button", { name: "Sign out of this browser" }).click();
  await expect(page.getByLabel("Instance credential")).toBeVisible();
  await signIn(page, "bob");
  await expect(
    page.getByRole("button", { name: "View submitted decision" }),
  ).toHaveCount(0);
  expect(backend.posts).toHaveLength(1);
});
test("late sensitive responses cannot populate a replacement actor's view", async ({
  page,
  baseURL,
}) => {
  const backend = await server(baseURL!);
  let release!: () => void;
  backend.delayInspect(
    new Promise<void>((resolve) => {
      release = resolve;
    }),
  );
  await page.goto(backend.url + reviewUrl);
  await signIn(page);
  await page.evaluate(async () => {
    await fetch("/v1/console/access/login", {
      method: "POST",
      credentials: "same-origin",
      headers: {
        "Content-Type": "application/json",
        "X-Ledgence-Console-Origin": location.origin,
      },
      body: JSON.stringify({ credential: "reader" }),
    });
    window.dispatchEvent(new Event("focus"));
  });
  await expect(
    page.getByText("Signed in as reader", { exact: false }),
  ).toBeVisible();
  release();
  await expect(
    page.getByText("Effective arguments", { exact: true }),
  ).toHaveCount(0);
  await expect(
    page.getByRole("button", { name: "Approve", exact: true }),
  ).toHaveCount(0);
  expect(backend.posts).toHaveLength(0);
});

for (const previouslyUncertain of [false, true]) {
  test(`access denial ${previouslyUncertain ? "preserves an earlier uncertain outcome" : "is reported as definitively rejected"}`, async ({
    page,
    baseURL,
  }) => {
    const backend = await server(baseURL!);
    if (previouslyUncertain) backend.loseFirst();
    else backend.denyNext();
    await page.goto(backend.url + reviewUrl);
    await signIn(page);
    await prepare(page);
    await page.getByRole("button", { name: "Confirm approval" }).click();
    if (previouslyUncertain) {
      await expect(page.getByRole("alert")).toBeVisible();
      backend.denyNext();
      await page
        .getByRole("button", { name: "Try again", exact: true })
        .click();
    }
    await expect(page.getByLabel("Instance credential")).toBeVisible();
    await signIn(page);
    await page.getByRole("button", { name: "View submitted decision" }).click();
    await expect(
      page.getByText(
        previouslyUncertain
          ? /Its outcome may be unknown/
          : /Access was denied before this decision was accepted/,
      ),
    ).toBeVisible();
    expect(backend.posts).toHaveLength(previouslyUncertain ? 2 : 1);
    await page.getByRole("button", { name: "Try again", exact: true }).click();
    await expect
      .poll(() => backend.posts.length)
      .toBe(previouslyUncertain ? 3 : 2);
    expect(backend.posts.at(-1)!.body).toBe(backend.posts[0]!.body);
  });
}

test("trusted-mode instance replacement cannot restore another instance's approval", async ({
  page,
  baseURL,
}) => {
  const backend = await server(baseURL!, true);
  backend.loseFirst();
  await page.goto(backend.url + reviewUrl);
  await prepare(page);
  await page.getByRole("button", { name: "Confirm approval" }).click();
  await expect(page.getByRole("alert")).toBeVisible();
  backend.restart("different-instance");
  await page.evaluate(() => window.dispatchEvent(new Event("focus")));
  await expect(
    page.getByRole("button", { name: "Approve", exact: true }),
  ).toBeVisible();
  await expect(
    page.getByRole("button", { name: "View submitted decision" }),
  ).toHaveCount(0);
  expect(backend.posts).toHaveLength(1);
});

test("sign-in and signed-in sidebar fit narrow and collapsed layouts", async ({
  page,
  baseURL,
}, info) => {
  const backend = await server(baseURL!);
  await page.setViewportSize({ width: 375, height: 812 });
  await page.goto(backend.url + reviewUrl);
  await expect(page.getByLabel("Instance credential")).toBeVisible();
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= innerWidth,
    ),
  ).toBe(true);
  await page.screenshot({
    path: info.outputPath("access-mobile.png"),
    fullPage: true,
  });
  await signIn(page);
  await expect(
    page.getByRole("button", { name: "Sign out of this browser" }),
  ).toBeVisible();
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= innerWidth,
    ),
  ).toBe(true);
  await page.screenshot({
    path: info.outputPath("access-mobile-signed-in.png"),
    fullPage: true,
  });
  await page.setViewportSize({ width: 1280, height: 900 });
  await page.getByRole("button", { name: "Collapse sidebar" }).click();
  await expect(
    page.getByRole("button", { name: "Sign out of this browser" }),
  ).toBeVisible();
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= innerWidth,
    ),
  ).toBe(true);
  await page.screenshot({
    path: info.outputPath("access-collapsed.png"),
    fullPage: true,
  });
});

for (const trusted of [false, true]) {
  test(`${trusted ? "trusted" : "credential"} access refresh preserves an unsent execution draft only in the same view`, async ({
    page,
    baseURL,
  }) => {
    const backend = await server(baseURL!, trusted);
    await page.goto(backend.url + "/console/executions/new");
    if (!trusted) await signIn(page);
    const program = page.getByRole("textbox", {
      name: "Program ID",
      exact: true,
    });
    await program.fill("draft-program");
    let release!: () => void;
    backend.delayAccess(
      new Promise<void>((resolve) => {
        release = resolve;
      }),
    );
    await page.evaluate(() => window.dispatchEvent(new Event("focus")));
    await expect(program).not.toBeVisible();
    release();
    await expect(program).toBeVisible();
    await expect(program).toHaveValue("draft-program");
    backend.delayAccess(undefined);
    if (trusted) backend.restart("replacement-instance");
    else
      await page.evaluate(async () => {
        await fetch("/v1/console/access/login", {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "X-Ledgence-Console-Origin": location.origin,
          },
          body: JSON.stringify({ credential: "bob" }),
        });
      });
    await page.evaluate(() => window.dispatchEvent(new Event("focus")));
    await expect(program).toHaveValue("");
    expect(backend.posts).toHaveLength(0);
  });
}

test("approval draft and modal are hidden during verification and restored for the same actor", async ({
  page,
  baseURL,
}) => {
  const backend = await server(baseURL!);
  await page.goto(backend.url + reviewUrl);
  await signIn(page);
  await prepare(page);
  await page
    .getByRole("textbox", { name: "Reason (optional)" })
    .fill("Unsent review draft");
  let release!: () => void;
  backend.delayAccess(
    new Promise<void>((resolve) => {
      release = resolve;
    }),
  );
  await page.evaluate(() => window.dispatchEvent(new Event("focus")));
  await expect(page.getByRole("dialog")).not.toBeVisible();
  release();
  await expect(
    page.getByRole("textbox", { name: "Reviewer", exact: true }),
  ).toHaveValue("Avery");
  await expect(
    page.getByRole("textbox", { name: "Reason (optional)" }),
  ).toHaveValue("Unsent review draft");
  expect(backend.posts).toHaveLength(0);
});

test("a permission change discards an unsent draft even for the same actor", async ({
  page,
  baseURL,
}) => {
  const backend = await server(baseURL!);
  await page.goto(backend.url + "/console/executions/new");
  await signIn(page);
  await page
    .getByRole("textbox", { name: "Program ID", exact: true })
    .fill("private-draft");
  backend.allowSubmit(false);
  await page.evaluate(() => window.dispatchEvent(new Event("focus")));
  await expect(
    page.getByText("Your credential does not permit submitting executions."),
  ).toBeVisible();
  backend.allowSubmit(true);
  await page.evaluate(() => window.dispatchEvent(new Event("focus")));
  await expect(
    page.getByRole("textbox", { name: "Program ID", exact: true }),
  ).toHaveValue("");
  expect(backend.posts).toHaveLength(0);
});

test("accepted approvals display their original verified actor and successful empty history distinctly", async ({
  page,
  baseURL,
}) => {
  const backend = await server(baseURL!, false, true);
  backend.approval(acceptedApproval());
  await page.goto(backend.url + reviewUrl);
  await signIn(page, "bob");
  await expect(
    page.getByText("Original operator", { exact: true }),
  ).toBeVisible();
  await expect(page.getByText("Caller label", { exact: true })).toBeVisible();
  expect(backend.auditRequests).toEqual([
    {
      filter: {
        kind: "approval_acceptance",
        workflow_id: approval.workflow_id,
        key: approval.key,
        activation_id: approval.activation_id,
        revision: approval.revision,
        decision_id: "decision-original",
      },
      cursor: null,
      limit: 2,
    },
  ]);
  await page.evaluate(() => window.dispatchEvent(new Event("focus")));
  await expect(
    page.getByText("Original operator", { exact: true }),
  ).toBeVisible();
  expect(backend.auditRequests).toHaveLength(1);
  backend.audit({ items: [] });
  await page.reload();
  await expect(
    page.getByText("No verified actor recorded.", { exact: true }),
  ).toBeVisible();
});

test("unavailable attribution needs manual retry and never claims an unverified historical actor", async ({
  page,
  baseURL,
}) => {
  const backend = await server(baseURL!, false, true);
  backend.approval(acceptedApproval());
  backend.audit({ status: 503 });
  await page.goto(backend.url + reviewUrl);
  await signIn(page);
  await expect(
    page.getByRole("button", { name: "Retry attribution" }),
  ).toBeVisible();
  await expect(
    page.getByText("No verified actor recorded.", { exact: true }),
  ).toHaveCount(0);
  await page.evaluate(() => window.dispatchEvent(new Event("focus")));
  await expect(
    page.getByRole("button", { name: "Retry attribution" }),
  ).toBeVisible();
  expect(backend.auditRequests).toHaveLength(1);
  backend.audit({});
  await page.getByRole("button", { name: "Retry attribution" }).click();
  await expect(
    page.getByText("Original operator", { exact: true }),
  ).toBeVisible();
  expect(backend.auditRequests).toHaveLength(2);
});

test("a late attribution response cannot repopulate a retired actor view", async ({
  page,
  baseURL,
}) => {
  const backend = await server(baseURL!, false, true);
  backend.approval(acceptedApproval());
  let release!: () => void;
  backend.audit({
    delay: new Promise<void>((resolve) => {
      release = resolve;
    }),
  });
  await page.goto(backend.url + reviewUrl);
  await signIn(page);
  await expect.poll(() => backend.auditRequests.length).toBe(1);
  await page.evaluate(async () => {
    await fetch("/v1/console/access/login", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Ledgence-Console-Origin": location.origin,
      },
      body: JSON.stringify({ credential: "reader" }),
    });
    window.dispatchEvent(new Event("focus"));
  });
  await expect(
    page.getByText("Signed in as reader", { exact: false }),
  ).toBeVisible();
  release();
  await expect(
    page.getByText("Original operator", { exact: true }),
  ).toHaveCount(0);
  await expect(page.getByText("Caller label", { exact: true })).toHaveCount(0);
  expect(backend.posts).toHaveLength(0);
});

test("same-actor credential rotation retains an uncertain command and the original acceptance evidence", async ({
  context,
  page,
  baseURL,
}) => {
  const backend = await server(baseURL!, false, true);
  backend.loseFirst();
  await page.goto(backend.url + reviewUrl);
  await signIn(page);
  await prepare(page);
  await page.getByRole("button", { name: "Confirm approval" }).click();
  await expect(page.getByRole("alert")).toBeVisible();
  await context.clearCookies();
  await page.evaluate(() => window.dispatchEvent(new Event("focus")));
  await expect(page.getByLabel("Instance credential")).toBeVisible();
  await signIn(page, "alice-rotated");
  expect(backend.posts).toHaveLength(1);
  await page.getByRole("button", { name: "View submitted decision" }).click();
  await page.getByRole("button", { name: "Try again", exact: true }).click();
  await expect.poll(() => backend.posts.length).toBe(2);
  expect(backend.posts[1]!.body).toBe(backend.posts[0]!.body);
  await expect(
    page.getByText(/This exact decision was already recorded/),
  ).toBeVisible();
  await expect(
    page.getByRole("dialog").getByText("alice", { exact: true }),
  ).toBeVisible();
});
