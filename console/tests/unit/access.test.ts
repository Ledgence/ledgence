// SPDX-License-Identifier: MIT
import { describe, expect, it } from "vitest";
import { AccessController, type Access } from "../../src/app/access-state";
import { decodeAccess } from "../../src/api/access";
import { requiredCapabilities } from "../../src/api/access-policy";
function actor(subject = "alice"): Extract<Access, { mode: "credentials" }> {
  return {
    mode: "credentials",
    actor_key: `actor-${subject}`,
    issuer: "urn:authority:test",
    subject,
    class: "operator",
    instance_id: "instance",
    scope: { tenant_id: "tenant", namespace: "demo" },
    capabilities: ["metadata_read", "sensitive_read", "approval_decide"],
    expires_at_ms: Date.now() + 60000,
  };
}
describe("Console access identity", () => {
  it("invalidates old leases and restores an exact pending command only to the same actor", () => {
    const controller = new AccessController();
    controller.accept(actor(), controller.begin());
    const lease = controller.lease()!;
    controller.remember("approval", {
      body: '{"amount":18446744073709551615}',
      identity: "decision",
      actor: lease.actor,
    });
    controller.lock("expired");
    expect(controller.current(lease)).toBe(false);
    expect(controller.recover("approval")).toBeUndefined();
    controller.accept(actor(), controller.begin());
    expect(controller.recover("approval")?.body).toBe(
      '{"amount":18446744073709551615}',
    );
    controller.accept(actor("bob"), controller.begin());
    expect(controller.recover("approval")).toBeUndefined();
  });
  it("discards pending commands on explicit logout and rejects late bootstrap results", () => {
    const controller = new AccessController();
    controller.accept(actor(), controller.begin());
    controller.remember("approval", {
      body: "secret payload",
      identity: "decision",
      actor: "actor-alice",
    });
    const old = controller.begin();
    const current = controller.begin("signed out", true);
    expect(controller.accept(actor(), old)).toBe(false);
    controller.accept(actor(), current);
    expect(controller.recover("approval")).toBeUndefined();
  });
  it("ignores late outcomes from an older attempt or a discarded actor", () => {
    const controller = new AccessController();
    controller.accept(actor(), controller.begin());
    const command = {
      body: "sensitive",
      identity: "decision",
      actor: "actor-alice",
    };
    const first = controller.remember("approval", command);
    const second = controller.remember("approval", command);
    controller.settle("approval", first, "denied");
    expect(controller.recover("approval")?.outcome).toBe("unconfirmed");
    controller.settle("approval", second, "denied");
    expect(controller.recover("approval")?.outcome).toBe("denied");
    controller.accept(actor("bob"), controller.begin());
    controller.settle("approval", second, "unconfirmed");
    expect(controller.recover("approval")).toBeUndefined();
  });
  it("preserves the view but retires requests while access is checked", async () => {
    const controller = new AccessController();
    controller.accept(actor(), controller.begin(), "instance");
    const view = controller.getSnapshot().view;
    const old = controller.lease()!;
    const epoch = controller.begin();
    expect(controller.getSnapshot().view).toBe(view);
    expect(controller.permits("sensitive_read")).toBe(true);
    expect(controller.current(old)).toBe(false);
    expect(() => controller.lease()).toThrow();
    const waiting = controller.ready(new AbortController().signal);
    controller.accept(actor(), epoch, "instance");
    expect(controller.getSnapshot().view).toBe(view);
    expect((await waiting)?.epoch).toBe(epoch);
    controller.accept(actor(), controller.begin(), "other-instance");
    expect(controller.getSnapshot().view).not.toBe(view);
  });
  it("cancels waiting reads and rejects replacement views without dispatch", async () => {
    const controller = new AccessController();
    controller.accept(actor(), controller.begin(), "instance");
    controller.begin();
    const abort = new AbortController();
    const stopped = controller.ready(abort.signal);
    abort.abort();
    await expect(stopped).rejects.toMatchObject({ name: "AbortError" });
    const replaced = controller.ready(new AbortController().signal);
    controller.accept(actor("bob"), controller.getSnapshot().epoch, "instance");
    await expect(replaced).rejects.toThrow("Access changed");
  });
  it("enforces credential expiry and current permissions", () => {
    const controller = new AccessController();
    const identity = actor();
    if (identity.mode !== "credentials") throw Error("fixture");
    controller.accept(
      { ...identity, capabilities: ["metadata_read"] },
      controller.begin(),
    );
    expect(controller.permits("sensitive_read")).toBe(false);
    controller.accept(
      { ...identity, expires_at_ms: Date.now() - 1 },
      controller.begin(),
    );
    expect(() => controller.lease()).toThrow("Access changed");
    expect(controller.getSnapshot().phase).toBe("locked");
  });
  it("strictly decodes modes without inventing an authenticated legacy actor", () => {
    expect(decodeAccess({ mode: "operator_trusted" })).toEqual({
      mode: "operator_trusted",
    });
    const valid = actor();
    expect(decodeAccess(valid)).toEqual(valid);
    for (const bad of [
      { mode: "operator_trusted", subject: "admin" },
      { ...actor(), class: "worker" },
      { ...actor(), capabilities: ["sensitive_read", "sensitive_read"] },
      { ...actor(), capabilities: ["worker_execute"] },
    ])
      expect(() => decodeAccess(bad)).toThrow();
  });
  it("distinguishes metadata, sensitive payloads, decisions and execution changes", () => {
    expect(requiredCapabilities("/v1/console/approvals/inbox", true)).toEqual([
      "metadata_read",
    ]);
    expect(requiredCapabilities("/v1/console/approvals/decide", true)).toEqual([
      "sensitive_read",
      "approval_decide",
    ]);
    expect(
      requiredCapabilities(
        "/v1/console/workflows/local-steps/inspect?id=x",
        false,
      ),
    ).toEqual(["sensitive_read"]);
    expect(requiredCapabilities("/v1/console/tasks", true)).toEqual([
      "execution_submit",
    ]);
  });
});

it("invalidates pending commands and cached views when provider availability changes", () => {
  const base = {
    ...actor(),
    operator_audit: true,
    native_effect_resolution: true,
    provider_operation: true,
  };
  expect(decodeAccess(base)).toEqual(base);
  expect(() => decodeAccess({ ...base, provider_operation: null })).toThrow();
  const controller = new AccessController();
  controller.accept(base, controller.begin());
  const before = controller.getSnapshot().view;
  controller.remember("approval", {
    body: "frozen",
    identity: "decision",
    actor: "actor-alice",
  });
  controller.accept({ ...base, provider_operation: false }, controller.begin());
  expect(controller.getSnapshot().view).toBeGreaterThan(before);
  expect(controller.recover("approval")).toBeUndefined();
});
