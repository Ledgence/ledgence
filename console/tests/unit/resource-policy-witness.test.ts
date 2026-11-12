// SPDX-License-Identifier: MIT
import type { Page, Response } from "@playwright/test";
import { describe, expect, it, vi } from "vitest";
import { inspectCommittedResource } from "../live/resource-policy-witness";

const origin = "http://127.0.0.1:8080";
const name = "qualified-resource";
const authority = {
  actor: "operator-1",
  scope: { tenant_id: "acme", namespace: "billing" },
};
function fixture() {
  const policy = {
    enabled: true,
    max_occupancy: 4,
    rate: null,
    offer_timeout_ms: 30000,
    start_timeout_ms: 5000,
  };
  const command = {
    name,
    request_id: "save-2",
    expected_revision: 1,
    reason: "Change occupancy",
    policy: structuredClone(policy),
  };
  const snapshot = {
    resource: {
      scope: structuredClone(authority.scope),
      name,
      revision: 2,
      policy,
      updated_at: 1794495600000,
    },
    offered: 0,
    occupied: 0,
    unknown: 0,
    quarantined: false,
  };
  const request = {
    method: () => "POST",
    headers: () => ({ "x-ledgence-actor": authority.actor }),
    postDataJSON: () => command,
  };
  const unavailable = vi.fn(() => {
    throw new Error("Browser response body is no longer available.");
  });
  const response = {
    status: () => 200,
    url: () => origin + "/v1/console/resources/put",
    request: () => request,
    json: unavailable,
    body: unavailable,
  };
  const readback = {
    status: () => 200,
    json: async () => structuredClone(snapshot),
  };
  const post = vi.fn(async () => readback);
  const page = { url: () => origin + "/console/resources", request: { post } };
  return {
    request,
    response,
    readback,
    snapshot,
    command,
    post,
    unavailable,
    inspect: () =>
      inspectCommittedResource(
        page as unknown as Page,
        response as unknown as Response,
        authority,
        name,
      ),
  };
}

describe("resource policy browser witness", () => {
  it("reads the persisted policy in the original session without reading or replaying the mutation", async () => {
    const f = fixture();
    expect(await f.inspect()).toEqual(f.snapshot);
    expect(f.unavailable).not.toHaveBeenCalled();
    expect(f.post).toHaveBeenCalledExactlyOnceWith(
      origin + "/v1/console/resources/inspect",
      {
        headers: {
          Origin: origin,
          "X-Ledgence-Console-Origin": origin,
          "X-Ledgence-Actor": authority.actor,
        },
        data: { name },
        maxRedirects: 0,
        maxRetries: 0,
      },
    );
    expect(f.snapshot.resource.policy).not.toHaveProperty("fair_sharing");
  });

  it.each([
    [
      "failed edit",
      (f: ReturnType<typeof fixture>) => {
        f.response.status = () => 409;
      },
    ],
    [
      "foreign endpoint",
      (f: ReturnType<typeof fixture>) => {
        f.response.url = () => "http://127.0.0.1:8081/v1/console/resources/put";
      },
    ],
    [
      "wrong operation",
      (f: ReturnType<typeof fixture>) => {
        f.response.url = () => origin + "/v1/console/resources/inspect";
      },
    ],
    [
      "wrong method",
      (f: ReturnType<typeof fixture>) => {
        f.request.method = () => "GET";
      },
    ],
    [
      "missing actor",
      (f: ReturnType<typeof fixture>) => {
        f.request.headers = () => ({ "x-ledgence-actor": "" });
      },
    ],
    [
      "different actor",
      (f: ReturnType<typeof fixture>) => {
        f.request.headers = () => ({ "x-ledgence-actor": "other-operator" });
      },
    ],
    [
      "different submitted name",
      (f: ReturnType<typeof fixture>) => {
        f.command.name = "another-resource";
      },
    ],
  ])("rejects %s before readback", async (_name, mutate) => {
    const f = fixture();
    mutate(f);
    await expect(f.inspect()).rejects.toThrow();
    expect(f.post).not.toHaveBeenCalled();
    expect(f.unavailable).not.toHaveBeenCalled();
  });

  it.each([
    [
      "unavailable readback",
      (f: ReturnType<typeof fixture>) => {
        f.readback.status = () => 403;
      },
    ],
    [
      "different resource",
      (f: ReturnType<typeof fixture>) => {
        f.snapshot.resource.name = "another-resource";
      },
    ],
    [
      "different scope",
      (f: ReturnType<typeof fixture>) => {
        f.snapshot.resource.scope.namespace = "elsewhere";
      },
    ],
    [
      "wrong revision",
      (f: ReturnType<typeof fixture>) => {
        f.snapshot.resource.revision = 3;
      },
    ],
    [
      "wrong policy",
      (f: ReturnType<typeof fixture>) => {
        f.snapshot.resource.policy.max_occupancy = 8;
      },
    ],
  ])(
    "rejects %s without weakening the accepted-write witness",
    async (_name, mutate) => {
      const f = fixture();
      mutate(f);
      await expect(f.inspect()).rejects.toThrow();
      expect(f.post).toHaveBeenCalledTimes(1);
      expect(f.unavailable).not.toHaveBeenCalled();
    },
  );
});
