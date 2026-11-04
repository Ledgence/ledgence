// SPDX-License-Identifier: MIT
import { describe, expect, it } from "vitest";
import {
  holdsFor,
  policiesFor,
  inspectFor,
  put,
  putReceiptFor,
  releaseReceiptFor,
  type QueueRelease,
} from "../../src/api/queue-policies";
import { requiredCapabilities } from "../../src/api/access-policy";
const scope = { tenant_id: "acme", namespace: "billing" };
const policy = {
  max_concurrency: 5,
  timeout: { after_ms: 2000, release: { mode: "confirm" as const } },
};
const snapshot = {
  record: {
    scope,
    queue: "bank",
    revision: "9007199254740993",
    policy,
    updated_at: 20,
  },
  occupied: "5",
  uncertain: "2",
};
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
  revision: "7",
  state: "uncertain",
  acquired_at: 100,
  deadline: 130,
  stop_requested_at: 131,
  released_at: null,
  release_kind: null,
};
describe("queue policy management boundaries", () => {
  it("separates scope, configured capacity and uncertain holds using exact counters", () => {
    expect(inspectFor(scope, "bank")(snapshot).record.revision).toBe(
      "9007199254740993",
    );
    expect(() =>
      inspectFor({ ...scope, namespace: "other" }, "bank")(snapshot),
    ).toThrow();
    expect(() => inspectFor(scope, "other")(snapshot)).toThrow();
    expect(() =>
      inspectFor(scope, "bank")({ ...snapshot, occupied: 5 }),
    ).toThrow();
    expect(() =>
      inspectFor(scope, "bank")({ ...snapshot, uncertain: "6" }),
    ).toThrow();
    expect(() =>
      inspectFor(
        scope,
        "bank",
      )({
        ...snapshot,
        record: {
          ...snapshot.record,
          policy: {
            max_concurrency: 5,
            timeout: {
              after_ms: 10,
              release: { mode: "after_grace", grace_ms: 0 },
            },
          },
        },
      }),
    ).toThrow();
  });
  it("refuses cross-queue, duplicate, nonprogressing and forward-skip page cursors", () => {
    expect(
      policiesFor(scope, 10, null)({ items: [snapshot], next: "bank" }).next,
    ).toBe("bank");
    for (const [items, next] of [
      [[snapshot, snapshot], null],
      [[snapshot], "future"],
      [[], "bank"],
    ] as const)
      expect(() => policiesFor(scope, 10, null)({ items, next })).toThrow();
    expect(() =>
      policiesFor(scope, 10, "bank")({ items: [snapshot], next: null }),
    ).toThrow();
    expect(() =>
      holdsFor(scope, "other", 10, false, null)({ items: [hold], next: null }),
    ).toThrow();
    expect(() =>
      holdsFor(
        scope,
        "bank",
        10,
        true,
        null,
      )({
        items: [
          {
            ...hold,
            state: "released",
            release_kind: "timeout",
            released_at: 140,
            stop_requested_at: null,
          },
        ],
        next: null,
      }),
    ).toThrow();
    expect(() =>
      holdsFor(
        scope,
        "bank",
        10,
        true,
        null,
      )({ items: [{ ...hold, stop_requested_at: 105 }], next: null }),
    ).toThrow();
  });
  it("requires forced expiry to match the originating grace and actual stop time", () => {
    const h = {
      ...hold,
      grace_ms: 10,
      force_at: 141,
      released_at: 142,
      state: "released",
      release_kind: "timeout",
    };
    const decode = holdsFor(scope, "bank", 10, true, null);
    expect(decode({ items: [h], next: null }).items[0]?.force_at).toBe(141);
    for (const changed of [
      { force_at: 140 },
      { grace_ms: null },
      { released_at: 140 },
      { policy_revision: "0" },
    ])
      expect(() =>
        decode({ items: [{ ...h, ...changed }], next: null }),
      ).toThrow();
  });
  it("accepts only source/revision-matching receipts for both original commands and replay", () => {
    const c = put({
      queue: "bank",
      request_id: "request_1",
      expected_revision: "9007199254740992",
      policy,
      reason: "Peak load",
    });
    for (const already_accepted of [false, true]) {
      expect(
        putReceiptFor({ record: snapshot.record, already_accepted }, scope, c)
          .record.revision,
      ).toBe("9007199254740993");
      expect(() =>
        putReceiptFor(
          {
            record: {
              ...snapshot.record,
              policy: { ...policy, max_concurrency: 4 },
            },
            already_accepted,
          },
          scope,
          c,
        ),
      ).toThrow();
      const cmd: QueueRelease = {
        queue: "bank",
        task_id: "task_1",
        attempt_id: "att_1",
        request_id: "r",
        expected_revision: "7",
        reason: "Verified upstream closure",
      };
      const h = {
        ...hold,
        state: "released",
        released_at: 150,
        release_kind: "operator",
        revision: "8",
      };
      expect(
        releaseReceiptFor({ hold: h, already_accepted }, scope, cmd).hold
          .revision,
      ).toBe("8");
      expect(() =>
        releaseReceiptFor(
          { hold: { ...h, attempt_id: "att_other" }, already_accepted },
          scope,
          cmd,
        ),
      ).toThrow();
      expect(() =>
        releaseReceiptFor(
          { hold: { ...h, release_kind: "timeout" }, already_accepted },
          scope,
          cmd,
        ),
      ).toThrow();
    }
  });
  it("requests the backend's capability for every scope-free Console operation", () => {
    for (const route of ["list", "inspect", "holds", "put", "release"])
      expect(
        requiredCapabilities("/v1/console/queue-policies/" + route, true),
      ).toEqual([
        route === "holds"
          ? "sensitive_read"
          : ["put", "release"].includes(route)
            ? "resource_control"
            : "metadata_read",
      ]);
  });
});
