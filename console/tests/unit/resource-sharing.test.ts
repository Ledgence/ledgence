// SPDX-License-Identifier: MIT
import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import {
  decisionsFor,
  inspectResource,
  resourceList,
} from "../../src/api/resource-sharing";
import { requiredCapabilities } from "../../src/api/access-policy";
const fixture = JSON.parse(
  readFileSync(
    new URL(
      "../../../crates/ledgence-orchestration-api/tests/fixtures/console-v16.json",
      import.meta.url,
    ),
    "utf8",
  ),
);
const scope = { tenant_id: "acme", namespace: "billing" };
const name = "model.requests";
const record = fixture.fair_resource_snapshot;
const page = fixture.resource_decision_page;
const offer = page.items[0];
const decode = decisionsFor(scope, name, null, 2);

describe("weighted resource contracts", () => {
  it("decodes the Rust FIFO and weighted policy projections, including the default group", () => {
    const read = inspectResource(scope, name);
    expect(read(record).resource.policy.fair_sharing?.default_group).toBe(
      "batch",
    );
    const fifo = structuredClone(record);
    delete fifo.resource.policy.fair_sharing;
    expect(read(fifo).resource.policy.fair_sharing).toBeNull();
    fifo.resource.policy.fair_sharing = null;
    expect(() => read(fifo)).toThrow();
    for (const invalid of [
      { groups: [], default_group: "batch" },
      {
        groups: [
          { name: "support", weight: 3, queues: ["support"] },
          { name: "batch", weight: 1, queues: ["support"] },
        ],
        default_group: "batch",
      },
      {
        groups: [{ name: "batch", weight: 1001, queues: [] }],
        default_group: "batch",
      },
      {
        groups: [{ name: "batch", weight: 1, queues: [] }],
        default_group: "unconfigured",
      },
    ]) {
      const bad = structuredClone(record);
      bad.resource.policy.fair_sharing = invalid;
      expect(() => read(bad)).toThrow();
    }
    expect(() => read({ ...record, offered: 3, unknown: 2 })).toThrow();
    expect(() => inspectResource(scope, "other")(record)).toThrow();
    const readPage = resourceList(scope, null, 2);
    expect(readPage({ items: [record], next: name }).next).toBe(name);
    expect(() => readPage({ items: [record, record], next: name })).toThrow();
    expect(() => readPage({ items: [], next: name })).toThrow();
    expect(() =>
      resourceList(scope, name, 2)({ items: [record], next: null }),
    ).toThrow();
  });
  it("preserves large sequence IDs, immutable offer generations and scope-bound cursors", () => {
    expect(decode(page).next).toBe("9007199254740993");
    expect(decode(page).items[0]?.cycle_round).toBe(2);
    const make = (overrides: object) => ({
      items: [{ ...offer, ...overrides }],
      next: null,
    });
    for (const overrides of [
      { sequence: 9007199254740992 },
      { sequence: "01" },
      { sequence: "18446744073709551616" },
      { policy_revision: "0" },
      { offer_generation: 0 },
      { scope: { ...scope, tenant_id: "foreign" } },
      { resource: "foreign" },
      { eligible_groups: 1 },
      { cycle_round: 4 },
      { weight: 0 },
      { offered_at: -1 },
      { subject: { ...offer.subject, generation: 2147483648 } },
      { subject: { ...offer.subject, arguments: "private" } },
    ])
      expect(() => decode(make(overrides))).toThrow();
    for (const before of [offer.sequence, "1"])
      expect(() => decisionsFor(scope, name, before, 2)(page)).toThrow();
    expect(() => decode({ items: [offer, offer], next: null })).toThrow();
    expect(() => decode({ ...page, next: "1" })).toThrow();
    expect(() => decode({ items: [offer] })).toThrow();
    for (const kind of ["workflow", "provider"])
      expect(
        decode(
          make({
            subject: {
              kind,
              workflow_id: "wf",
              origin_activation_id: "act",
              key: "invoke",
            },
          }),
        ).items[0]?.subject.kind,
      ).toBe(kind);
    const lone = make({ reason: "sole_eligible", eligible_groups: 1 });
    expect(decode(lone).items[0]?.reason).toBe("sole_eligible");
  });
  it("requires only metadata read for all three scope-bound endpoints", () => {
    for (const path of ["inspect", "list", "decisions"])
      expect(
        requiredCapabilities("/v1/console/resources/" + path, true),
      ).toEqual(["metadata_read"]);
  });
});
