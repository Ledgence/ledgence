// SPDX-License-Identifier: MIT
import { describe, expect, it } from "vitest";
import raw from "../../../crates/ledgence-orchestration-api/tests/fixtures/console-v14.json?raw";
import { sourceInspection, sourcesFor } from "../../src/api/resource-sources";
import { resourcePut, resourceReceipt } from "../../src/api/resource-sharing";
import { requiredCapabilities } from "../../src/api/access-policy";
import { auditEvent } from "../../src/api/audit";

const fixture = JSON.parse(raw);
const retained = fixture.resource_sources;
const row = retained.items[0],
  subject = row.subject;
const page = sourcesFor(row.scope, row.resource, "unknown_occupied", null, 1);
describe("resource management", () => {
  it("separates policy changes and audited source discovery from metadata", () => {
    expect(requiredCapabilities("/v1/console/resources/put", true)).toEqual([
      "resource_control",
    ]);
    expect(requiredCapabilities("/v1/console/resources/sources", true)).toEqual(
      ["sensitive_read"],
    );
    expect(
      requiredCapabilities("/v1/console/task-admissions/inspect", true),
    ).toEqual(["sensitive_read"]);
    const event = {
      event_id: "evt",
      recorded_at: 10,
      audience: {
        authority_id: "local",
        instance_id: "local",
        scope: row.scope,
      },
      actor: {
        issuer: "issuer",
        subject: "admin",
        class: "operator",
        credential_id: "id",
      },
      operation: {
        kind: "sensitive_response",
        read: "resource_sources",
        resource: { kind: "resource", name: row.resource },
      },
    };
    expect(auditEvent(event).operation.kind).toBe("sensitive_response");
    expect(() =>
      auditEvent({
        ...event,
        operation: {
          ...event.operation,
          resource: { kind: "task", task_id: "a" },
        },
      }),
    ).toThrow();
  });
  it("validates receipt scope, revision, complete policy, and FIFO omission", () => {
    const r = fixture.fair_resource_snapshot.resource;
    const cmd = resourcePut({
      name: r.name,
      expected_revision: r.revision - 1,
      request_id: "policy-id",
      policy: r.policy,
      reason: "change",
    });
    expect(
      resourceReceipt({ already_accepted: true, resource: r }, r.scope, cmd)
        .already_accepted,
    ).toBe(true);
    for (const other of [
      { ...r, revision: 1 },
      { ...r, scope: { ...r.scope, namespace: "other" } },
      { ...r, policy: { ...r.policy, max_occupancy: 2 } },
    ])
      expect(() =>
        resourceReceipt(
          { resource: other, already_accepted: false },
          r.scope,
          cmd,
        ),
      ).toThrow();
    expect(() =>
      resourcePut({ ...cmd, policy: { ...cmd.policy, fair_sharing: null } }),
    ).toThrow();
  });
  it("binds provider discovery to its resource, state, position, occurrence and approval", () => {
    const found = page(retained).items[0]!;
    expect(found.sequence).toBe("9007199254740993");
    for (const edit of [
      { scope: { ...row.scope, namespace: "foreign" } },
      { resource: "other" },
      { state: "possibly_dispatched" },
      { subject: { ...subject, arguments: "secret" } },
    ])
      expect(() =>
        page({ items: [{ ...row, ...edit }], next_cursor: null }),
      ).toThrow();
    expect(() =>
      sourcesFor(row.scope, row.resource, null, row.sequence, 1)(retained),
    ).toThrow();
    const read = sourceInspection(found, "instance_demo");
    expect(read.body).toEqual({
      workflow_id: subject.workflow_id,
      origin_activation_id: subject.origin_activation_id,
      key: subject.key,
    });
    const record = fixture.admission.provider.unknown_pending;
    expect(read.decode(record).source).toBe(record.source.source_id);
    const wrong = { ...found, subject: { ...subject, approval_key: "other" } };
    expect(() =>
      sourceInspection(wrong, "instance_demo").decode(record),
    ).toThrow();
    expect(() => sourceInspection(found, "foreign").decode(record)).toThrow();
    const wf = fixture.admission.snapshot;
    const workflow = sourceInspection(
      {
        ...found,
        subject: {
          kind: "workflow",
          workflow_id: wf.occurrence.workflow_id,
          origin_activation_id: wf.occurrence.origin_activation_id,
          key: wf.occurrence.key,
        },
      },
      "instance_demo",
    );
    expect(workflow.decode(wf).state).toBe("finished");
  });
});
