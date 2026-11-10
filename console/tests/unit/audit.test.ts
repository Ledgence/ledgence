// SPDX-License-Identifier: MIT
import { expect, it } from "vitest";
import {
  acceptanceFor,
  acceptanceQuery,
  auditEvent,
} from "../../src/api/audit";
import { acceptedApproval, acceptanceEvent } from "../approval-audit-fixture";
import { decodeAccess } from "../../src/api/access";
import { AccessController, type Access } from "../../src/app/access-state";
const scope = { tenant_id: "acme", namespace: "demo" };
it("validates the full original acceptance independently of the reviewer or current actor", () => {
  const approval = acceptedApproval();
  const expected = structuredClone(approval);
  const event = acceptanceEvent();
  expect(
    acceptanceFor(
      approval,
      "instance-test",
      scope,
    )({ items: [event], next_cursor: null }),
  ).toEqual(event);
  expect(approval).toEqual(expected);
  expect(acceptanceQuery(approval)).toEqual({
    filter: {
      kind: "approval_acceptance",
      workflow_id: approval.workflow_id,
      key: approval.key,
      activation_id: approval.activation_id,
      revision: approval.revision,
      decision_id: approval.decision!.decision_id,
    },
    cursor: null,
    limit: 2,
  });
  expect(
    acceptanceFor(
      approval,
      "instance-test",
      scope,
    )({ items: [], next_cursor: null }),
  ).toBeNull();
});
it("rejects wrong occurrence, result, timestamp, audience and ambiguous responses", () => {
  const decode = acceptanceFor(acceptedApproval(), "instance-test", scope);
  for (const field of [
    "workflow_id",
    "key",
    "activation_id",
    "revision",
    "decision_id",
    "decision",
  ] as const) {
    const event = acceptanceEvent();
    Object.assign(event.operation, {
      [field]:
        field === "revision"
          ? "1"
          : field === "decision"
            ? "reject"
            : "different",
    });
    expect(() => decode({ items: [event], next_cursor: null })).toThrow();
  }
  for (const event of [
    acceptanceEvent("other"),
    { ...acceptanceEvent(), recorded_at: 50 },
    {
      ...acceptanceEvent(),
      audience: {
        ...acceptanceEvent().audience,
        scope: { ...scope, namespace: "other" },
      },
    },
  ])
    expect(() => decode({ items: [event], next_cursor: null })).toThrow();
  expect(() =>
    decode({
      items: [acceptanceEvent(), acceptanceEvent()],
      next_cursor: null,
    }),
  ).toThrow();
  expect(() =>
    decode({ items: [acceptanceEvent()], next_cursor: "more" }),
  ).toThrow();
  expect(() => decode({ items: [] })).toThrow();
});
it("does not accept sensitive-response observers as the original approver", () => {
  const event = {
    ...acceptanceEvent(),
    operation: {
      kind: "sensitive_response",
      read: "approval_inspect",
      resource: {
        kind: "approval",
        workflow_id: acceptedApproval().workflow_id,
        key: acceptedApproval().key,
      },
    },
  };
  expect(auditEvent(event).operation.kind).toBe("sensitive_response");
  expect(() =>
    acceptanceFor(
      acceptedApproval(),
      "instance-test",
      scope,
    )({ items: [event], next_cursor: null }),
  ).toThrow();
  expect(() =>
    auditEvent({
      ...event,
      operation: { ...event.operation, read: "task_result" },
    }),
  ).toThrow();
  expect(() =>
    auditEvent({
      ...acceptanceEvent(),
      actor: { ...acceptanceEvent().actor, class: "worker" },
    }),
  ).toThrow();
  expect(() =>
    auditEvent({
      ...acceptanceEvent(),
      actor: { ...acceptanceEvent().actor, token: "forbidden" },
    }),
  ).toThrow();
});
it("audit discovery defaults absent to false semantics and changes invalidate preserved views", () => {
  const access: Access = {
    mode: "credentials",
    actor_key: "actor",
    issuer: "issuer",
    subject: "subject",
    class: "operator",
    instance_id: "instance-test",
    scope,
    capabilities: ["metadata_read"],
    expires_at_ms: Date.now() + 60000,
  };
  expect(decodeAccess(access)).toEqual(access);
  expect(decodeAccess({ ...access, operator_audit: true })).toEqual({
    ...access,
    operator_audit: true,
  });
  for (const value of [null, "true", 1])
    expect(() => decodeAccess({ ...access, operator_audit: value })).toThrow();
  const controller = new AccessController();
  controller.accept(access, controller.begin(), "instance-test");
  const original = controller.getSnapshot().view;
  controller.accept(
    { ...access, operator_audit: false },
    controller.begin(),
    "instance-test",
  );
  expect(controller.getSnapshot().view).toBe(original);
  controller.accept(
    { ...access, operator_audit: true },
    controller.begin(),
    "instance-test",
  );
  expect(controller.getSnapshot().view).not.toBe(original);
});

it("binds effect inspection to one exact workflow occurrence", () => {
  const event = {
    ...acceptanceEvent(),
    operation: {
      kind: "sensitive_response",
      read: "effect_inspect",
      resource: {
        kind: "effect",
        workflow_id: "workflow",
        activation_id: "activation",
        approval_key: "refund",
      },
    },
  };
  expect(auditEvent(event).operation).toEqual(event.operation);
  expect(() =>
    auditEvent({
      ...event,
      operation: { ...event.operation, read: "approval_inspect" },
    }),
  ).toThrow();
  expect(() =>
    auditEvent({
      ...event,
      operation: {
        ...event.operation,
        resource: { ...event.operation.resource, owner: {} },
      },
    }),
  ).toThrow();
});

it("binds audited hold reads to a queue and preserves queue write revisions beyond JS precision", () => {
  const event = acceptanceEvent();
  const read = {
    kind: "sensitive_response",
    read: "queue_holds",
    resource: { kind: "queue", queue: "bank" },
  };
  expect(auditEvent({ ...event, operation: read }).operation).toEqual(read);
  expect(() =>
    auditEvent({
      ...event,
      operation: { ...read, resource: { kind: "task", task_id: "task_1" } },
    }),
  ).toThrow();
  const common = {
    queue: "bank",
    request_id: "request_1",
    reason: "Change confirmed by operator",
    revision: "9007199254740993",
  };
  for (const operation of [
    {
      ...common,
      kind: "queue_policy_accepted",
      policy_digest: "sha256:" + "a".repeat(64),
    },
    {
      ...common,
      kind: "queue_hold_released",
      task_id: "task_1",
      attempt_id: "att_1",
    },
  ]) {
    expect(auditEvent({ ...event, operation }).operation).toEqual(operation);
    expect(() =>
      auditEvent({
        ...event,
        operation: { ...operation, revision: Number(common.revision) },
      }),
    ).toThrow();
  }
});

it("decodes artifact download audit bound to a task or workflow root", () => {
  for (const kind of ["task", "workflow"]) {
    const operation = {
      kind: "sensitive_response",
      read: "artifact_download",
      resource: {
        kind: "artifact",
        root: { kind, id: "root_123" },
        artifact_id: "art_123",
      },
    };
    expect(auditEvent({ ...acceptanceEvent(), operation }).operation).toEqual(
      operation,
    );
    for (const resource of [
      { ...operation.resource, root: { kind: "unknown", id: "root_123" } },
      { ...operation.resource, root: { kind, id: "" } },
      { ...operation.resource, artifact_id: "" },
      { kind: "task", task_id: "root_123" },
    ])
      expect(() =>
        auditEvent({
          ...acceptanceEvent(),
          operation: { ...operation, resource },
        }),
      ).toThrow();
    expect(() =>
      auditEvent({
        ...acceptanceEvent(),
        operation: { ...operation, read: "task_result" },
      }),
    ).toThrow();
  }
});
