// SPDX-License-Identifier: MIT
import { describe, expect, it } from "vitest";
import fixture from "../../../crates/ledgence-orchestration-api/tests/fixtures/task-admission-v1.json";
import { submissionCommand } from "../../src/api/resources";
import { taskAdmissionFor } from "../../src/api/task-admissions";

const decode = taskAdmissionFor("task", 1, "model.requests", "instance");
describe("Rust task admission ledger", () => {
  it("decodes every durable state and only attributes an accepted invocation to its source", () => {
    for (const raw of Object.values(fixture.snapshots)) {
      const result = decode(raw);
      expect(result.state).toBe(raw.state);
      expect(result.source?.source_id ?? null).toBe(
        raw.source?.source_id ?? null,
      );
    }
    expect(decode(fixture.snapshots.preparing).source).toBeNull();
    expect(decode(fixture.snapshots.unknown).state).toBe("unknown_occupied");
  });
  it("requires closure evidence for finished admission and rejects a phantom offer while waiting", () => {
    const r = fixture.snapshots;
    expect(() => decode({ ...r.waiting, offer: r.offered.offer })).toThrow();
    expect(() => decode({ ...r.finished, close: null })).toThrow();
    expect(() =>
      decode({
        ...r.finished,
        close: { ...r.finished.close, already_accepted: true },
      }),
    ).toThrow();
    expect(() => decode({ ...r.deferred, owner: null, close: null })).toThrow();
    expect(() => decode({ ...r.released, close: r.finished.close })).toThrow();
  });
  it("omits admission for an ordinary submission, rejecting an explicit null", () => {
    const input = {
      program: { id: "hello", version: "1.0.0" },
      queue: "demo",
      data: {},
      correlation_key: null,
      retry_policy: { max_attempts: 1, retry_delay_ms: 0 },
      attempt_timeout_ms: 60000,
    };
    expect(submissionCommand(input)).toEqual(input);
    expect(
      submissionCommand({ ...input, admission: { resource: "pool" } }),
    ).toEqual({ ...input, admission: { resource: "pool" } });
    expect(() => submissionCommand({ ...input, admission: null })).toThrow();
  });
  it("rejects records from another generation, worker instance or resource", () => {
    const record = fixture.snapshots.unknown;
    expect(() =>
      taskAdmissionFor("task", 2, "model.requests", "instance")(record),
    ).toThrow();
    expect(() =>
      taskAdmissionFor("task", 1, "other", "instance")(record),
    ).toThrow();
    expect(() =>
      taskAdmissionFor("task", 1, "model.requests", "foreign")(record),
    ).toThrow();
    expect(() =>
      decode({ ...record, owner: { ...record.owner, attempt_id: "other" } }),
    ).toThrow();
  });
});
