// SPDX-License-Identifier: MIT
import * as s from "./schema";
import { ContractError } from "./codecs";
import { stringifyUserJson } from "./json";

const scope = s.object({ tenant_id: s.id, namespace: s.id });
const generation = s.integer(1, 2147483647);
const positive = s.integer(1, Number.MAX_SAFE_INTEGER);
const resource = s.refine(
  s.id,
  (v) => /^[A-Za-z0-9_.:-]{1,128}$/.test(v),
  "Invalid task resource name.",
);
const digest = s.refine(
  s.string,
  (v) => /^sha256:[0-9a-f]{64}$/.test(v),
  "Invalid task admission digest.",
);
export const taskAdmissionOccurrence = s.object({
  scope,
  task_id: s.id,
  generation,
});
const owner = s.object({
  scope,
  task_id: s.id,
  attempt_id: s.id,
  lease_id: s.id,
  generation,
  worker_session_id: s.id,
  consumer_id: s.integer(),
});
const offer = s.object({
  generation: positive,
  policy_revision: positive,
  expires_at: s.timestamp,
});
const source = s.object({
  owner,
  offer_generation: positive,
  resource,
  binding_digest: digest,
  start_id: s.id,
  source_id: s.id,
  worker: s.object({
    audience: s.object({ authority_id: s.id, instance_id: s.id, scope }),
    issuer: s.id,
    subject: s.id,
    credential_id: s.id,
  }),
  accepted_at: s.timestamp,
  start_deadline_at: s.timestamp,
});
const close = s.object({
  owner,
  offer_generation: positive,
  start_id: s.id,
  operation_id: s.id,
  outcome: s.enumeration("not_invoked", "quiescent"),
  source: s.nullable(source),
  disposition: s.enumeration(
    "deferred",
    "charged_not_invoked",
    "released",
    "already_released",
    "closed",
  ),
  accepted_at: s.timestamp,
  already_accepted: s.boolean,
});
const state = s.enumeration(
  "waiting",
  "offered",
  "preparing",
  "possibly_dispatched",
  "unknown_occupied",
  "finished",
  "closed_before_start",
  "administratively_released",
);
const snapshot = s.object({
  occurrence: taskAdmissionOccurrence,
  resource,
  program: s.object({
    program: s.object({ id: s.id, version: s.id }),
    digest,
    size: positive,
  }),
  state,
  revision: positive,
  created_at: s.timestamp,
  updated_at: s.timestamp,
  offer: s.nullable(offer),
  owner: s.nullable(owner),
  source: s.nullable(source),
  close: s.nullable(close),
});
const same = (a: unknown, b: unknown) =>
  stringifyUserJson(a) === stringifyUserJson(b);
export function taskAdmissionFor(
  taskId: string,
  attempt: number,
  name: string,
  instanceId: string,
) {
  return (raw: unknown) => {
    const v = snapshot(raw);
    const invalid = () => {
      throw new ContractError(
        "The task admission record does not match the requested execution.",
      );
    };
    if (
      v.occurrence.task_id !== taskId ||
      v.occurrence.generation !== attempt ||
      v.resource !== name ||
      v.updated_at < v.created_at
    )
      invalid();
    const o = v.owner,
      a = v.offer,
      src = v.source,
      c = v.close;
    if (
      o &&
      (!same(o.scope, v.occurrence.scope) ||
        o.task_id !== taskId ||
        o.generation !== attempt)
    )
      invalid();
    if (
      src &&
      (!same(src.owner, o) ||
        src.offer_generation !== a?.generation ||
        src.resource !== name ||
        src.worker.audience.instance_id !== instanceId ||
        !same(src.worker.audience.scope, v.occurrence.scope) ||
        src.accepted_at < v.created_at ||
        src.accepted_at > v.updated_at ||
        !a ||
        src.accepted_at >= a.expires_at ||
        src.start_deadline_at > a.expires_at ||
        src.start_deadline_at <= src.accepted_at ||
        src.start_deadline_at - src.accepted_at > 30000)
    )
      invalid();
    if (c) {
      if (
        c.already_accepted ||
        !same(c.owner, o) ||
        !same(c.source, src) ||
        c.offer_generation !== a?.generation ||
        c.accepted_at < v.created_at ||
        c.accepted_at > v.updated_at ||
        (src &&
          (c.start_id !== src.start_id || c.accepted_at < src.accepted_at))
      )
        invalid();
      if (
        !{
          deferred: !src && c.outcome === "not_invoked",
          charged_not_invoked: !!src && c.outcome === "not_invoked",
          released: !!src && c.outcome === "quiescent",
          already_released: !!src,
          closed: !src,
        }[c.disposition]
      )
        invalid();
    }
    if (
      !{
        waiting: !a && !o && !src && !c,
        offered: !!a && !o && !src && !c,
        preparing: !!a && !!o && !src && !c,
        possibly_dispatched: !!src && !c,
        unknown_occupied: !!src && !c,
        finished:
          !!src &&
          (c?.disposition === "released" ||
            c?.disposition === "charged_not_invoked"),
        administratively_released:
          !!src && (!c || c.disposition === "already_released"),
        closed_before_start:
          !src &&
          !!o === !!a &&
          (!c || c.disposition === "closed" || c.disposition === "deferred"),
      }[v.state]
    )
      invalid();
    return v;
  };
}
