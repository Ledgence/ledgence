// SPDX-License-Identifier: MIT
import * as s from "./schema";
import { useQuery } from "@tanstack/react-query";
import { useInstance } from "../app/instance";
import { usePermission } from "../app/access-session";
import { request } from "./client";
import { retryableRead } from "./errors";

export const artifactMaximumBytes = 64 * 1024 * 1024;
const identifier = s.refine(
  s.id,
  (v) => new TextEncoder().encode(v).length <= 128,
  "Invalid artifact identity.",
);
const digest = s.refine(
  s.string,
  (v) => /^sha256:[a-f0-9]{64}$/.test(v),
  "Invalid artifact digest.",
);
const mediaType = s.refine(
  s.string,
  (v) => v.length <= 128 && /^[\x20-\x7e]+$/.test(v) && v.includes("/"),
  "Invalid artifact media type.",
);
const filename = s.refine(
  s.id,
  (v) =>
    new TextEncoder().encode(v).length <= 255 &&
    !/[\\/]/.test(v) &&
    v !== "." &&
    v !== "..",
  "Invalid artifact display filename.",
);
export const artifactTarget = s.variant({
  task: s.object({ kind: s.enumeration("task"), id: identifier }),
  workflow: s.object({ kind: s.enumeration("workflow"), id: identifier }),
});
export type ArtifactTarget = s.Decoded<typeof artifactTarget>;
export const artifactRef = s.object({
  schema: s.enumeration("ledgence.artifact.v1"),
  id: identifier,
  digest,
  size: s.integer(0, artifactMaximumBytes),
  media_type: mediaType,
});
export type ArtifactRef = s.Decoded<typeof artifactRef>;
const producer = s.variant({
  task: s.object({ kind: s.enumeration("task"), task_id: identifier }),
  workflow_activation: s.object({
    kind: s.enumeration("workflow_activation"),
    workflow_id: identifier,
    activation_id: identifier,
  }),
  verified_actor: s.object({
    kind: s.enumeration("verified_actor"),
    identity: s.object({
      issuer: s.refine(
        s.id,
        (v) => new TextEncoder().encode(v).length <= 256,
        "Invalid artifact issuer.",
      ),
      subject: identifier,
    }),
  }),
  trusted_instance: s.object({
    kind: s.enumeration("trusted_instance"),
    instance_id: identifier,
  }),
});
// The Console projection intentionally omits the private tenancy Scope.
export const artifactManifest = s.refine(
  s.object({
    reference: artifactRef,
    root: artifactTarget,
    producer,
    key: identifier,
    filename: s.nullable(filename),
    created_at: s.timestamp,
    accepted_at: s.timestamp,
    first_attempt_id: s.nullable(identifier),
  }),
  (v) => v.created_at > 0 && v.accepted_at >= v.created_at,
  "Invalid artifact creation or acceptance time.",
);
export type ArtifactManifest = s.Decoded<typeof artifactManifest>;
export const artifactPage = s.refine(
  s.object({
    items: s.array(artifactManifest, 100),
    next_cursor: s.nullable(
      s.refine(
        s.string,
        (v) =>
          v.length > 0 &&
          new TextEncoder().encode(v).length <= 4096 &&
          [...v].every(
            (character) =>
              character.charCodeAt(0) >= 32 && character.charCodeAt(0) !== 127,
          ),
        "Invalid artifact page cursor.",
      ),
    ),
  }),
  (v) =>
    new Set(v.items.map((item) => item.reference.id)).size === v.items.length &&
    v.items.every(
      (item) =>
        item.root.kind === v.items[0]?.root.kind &&
        item.root.id === v.items[0]?.root.id,
    ),
  "Artifact page contains inconsistent identities.",
);
export function artifactPageFor(limit: number, cursor: string | null) {
  return s.refine(
    artifactPage,
    (page) =>
      page.items.length <= limit &&
      (page.next_cursor === null ||
        (page.items.length > 0 && page.next_cursor !== cursor)),
    "Artifact page did not advance within the requested bound.",
  );
}
export function sameArtifactRef(a: ArtifactRef, b: ArtifactRef) {
  return (
    a.schema === b.schema &&
    a.id === b.id &&
    a.digest === b.digest &&
    a.size === b.size &&
    a.media_type === b.media_type
  );
}
export function useArtifacts(
  target: ArtifactTarget,
  limit: number,
  cursor: string | null,
  active: boolean,
) {
  const config = useInstance();
  const permitted = usePermission("metadata_read");
  const body = JSON.stringify({ target, limit, cursor });
  return useQuery({
    queryKey: [
      globalThis.location.origin,
      config.contract_version,
      config.instance_id,
      "artifacts/list",
      body,
    ],
    enabled: permitted && config.capabilities.execution_artifacts,
    queryFn: async ({ signal }) =>
      (
        await request(
          "/v1/console/artifacts/list",
          artifactPageFor(limit, cursor),
          signal,
          config.instance_id,
          { body, maximumBytes: 16384 },
          512 * 1024,
        )
      ).value,
    refetchInterval: ({ state }) =>
      active && !cursor && !(state.error && !retryableRead(state.error))
        ? config.polling.lists_ms
        : false,
  });
}
