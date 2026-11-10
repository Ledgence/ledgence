// SPDX-License-Identifier: MIT
import * as s from "./schema";
import { ContractError, record } from "./codecs";
import { request } from "./client";
import type { Access } from "../app/access-state";
function checked(result: { value: Access; headers: Headers }) {
  if (
    result.value.mode === "credentials" &&
    result.headers.get("Ledgence-Instance-Id") !== result.value.instance_id
  )
    throw new ContractError("Access belongs to a different instance.");
  return {
    access: result.value,
    instanceId: s.id(result.headers.get("Ledgence-Instance-Id")),
  };
}
const credentialShape = {
  mode: s.enumeration("credentials"),
  actor_key: s.id,
  issuer: s.id,
  subject: s.id,
  class: s.enumeration("operator", "service"),
  instance_id: s.id,
  scope: s.object({ tenant_id: s.id, namespace: s.id }),
  capabilities: s.array(
    s.enumeration(
      "metadata_read",
      "sensitive_read",
      "execution_submit",
      "execution_control",
      "program_publish",
      "artifact_write",
      "approval_decide",
      "worker_execute",
      "effect_resolve",
      "resource_control",
    ),
    10,
  ),
  expires_at_ms: s.timestamp,
};
const credentials = s.object(credentialShape);
const auditedCredentials = s.object({
  ...credentialShape,
  operator_audit: s.boolean,
});
const nativeCredentials = s.object({
  ...credentialShape,
  operator_audit: s.boolean,
  native_effect_resolution: s.boolean,
});
const providerCredentials = s.object({
  ...credentialShape,
  operator_audit: s.boolean,
  native_effect_resolution: s.boolean,
  provider_operation: s.boolean,
});
export function decodeAccess(value: unknown): Access {
  if (
    value &&
    typeof value === "object" &&
    "mode" in value &&
    value.mode === "operator_trusted"
  ) {
    record(value, ["mode"]);
    return { mode: "operator_trusted" };
  }
  const result =
    value && typeof value === "object" && "provider_operation" in value
      ? providerCredentials(value)
      : value &&
          typeof value === "object" &&
          "native_effect_resolution" in value
        ? nativeCredentials(value)
        : value && typeof value === "object" && "operator_audit" in value
          ? auditedCredentials(value)
          : credentials(value);
  if (
    new Set(result.capabilities).size !== result.capabilities.length ||
    result.capabilities.includes("worker_execute")
  )
    throw new ContractError("Invalid Console access capabilities.");
  return result;
}
export async function readAccess(signal: AbortSignal) {
  return checked(
    await request(
      "/v1/console/access",
      decodeAccess,
      signal,
      undefined,
      undefined,
      16384,
      null,
    ),
  );
}
export async function login(credential: string, signal: AbortSignal) {
  return checked(
    await request(
      "/v1/console/access/login",
      decodeAccess,
      signal,
      undefined,
      { body: JSON.stringify({ credential }), maximumBytes: 8192 },
      16384,
      null,
    ),
  );
}

export async function logout(actor: string, signal: AbortSignal) {
  return (
    await request(
      "/v1/console/access/logout",
      s.object({ signed_out: s.boolean }),
      signal,
      undefined,
      { body: "{}", maximumBytes: 2 },
      16384,
      null,
      actor,
    )
  ).value;
}
