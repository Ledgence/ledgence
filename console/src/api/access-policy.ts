// SPDX-License-Identifier: MIT
import type { AccessCapability } from "../app/access-state";
export function permitsRequest(
  path: string,
  write: boolean,
  permits: (capability: AccessCapability) => boolean,
): boolean {
  if (path === "/v1/console/config")
    return permits("metadata_read") || permits("sensitive_read");
  return requiredCapabilities(path, write).every(permits);
}
export function requiredCapabilities(
  path: string,
  write: boolean,
): AccessCapability[] {
  const route = path.split("?")[0]!.replace("/v1/console/", "");
  if (route === "executions/progress") return ["metadata_read"];
  if (route === "budgets/inspect" || route === "budgets/entries")
    return ["metadata_read"];
  if (
    ["resources/list", "resources/inspect", "resources/decisions"].includes(
      route,
    )
  )
    return ["metadata_read"];
  if (route === "resources/put") return ["resource_control"];
  if (route === "resources/sources" || route === "task-admissions/inspect")
    return ["sensitive_read"];
  if (route === "queue-policies/list" || route === "queue-policies/inspect")
    return ["metadata_read"];
  if (route === "queue-policies/holds") return ["sensitive_read"];
  if (route === "queue-policies/put" || route === "queue-policies/release")
    return ["resource_control"];
  if (route === "audit/events" || route === "workflows/approved-effects/inbox")
    return ["metadata_read"];
  if (route === "workflows/approved-effects/inspect") return ["sensitive_read"];
  if (route === "workflows/approved-effects/resolve") return ["effect_resolve"];
  if (route === "approvals/decide")
    return ["sensitive_read", "approval_decide"];
  if (
    [
      "admissions/inspect",
      "approvals/inspect",
      "approvals/list",
      "workflows/input",
      "tasks/inspect",
      "tasks/result",
      "workflows/result",
      "workflows/local-steps/inspect",
    ].includes(route)
  )
    return ["sensitive_read"];
  if (!write || route === "approvals/inbox") return ["metadata_read"];
  if (route === "tasks" || route === "workflows") return ["execution_submit"];
  if (route === "programs/register") return ["program_publish"];
  return ["execution_control"];
}
