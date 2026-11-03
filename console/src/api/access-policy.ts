// SPDX-License-Identifier: MIT
import type { AccessCapability } from "../app/access-state";
export function requiredCapabilities(
  path: string,
  write: boolean,
): AccessCapability[] {
  const route = path.split("?")[0]!.replace("/v1/console/", "");
  if (route === "approvals/decide")
    return ["sensitive_read", "approval_decide"];
  if (
    [
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
