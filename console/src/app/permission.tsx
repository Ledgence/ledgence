// SPDX-License-Identifier: MIT
import type { ReactNode } from "react";
import { usePermission } from "./access-session";
import type { AccessCapability } from "./access-state";
export function Permission({
  capability,
  children,
}: {
  capability: AccessCapability;
  children: ReactNode;
}) {
  return usePermission(capability) ? children : null;
}
