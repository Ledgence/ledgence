// SPDX-License-Identifier: MIT
import { useSyncExternalStore } from "react";
import { accessController, type AccessCapability } from "./access-state";
import { logout } from "../api/access";
export const channelName = "ledgence-console-access";
export function announce(discard: boolean) {
  if (typeof BroadcastChannel === "undefined") return;
  const channel = new BroadcastChannel(channelName);
  channel.postMessage({ changed: true, discard });
  channel.close();
}
export function useAccess() {
  return useSyncExternalStore(
    accessController.subscribe,
    accessController.getSnapshot,
  );
}
export function usePermission(capability: AccessCapability) {
  useAccess();
  return accessController.permits(capability);
}
export async function signOut() {
  const lease = accessController.lease();
  if (!lease?.actor) return;
  const epoch = accessController.begin("Signing out of this browser", true);
  announce(true);
  try {
    await logout(lease.actor, new AbortController().signal);
    announce(true);
    accessController.lock(
      "Signed out of this browser. Your credential remains valid until it expires or is revoked.",
      epoch,
    );
  } catch {
    accessController.lock(
      "Could not confirm sign-out. Check access before continuing.",
      epoch,
    );
  }
}
