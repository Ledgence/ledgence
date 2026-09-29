// SPDX-License-Identifier: MIT
import type { GraphPresentation } from "../features/explorer/layout";
export type Position = {
  x: number;
  y: number;
  focus: string | null;
  containers: Record<string, { x: number; y: number }>;
  url: string;
};
export const positions = new Map<string, Position>();
export const recentPaths = new Map<string, { key: string; url: string }>();
export function trim<T>(map: Map<string, T>) {
  if (map.size > 100) {
    const oldest = map.keys().next().value;
    if (oldest) map.delete(oldest);
  }
}
export function contextDestination(path: string) {
  const previous = recentPaths.get(path);
  return previous
    ? { to: previous.url, state: { restoreNavigationKey: previous.key } }
    : { to: path };
}
export function restorationKey(state: unknown): string | null {
  return state &&
    typeof state === "object" &&
    "restoreNavigationKey" in state &&
    typeof state.restoreNavigationKey === "string"
    ? state.restoreNavigationKey
    : null;
}
export type ExplorerViewState = {
  graph?: GraphPresentation;
  start: string;
  end: string;
  search: string;
  follow: boolean;
};
const explorerStates = new Map<string, ExplorerViewState>();
export function readExplorerState(
  key: string,
  state: unknown,
): ExplorerViewState {
  return (
    explorerStates.get(key) ??
    explorerStates.get(restorationKey(state) ?? "") ?? {
      start: "",
      end: "",
      search: "",
      follow: false,
    }
  );
}
export function saveExplorerState(key: string, value: ExplorerViewState) {
  explorerStates.set(key, value);
  trim(explorerStates);
}
