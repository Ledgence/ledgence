import { useEffect, useRef, useSyncExternalStore } from "react";
import { focusManager, onlineManager } from "@tanstack/react-query";
import { retryableRead } from "./errors";

const subscribeFocus = (listener: () => void) =>
  focusManager.subscribe(listener);
const subscribeOnline = (listener: () => void) =>
  onlineManager.subscribe(listener);
const focused = () => focusManager.isFocused();
const online = () => onlineManager.isOnline();

// Parent and child metadata are independent observations. A parent becoming
// terminal must not leave the last child observation permanently preterminal.
export function useTerminalRefresh(
  active: boolean,
  identity: readonly (string | number | null)[],
  query: {
    isFetching: boolean;
    error: unknown;
    refetch: () => Promise<unknown>;
  },
  enabled = true,
) {
  const key = JSON.stringify(identity);
  const previous = useRef({ key, active, pending: false, waiting: false });
  const isFocused = useSyncExternalStore(subscribeFocus, focused);
  const isOnline = useSyncExternalStore(subscribeOnline, online);
  const { isFetching, error, refetch } = query;
  useEffect(() => {
    if (previous.current.key !== key) {
      previous.current = { key, active, pending: false, waiting: false };
      return;
    }
    const state = previous.current;
    if (state.active && !active) {
      state.pending = true;
      state.waiting = isFetching;
    }
    state.active = active;
    if (!enabled || (error && !retryableRead(error))) {
      state.pending = false;
      return;
    }
    if (!state.pending) return;
    if (state.waiting) {
      // Refetch would join an initial in-flight read. Let that earlier read
      // settle before starting the observation made after parent completion.
      if (isFetching) return;
      state.waiting = false;
    } else if (isFetching) {
      // Focus/reconnect or a manual refresh already started the final read.
      state.pending = false;
      return;
    }
    if (!isFocused || !isOnline) return;
    state.pending = false;
    // Keep the query's bounded retry policy; do not restart periodic polling.
    void refetch();
  }, [active, key, enabled, isFetching, error, refetch, isFocused, isOnline]);
}
