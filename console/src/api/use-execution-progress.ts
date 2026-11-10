// SPDX-License-Identifier: MIT
import {
  useEffect,
  useMemo,
  useRef,
  useState,
  useSyncExternalStore,
} from "react";
import { focusManager, onlineManager } from "@tanstack/react-query";
import { useInstance } from "../app/instance";
import { useAccess, usePermission } from "../app/access-session";
import { ProgressFeed } from "./progress-feed";
import { readProgress, type ProgressTarget } from "./progress";
const subscribeFocus = (listener: () => void) =>
  focusManager.subscribe(listener);
const subscribeOnline = (listener: () => void) =>
  onlineManager.subscribe(listener);
const focused = () => focusManager.isFocused();
const online = () => onlineManager.isOnline();

export function useExecutionProgress(target: ProgressTarget) {
  const config = useInstance();
  const access = useAccess();
  const permitted = usePermission("metadata_read");
  const isFocused = useSyncExternalStore(subscribeFocus, focused);
  const isOnline = useSyncExternalStore(subscribeOnline, online);
  const [paused, setPaused] = useState(false);
  const [refresh, setRefresh] = useState(0);
  const instanceId = config.instance_id;
  const { kind, id } = target;
  // Identity changes replace both the retained events and the cursor before render.
  const session = useMemo(
    () => ({
      view: access.view,
      feed: new ProgressFeed({ kind, id }, (query, signal) =>
        readProgress(instanceId, query, signal),
      ),
    }),
    [instanceId, kind, id, access.view],
  );
  const feed = session.feed;
  const consumedRefresh = useRef(0);
  const snapshot = useSyncExternalStore(feed.subscribe, feed.getSnapshot);
  const available =
    permitted && access.phase === "ready" && isFocused && isOnline;
  useEffect(() => {
    if (available && !paused) {
      const force = refresh > consumedRefresh.current;
      consumedRefresh.current = refresh;
      feed.start(force);
    } else feed.stop();
    return () => feed.stop();
  }, [feed, available, paused, refresh, access.epoch]);
  return {
    ...snapshot,
    paused,
    available,
    pause: () => setPaused(true),
    resume: () => {
      setPaused(false);
      setRefresh((value) => value + 1);
    },
  };
}
