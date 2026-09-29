// SPDX-License-Identifier: MIT
import { useLayoutEffect, useRef } from "react";
import { useLocation } from "react-router";
import {
  positions,
  recentPaths,
  restorationKey,
  trim,
  type Position,
} from "./navigation-state";
export function NavigationMemory() {
  const location = useLocation();
  const previousPath = useRef(location.pathname);
  useLayoutEffect(() => {
    const entry =
      positions.get(location.key) ??
      positions.get(restorationKey(location.state) ?? "");
    const content = document.getElementById("main-content");
    const restore = () => {
      let ready = true;
      for (const [name, position] of Object.entries(entry?.containers ?? {})) {
        const element = Array.from(
          content?.querySelectorAll<HTMLElement>("[data-scroll-memory]") ?? [],
        ).find((item) => item.dataset.scrollMemory === name);
        if (element) {
          element.scrollLeft = position.x;
          element.scrollTop = position.y;
        } else ready = false;
      }
      if (entry) window.scrollTo(entry.x, entry.y);
      if (!entry?.focus) return ready;
      const target = Array.from(
        content?.querySelectorAll<HTMLElement>("[data-focus-key],a[href]") ??
          [],
      ).find(
        (element) =>
          (element.dataset.focusKey ??
            (element instanceof HTMLAnchorElement ? element.href : null)) ===
          entry.focus,
      );
      if (target) {
        target.focus({ preventScroll: true });
        return ready;
      }
      return false;
    };
    if (previousPath.current !== location.pathname && !entry) {
      window.scrollTo(0, 0);
      content?.focus({ preventScroll: true });
    }
    previousPath.current = location.pathname;
    let focus = entry?.focus ?? null;
    const containers: Position["containers"] = { ...entry?.containers };
    const rememberScroll = (event: Event) => {
      const element = event.target;
      if (element instanceof HTMLElement && element.dataset.scrollMemory)
        containers[element.dataset.scrollMemory] = {
          x: element.scrollLeft,
          y: element.scrollTop,
        };
    };
    document.addEventListener("scroll", rememberScroll, true);
    const observer = new MutationObserver(() => {
      if (restore()) observer.disconnect();
    });
    if (entry && !restore() && content)
      observer.observe(content, { childList: true, subtree: true });
    const timer = setTimeout(() => observer.disconnect(), 3000);
    const interruptRestoration = () => observer.disconnect();
    document.addEventListener("pointerdown", interruptRestoration, {
      once: true,
    });
    document.addEventListener("keydown", interruptRestoration, { once: true });
    const track = (event: FocusEvent) => {
      if (
        event.target instanceof HTMLElement &&
        content?.contains(event.target)
      )
        focus =
          event.target.dataset.focusKey ??
          (event.target instanceof HTMLAnchorElement
            ? event.target.href
            : focus);
    };
    document.addEventListener("focusin", track);
    recentPaths.set(location.pathname, {
      key: location.key,
      url: location.pathname + location.search,
    });
    trim(recentPaths);
    return () => {
      document.removeEventListener("focusin", track);
      clearTimeout(timer);
      observer.disconnect();
      document.removeEventListener("scroll", rememberScroll, true);
      document.removeEventListener("pointerdown", interruptRestoration);
      document.removeEventListener("keydown", interruptRestoration);
      for (const element of content?.querySelectorAll<HTMLElement>(
        "[data-scroll-memory]",
      ) ?? [])
        containers[element.dataset.scrollMemory ?? ""] = {
          x: element.scrollLeft,
          y: element.scrollTop,
        };
      positions.set(location.key, {
        x: window.scrollX,
        y: window.scrollY,
        focus,
        containers,
        url: location.pathname + location.search,
      });
      trim(positions);
    };
  }, [location.key, location.pathname, location.search, location.state]);
  return null;
}
