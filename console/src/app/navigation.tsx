import { useLayoutEffect, useRef } from "react";
import { useLocation } from "react-router";
const positions = new Map<
  string,
  { x: number; y: number; href: string | null }
>();
export function NavigationMemory() {
  const location = useLocation();
  const address = location.pathname + location.search;
  const previousPath = useRef(location.pathname);
  useLayoutEffect(() => {
    const entry = positions.get(address);
    let href = entry?.href ?? null;
    const content = document.getElementById("main-content");
    const restore = () => {
      if (entry?.href) {
        const link = Array.from(
          content?.querySelectorAll<HTMLAnchorElement>("a[href]") ?? [],
        ).find((a) => a.href === entry.href);
        if (link) {
          link.focus({ preventScroll: true });
          window.scrollTo(entry.x, entry.y);
          return true;
        }
      }
      return false;
    };
    if (previousPath.current !== location.pathname) {
      window.scrollTo(entry?.x ?? 0, entry?.y ?? 0);
      if (!entry) content?.focus({ preventScroll: true });
    }
    previousPath.current = location.pathname;
    const observer = new MutationObserver(() => {
      if (restore()) observer.disconnect();
    });
    if (entry && !restore() && content)
      observer.observe(content, { childList: true, subtree: true });
    const timer = setTimeout(() => observer.disconnect(), 3000);
    const track = (event: FocusEvent) => {
      if (
        event.target instanceof HTMLAnchorElement &&
        content?.contains(event.target)
      )
        href = event.target.href;
    };
    document.addEventListener("focusin", track);
    return () => {
      document.removeEventListener("focusin", track);
      clearTimeout(timer);
      observer.disconnect();
      positions.set(address, { x: window.scrollX, y: window.scrollY, href });
      if (positions.size > 100) {
        const oldest = positions.keys().next().value;
        if (oldest) positions.delete(oldest);
      }
    };
  }, [address, location.pathname]);
  return null;
}
