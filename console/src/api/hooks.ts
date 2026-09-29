import { useQuery } from "@tanstack/react-query";
import { useLocation, useSearchParams } from "react-router";
import { useInstance } from "../app/instance";
import { retryableRead } from "./errors";
import { apiPath, request } from "./client";
import type { Decoder } from "./schema";
export function useResource<T>(
  resource: string,
  parameters: Record<string, string | number | null | undefined>,
  decode: Decoder<T>,
  options: {
    enabled?: boolean;
    interval?: number | false | ((value: T | undefined) => number | false);
    staleTime?: number;
    maximumBytes?: number;
  } = {},
) {
  const config = useInstance();
  const path = apiPath(resource, parameters);
  return useQuery({
    queryKey: [
      location.origin,
      config.contract_version,
      config.instance_id,
      path,
    ],
    queryFn: async ({ signal }) =>
      (
        await request(
          path,
          decode,
          signal,
          config.instance_id,
          undefined,
          options.maximumBytes ?? config.limits.metadata_max_bytes,
        )
      ).value,
    enabled: options.enabled ?? true,
    refetchInterval: (query) =>
      query.state.error && !retryableRead(query.state.error)
        ? false
        : typeof options.interval === "function"
          ? options.interval(query.state.data)
          : (options.interval ?? false),
    staleTime: options.staleTime ?? 0,
  });
}
// Cursor predecessors follow the current URL, including browser Back/Forward.
// Keep at most 32 edges per query and 64 queries, without enlarging URLs.
const pageHistories = new Map<string, Map<string, string>>();
export function usePagination(prefix = "") {
  const config = useInstance();
  const location = useLocation();
  const [params, setParams] = useSearchParams();
  const cursor = params.get(`${prefix}cursor`);
  const size = Number(
    params.get(`${prefix}limit`) ?? config.limits.default_page_size,
  );
  const limit =
    [25, 50, 100].includes(size) && size <= config.limits.max_page_size
      ? size
      : config.limits.default_page_size;
  const filters = new URLSearchParams(params);
  // Attempt selection is display-only; other pagers' limits are independent.
  for (const key of [...filters.keys()])
    if (
      key.endsWith("cursor") ||
      key.endsWith("previous") ||
      key.endsWith("limit") ||
      key === "attempt" ||
      key === "node" ||
      key === "view"
    )
      filters.delete(key);
  filters.sort();
  const historyKey = [
    globalThis.location.origin,
    config.contract_version,
    config.instance_id,
    location.pathname,
    prefix,
    limit,
    filters.toString(),
  ].join("|");
  const predecessor = cursor
    ? pageHistories.get(historyKey)?.get(cursor)
    : undefined;
  const previous = predecessor === undefined ? [] : [predecessor];
  function remember(nextCursor: string) {
    const history = new Map(pageHistories.get(historyKey));
    // Refresh this edge without discarding predecessors needed by browser history.
    history.delete(nextCursor);
    history.set(nextCursor, cursor ?? "");
    if (history.size > 32) {
      const oldest = history.keys().next().value;
      if (oldest !== undefined) history.delete(oldest);
    }
    pageHistories.delete(historyKey);
    pageHistories.set(historyKey, history);
    if (pageHistories.size > 64) {
      const key = pageHistories.keys().next().value;
      if (key) pageHistories.delete(key);
    }
  }
  function change(next: URLSearchParams) {
    next.delete(`${prefix}previous`);
    setParams(next, { preventScrollReset: true, state: location.state });
  }
  return {
    cursor,
    limit,
    previous,
    next(nextCursor: string) {
      remember(nextCursor);
      const p = new URLSearchParams(params);
      p.set(`${prefix}cursor`, nextCursor);
      change(p);
    },
    back() {
      const p = new URLSearchParams(params);
      if (predecessor) p.set(`${prefix}cursor`, predecessor);
      else p.delete(`${prefix}cursor`);
      change(p);
    },
    size(value: number) {
      const p = new URLSearchParams(params);
      p.set(`${prefix}limit`, String(value));
      p.delete(`${prefix}cursor`);
      change(p);
    },
    reset() {
      pageHistories.delete(historyKey);
      const p = new URLSearchParams(params);
      p.delete(`${prefix}cursor`);
      change(p);
    },
  };
}
