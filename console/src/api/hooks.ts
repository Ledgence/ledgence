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
// Previous-page cursors are bounded in memory, not multiplied into URLs.
const pageHistories = new Map<string, string[]>();
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
  for (const key of [...filters.keys()])
    if (key.endsWith("cursor") || key.endsWith("previous")) filters.delete(key);
  filters.sort();
  const historyKey = [
    globalThis.location.origin,
    config.contract_version,
    config.instance_id,
    location.pathname,
    prefix,
    filters.toString(),
  ].join("|");
  const previous = pageHistories.get(historyKey) ?? [];
  function remember(history: string[]) {
    pageHistories.set(historyKey, history.slice(-32));
    if (pageHistories.size > 64) {
      const key = pageHistories.keys().next().value;
      if (key) pageHistories.delete(key);
    }
  }
  function change(next: URLSearchParams) {
    next.delete(`${prefix}previous`);
    setParams(next, { preventScrollReset: true });
  }
  return {
    cursor,
    limit,
    previous,
    next(nextCursor: string) {
      remember([...previous, cursor ?? ""]);
      const p = new URLSearchParams(params);
      p.set(`${prefix}cursor`, nextCursor);
      change(p);
    },
    back() {
      const p = new URLSearchParams(params);
      const last = previous.at(-1);
      remember(previous.slice(0, -1));
      if (last) p.set(`${prefix}cursor`, last);
      else p.delete(`${prefix}cursor`);
      change(p);
    },
    size(value: number) {
      remember([]);
      const p = new URLSearchParams(params);
      p.set(`${prefix}limit`, String(value));
      p.delete(`${prefix}cursor`);
      change(p);
    },
    reset() {
      remember([]);
      const p = new URLSearchParams(params);
      p.delete(`${prefix}cursor`);
      change(p);
    },
  };
}
