// SPDX-License-Identifier: MIT
import { useMemo } from "react";
import {
  infiniteQueryOptions,
  useInfiniteQuery,
  useQueryClient,
  type InfiniteData,
  type QueryClient,
  type QueryState,
} from "@tanstack/react-query";
import { useInstance } from "../app/instance";
import type { ConsoleConfig } from "../api/contracts";
import { apiPath, request } from "../api/client";
import { ContractError } from "../api/codecs";
import { retryableRead } from "../api/errors";
import { executionPage, type Execution } from "../api/explorer";

type Parameters = Record<string, string | number | null | undefined>;
type ExecutionPage = ReturnType<typeof executionPage>;
type HistoryData = InfiniteData<ExecutionPage, string | null>;

// Kept separate from the hook so the cache and cursor policy can be exercised
// with the same TanStack observer used by React, without browser timing.
export function executionHistoryOptions(
  config: ConsoleConfig,
  parameters: Parameters,
  capabilityEnabled: boolean,
  client: QueryClient,
  origin: string,
) {
  const initialPageParam =
    parameters.cursor == null || parameters.cursor === ""
      ? null
      : String(parameters.cursor);
  const filters = Object.fromEntries(
    Object.entries(parameters)
      .filter(([key]) => key !== "cursor")
      .sort(([a], [b]) => a.localeCompare(b)),
  );
  const path = apiPath("executions", filters);
  const queryKey = [
    origin,
    config.contract_version,
    config.instance_id,
    "infinite",
    path,
    initialPageParam,
  ] as const;
  const automatic = (state: QueryState<HistoryData, Error>) =>
    capabilityEnabled &&
    (state.data?.pages.length ?? 0) <= 1 &&
    (initialPageParam === null || !state.data) &&
    state.fetchMeta?.fetchMore?.direction !== "forward" &&
    (!state.error || retryableRead(state.error));
  return infiniteQueryOptions<
    ExecutionPage,
    Error,
    HistoryData,
    typeof queryKey,
    string | null
  >({
    queryKey,
    initialPageParam,
    queryFn: async ({ pageParam, signal }): Promise<ExecutionPage> =>
      (
        await request(
          apiPath("executions", { ...filters, cursor: pageParam }),
          (value) => {
            const page = executionPage(value);
            const loaded = client.getQueryData<HistoryData>(queryKey);
            // Validate before caching: a bad continuation must retain the rows
            // already shown and surface a retryable-by-the-user page error.
            if (
              page.next_cursor !== null &&
              (page.next_cursor === pageParam ||
                loaded?.pageParams.includes(page.next_cursor))
            )
              throw new ContractError(
                "The server returned a non-advancing execution cursor.",
              );
            return page;
          },
          signal,
          config.instance_id,
          undefined,
          config.limits.metadata_max_bytes,
        )
      ).value,
    getNextPageParam: (page) => page.next_cursor,
    // A multi-page traversal is intentionally frozen: background invalidation
    // must not replace its page boundaries. Explicit fetchNextPage still works.
    enabled: (query) => automatic(query.state),
    refetchOnMount: false,
    refetchOnWindowFocus: (query) => automatic(query.state),
    refetchOnReconnect: (query) => automatic(query.state),
    refetchInterval: (query) =>
      automatic(query.state) ? config.polling.lists_ms : false,
    refetchIntervalInBackground: false,
    staleTime: 0,
  });
}

export function executionHistoryRows(data: HistoryData | undefined) {
  const rows: { item: Execution; observedAt: number }[] = [];
  const seen = new Set<string>();
  let observedAt: number | null = null;
  for (const page of data?.pages ?? []) {
    observedAt =
      observedAt === null
        ? page.observed_at
        : Math.min(observedAt, page.observed_at);
    for (const item of page.items) {
      const identity = `${item.kind}:${item.id}`;
      if (seen.has(identity)) continue;
      seen.add(identity);
      rows.push({ item, observedAt: page.observed_at });
    }
  }
  return { rows, observedAt };
}

export function useExecutionHistory(parameters: Parameters, enabled: boolean) {
  const config = useInstance();
  const client = useQueryClient();
  const query = useInfiniteQuery(
    executionHistoryOptions(
      config,
      parameters,
      enabled,
      client,
      location.origin,
    ),
  );
  const result = useMemo(() => executionHistoryRows(query.data), [query.data]);
  // Restart at the live first page, including when a legacy cursor anchored
  // this traversal. Resetting discards old boundaries before fetching again.
  const refresh = () =>
    client.resetQueries({
      queryKey: executionHistoryOptions(
        config,
        { ...parameters, cursor: null },
        enabled,
        client,
        location.origin,
      ).queryKey,
      exact: true,
    });
  return { query, refresh, ...result };
}
