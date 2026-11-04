// SPDX-License-Identifier: MIT
import { useQuery } from "@tanstack/react-query";
import { apiPath, request } from "../../api/client";
import { useInstance } from "../../app/instance";
import { retryableRead } from "../../api/errors";
import type { Decoder } from "../../api/schema";

export function useQueueRead<T>(
  route: "list" | "inspect" | "holds",
  input: object,
  decode: Decoder<T>,
  enabled: boolean,
) {
  const config = useInstance();
  const path = apiPath("queue-policies/" + route);
  const body = JSON.stringify(input);
  return useQuery({
    queryKey: [
      location.origin,
      config.contract_version,
      config.instance_id,
      path,
      body,
    ],
    queryFn: async ({ signal }) =>
      (
        await request(
          path,
          decode,
          signal,
          config.instance_id,
          { body, maximumBytes: 16384 },
          config.limits.metadata_max_bytes,
        )
      ).value,
    enabled: enabled && config.capabilities.queue_policies,
    refetchInterval: ({ state }) =>
      // Each hold disclosure is audited. Only a person intentionally refreshes it.
      route === "holds" || (state.error && !retryableRead(state.error))
        ? false
        : config.polling.lists_ms,
    ...(route === "holds"
      ? {
          gcTime: 0,
          retry: false,
          refetchOnWindowFocus: false,
          refetchOnReconnect: false,
        }
      : {}),
  });
}
