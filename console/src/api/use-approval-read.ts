// SPDX-License-Identifier: MIT
import { useQuery } from "@tanstack/react-query";
import { useInstance } from "../app/instance";
import { apiPath, request } from "./client";
import { retryableRead } from "./errors";
import type { Decoder } from "./schema";

// Approval reads use POST so the cache identity includes the request body.
export function useApprovalRead<T>(
  operation: "inbox" | "inspect" | "list",
  input: object,
  decode: Decoder<T>,
  interval: (value: T | undefined) => number | false,
) {
  const config = useInstance();
  const path = apiPath(`approvals/${operation}`);
  const body = JSON.stringify(input);
  return useQuery({
    enabled: config.capabilities.workflows,
    queryKey: [
      globalThis.location.origin,
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
          { body, maximumBytes: config.limits.submission_max_bytes },
          1024 * 1024,
        )
      ).value,
    refetchInterval: ({ state }) =>
      state.error && !retryableRead(state.error) ? false : interval(state.data),
  });
}
