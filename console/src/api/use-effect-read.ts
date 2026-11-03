// SPDX-License-Identifier: MIT
import { useQuery } from "@tanstack/react-query";
import { useInstance } from "../app/instance";
import { apiPath, request } from "./client";
import { retryableRead } from "./errors";
import type { Decoder } from "./schema";

export function useEffectRead<T>(
  operation: "inbox" | "inspect",
  input: object,
  decode: Decoder<T>,
  enabled: boolean,
) {
  const config = useInstance();
  const path = apiPath(`workflows/approved-effects/${operation}`);
  const body = JSON.stringify(input);
  return useQuery({
    enabled: enabled && config.capabilities.workflows,
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
          { body, maximumBytes: 16384 },
          1024 * 1024,
        )
      ).value,
    refetchInterval: ({ state }) =>
      operation === "inbox" &&
      !("cursor" in input && input.cursor) &&
      !(state.error && !retryableRead(state.error))
        ? config.polling.lists_ms
        : false,
  });
}
