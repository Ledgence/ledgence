import { QueryClient } from "@tanstack/react-query";
import { ApiError, retryableRead } from "../api/errors";
export function createQueryClient(): QueryClient {
  return new QueryClient({
    defaultOptions: {
      queries: {
        staleTime: 0,
        gcTime: 300000,
        refetchOnWindowFocus: (query) =>
          !query.state.error || retryableRead(query.state.error),
        refetchOnReconnect: (query) =>
          !query.state.error || retryableRead(query.state.error),
        refetchIntervalInBackground: false,
        retry: (count, error) => count < 2 && retryableRead(error),
        retryDelay: (attempt, error) =>
          Math.max(
            error instanceof ApiError ? (error.retryAfterMs ?? 0) : 0,
            Math.min(30000, 1000 * 2 ** attempt) * (0.8 + Math.random() * 0.4),
          ),
      },
      mutations: { retry: false, networkMode: "always" },
    },
  });
}
