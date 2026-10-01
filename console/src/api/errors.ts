export class ApiError extends Error {
  readonly status: number | null;
  readonly requestId: string | null;
  readonly retryAfterMs: number | null;
  constructor(
    message: string,
    status: number | null = null,
    requestId: string | null = null,
    retryAfterMs: number | null = null,
  ) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.requestId = requestId;
    this.retryAfterMs = retryAfterMs;
  }
}
export function retryableRead(error: unknown): boolean {
  return (
    error instanceof ApiError &&
    (error.status === null || [408, 429, 502, 503, 504].includes(error.status))
  );
}
