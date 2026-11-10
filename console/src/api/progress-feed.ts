// SPDX-License-Identifier: MIT
import { ApiError, retryableRead } from "./errors";
import {
  appendProgress,
  emptyProgressWindow,
  type ProgressPage,
  type ProgressRequest,
  type ProgressTarget,
  type ProgressWindow,
} from "./progress";

export type ProgressPhase =
  | "loading"
  | "following"
  | "paused"
  | "complete"
  | "limited"
  | "reconnecting"
  | "error";
export type ProgressSnapshot = ProgressWindow & {
  phase: ProgressPhase;
  error: unknown;
};
type ReadProgress = (
  query: ProgressRequest,
  signal: AbortSignal,
) => Promise<ProgressPage>;
function sleep(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    const finish = () => {
      signal.removeEventListener("abort", abort);
      resolve();
    };
    const timer = setTimeout(finish, ms);
    const abort = () => {
      clearTimeout(timer);
      signal.removeEventListener("abort", abort);
      reject(signal.reason);
    };
    signal.addEventListener("abort", abort, { once: true });
    if (signal.aborted) abort();
  });
}

/** One request at a time, with cancellation owning every timer and fetch. */
export class ProgressFeed {
  private snapshot: ProgressSnapshot = {
    ...emptyProgressWindow(),
    phase: "loading",
    error: null,
  };
  private listeners = new Set<() => void>();
  private operation: AbortController | null = null;
  constructor(
    private readonly target: ProgressTarget,
    private readonly read: ReadProgress,
  ) {}
  getSnapshot = () => this.snapshot;
  subscribe = (listener: () => void) => {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  };
  private update(next: Partial<ProgressSnapshot>) {
    this.snapshot = { ...this.snapshot, ...next };
    this.listeners.forEach((listener) => listener());
  }
  start(force = false) {
    if (this.operation) return;
    if (
      !force &&
      ["error", "complete", "limited"].includes(this.snapshot.phase)
    )
      return;
    const controller = new AbortController();
    this.operation = controller;
    this.update({
      phase: this.snapshot.page ? "following" : "loading",
      error: null,
    });
    void this.run(controller)
      .catch((error: unknown) => {
        if (this.operation === controller && !controller.signal.aborted)
          this.update({ phase: "error", error });
      })
      .finally(() => {
        if (this.operation === controller) this.operation = null;
      });
  }
  stop() {
    this.operation?.abort();
    this.operation = null;
    if (!["error", "complete", "limited"].includes(this.snapshot.phase))
      this.update({ phase: "paused" });
  }
  private async run(controller: AbortController) {
    const signal = controller.signal;
    let failures = 0;
    let catchupPages = 0;
    while (!signal.aborted) {
      const previous = this.snapshot.page;
      const query: ProgressRequest = {
        target: this.target,
        cursor: previous?.next_cursor ?? null,
        limit: 100,
        wait_ms: previous && !previous.has_more ? 5000 : 0,
      };
      const started = performance.now();
      let page: ProgressPage;
      try {
        page = await this.read(query, signal);
      } catch (error) {
        if (signal.aborted || this.operation !== controller) return;
        if (retryableRead(error) && failures < 2) {
          const retryAfter =
            error instanceof ApiError ? (error.retryAfterMs ?? 0) : 0;
          // A longer server backoff is explicit instead of retaining an invisible timer.
          if (retryAfter <= 30000) {
            this.update({ phase: "reconnecting", error });
            await sleep(Math.max(retryAfter, 1000 * 2 ** failures++), signal);
            continue;
          }
        }
        throw error;
      }
      if (signal.aborted || this.operation !== controller) return;
      const window = appendProgress(this.snapshot, page);
      failures = 0;
      if (page.terminal && !page.has_more) {
        this.update({ ...window, phase: "complete", error: null });
        return;
      }
      if (page.has_more && ++catchupPages >= 5) {
        this.update({ ...window, phase: "limited", error: null });
        return;
      }
      if (!page.has_more) catchupPages = 0;
      this.update({ ...window, phase: "following", error: null });
      // Some adapters return immediately; an empty reply must never create a spin loop.
      if (!page.has_more)
        await sleep(Math.max(0, 250 - (performance.now() - started)), signal);
    }
  }
}
