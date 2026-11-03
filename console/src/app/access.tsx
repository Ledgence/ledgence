// SPDX-License-Identifier: MIT
import { announce, channelName, useAccess } from "./access-session";
import {
  Fragment,
  useLayoutEffect,
  useEffect,
  useRef,
  useState,
  type ReactNode,
  type FormEvent,
} from "react";
import { accessController } from "./access-state";
import { readAccess, login } from "../api/access";
import { ApiError } from "../api/errors";
import { AppShell } from "../components/app-shell";
import { LoadingState } from "../components/async-state";
import { Button } from "../components/ui/button";

export function AccessBoundary({ children }: { children: ReactNode }) {
  const state = useAccess();
  const [credential, setCredential] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const pending = useRef<AbortController | null>(null);
  const position = useRef<{ view: number; x: number; y: number } | null>(null);
  useEffect(() => {
    let alive = true;
    async function refresh(discard = false) {
      pending.current?.abort();
      const controller = new AbortController();
      pending.current = controller;
      const before = accessController.getSnapshot();
      if (before.phase === "ready")
        position.current = { view: before.view, x: scrollX, y: scrollY };
      const epoch = accessController.begin("Checking your access", discard);
      if (document.hidden) return;
      try {
        const result = await readAccess(controller.signal);
        if (alive)
          accessController.accept(result.access, epoch, result.instanceId);
      } catch (error) {
        if (!alive || controller.signal.aborted) return;
        accessController.lock(
          error instanceof ApiError && error.status === 401
            ? "Sign in with your instance credential."
            : "Access could not be verified. Sign in or check the connection.",
          epoch,
        );
      }
    }
    const visibility = () => {
      void refresh();
    };
    const channel =
      typeof BroadcastChannel !== "undefined"
        ? new BroadcastChannel(channelName)
        : null;
    if (channel)
      channel.onmessage = (event) => {
        if (event.data?.changed === true)
          void refresh(event.data.discard === true);
      };
    document.addEventListener("visibilitychange", visibility);
    window.addEventListener("focus", visibility);
    void refresh();
    return () => {
      alive = false;
      pending.current?.abort();
      channel?.close();
      document.removeEventListener("visibilitychange", visibility);
      window.removeEventListener("focus", visibility);
    };
  }, []);
  useEffect(() => {
    if (state.phase !== "ready" || state.access?.mode !== "credentials") return;
    const expires = state.access.expires_at_ms;
    let timer: ReturnType<typeof setTimeout>;
    function schedule() {
      const remaining = expires - Date.now();
      if (remaining <= 0) {
        accessController.lock(
          "Your credential expired. Sign in again.",
          state.epoch,
        );
        return;
      }
      timer = setTimeout(schedule, Math.min(2147483647, remaining));
    }
    schedule();
    return () => clearTimeout(timer);
  }, [state]);
  useLayoutEffect(() => {
    if (state.phase !== "ready" || !position.current) return;
    const saved = position.current;
    position.current = null;
    if (saved.view === state.view) window.scrollTo(saved.x, saved.y);
  }, [state.phase, state.view]);
  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (submitting || !credential) return;
    const secret = credential;
    setCredential("");
    setSubmitting(true);
    pending.current?.abort();
    const controller = new AbortController();
    pending.current = controller;
    const epoch = accessController.begin("Signing in");
    try {
      const result = await login(secret, controller.signal);
      if (accessController.accept(result.access, epoch, result.instanceId))
        announce(false);
    } catch {
      if (!controller.signal.aborted)
        accessController.lock(
          "Sign-in failed. Check your credential and instance connection.",
          epoch,
        );
    } finally {
      setSubmitting(false);
    }
  }
  return (
    <>
      {state.access && (
        <Fragment key={state.view}>
          <div hidden={state.phase !== "ready"} inert={state.phase !== "ready"}>
            {children}
          </div>
        </Fragment>
      )}
      {state.phase !== "ready" || !state.access ? (
        <AppShell instanceName="Ledgence">
          <section className="card access-card" aria-label="Instance access">
            <h1>Access your instance</h1>
            {!state.enabled || state.phase === "checking" ? (
              <LoadingState label={state.message || "Checking your access"} />
            ) : (
              <>
                <p role="status">{state.message}</p>
                <form
                  onSubmit={(event) => {
                    void submit(event);
                  }}
                >
                  <label>
                    Instance credential
                    <input
                      type="password"
                      value={credential}
                      onChange={(event) => setCredential(event.target.value)}
                      maxLength={4096}
                      autoComplete="off"
                      spellCheck={false}
                      required
                    />
                  </label>
                  <p className="muted">
                    Use the individual credential supplied by your instance
                    administrator. Sign out clears this browser's session.
                  </p>
                  <Button type="submit" disabled={submitting || !credential}>
                    Sign in
                  </Button>
                </form>
                <Button
                  variant="outline"
                  onClick={() => {
                    window.dispatchEvent(new Event("focus"));
                  }}
                >
                  Check access again
                </Button>
              </>
            )}
          </section>
        </AppShell>
      ) : null}
    </>
  );
}
