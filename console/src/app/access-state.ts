// SPDX-License-Identifier: MIT
import { ApiError } from "../api/errors";

export type AccessCapability =
  | "metadata_read"
  | "sensitive_read"
  | "execution_submit"
  | "execution_control"
  | "program_publish"
  | "artifact_write"
  | "approval_decide"
  | "worker_execute"
  | "resource_control"
  | "effect_resolve";
export type Access =
  | { mode: "operator_trusted" }
  | {
      mode: "credentials";
      actor_key: string;
      issuer: string;
      subject: string;
      class: "operator" | "service";
      instance_id: string;
      scope: { tenant_id: string; namespace: string };
      capabilities: AccessCapability[];
      expires_at_ms: number;
      operator_audit?: boolean;
      native_effect_resolution?: boolean;
      provider_operation?: boolean;
    };
export type AccessState = {
  enabled: boolean;
  phase: "ready" | "checking" | "locked";
  epoch: number;
  view: number;
  access: Access | null;
  message: string;
};
export type AccessLease = {
  owner: AccessController;
  epoch: number;
  actor: string | null;
};
export type PendingCommand = {
  body: string;
  identity: string;
  actor: string | null;
  outcome: "unconfirmed" | "denied";
  generation: number;
};
export class AccessChanged extends ApiError {
  constructor() {
    super("Access changed. Verify your identity before continuing.", 403);
  }
}
export class AccessController {
  private state: AccessState = {
    enabled: false,
    phase: "ready",
    epoch: 0,
    view: 0,
    access: null,
    message: "",
  };
  private listeners = new Set<() => void>();
  private pending = new Map<string, PendingCommand>();
  private previousActor: string | null = null;
  private generation = 0;
  private viewIdentity = "";
  getSnapshot = () => this.state;
  subscribe = (listener: () => void) => {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  };
  private update(next: Partial<AccessState>) {
    this.state = { ...this.state, ...next };
    this.listeners.forEach((listener) => listener());
  }
  begin(message = "Checking access", discard = false) {
    if (discard) this.pending.clear();
    this.update({
      enabled: true,
      phase: "checking",
      epoch: this.state.epoch + 1,
      access: discard ? null : this.state.access,
      view: this.state.view + (discard ? 1 : 0),
      message,
    });
    return this.state.epoch;
  }
  accept(access: Access, epoch: number, instanceId = "") {
    if (epoch !== this.state.epoch) return false;
    if (access.mode === "credentials" && access.expires_at_ms <= Date.now()) {
      this.lock("Your credential expired. Sign in again.", epoch);
      return false;
    }
    const actor = access.mode === "credentials" ? access.actor_key : null;
    const identity = JSON.stringify(
      access.mode === "operator_trusted"
        ? [access.mode, instanceId]
        : [
            access.mode,
            instanceId,
            access.actor_key,
            access.issuer,
            access.subject,
            access.class,
            access.instance_id,
            access.scope.tenant_id,
            access.scope.namespace,
            [...access.capabilities].sort(),
            access.operator_audit === true,
            access.native_effect_resolution === true,
            access.provider_operation === true,
          ],
    );
    const changed = identity !== this.viewIdentity;
    if (changed) this.pending.clear();
    this.viewIdentity = identity;
    this.previousActor = actor;
    this.update({
      phase: "ready",
      access,
      message: "",
      view: this.state.view + (changed ? 1 : 0),
    });
    return true;
  }
  lock(message: string, epoch = this.state.epoch) {
    if (epoch !== this.state.epoch) return;
    this.update({ phase: "locked", epoch: epoch + 1, access: null, message });
  }
  async ready(signal: AbortSignal): Promise<AccessLease | undefined> {
    const view = this.state.view;
    if (this.state.phase === "checking")
      await new Promise<void>((resolve, reject) => {
        let unsubscribe = () => {};
        const finish = (error?: unknown) => {
          unsubscribe();
          signal.removeEventListener("abort", abort);
          if (error) reject(error);
          else resolve();
        };
        const abort = () =>
          finish(signal.reason ?? new DOMException("Aborted", "AbortError"));
        unsubscribe = this.subscribe(() => {
          if (this.state.view !== view || this.state.phase === "locked")
            finish(new AccessChanged());
          else if (this.state.phase === "ready") finish();
        });
        signal.addEventListener("abort", abort, { once: true });
        if (signal.aborted) abort();
      });
    if (signal.aborted)
      throw signal.reason ?? new DOMException("Aborted", "AbortError");
    if (this.state.view !== view) throw new AccessChanged();
    return this.lease();
  }
  lease(): AccessLease | undefined {
    if (!this.state.enabled) return undefined;
    if (this.state.phase !== "ready") throw new AccessChanged();
    if (
      this.state.access?.mode === "credentials" &&
      this.state.access.expires_at_ms <= Date.now()
    ) {
      this.lock("Your credential expired. Sign in again.");
      throw new AccessChanged();
    }
    return { owner: this, epoch: this.state.epoch, actor: this.previousActor };
  }
  current(lease: AccessLease): boolean {
    return (
      this.state.phase === "ready" &&
      this.state.epoch === lease.epoch &&
      this.previousActor === lease.actor &&
      (this.state.access?.mode !== "credentials" ||
        this.state.access.expires_at_ms > Date.now())
    );
  }
  remember(
    key: string,
    command: Omit<PendingCommand, "outcome" | "generation">,
  ) {
    if (!this.state.enabled) return undefined;
    const generation = ++this.generation;
    this.pending.set(key, { ...command, outcome: "unconfirmed", generation });
    return generation;
  }
  settle(
    key: string,
    generation: number | undefined,
    outcome: PendingCommand["outcome"],
  ) {
    const command = this.pending.get(key);
    if (command && command.generation === generation)
      this.pending.set(key, { ...command, outcome });
  }
  recover(key: string): PendingCommand | undefined {
    if (!this.state.enabled || this.state.phase !== "ready") return undefined;
    const command = this.pending.get(key);
    return command?.actor === this.previousActor ? command : undefined;
  }
  forget(key: string) {
    this.pending.delete(key);
  }
  permits(capability: AccessCapability): boolean {
    if (!this.state.enabled) return true;
    return (
      this.state.phase !== "locked" &&
      (this.state.access?.mode === "operator_trusted" ||
        !!this.state.access?.capabilities.includes(capability))
    );
  }
}
export const accessController = new AccessController();
