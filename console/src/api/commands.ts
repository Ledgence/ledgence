import {
  accessController,
  AccessChanged,
  type AccessLease,
} from "../app/access-state";
import { useRef, useState } from "react";
import { useMutation } from "@tanstack/react-query";
import { useInstance } from "../app/instance";
import { apiPath, request } from "./client";
import { ApiError } from "./errors";
import { stringifyUserJson } from "./json";
import type { Decoder } from "./schema";
export interface FrozenCommand {
  readonly body: string;
  readonly identity: string;
}
export function freezeCommand(value: unknown, identity: string): FrozenCommand {
  return Object.freeze({ body: stringifyUserJson(value), identity });
}
interface Operation<T> {
  readonly command: FrozenCommand;
  readonly path: string;
  readonly instanceId: string;
  readonly maximumBytes: number;
  readonly decode: Decoder<T>;
  readonly onSuccess: (value: T) => void;
  readonly access: AccessLease | undefined;
  readonly view: number;
  readonly generation?: number | undefined;
  readonly priorUnconfirmed?: boolean;
}
export function useCommand<T>(
  resource: string,
  decode: Decoder<T>,
  onSuccess: (value: T) => void,
  recoveryKey?: string,
) {
  const config = useInstance();
  const [command, setCommand] = useState<FrozenCommand | null>(() =>
    recoveryKey ? (accessController.recover(recoveryKey) ?? null) : null,
  );
  const snapshot = useRef<Operation<T> | null>(
    command
      ? {
          command,
          path: apiPath(resource),
          instanceId: config.instance_id,
          maximumBytes: config.limits.submission_max_bytes,
          decode,
          onSuccess,
          access: undefined,
          view: accessController.getSnapshot().view,
        }
      : null,
  );
  const inFlight = useRef(false);
  const mutation = useMutation({
    mutationFn: async (operation: Operation<T>) =>
      (
        await request(
          operation.path,
          operation.decode,
          new AbortController().signal,
          operation.instanceId,
          {
            body: operation.command.body,
            maximumBytes: operation.maximumBytes,
          },
          10 * 1024 * 1024,
          operation.access,
        )
      ).value,
    onSuccess: (value, operation) => {
      if (operation.access && !operation.access.owner.current(operation.access))
        return;
      if (recoveryKey) accessController.forget(recoveryKey);
      operation.onSuccess(value);
    },
    onError: (error, operation) => {
      if (!recoveryKey) return;
      const denied =
        error instanceof ApiError &&
        !(error instanceof AccessChanged) &&
        (error.status === 401 || error.status === 403);
      accessController.settle(
        recoveryKey,
        operation.generation,
        denied && !operation.priorUnconfirmed ? "denied" : "unconfirmed",
      );
    },
    onSettled: () => {
      inFlight.current = false;
    },
    retry: false,
  });
  function start(operation: Operation<T>) {
    const previous = recoveryKey
      ? accessController.recover(recoveryKey)
      : undefined;
    return Object.freeze({
      ...operation,
      priorUnconfirmed: previous?.outcome === "unconfirmed",
      generation: recoveryKey
        ? accessController.remember(recoveryKey, {
            ...operation.command,
            actor: operation.access?.actor ?? null,
          })
        : undefined,
    });
  }
  return {
    command,
    mutation,
    send(next: FrozenCommand) {
      // A synchronous guard covers two events before React renders disabled controls.
      if (inFlight.current || snapshot.current) return;
      const operation = start({
        command: next,
        path: apiPath(resource),
        instanceId: config.instance_id,
        maximumBytes: config.limits.submission_max_bytes,
        decode,
        onSuccess,
        access: accessController.lease(),
        view: accessController.getSnapshot().view,
      });
      snapshot.current = operation;
      inFlight.current = true;
      setCommand(next);
      mutation.mutate(operation);
    },
    retry() {
      const operation = snapshot.current;
      if (operation && !inFlight.current) {
        if (operation.view !== accessController.getSnapshot().view)
          throw new AccessChanged();
        const access = accessController.lease();
        if (operation.access && operation.access.actor !== access?.actor)
          throw new AccessChanged();
        const next = start({ ...operation, access });
        snapshot.current = next;
        inFlight.current = true;
        mutation.mutate(next);
      }
    },
    reset() {
      if (!inFlight.current) {
        if (recoveryKey) accessController.forget(recoveryKey);
        snapshot.current = null;
        setCommand(null);
        mutation.reset();
      }
    },
  };
}
