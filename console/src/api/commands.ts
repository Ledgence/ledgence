import { useRef, useState } from "react";
import { useMutation } from "@tanstack/react-query";
import { useInstance } from "../app/instance";
import { apiPath, request } from "./client";
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
}
export function useCommand<T>(
  resource: string,
  decode: Decoder<T>,
  onSuccess: (value: T) => void,
) {
  const config = useInstance();
  const [command, setCommand] = useState<FrozenCommand | null>(null);
  const snapshot = useRef<Operation<T> | null>(null);
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
        )
      ).value,
    onSuccess: (value, operation) => operation.onSuccess(value),
    onSettled: () => {
      inFlight.current = false;
    },
    retry: false,
  });
  return {
    command,
    mutation,
    send(next: FrozenCommand) {
      // A synchronous guard covers two events before React renders disabled controls.
      if (inFlight.current || snapshot.current) return;
      const operation = Object.freeze({
        command: next,
        path: apiPath(resource),
        instanceId: config.instance_id,
        maximumBytes: config.limits.submission_max_bytes,
        decode,
        onSuccess,
      });
      snapshot.current = operation;
      inFlight.current = true;
      setCommand(next);
      mutation.mutate(operation);
    },
    retry() {
      const operation = snapshot.current;
      if (operation && !inFlight.current) {
        inFlight.current = true;
        mutation.mutate(operation);
      }
    },
    reset() {
      if (!inFlight.current) {
        snapshot.current = null;
        setCommand(null);
        mutation.reset();
      }
    },
  };
}
