// SPDX-License-Identifier: MIT
import { useEffect, useRef } from "react";
import { accessController } from "../../app/access-state";
import { useCommand, freezeCommand } from "../../api/commands";
import { parseUserJson } from "../../api/json";
import { ContractError } from "../../api/codecs";
export function useQueueCommand<T extends { request_id: string }, R>(
  resource: "put" | "release",
  key: string,
  decode: (raw: unknown) => T,
  receipt: (raw: unknown, expected: T) => R,
  onSuccess: (value: R) => void,
) {
  const mounted = useRef(true);
  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
    };
  }, []);
  const saved = accessController.recover(key);
  const expected = useRef<T | null>(
    saved ? decode(parseUserJson(saved.body, 16384)) : null,
  );
  const command = useCommand(
    "queue-policies/" + resource,
    (raw) => {
      if (!expected.current)
        throw new ContractError("Missing the original queue request.");
      return receipt(raw, expected.current);
    },
    (value) => {
      if (mounted.current) onSuccess(value);
    },
    key,
  );
  return {
    ...command,
    reset() {
      if (!command.mutation.isPending) {
        expected.current = null;
        command.reset();
      }
    },
    send(value: T) {
      // Keep the decoder bound to the same command even on double submit before rerender.
      if (expected.current !== null) return;
      expected.current = value;
      command.send(freezeCommand(value, value.request_id));
    },
  };
}

export const policyKey = (instance: string, queue: string) =>
  JSON.stringify(["queue-policy", instance, queue]);
