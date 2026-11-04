// SPDX-License-Identifier: MIT
import { ApiError } from "../../api/errors";
import { CommandFeedback } from "../../components/command-feedback";
import { Button } from "../../components/ui/button";

export function PendingRequest({
  identity,
  body,
  pending,
  error,
  retry,
  reset,
}: {
  identity: string;
  body: string;
  pending: boolean;
  error: unknown;
  retry: () => void;
  reset: () => void;
}) {
  return (
    <div className="stack">
      {pending ? (
        <p role="status">Sending request…</p>
      ) : (
        <>
          {error instanceof ApiError && error.status === 409 && (
            <p className="notice">
              The revision changed. Reload the latest record before preparing a
              new request.
            </p>
          )}
          {error ? (
            <CommandFeedback
              error={error}
              identity={identity}
              retry={retry}
              reset={reset}
            />
          ) : (
            <p className="notice">
              An earlier request may already have been accepted. Retry it to
              retrieve its receipt.
            </p>
          )}
          <details>
            <summary>Original Console request</summary>
            <p className="muted">
              Save the request before leaving this browser session if you need
              to reconcile it later.
            </p>
            <textarea
              aria-label="Original request body"
              readOnly
              value={body}
            />
          </details>
          <Button variant="outline" onClick={retry}>
            Retry exact request
          </Button>
        </>
      )}
    </div>
  );
}
