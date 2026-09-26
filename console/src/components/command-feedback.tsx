import { ApiError } from "../api/errors";
import { useState } from "react";
import { Button } from "./ui/button";
import { CopyText, QueryError } from "./resource-ui";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogTitle,
} from "./ui/dialog";
export function CommandFeedback({
  error,
  identity,
  retry,
  reset,
}: {
  error: unknown;
  identity: string;
  retry: () => void;
  reset: () => void;
}) {
  const [open, setOpen] = useState(false);
  const rejected =
    error instanceof ApiError &&
    error.status !== null &&
    error.status >= 400 &&
    error.status < 500 &&
    error.status !== 408 &&
    error.status !== 429;
  return (
    <>
      <QueryError error={error} retry={retry} />
      <div className="notice">
        <p>
          {rejected
            ? "The server rejected this request. Retrying preserves its exact content; separate the operation to edit a new command."
            : "The operation may have been accepted. Retrying sends the same bytes and identity. A new operation can create additional work."}
        </p>
        <p>
          Operation identity <CopyText value={identity} />
        </p>
        <Button variant="outline" onClick={() => setOpen(true)}>
          Separate this operation
        </Button>
      </div>
      <Dialog open={open} onOpenChange={setOpen}>
        <DialogContent>
          <DialogTitle>Create a separate operation?</DialogTitle>
          <DialogDescription>
            Keep the identity above for reconciliation. Continuing clears this
            draft and allows a new operation; it does not undo earlier work.
          </DialogDescription>
          <Button
            onClick={() => {
              reset();
              setOpen(false);
            }}
          >
            Continue with a new operation
          </Button>
        </DialogContent>
      </Dialog>
    </>
  );
}
