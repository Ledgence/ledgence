"""An ordinary synchronous Python program invoked by Ledgence (MIT)."""

import os
from ledgence_worker import current_invocation


def handle(event):
    invocation = current_invocation()
    print("Processing event", invocation.event_id, "attempt", invocation.attempt_id)
    return {
        "message": "Hello from Ledgence",
        "input": event["data"],
        "event_type": event["type"],
        "pid": os.getpid(),
    }
