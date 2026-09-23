"""An ordinary synchronous Python program invoked by Ledgence (MIT)."""

import os
from ledgence.worker import current_invocation, get_logger

log = get_logger(__name__)


def handle(event):
    invocation = current_invocation()
    log.info("Processing event", extra={"attributes": {"event_type": event["type"]}})
    return {
        "message": "Hello from Ledgence",
        "input": event["data"],
        "event_type": event["type"],
        "task_id": invocation.task_id,
        "pid": os.getpid(),
    }
