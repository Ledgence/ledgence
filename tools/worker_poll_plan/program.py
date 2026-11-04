"""Trusted local fixture program, published with its dependencies by the harness. MIT."""
import json
import os
from pathlib import Path
import time


def handle(event):
    data = event["data"]
    started = time.monotonic_ns()
    result = {"task": event["ldgtaskid"], "attempt": event["ldgattemptid"],
              "pid": os.getpid(), "queue": data["queue"], "started_ns": started}
    with open(data["marker"], "a", encoding="utf-8") as marker:
        marker.write(json.dumps(result) + "\n")
    if data["gate"] is not None:
        gate = Path(data["gate"])
        deadline = time.monotonic() + 90
        while not gate.exists():
            if time.monotonic() >= deadline:
                raise RuntimeError("fixture gate timed out")
            time.sleep(.02)
    time.sleep(data["duration_ms"] / 1000)
    return {**result, "finished_ns": time.monotonic_ns()}
