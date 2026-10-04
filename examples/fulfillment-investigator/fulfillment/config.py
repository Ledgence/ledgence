"""Application contracts for the fulfillment investigation example (MIT)."""
from pathlib import Path
import re

PROGRAM = "fulfillment-investigator"
VERSION = "1.0.0"
QUEUE = "fulfillment"
DEFAULT_MODEL = "gpt-6-luna"
SOURCES = ("orders", "warehouse", "carrier", "support")
TOPICS = ("carrier", "warehouse", "source_health", "support")
SOURCE_WAIT = "warehouse:corrected"
APPROVAL_KEY = "publish-report"


def submission(value):
    if type(value) is not dict or set(value) - {
        "store", "scenario", "queue", "mode", "model", "source_timeout_ms", "approval_timeout_ms"
    }:
        raise ValueError("expected a fulfillment submission object with documented fields")
    store = value.get("store")
    if (type(store) is not str or not 1 <= len(store) <= 2048
            or any(ord(c) < 32 for c in store) or not Path(store).is_absolute()):
        raise ValueError("store must be an absolute shared data directory")
    scenario = value.get("scenario", "data-gap")
    if scenario not in ("data-gap", "real-delay"):
        raise ValueError("scenario must be data-gap or real-delay")
    mode = value.get("mode", "fixture")
    if mode not in ("fixture", "codex"):
        raise ValueError("mode must be fixture or codex")
    queue, model = value.get("queue", QUEUE), value.get("model", DEFAULT_MODEL)
    for label, item in (("queue", queue), ("model", model)):
        if type(item) is not str or re.fullmatch(r"[A-Za-z0-9_.:/-]{1,128}", item) is None:
            raise ValueError(label + " must be a bounded identifier")
    result = dict(store=str(Path(store).resolve()), scenario=scenario, mode=mode, queue=queue, model=model)
    for name in ("source_timeout_ms", "approval_timeout_ms"):
        timeout = value.get(name, 3_600_000)
        if type(timeout) is not int or not 0 <= timeout <= 86_400_000:
            raise ValueError(name + " must be an integer from 0 through 86400000")
        result[name] = timeout
    return result
