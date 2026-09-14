"""Bounded best-effort structured log snapshots (MIT)."""

import json
import logging
import threading
from itertools import islice
from datetime import datetime, timezone

from . import _invocation

_sink = None
_lock = threading.Lock()
_MAX_TEXT = 4096
_MAX_ATTRIBUTES = 32


def _bounded(value, depth=0, budget=None):
    # Limit the whole attribute tree, not just each branch. A branching object
    # must not multiply per-value allowances into an unbounded encoded record.
    if budget is None:
        budget = [64, _MAX_TEXT]
    budget[0] -= 1
    if budget[0] < 0:
        return "[truncated]"
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        text = value[:max(0, budget[1])]
        budget[1] -= len(text)
        return text
    if depth >= 4:
        return "[truncated]"
    if isinstance(value, dict):
        result = {}
        for key, item in islice(value.items(), _MAX_ATTRIBUTES):
            if budget[0] <= 0:
                break
            if isinstance(key, str):
                bounded_key = key[:min(256, max(0, budget[1]))]
                budget[1] -= len(bounded_key)
                result[bounded_key] = _bounded(item, depth + 1, budget)
        return result
    if isinstance(value, (list, tuple)):
        result = []
        for item in value[:_MAX_ATTRIBUTES]:
            if budget[0] <= 0:
                break
            result.append(_bounded(item, depth + 1, budget))
        return result
    return "[unsupported]"


def _snapshot(record):
    from .otel import _active_ids, _api

    frame = {"v": getattr(_sink, "protocol_version", 2), "type": "log",
             "time": datetime.fromtimestamp(record.created, timezone.utc).isoformat(),
             "severity": record.levelname[:32], "logger": record.name[:256],
             "message": record.getMessage()[:_MAX_TEXT],
             "attributes": _bounded(getattr(record, "attributes", {}))}
    invocation = _invocation.get()
    if invocation is not None:
        frame["invocation"] = {key: getattr(invocation, key) for key in (
            "event_id", "attempt_id", "source", "tenant_id", "namespace",
            "run_id", "task_id", "attempt_no") if getattr(invocation, key) is not None}
        if frame["v"] >= 3:
            frame["invocation"].update({key: getattr(invocation, key) for key in
                                        ("workflow_id", "activation_id")
                                        if getattr(invocation, key) is not None})
    active = _active_ids()
    if active is None and _api is None and invocation is not None:
        carrier = invocation.processing_context
        if carrier is not None:
            parts = carrier.traceparent.split("-")
            active = {"trace_id": parts[1], "span_id": parts[2]}
    if active is not None:
        frame.update(active)
    return frame


class _Handler(logging.Handler):
    def emit(self, record):
        sink = _sink
        if sink is None:
            return
        try:
            # Encoding happens synchronously: mutable attributes, context changes,
            # and later delivery cannot relabel a record as another invocation.
            frame = _snapshot(record)
            limit = sink.log_limit
            mandatory = dict(frame, message="", attributes={})
            if len(json.dumps(mandatory, ensure_ascii=False, allow_nan=False,
                              separators=(",", ":")).encode("utf-8")) + 1 > limit:
                sink.drop_log()
                return
            # Shrink optional content, never identity. Encoding includes newline.
            for _ in range(15):
                encoded = json.dumps(frame, ensure_ascii=False, allow_nan=False,
                                     separators=(",", ":")).encode("utf-8") + b"\n"
                if len(encoded) <= limit:
                    sink.offer_log(encoded)
                    return
                frame["attributes"] = {}
                frame["message"] = frame["message"][:len(frame["message"]) // 2]
            sink.drop_log()
        except Exception:
            # Malformed messages/attributes are telemetry loss, never task failure.
            sink.drop_log()


def get_logger(name):
    logger = logging.getLogger(name)
    with _lock:
        if not any(isinstance(handler, _Handler) for handler in logger.handlers):
            logger.addHandler(_Handler())
        if logger.level == logging.NOTSET:
            logger.setLevel(logging.INFO)
    # Keep propagation and existing application/root handlers untouched.
    return logger
