import json
from aiohttp import web

SCOPE = {"tenant_id": "tenant", "namespace": "tests"}


def status(state="queued", task_id="task", *, count=None):
    if count is None:
        count = 0 if state in {"queued", "cancelled"} else 1
    return {"scope": dict(SCOPE), "task_id": task_id, "run_id": "run", "queue": "queue",
            "correlation_key": None, "state": state, "attempt_count": count,
            "current_attempt_id": "attempt" if state == "active" else None,
            "latest_attempt_id": "attempt" if count else None,
            "submitted_at": 1, "available_at": 1,
            "terminal_at": 3 if state in {"succeeded", "failed", "cancelled"} else None,
            "cancel_requested_at": 2 if state == "cancelled" else None}


def result(state="succeeded", output=None, failure=None, *, quiescence="confirmed"):
    task = status(state)
    if state in {"queued", "active"}:
        outcome = None
    elif state == "cancelled":
        outcome = {"kind": "cancelled"}
    else:
        outcome = {"kind": state, "attempt_id": "attempt", "quiescence": quiescence,
                   "execution_may_have_started": True}
        outcome["output" if state == "succeeded" else "failure"] = (
            output if state == "succeeded" else failure)
    return {"task": task, "outcome": outcome}


def submitted(command):
    value = json.loads(json.dumps(command["input"]))
    value.setdefault("retry_policy", {"max_attempts": 3, "retry_delay_ms": 5000})
    value.setdefault("attempt_timeout_ms", 300000)
    return {"task_id": "task", "run_id": "run", "idempotency_key": command["idempotency_key"],
            "input": value, "descriptor": {"program": dict(value["program"]),
            "digest": "sha256:" + "a" * 64, "size": 1},
            "origin_trace": command.get("origin_trace"), "state": "queued", "submitted_at": 1,
            "available_at": 1, "terminal_at": None, "current_attempt_id": None,
            "attempt_count": 0, "cancel_requested_at": None}


def response(value, *, code=200):
    return web.Response(body=json.dumps(value, ensure_ascii=False, allow_nan=False,
                                        separators=(",", ":")).encode(),
                        status=code, headers={"Content-Type": "application/json",
                                             "Request-Id": "request", "Cache-Control": "no-store"})
