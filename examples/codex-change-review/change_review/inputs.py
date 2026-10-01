"""Application submission contract shared by the workflow and companion client."""
import re

from .config import DEFAULT_APPROVAL_TIMEOUT_MS, DEFAULT_MODEL, MAX_APPROVAL_TIMEOUT_MS


def submission(data):
    if type(data) is not dict or not {"change_id"} <= data.keys() <= {"change_id", "model", "publication", "approval_timeout_ms"}:
        raise ValueError("expected change_id and optional model, publication and approval_timeout_ms")
    for field, value in (("change_id", data["change_id"]), ("model", data.get("model", DEFAULT_MODEL))):
        if type(value) is not str or not re.fullmatch(r"[A-Za-z0-9_.-]{1,80}", value):
            raise ValueError(f"{field} must be a simple identifier of 1 to 80 characters")
    timeout = data.get("approval_timeout_ms", DEFAULT_APPROVAL_TIMEOUT_MS)
    if type(timeout) is not int or not 0 <= timeout <= MAX_APPROVAL_TIMEOUT_MS:
        raise ValueError("approval_timeout_ms must be an integer from 0 through 86400000")
    if "publication" in data:
        from .publishing import validate_publication
        validate_publication(data["publication"])
    return {"change_id": data["change_id"], "model": data.get("model", DEFAULT_MODEL),
            "approval_timeout_ms": timeout,
            **({"publication": dict(data["publication"])} if "publication" in data else {})}
