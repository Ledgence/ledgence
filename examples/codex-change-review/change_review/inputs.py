"""Application submission contract shared by the workflow and companion client."""
import re

from .config import DEFAULT_MODEL


def submission(data):
    if type(data) is not dict or not {"change_id"} <= data.keys() <= {"change_id", "model", "publication"}:
        raise ValueError("expected change_id, optional model and optional publication")
    for field, value in (("change_id", data["change_id"]), ("model", data.get("model", DEFAULT_MODEL))):
        if type(value) is not str or not re.fullmatch(r"[A-Za-z0-9_.-]{1,80}", value):
            raise ValueError(f"{field} must be a simple identifier of 1 to 80 characters")
    if "publication" in data:
        from .publishing import validate_publication
        validate_publication(data["publication"])
    return {"change_id": data["change_id"], "model": data.get("model", DEFAULT_MODEL),
            **({"publication": dict(data["publication"])} if "publication" in data else {})}
