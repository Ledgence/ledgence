"""Small immutable artifact store. Application idempotency, not exactly-once I/O (MIT)."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import tempfile
from typing import Any


def canonical_json(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":"),
                       ensure_ascii=False, allow_nan=False) + "\n").encode("utf-8")


def store_path(store: str, relative: str) -> Path:
    """Resolve a relative artifact path without escaping the operator's store."""
    if not isinstance(relative, str) or not relative or Path(relative).is_absolute():
        raise ValueError("artifact path must be relative")
    base = Path(store).expanduser().resolve()
    path = (base / relative).resolve()
    if not path.is_relative_to(base) or path == base or ".." in Path(relative).parts:
        raise ValueError("artifact path escapes store")
    return path


def write_named(store: str, relative: str, data: bytes) -> Path:
    """Create once, or reconcile an identical prior write after a lost response."""
    path = store_path(store, relative)
    path.parent.mkdir(parents=True, exist_ok=True)
    # Resolve again after directory creation, including any existing symlinks.
    path = store_path(store, relative)
    descriptor, temporary = tempfile.mkstemp(prefix=".pending-", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as output:
            output.write(data)
            output.flush()
            os.fsync(output.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError:
            if path.read_bytes() != data:
                raise ValueError(f"immutable artifact differs: {relative}") from None
    finally:
        Path(temporary).unlink(missing_ok=True)
    return path


def write_bytes(store: str, data: bytes, suffix: str = ".bin") -> dict:
    if not isinstance(data, bytes) or not re.fullmatch(r"\.[a-z0-9]{1,8}", suffix):
        raise ValueError("invalid artifact bytes or suffix")
    digest = hashlib.sha256(data).hexdigest()
    relative = f"artifacts/{digest}{suffix}"
    write_named(store, relative, data)
    return {"path": relative, "sha256": digest, "bytes": len(data)}


def read_bytes(store: str, reference: dict) -> bytes:
    if (not isinstance(reference, dict)
            or not isinstance(reference.get("sha256"), str)
            or not re.fullmatch(r"[a-f0-9]{64}", reference["sha256"])
            or type(reference.get("bytes")) is not int or reference["bytes"] < 0):
        raise ValueError("invalid artifact reference")
    value = store_path(store, reference.get("path")).read_bytes()
    if len(value) != reference["bytes"] or hashlib.sha256(value).hexdigest() != reference["sha256"]:
        raise ValueError("artifact digest or length mismatch")
    return value


def resolve_reference(store: str, reference: dict) -> Path:
    read_bytes(store, reference)
    return store_path(store, reference["path"])


def write_json(store: str, value: Any) -> dict:
    return write_bytes(store, canonical_json(value), ".json")


def read_json(store: str, reference: dict) -> Any:
    return json.loads(read_bytes(store, reference))
