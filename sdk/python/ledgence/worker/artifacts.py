"""Bounded execution files carried by references, not workflow JSON payloads (MIT).

Use ``await artifacts.put_file(path, key="report")`` in a protocol-3 task or
workflow. Pass the returned JSON reference to another task in the same root and
call ``await artifacts.download(reference, destination)`` there. The host binds
all requests to the real invocation lease; this helper never accepts identity
or storage credentials. Destination files are never overwritten.
"""

from contextvars import ContextVar
from pathlib import Path
import os
import re


FEATURE = "execution_artifacts.v1"
MAX_BYTES = 64 * 1024 * 1024
_current = ContextVar("ledgence_execution_artifacts", default=None)


class _Binding:
    def __init__(self, rpc, enabled):
        self.rpc = rpc
        self.enabled = enabled
        self.active = True


def _bind(rpc, enabled):
    binding = _Binding(rpc, enabled)
    return binding, _current.set(binding)


def _unbind(binding, token):
    binding.active = False
    _current.reset(token)


def _binding():
    value = _current.get()
    if value is None or not value.active:
        raise RuntimeError("no Ledgence invocation is active")
    if not value.enabled or value.rpc is None:
        raise RuntimeError("execution artifacts are unavailable for this invocation")
    return value


def _text(value, field, limit):
    if (not isinstance(value, str) or not value or len(value.encode("utf-8")) > limit
            or any(ord(char) < 32 or ord(char) == 127 for char in value)):
        raise ValueError(field + " must be nonempty bounded text without control characters")
    return value


def _media_type(value):
    _text(value, "media type", 128)
    if not value.isascii() or "/" not in value:
        raise ValueError("media type must contain an ASCII type/subtype")
    return value


def _path(value):
    # The subprocess has its own working directory. Never let the host interpret
    # a relative path against the Rust worker's unrelated cwd.
    value = os.fspath(value)
    if not isinstance(value, str) or not value or "\x00" in value:
        raise ValueError("path must be nonempty text without NUL")
    return str(Path(value).absolute())


def _reference(value):
    if (type(value) is not dict
            or set(value) != {"schema", "id", "digest", "size", "media_type"}
            or value.get("schema") != "ledgence.artifact.v1"
            or type(value.get("size")) is not int or not 0 <= value["size"] <= MAX_BYTES
            or not isinstance(value.get("digest"), str)
            or re.fullmatch(r"sha256:[0-9a-f]{64}", value["digest"]) is None):
        raise ValueError("invalid execution artifact reference")
    _text(value["id"], "artifact id", 128)
    _media_type(value["media_type"])
    return dict(value)


async def put_file(path, *, key, media_type="application/octet-stream", filename=None):
    """Publish a regular file under this logical producer's stable key.

    Repeating the same key and metadata returns the accepted reference. Reusing
    a key for different content conflicts. Uncertain transfers need explicit
    reconciliation; this method does not silently replace or retry an upload.
    """
    binding = _binding()
    payload = {"path": _path(path), "key": _text(key, "key", 128),
               "media_type": _media_type(media_type), "filename": filename}
    if filename is not None:
        _text(filename, "filename", 255)
        if filename in {".", ".."} or "/" in filename or "\\" in filename:
            raise ValueError("filename must be a single path component")
    return _reference(_result(await binding.rpc("artifacts.put_file", payload)))


async def download(reference, destination):
    """Verify and atomically publish a file without overwriting a destination.

    References are valid within their retained execution root. Returns the
    absolute destination Path only after the Rust host verifies the bytes.
    """
    binding = _binding()
    path = _path(destination)
    result = await binding.rpc("artifacts.download", {"reference": _reference(reference), "path": path})
    if type(result) is not dict or result != {"path": path}:
        raise RuntimeError("artifact download acknowledgement changed destination")
    return Path(path)


class ArtifactUploadUncertain(RuntimeError):
    """Persist selection/operation and recover explicitly; never blindly resend bytes."""
    def __init__(self, message, selection, operation, status=None):
        super().__init__(message)
        self.selection = selection
        self.operation = operation
        self.status = status


class ArtifactRecoveryRequired(ArtifactUploadUncertain):
    """A known upload generation requires explicit reconciliation or replacement."""


def _selection(value):
    if (type(value) is not dict or set(value) != {"artifact_id", "upload_id", "revision"}
            or type(value["revision"]) is not int or not 1 <= value["revision"] < (1 << 63)):
        raise ValueError("invalid artifact selection")
    _text(value["artifact_id"], "artifact id", 128)
    _text(value["upload_id"], "upload id", 128)
    return dict(value)


def _status(value):
    required = {"selection", "selected", "stage", "prepare_expires_at", "ingress_expires_at",
                "dispatch_expires_at", "successor", "reference", "retry"}
    if (type(value) is not dict or set(value) != required or type(value["selected"]) is not bool
            or type(value["stage"]) is not str or value["stage"] not in {"prepared", "receiving", "writing", "uncertain", "ready", "expired_no_write", "retired"}
            or type(value["retry"]) is not str or value["retry"] not in {"upload", "wait", "reconcile", "replace", "complete", "unavailable"}):
        raise ValueError("invalid artifact upload status")
    _selection(value["selection"])
    for field in ("prepare_expires_at", "ingress_expires_at", "dispatch_expires_at"):
        if value[field] is not None and (type(value[field]) is not int or not 0 < value[field] <= 253402300799999):
            raise ValueError("invalid artifact upload timestamp")
    if value["prepare_expires_at"] is None:
        raise ValueError("missing artifact preparation expiry")
    selection = value["selection"]
    if value["successor"] is not None:
        successor = _selection(value["successor"])
        if (successor["artifact_id"] != selection["artifact_id"] or successor["revision"] != selection["revision"] + 1
                or successor["upload_id"] == selection["upload_id"] or value["selected"]):
            raise ValueError("invalid artifact successor")
    if value["reference"] is not None:
        reference = _reference(value["reference"])
        if reference["id"] != selection["artifact_id"] or not value["selected"] or value["stage"] != "ready":
            raise ValueError("invalid artifact acceptance")
    if ((value["stage"] == "receiving") != (value["ingress_expires_at"] is not None)
            or (value["selected"] and value["stage"] == "ready") != (value["reference"] is not None)):
        raise ValueError("inconsistent artifact stage")
    if value["stage"] in {"writing", "uncertain", "ready"} and value["dispatch_expires_at"] is None:
        raise ValueError("missing artifact dispatch provenance")
    allowed = {"prepared": {"upload", "replace"}, "receiving": {"wait", "replace"},
               "writing": {"reconcile", "replace"}, "uncertain": {"reconcile", "replace"},
               "ready": {"complete"}, "expired_no_write": {"replace"}, "retired": {"unavailable"}}
    if ((value["selected"] and value["retry"] not in allowed[value["stage"]])
            or (not value["selected"] and value["retry"] != "unavailable")):
        raise ValueError("inconsistent artifact recovery permission")
    return value


def _result(value):
    if type(value) is dict and "artifact_error" in value:
        if set(value) != {"artifact_error"}:
            raise ValueError("invalid artifact recovery reply")
        error = value["artifact_error"]
        if type(error) is not dict or set(error) != {"kind", "selection", "status", "operation", "message"}:
            raise ValueError("invalid artifact recovery details")
        selection = _selection(error["selection"]) if error["selection"] is not None else None
        status = _status(error["status"]) if error["status"] is not None else None
        operation = error["operation"]
        if (type(operation) is not dict or type(error["kind"]) is not str
                or error["kind"] not in {"recovery_required", "uncertain"}):
            raise ValueError("invalid artifact recovery operation")
        if operation.get("kind") == "put_file":
            if set(operation) != {"kind"} or selection is None:
                raise ValueError("invalid artifact upload recovery")
        elif operation.get("kind") == "reconcile":
            if set(operation) != {"kind", "selection"} or _selection(operation["selection"]) != selection:
                raise ValueError("invalid artifact reconciliation recovery")
        elif operation.get("kind") == "replace":
            if (set(operation) != {"kind", "upload_id", "expected_selection_revision"}
                    or type(operation["expected_selection_revision"]) is not int
                    or not 1 <= operation["expected_selection_revision"] < (1 << 63) - 1):
                raise ValueError("invalid artifact replacement recovery")
            _text(operation["upload_id"], "upload id", 128)
        else:
            raise ValueError("invalid artifact recovery operation")
        if status is not None and status["selection"] != selection:
            raise ValueError("artifact recovery status changed selection")
        if error["kind"] == "recovery_required" and status is None:
            raise ValueError("artifact recovery status is missing")
        _text(error["message"], "recovery message", 512)
        exception = ArtifactRecoveryRequired if error["kind"] == "recovery_required" else ArtifactUploadUncertain
        raise exception(error["message"], selection, error["operation"], status)
    return value


async def upload_status(upload_id):
    """Inspect a generation using the current host-owned invocation authority."""
    result = await _binding().rpc("artifacts.upload_status", {"upload_id": _text(upload_id, "upload id", 128)})
    status = _status(result)
    if status["selection"]["upload_id"] != upload_id:
        raise ValueError("artifact status changed upload identity")
    return status


async def reconcile(selection):
    """Explicitly inspect/adopt verified bytes; this never sends the file again."""
    selection = _selection(selection)
    status = _status(_result(await _binding().rpc("artifacts.reconcile", {"selection": selection})))
    if status["selection"] != selection:
        raise ValueError("artifact reconciliation changed selection")
    return status


async def replace(upload_id, *, expected_selection_revision):
    """Request one CAS-bound successor; repeat only this same explicit command."""
    _text(upload_id, "upload id", 128)
    if type(expected_selection_revision) is not int or not 1 <= expected_selection_revision < (1 << 63) - 1:
        raise ValueError("invalid expected selection revision")
    command = {"upload_id": upload_id, "expected_selection_revision": expected_selection_revision}
    result = _result(await _binding().rpc("artifacts.replace", command))
    if type(result) is not dict or set(result) != {"replaced", "replacement", "current"}:
        raise ValueError("invalid artifact replacement receipt")
    old, new = _selection(result["replaced"]), _selection(result["replacement"])
    status = _status(result["current"])
    if (old["upload_id"] != upload_id or old["revision"] != expected_selection_revision
            or new["artifact_id"] != old["artifact_id"] or new["revision"] != old["revision"] + 1
            or new["upload_id"] == old["upload_id"] or status["selection"] != new):
        raise ValueError("artifact replacement changed original CAS identity")
    return result
