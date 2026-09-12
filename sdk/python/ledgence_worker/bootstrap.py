"""Persistent JSON-lines process protocol. Run with CPython -I -S -B (MIT)."""

import sys

# Also protect callers that launch this entry point without -B. Imports must not
# modify immutable artifacts, including when the worker runs with elevated rights.
sys.dont_write_bytecode = True

import argparse
import contextvars
import importlib
from importlib.machinery import PathFinder
import inspect
import json
import math
import os
from pathlib import Path
import traceback


PROTOCOL_VERSION = 1
# Keep the portable wire profile in step with ledgence_worker_api's validator.
MAX_WIRE_VALUE_DEPTH = 64


class ProtocolError(Exception):
    pass


def _reject_constant(value):
    raise ProtocolError("non-finite JSON number: " + value)


def _encode(value, limit):
    encoded = json.dumps(
        value, ensure_ascii=False, allow_nan=False, separators=(",", ":")
    ).encode("utf-8") + b"\n"
    if len(encoded) > limit:
        raise ProtocolError("protocol output exceeds the configured byte limit")
    return encoded


def _validate_wire_value(value, depth=0, ancestors=None):
    """Validate the shared JSON value profile before reporting handler success."""
    if value is None or isinstance(value, bool):
        return
    if isinstance(value, str):
        value.encode("utf-8", errors="strict")
        return
    if isinstance(value, int):
        if not -(1 << 63) <= value <= (1 << 64) - 1:
            raise ProtocolError("integer output exceeds the i64/u64 range")
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ProtocolError("floating-point output must be finite")
        return
    if not isinstance(value, (dict, list, tuple)):
        raise ProtocolError("output must contain only JSON-compatible values")
    if depth >= MAX_WIRE_VALUE_DEPTH:
        raise ProtocolError("output exceeds the maximum JSON container depth")
    if ancestors is None:
        ancestors = set()
    identity = id(value)
    if identity in ancestors:
        raise ProtocolError("output contains a circular reference")
    ancestors.add(identity)
    try:
        if isinstance(value, dict):
            for key, item in value.items():
                if not isinstance(key, str):
                    raise ProtocolError("JSON object keys must be strings")
                key.encode("utf-8", errors="strict")
                _validate_wire_value(item, depth + 1, ancestors)
        else:
            for item in value:
                _validate_wire_value(item, depth + 1, ancestors)
    finally:
        ancestors.remove(identity)


def _failure(envelope, kind, error, limit):
    """Fit the entire failure frame, including identities and UTF-8 escaping."""
    try:
        message = str(error)
    except Exception:
        message = "error message could not be formatted"
    # This is infrastructure error text, not user output. Replace invalid scalar
    # sequences so even a malformed exception cannot corrupt the response pipe.
    message = message[:512].encode("utf-8", errors="replace").decode("utf-8")
    response = dict(envelope, status="error", error={"kind": kind, "message": ""})
    _encode(response, limit)  # The caller must establish this fits before invoking.
    low, high = 0, len(message)
    while low < high:
        middle = (low + high + 1) // 2
        response["error"]["message"] = message[:middle]
        try:
            _encode(response, limit)
            low = middle
        except ProtocolError:
            high = middle - 1
    response["error"]["message"] = message[:low]
    return response


def _inside_artifact(path, root):
    return Path(path).resolve(strict=True).is_relative_to(root)


def _check_module_origin(spec, root):
    if spec is None:
        raise ProtocolError("handler module was not found in the artifact")
    if spec.origin is not None and not _inside_artifact(spec.origin, root):
        raise ProtocolError("handler module must originate inside the artifact")
    locations = spec.submodule_search_locations
    if spec.origin is None and not locations:
        raise ProtocolError("handler module has no artifact origin")
    if locations and not all(_inside_artifact(path, root) for path in locations):
        raise ProtocolError("handler package search path leaves the artifact")


def _load_handler(root, module_name, function_name):
    # Changing sys.path does not replace a module already loaded by the bootstrap.
    # Reject those names explicitly instead of silently executing unrelated code.
    names = [".".join(module_name.split(".")[:i])
             for i in range(1, len(module_name.split(".")) + 1)]
    for name in names:
        if name in sys.modules:
            raise ProtocolError("handler module conflicts with a preloaded module: " + name)
    search_path = [str(root)]
    for index, name in enumerate(names):
        spec = PathFinder.find_spec(name, search_path)
        _check_module_origin(spec, root)
        module = importlib.import_module(name)
        # Package initialization may alter import paths; inspect the actual module
        # before resolving its child or selecting the callable.
        _check_module_origin(getattr(module, "__spec__", None), root)
        if index + 1 < len(names):
            if not module.__spec__.submodule_search_locations:
                raise ProtocolError("handler parent module is not a package")
            search_path = module.__spec__.submodule_search_locations
    handler = getattr(module, function_name)
    if not callable(handler) or inspect.iscoroutinefunction(handler):
        raise ProtocolError("handler must be a synchronous callable")
    return handler


def _write(protocol, value, limit):
    # FileIO.write may return a short write for large frames. Never interleave frames.
    pending = memoryview(_encode(value, limit))
    while pending:
        count = protocol.write(pending)
        if count is None or count <= 0:
            raise ProtocolError("protocol output pipe closed")
        pending = pending[count:]


def _text(value, field):
    if not isinstance(value, str) or not value:
        raise ProtocolError(field + " must be a nonempty string")
    value.encode("utf-8", errors="strict")
    return value


def _invoke(handler, event, event_id, attempt_id, limit):
    from ledgence_worker import InvocationContext, _invocation

    envelope = {
        "v": PROTOCOL_VERSION,
        "type": "result",
        "event_id": event_id,
        "attempt_id": attempt_id,
    }
    # A protocol with separate input/output limits may accept identities too large
    # for even an empty failure result. Reject that before running application code.
    _failure(envelope, "invalid_output", "", limit)
    token = _invocation.set(InvocationContext(event_id, attempt_id))
    try:
        output = handler(event)
        try:
            if inspect.isawaitable(output):
                if inspect.iscoroutine(output):
                    output.close()
                raise TypeError("Ledgence handlers must be synchronous; awaitable returned")
            _validate_wire_value(output)
            response = dict(envelope, status="success", output=output)
            _encode(response, limit)
        except (TypeError, ValueError, OverflowError, RecursionError, ProtocolError) as exc:
            return _failure(envelope, "invalid_output", exc, limit)
        return response
    except Exception as exc:
        traceback.print_exc(file=sys.stderr)
        return _failure(envelope, "business_error", exc, limit)
    finally:
        _invocation.reset(token)


def main():
    # Retain an exclusive protocol descriptor before redirecting *both* Python
    # stdout and native writes to fd 1. User prints cannot corrupt JSON frames.
    protocol = os.fdopen(os.dup(sys.stdout.fileno()), "wb", buffering=0)
    os.dup2(sys.stderr.fileno(), sys.stdout.fileno())
    sys.stdout = sys.stderr

    parser = argparse.ArgumentParser()
    parser.add_argument("--package-root", required=True)
    parser.add_argument("--handler", required=True)
    parser.add_argument("--python-version", required=True)
    parser.add_argument("--max-input-bytes", type=int, default=1048576)
    parser.add_argument("--max-output-bytes", type=int, default=1048576)
    args = parser.parse_args()
    if args.max_input_bytes < 256 or args.max_output_bytes < 256:
        raise ProtocolError("protocol byte limits must be at least 256")
    actual_version = "%d.%d" % sys.version_info[:2]
    if (sys.implementation.name != "cpython" or sys.version_info < (3, 11)
            or actual_version != args.python_version):
        raise ProtocolError(
            "program requires CPython %s; worker has %s"
            % (args.python_version, actual_version)
        )
    root = Path(args.package_root).resolve(strict=True)
    if not root.is_dir():
        raise ProtocolError("package root must be a directory")
    module_name, separator, function_name = args.handler.partition(":")
    if not separator or not module_name or not function_name or ":" in function_name:
        raise ProtocolError("handler must be module:function")
    if not all(part.isidentifier() for part in module_name.split(".")):
        raise ProtocolError("handler module must be a dotted Python identifier")
    if not function_name.isidentifier():
        raise ProtocolError("handler function must be a Python identifier")

    # -I -S omits implicit project/site imports. Load the helper first, then add
    # the exact prepared artifact root for application code and vendored deps.
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    import ledgence_worker  # noqa: F401

    sys.path.insert(0, str(root))
    handler = _load_handler(root, module_name, function_name)
    _write(
        protocol,
        {"v": PROTOCOL_VERSION, "type": "ready", "pid": os.getpid(),
         "python_version": actual_version},
        args.max_output_bytes,
    )
    while True:
        line = sys.stdin.buffer.readline(args.max_input_bytes + 1)
        if not line:
            return
        if len(line) > args.max_input_bytes or not line.endswith(b"\n"):
            raise ProtocolError("protocol input exceeds limit or has no newline")
        message = json.loads(line, parse_constant=_reject_constant)
        if not isinstance(message, dict) or message.get("v") != PROTOCOL_VERSION:
            raise ProtocolError("invalid protocol version or envelope")
        if message.get("type") == "shutdown":
            # Acknowledge while still alive. The parent owns group termination
            # and reaping; exiting here would race Darwin's zombie-only killpg.
            _write(protocol, {"v": PROTOCOL_VERSION, "type": "closing"}, args.max_output_bytes)
            # EOF also permits exit if the parent disappears before signaling.
            while sys.stdin.buffer.read(4096):
                pass
            return
        if message.get("type") != "invoke":
            raise ProtocolError("expected invoke or shutdown")
        event_id = _text(message.get("event_id"), "event_id")
        attempt_id = _text(message.get("attempt_id"), "attempt_id")
        event = message.get("event")
        if (not isinstance(event, dict) or event.get("id") != event_id
                or event.get("ldgattemptid") != attempt_id):
            raise ProtocolError("invocation event identity does not match event")
        # A fresh Context prevents contextvars set by an earlier invocation
        # leaking into the next one in this persistent process.
        response = contextvars.Context().run(
            _invoke, handler, event, event_id, attempt_id, args.max_output_bytes
        )
        _write(protocol, response, args.max_output_bytes)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        traceback.print_exc(file=sys.stderr)
        sys.exit(70)
