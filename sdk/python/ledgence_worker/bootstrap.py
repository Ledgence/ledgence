"""Persistent JSON-lines process protocol. Run with CPython -I -S (MIT)."""

import argparse
import contextvars
import importlib
import inspect
import json
import os
from pathlib import Path
import sys
import traceback


PROTOCOL_VERSION = 1


class ProtocolError(Exception):
    pass


def _reject_constant(value):
    raise ProtocolError("non-finite JSON number: " + value)


def _encode(value, limit):
    encoded = json.dumps(
        value, ensure_ascii=True, allow_nan=False, separators=(",", ":")
    ).encode("utf-8") + b"\n"
    if len(encoded) > limit:
        raise ProtocolError("protocol output exceeds the configured byte limit")
    return encoded


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
    return value


def _invoke(handler, event, event_id, attempt_id, limit):
    from ledgence_worker import InvocationContext, _invocation

    token = _invocation.set(InvocationContext(event_id, attempt_id))
    envelope = {
        "v": PROTOCOL_VERSION,
        "type": "result",
        "event_id": event_id,
        "attempt_id": attempt_id,
    }
    try:
        output = handler(event)
        if inspect.isawaitable(output):
            if inspect.iscoroutine(output):
                output.close()
            raise TypeError("Ledgence handlers must be synchronous; awaitable returned")
        try:
            response = dict(envelope, status="success", output=output)
            _encode(response, limit)
        except (TypeError, ValueError, OverflowError, RecursionError, ProtocolError) as exc:
            return dict(
                envelope,
                status="error",
                error={"kind": "invalid_output", "message": str(exc)[:512]},
            )
        return response
    except Exception as exc:
        traceback.print_exc(file=sys.stderr)
        return dict(
            envelope,
            status="error",
            error={"kind": "business_error", "message": str(exc)[:512]},
        )
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
    handler = getattr(importlib.import_module(module_name), function_name)
    if not callable(handler) or inspect.iscoroutinefunction(handler):
        raise ProtocolError("handler must be a synchronous callable")
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
