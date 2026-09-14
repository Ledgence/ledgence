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
import re
from pathlib import Path
import traceback


# Keep the portable wire profile in step with ledgence_worker_api's validator.
MAX_WIRE_VALUE_DEPTH = 64
# Match ledgence_worker_api.DEFAULT_RUNTIME_FRAME_MAX_BYTES. The Rust adapter
# supplies both flags explicitly; direct helper launches use the same default.
DEFAULT_RUNTIME_FRAME_MAX_BYTES = 2 * 1024 * 1024


class ProtocolError(Exception):
    pass


def _unique_fields(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ProtocolError("duplicate JSON object field")
        value[key] = item
    return value


def _reject_constant(value):
    raise ProtocolError("non-finite JSON number: " + value)


def _encode(value, limit):
    encoded = json.dumps(
        value, ensure_ascii=False, allow_nan=False, separators=(",", ":")
    ).encode("utf-8") + b"\n"
    if len(encoded) > limit:
        raise ProtocolError("protocol output exceeds the configured byte limit")
    return encoded


def _validate_wire_value(value, depth=0, ancestors=None, max_depth=MAX_WIRE_VALUE_DEPTH):
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
    if depth >= max_depth:
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
                _validate_wire_value(item, depth + 1, ancestors, max_depth)
        else:
            for item in value:
                _validate_wire_value(item, depth + 1, ancestors, max_depth)
    finally:
        ancestors.remove(identity)


def _failure(envelope, kind, error, limit, status="error"):
    """Fit the entire failure frame, including identities and UTF-8 escaping."""
    try:
        message = str(error)
    except Exception:
        message = "error message could not be formatted"
    # This is infrastructure error text, not user output. Replace invalid scalar
    # sequences so even a malformed exception cannot corrupt the response pipe.
    message = message[:512].encode("utf-8", errors="replace").decode("utf-8")
    response = dict(envelope, status=status, error={"kind": kind, "message": ""})
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


def _load_handler(root, module_name, function_name, allow_async=False):
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
    if not callable(handler) or (inspect.iscoroutinefunction(handler) and not allow_async):
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


def _invoke(handler, event, event_id, attempt_id, limit, version=1, processing_context=None,
            extension=None, rpc=None):
    from ledgence_worker import InvocationContext, _invocation
    from ledgence_worker.otel import _activate

    envelope = {
        "v": version,
        "type": "result",
        "event_id": event_id,
        "attempt_id": attempt_id,
    }
    # A protocol with separate input/output limits may accept identities too large
    # for even an empty failure result. Reject that before running application code.
    _failure(envelope, "invalid_output", "", limit,
             status="runtime_error" if extension is not None else "error")
    token = _invocation.set(InvocationContext(
        event_id, attempt_id, source=event.get("source"),
        tenant_id=event.get("ldgtenantid"), namespace=event.get("ldgnamespace"),
        run_id=event.get("ldgrunid"), task_id=event.get("ldgtaskid"),
        attempt_no=event.get("ldgattemptno"), processing_context=processing_context,
        workflow_id=event.get("ldgworkflowid"), activation_id=event.get("ldgactivationid"),
    ))
    try:
        with _activate(processing_context):
            if version >= 3:
                import asyncio
                return asyncio.run(_invoke_async(handler, event, envelope, limit, extension, rpc))
            return _invoke_output(handler, event, envelope, limit)
    finally:
        _invocation.reset(token)


def _invoke_output(handler, event, envelope, limit):
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


async def _invoke_async(handler, event, envelope, limit, extension, rpc):
    import asyncio
    from ledgence_worker.workflow import SCHEMA, WorkflowContext, _workflow

    context = None
    token = None
    failed = True
    try:
        if extension is not None:
            if (not isinstance(extension, dict) or set(extension) != {"schema", "payload"}
                    or extension["schema"] != SCHEMA):
                raise ProtocolError("unsupported runtime extension")
            context = WorkflowContext(extension["payload"], rpc)
            if (context.activation_id != event.get("ldgtaskid")
                    or context.activation_id != event.get("ldgactivationid")
                    or context.workflow_id != event.get("ldgworkflowid")):
                raise ProtocolError("workflow activation context does not match the event identity")
            token = _workflow.set(context)
        output = handler(event)
        if inspect.isawaitable(output):
            output = await output
        if context is not None:
            # Wait for every owned local operation before producing a checkpoint.
            # This preserves durable commits even when an observer did not await.
            await context._drain()
            output = context._validate_decision(output)
        try:
            _validate_wire_value(output, max_depth=96 if context is not None else MAX_WIRE_VALUE_DEPTH)
            response = dict(envelope, status="success", output=output)
            _encode(response, limit)
        except (TypeError, ValueError, OverflowError, RecursionError, ProtocolError) as exc:
            if context is not None:
                raise
            return _failure(envelope, "invalid_output", exc, limit)
        failed = False
        return response
    except Exception as exc:
        traceback.print_exc(file=sys.stderr)
        return _failure(envelope, "workflow_error" if extension is not None else "business_error", exc, limit,
                        status="runtime_error" if extension is not None else "error")
    finally:
        if context is not None:
            await context._finish(cancel=failed)
        if token is not None:
            _workflow.reset(token)


class _RuntimeRpc:
    """One serialized control exchange; local asynchronous work remains parallel."""

    def __init__(self, write, stream, input_limit, event_id, attempt_id):
        import asyncio
        self._write = write
        self._stream = stream
        self._input_limit = input_limit
        self._identity = {"v": 3, "event_id": event_id, "attempt_id": attempt_id}
        self._sequence = 0
        self._lock = asyncio.Lock()

    async def __call__(self, operation, payload):
        import asyncio
        async with self._lock:
            self._sequence += 1
            request = dict(self._identity, type="runtime_request", id=self._sequence,
                           operation=operation, payload=payload)
            exchange = asyncio.create_task(asyncio.to_thread(self._exchange, request))
            try:
                return await asyncio.shield(exchange)
            except asyncio.CancelledError:
                # A cancelled await does not cancel the blocking pipe operation.
                # Retain it until completion; the Rust owner bounds process life.
                await exchange
                raise

    def _exchange(self, request):
        self._write(request)
        line = self._stream.readline(self._input_limit + 1)
        if not line or len(line) > self._input_limit or not line.endswith(b"\n"):
            raise ProtocolError("invalid runtime reply frame size or EOF")
        reply = json.loads(line, parse_constant=_reject_constant, object_pairs_hook=_unique_fields)
        required = {"v", "type", "event_id", "attempt_id", "id", "result"}
        if (not isinstance(reply, dict) or set(reply) != required
                or reply.get("type") != "runtime_reply"
                or type(reply.get("id")) is not int or reply["id"] != request["id"]
                or any(type(reply.get(key)) is not type(value) or reply[key] != value
                       for key, value in self._identity.items())):
            raise ProtocolError("runtime reply identity or envelope mismatch")
        return reply["result"]


def _processing_context(value):
    from ledgence_worker import TraceContext

    if value is None:
        return None
    if not isinstance(value, dict) or set(value) - {"traceparent", "tracestate"}:
        raise ProtocolError("invalid processing context")
    trace = value.get("traceparent")
    if (not isinstance(trace, str)
            or not re.fullmatch(r"00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}", trace)
            or trace[3:35] == "0" * 32 or trace[36:52] == "0" * 16):
        raise ProtocolError("invalid processing traceparent")
    state = value.get("tracestate")
    if state is not None:
        if not isinstance(state, str) or len(state) > 512 or not state.isascii():
            raise ProtocolError("invalid processing tracestate")
        members = state.split(",")
        keys = set()
        if len(members) > 32:
            raise ProtocolError("invalid processing tracestate")
        for member in members:
            member = member.strip(" ")
            if not member:
                continue
            key, equal, item = member.partition("=")
            if (not equal or not item or len(item) > 256 or key in keys
                    or not re.fullmatch(
                        r"(?:[a-z][a-z0-9_*/-]{0,255}|[a-z0-9][a-z0-9_*/-]{0,240}@[a-z][a-z0-9_*/-]{0,13})", key)
                    or any(not 0x20 <= ord(char) <= 0x7e or char in ",=" for char in item)):
                raise ProtocolError("invalid processing tracestate")
            keys.add(key)
    return TraceContext(trace, state)


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
    parser.add_argument("--protocol-version", type=int, choices=(1, 2, 3), default=1)
    parser.add_argument("--max-input-bytes", type=int, default=DEFAULT_RUNTIME_FRAME_MAX_BYTES)
    parser.add_argument("--max-output-bytes", type=int, default=DEFAULT_RUNTIME_FRAME_MAX_BYTES)
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
    import ledgence_worker
    if args.protocol_version >= 3:
        # Load runtime dependencies before application imports, preserving the
        # original preloaded-module surface for synchronous v1/v2 packages.
        import asyncio

    writer = None
    if args.protocol_version >= 2:
        from ledgence_worker._protocol import ProtocolWriter
        from ledgence_worker import _logging

        writer = ProtocolWriter(protocol, args.max_output_bytes)
        writer.protocol_version = args.protocol_version
        _logging._sink = writer

    def write(value, closing=False):
        if writer is None:
            _write(protocol, value, args.max_output_bytes)
        else:
            writer.control(_encode(value, args.max_output_bytes), closing=closing)

    sys.path.insert(0, str(root))
    handler = _load_handler(root, module_name, function_name, allow_async=args.protocol_version >= 3)
    write({"v": args.protocol_version, "type": "ready", "pid": os.getpid(),
           "python_version": actual_version})
    while True:
        line = sys.stdin.buffer.readline(args.max_input_bytes + 1)
        if not line:
            return
        if len(line) > args.max_input_bytes or not line.endswith(b"\n"):
            raise ProtocolError("protocol input exceeds limit or has no newline")
        message = json.loads(line, parse_constant=_reject_constant)
        if not isinstance(message, dict) or message.get("v") != args.protocol_version:
            raise ProtocolError("invalid protocol version or envelope")
        if message.get("type") == "shutdown":
            # Acknowledge while still alive. The parent owns group termination
            # and reaping; exiting here would race Darwin's zombie-only killpg.
            if args.protocol_version >= 2:
                ledgence_worker._shutdown()
            write({"v": args.protocol_version, "type": "closing"}, closing=True)
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
        processing = None
        if args.protocol_version >= 2:
            if "processing_context" not in message:
                raise ProtocolError("v2+ invocation requires processing_context (null when disabled)")
            processing = _processing_context(message["processing_context"])
        extension = message.get("extension") if args.protocol_version >= 3 else None
        rpc = _RuntimeRpc(write, sys.stdin.buffer, args.max_input_bytes, event_id, attempt_id) if args.protocol_version >= 3 else None
        # A fresh Context prevents contextvars set by an earlier invocation
        # leaking into the next one in this persistent process.
        response = contextvars.Context().run(
            _invoke, handler, event, event_id, attempt_id, args.max_output_bytes,
            args.protocol_version, processing, extension, rpc
        )
        write(response)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        traceback.print_exc(file=sys.stderr)
        sys.exit(70)
