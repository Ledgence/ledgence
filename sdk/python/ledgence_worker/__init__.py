"""Small, dependency-free helpers for Ledgence Python programs (MIT)."""

from contextvars import ContextVar
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class TraceContext:
    """Invocation-local W3C carrier, separate from the immutable event origin."""

    traceparent: str
    tracestate: Optional[str] = None


@dataclass(frozen=True)
class InvocationContext:
    """Envelope identity for the invocation currently handled by this process."""

    event_id: str
    attempt_id: str
    source: Optional[str] = None
    tenant_id: Optional[str] = None
    namespace: Optional[str] = None
    run_id: Optional[str] = None
    task_id: Optional[str] = None
    attempt_no: Optional[int] = None
    processing_context: Optional[TraceContext] = None
    workflow_id: Optional[str] = None
    activation_id: Optional[str] = None
    parent_workflow_id: Optional[str] = None
    root_workflow_id: Optional[str] = None


_invocation: ContextVar[Optional[InvocationContext]] = ContextVar(
    "ledgence_invocation", default=None
)
_shutdown_callbacks = []


def current_invocation() -> InvocationContext:
    """Return this invocation's identifiers; fail when no invocation is active."""
    value = _invocation.get()
    if value is None:
        raise RuntimeError("no Ledgence invocation is active")
    return value


def register_shutdown(callback):
    """Call an application-owned provider's shutdown before the closing ACK.

    Called once on graceful protocol shutdown. The parent process's shutdown
    deadline bounds callbacks; cancellation/crashes do not promise a flush.
    """
    if not callable(callback):
        raise TypeError("shutdown callback must be callable")
    if callback not in _shutdown_callbacks:
        _shutdown_callbacks.append(callback)


def _shutdown():
    import traceback

    callbacks = _shutdown_callbacks[:]
    _shutdown_callbacks.clear()
    for callback in callbacks:
        try:
            callback()
        except Exception:
            traceback.print_exc()


def get_logger(name):
    """Return a standard logger with a contextual, best-effort Ledgence handler."""
    from ._logging import get_logger as configured_logger

    return configured_logger(name)


__all__ = ["InvocationContext", "TraceContext", "current_invocation", "get_logger",
           "register_shutdown"]
