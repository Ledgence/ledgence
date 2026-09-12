"""Small, dependency-free helpers for Ledgence Python programs (MIT)."""

from contextvars import ContextVar
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class InvocationContext:
    """Execution identifiers for the invocation currently handled by this process."""

    event_id: str
    attempt_id: str


_invocation: ContextVar[Optional[InvocationContext]] = ContextVar(
    "ledgence_invocation", default=None
)


def current_invocation() -> InvocationContext:
    """Return this invocation's identifiers; fail when no invocation is active."""
    value = _invocation.get()
    if value is None:
        raise RuntimeError("no Ledgence invocation is active")
    return value


__all__ = ["InvocationContext", "current_invocation"]
