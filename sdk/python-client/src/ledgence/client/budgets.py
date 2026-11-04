"""Run allowances shared by a root execution and its owned descendants."""

from __future__ import annotations

import asyncio
from dataclasses import asdict, dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from .effect_models import EffectOccurrence

from . import codec
from .errors import InputError, ProtocolError
from .models import Scope, _scope


@dataclass(frozen=True)
class RunBudgetPolicy:
    max_calls: int | None = None
    max_delegations: int | None = None
    deadline_at_ms: int | None = None

    def __post_init__(self):
        if not self.to_dict():
            raise InputError("a run budget requires at least one limit")
        for name in ("max_calls", "max_delegations"):
            value = getattr(self, name)
            if value is not None:
                codec.integer(value, name, 0, 1_000_000)
        if self.deadline_at_ms is not None:
            codec.integer(self.deadline_at_ms, "deadline_at_ms", 0, codec.MAX_TIMESTAMP)

    def to_dict(self):
        return {key: value for key, value in asdict(self).items() if value is not None}


def _policy(raw) -> RunBudgetPolicy:
    codec.fields(raw, set(), {"max_calls", "max_delegations", "deadline_at_ms"})
    if None in raw.values():
        raise InputError("omit unused run budget limits")
    return RunBudgetPolicy(**raw)


@dataclass(frozen=True, slots=True)
class RunBudgetSubject:
    kind: Literal["task", "workflow"]
    id: str

    def __post_init__(self):
        if self.kind not in ("task", "workflow"):
            raise InputError("invalid run budget subject")
        codec.text(self.id, "execution id")

    def to_dict(self):
        return {"kind": self.kind, self.kind + "_id": self.id}


class RunBudgetDenial(StrEnum):
    CALLS_EXHAUSTED = "calls_exhausted"
    DELEGATIONS_EXHAUSTED = "delegations_exhausted"
    DEADLINE_EXCEEDED = "deadline_exceeded"
    CANCELLED = "cancelled"


@dataclass(frozen=True, slots=True)
class RunBudgetUsage:
    kind: Literal["unknown", "reported", "estimated"]
    input_tokens: int | None = None
    output_tokens: int | None = None


@dataclass(frozen=True, slots=True)
class RunBudgetSnapshot:
    scope: Scope
    subject: RunBudgetSubject
    root: RunBudgetSubject
    policy: RunBudgetPolicy
    calls: int
    delegations: int
    observed_at: int


@dataclass(frozen=True, slots=True)
class RunBudgetEntry:
    scope: Scope
    root: RunBudgetSubject
    sequence: int
    subject: RunBudgetSubject
    action: Literal["call", "delegation"]
    outcome: Literal["authorized", "denied"]
    reason: RunBudgetDenial | None
    source_id: str
    usage: RunBudgetUsage
    recorded_at: int
    approval: EffectOccurrence | None = None


@dataclass(frozen=True, slots=True)
class RunBudgetPage:
    scope: Scope
    subject: RunBudgetSubject
    root: RunBudgetSubject
    items: tuple[RunBudgetEntry, ...]
    next: int | None


def _subject(raw) -> RunBudgetSubject:
    if type(raw) is not dict or raw.get("kind") not in ("workflow", "task"):
        raise InputError("invalid budget execution")
    key = raw["kind"] + "_id"
    codec.fields(raw, {"kind", key})
    return RunBudgetSubject(raw["kind"], raw[key])


def _subject_for(task_id: str | None, workflow_id: str | None):
    if (task_id is None) == (workflow_id is None):
        raise InputError("supply exactly one of task_id or workflow_id")
    if task_id is not None:
        return RunBudgetSubject("task", task_id)
    return RunBudgetSubject("workflow", workflow_id)


def _snapshot(raw, scope, subject):
    codec.fields(
        raw,
        {"scope", "subject", "root", "policy", "calls", "delegations", "observed_at"},
    )
    if _scope(raw["scope"]) != scope or _subject(raw["subject"]) != subject:
        raise InputError("budget snapshot belongs to another execution")
    policy = _policy(raw["policy"])
    calls = codec.decimal_u64(raw["calls"], "calls")
    delegations = codec.decimal_u64(raw["delegations"], "delegations")
    if (policy.max_calls is not None and calls > policy.max_calls) or (
        policy.max_delegations is not None and delegations > policy.max_delegations
    ):
        raise InputError("usage exceeds immutable execution limits")
    root = _subject(raw["root"])
    if root.kind == "task" and root != subject:
        raise InputError("a task cannot own budget descendants")
    return RunBudgetSnapshot(
        scope,
        subject,
        root,
        policy,
        calls,
        delegations,
        codec.integer(raw["observed_at"], "observed_at", 0, codec.MAX_TIMESTAMP),
    )


def _usage(raw):
    if type(raw) is not dict:
        raise InputError("invalid run budget usage")
    if raw.get("kind") == "unknown":
        codec.fields(raw, {"kind"})
        return RunBudgetUsage("unknown")
    codec.fields(raw, {"kind"}, {"input_tokens", "output_tokens"})
    if raw["kind"] not in ("reported", "estimated") or not set(raw) - {"kind"}:
        raise InputError("usage report requires token counts and provenance")
    counts = {k: codec.integer(raw[k], k, 0, 2**53 - 1) for k in raw if k != "kind"}
    return RunBudgetUsage(raw["kind"], **counts)


def _entry(raw, scope):
    codec.fields(
        raw,
        {
            "scope",
            "root",
            "sequence",
            "subject",
            "action",
            "outcome",
            "reason",
            "source_id",
            "usage",
            "recorded_at",
        },
        {"approval"},
    )
    if _scope(raw["scope"]) != scope:
        raise InputError("budget entry belongs to another scope")
    action, outcome = raw["action"], raw["outcome"]
    if action not in ("call", "delegation") or outcome not in ("authorized", "denied"):
        raise InputError("unknown budget entry")
    reason = None if raw["reason"] is None else RunBudgetDenial(raw["reason"])
    if (action == "call" and reason == RunBudgetDenial.DELEGATIONS_EXHAUSTED) or (
        action == "delegation" and reason == RunBudgetDenial.CALLS_EXHAUSTED
    ):
        raise InputError("budget reason disagrees with entry action")
    if (reason is None) != (outcome == "authorized"):
        raise InputError("budget denial and reason disagree")
    usage = _usage(raw["usage"])
    if usage.kind != "unknown" and (outcome == "denied" or action != "call"):
        raise InputError("usage attributed to a non-call entry")
    subject = _subject(raw["subject"])
    approval = None
    if "approval" in raw:
        from .effect_models import _occurrence

        approval = _occurrence(raw["approval"], scope)
        if (
            action != "call"
            or subject.kind != "workflow"
            or approval.workflow_id != subject.id
        ):
            raise InputError("approval belongs to another budget operation")
    return RunBudgetEntry(
        scope,
        _subject(raw["root"]),
        codec.decimal_u64(raw["sequence"], "sequence", 1),
        subject,
        action,
        outcome,
        reason,
        codec.text(raw["source_id"], "source_id"),
        usage,
        codec.integer(raw["recorded_at"], "recorded_at", 0, codec.MAX_TIMESTAMP),
        approval,
    )


def _page(raw, scope, subject, before, limit):
    codec.fields(raw, {"scope", "subject", "root", "items", "next"})
    if _scope(raw["scope"]) != scope or _subject(raw["subject"]) != subject:
        raise InputError("budget page belongs to another execution")
    root = _subject(raw["root"])
    if root.kind == "task" and root != subject:
        raise InputError("a task cannot own budget descendants")
    if type(raw["items"]) is not list or len(raw["items"]) > limit:
        raise InputError("invalid budget entry page")
    items = tuple(_entry(item, scope) for item in raw["items"])
    previous = before
    for item in items:
        if (
            (root.kind == "task" and item.subject != root)
            or item.root != root
            or (previous is not None and item.sequence >= previous)
        ):
            raise InputError("budget entries disagree with the requested page")
        previous = item.sequence
    cursor = None if raw["next"] is None else codec.decimal_u64(raw["next"], "next", 1)
    if cursor is not None and (not items or cursor != previous):
        raise InputError("invalid budget entry cursor")
    return RunBudgetPage(scope, subject, root, items, cursor)


class Budgets:
    def __init__(self, client):
        self._client = client

    async def _read(self, path, body, decode):
        def parse(raw):
            try:
                return decode(raw)
            except (KeyError, TypeError, ValueError, InputError) as exc:
                raise ProtocolError("invalid run budget response") from exc

        return await self._client._require_transport().exchange(
            "POST",
            "/v1/budgets/" + path,
            body=codec.encode(body, 16384),
            deadline=asyncio.get_running_loop().time() + self._client.request_timeout,
            parser=parse,
            limit=256 * 1024,
        )

    async def inspect(
        self, *, task_id: str | None = None, workflow_id: str | None = None
    ) -> RunBudgetSnapshot:
        """Resolve the execution's budget and inspect its shared counters."""
        subject = _subject_for(task_id, workflow_id)
        scope = self._client.scope
        return await self._read(
            "inspect",
            {"scope": asdict(scope), "subject": subject.to_dict()},
            lambda raw: _snapshot(raw, scope, subject),
        )

    async def entries(
        self,
        *,
        task_id: str | None = None,
        workflow_id: str | None = None,
        before: int | None = None,
        limit: int = 50,
    ) -> RunBudgetPage:
        """Read newest entries for the shared root. Continue with the exclusive page.next."""
        subject = _subject_for(task_id, workflow_id)
        codec.integer(limit, "limit", 1, 100)
        if before is not None:
            codec.integer(before, "before", 1, 2**64 - 1)
        scope = self._client.scope
        return await self._read(
            "entries",
            {
                "scope": asdict(scope),
                "subject": subject.to_dict(),
                "before": None if before is None else str(before),
                "limit": limit,
            },
            lambda raw: _page(raw, scope, subject, before, limit),
        )
