"""Bounded operator metadata for committed weighted resource offers."""

from dataclasses import dataclass
from typing import Literal

from . import codec
from .admission_models import name as group_name, timestamp
from .errors import InputError
from .models import Scope, _scope


@dataclass(frozen=True, slots=True)
class TaskDecisionSubject:
    task_id: str
    generation: int
    kind: Literal["task"] = "task"


@dataclass(frozen=True, slots=True)
class WorkflowDecisionSubject:
    kind: Literal["workflow", "provider"]
    workflow_id: str
    origin_activation_id: str
    key: str


@dataclass(frozen=True, slots=True)
class ResourceOfferDecision:
    scope: Scope
    resource: str
    sequence: int
    policy_revision: int
    group: str
    weight: int
    eligible_groups: int
    cycle_round: int
    reason: Literal["weighted", "sole_eligible"]
    queue: str
    subject: TaskDecisionSubject | WorkflowDecisionSubject
    offer_generation: int
    offered_at: int


@dataclass(frozen=True, slots=True)
class ResourceDecisionPage:
    items: tuple[ResourceOfferDecision, ...]
    next: int | None


def _subject(raw):
    if type(raw) is not dict:
        raise InputError("invalid resource offer subject")
    if raw.get("kind") == "task":
        codec.fields(raw, {"kind", "task_id", "generation"})
        return TaskDecisionSubject(
            codec.text(raw["task_id"], "task_id"),
            codec.integer(raw["generation"], "generation", 1, 2**31 - 1),
        )
    codec.fields(raw, {"kind", "workflow_id", "origin_activation_id", "key"})
    if raw["kind"] not in ("workflow", "provider"):
        raise InputError("unrecognized resource offer subject")
    return WorkflowDecisionSubject(
        raw["kind"],
        *(
            codec.text(raw[k], k)
            for k in ("workflow_id", "origin_activation_id", "key")
        ),
    )


def _decision(raw, scope, resource):
    codec.fields(
        raw,
        {
            "scope",
            "resource",
            "sequence",
            "policy_revision",
            "group",
            "weight",
            "eligible_groups",
            "cycle_round",
            "reason",
            "queue",
            "subject",
            "offer_generation",
            "offered_at",
        },
    )
    if _scope(raw["scope"]) != scope or raw["resource"] != resource:
        raise InputError("foreign resource offer decision")
    count = codec.integer(raw["eligible_groups"], "eligible_groups", 1, 16)
    weight = codec.integer(raw["weight"], "weight", 1, 1000)
    if raw["reason"] != ("sole_eligible" if count == 1 else "weighted"):
        raise InputError("reason disagrees with eligible group count")
    return ResourceOfferDecision(
        scope,
        resource,
        codec.decimal_u64(raw["sequence"], "sequence", 1),
        codec.decimal_u64(raw["policy_revision"], "policy_revision", 1),
        group_name(raw["group"]),
        weight,
        count,
        codec.integer(raw["cycle_round"], "cycle_round", 1, weight),
        raw["reason"],
        codec.text(raw["queue"], "queue"),
        _subject(raw["subject"]),
        codec.decimal_u64(raw["offer_generation"], "offer_generation", 1),
        timestamp(raw["offered_at"]),
    )


def parse_page(raw, scope, resource, before, limit):
    codec.fields(raw, {"items", "next"})
    if type(raw["items"]) is not list or len(raw["items"]) > limit:
        raise InputError("invalid resource offer decision page")
    items = tuple(_decision(row, scope, resource) for row in raw["items"])
    last = before
    for item in items:
        if last is not None and item.sequence >= last:
            raise InputError("resource offer decisions are not in descending order")
        last = item.sequence
    following = (
        None if raw["next"] is None else codec.decimal_u64(raw["next"], "next", 1)
    )
    if following is not None and (not items or following != last):
        raise InputError("invalid resource decision cursor")
    return ResourceDecisionPage(items, following)
