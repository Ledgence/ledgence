"""Real provider response loss followed by native, worker-free operator rendezvous (MIT)."""
import asyncio
from enum import StrEnum
from http.client import HTTPConnection, HTTPException
import json
from pathlib import Path
from urllib.parse import urlsplit

from ledgence.worker.workflow import ApprovalAction, ApprovalStatus, EffectOutcomeUnknown, Workflow


class Entry(StrEnum):
    PROPOSE = "propose"
    APPLY = "apply"
    RESOLVED = "resolved"


workflow = Workflow(Entry)


def issue_refund(*, provider, effect_key, amount):
    target = urlsplit(provider)
    if target.scheme != "http" or target.hostname != "127.0.0.1" or not target.port or target.path:
        raise ValueError("fixture requires its owned loopback provider")
    connection = HTTPConnection(target.hostname, target.port, timeout=30)
    try:
        body = json.dumps({"effect_key": effect_key, "arguments": {"amount": amount, "currency": "USD"}})
        connection.request("POST", "/refunds", body, {"Content-Type": "application/json"})
        response = connection.getresponse()
        raw = response.read(8193)
        if response.status not in (200, 201) or len(raw) > 8192:
            raise EffectOutcomeUnknown(effect_key)
        return json.loads(raw)
    except (OSError, HTTPException, ValueError) as error:
        raise EffectOutcomeUnknown(effect_key) from error
    finally:
        connection.close()


def lookup_refund(*, provider, effect_key, amount):
    # This scenario deliberately requires an operator; it cannot infer a result.
    raise EffectOutcomeUnknown(effect_key)


@workflow.entrypoint(Entry.PROPOSE, default=True)
def propose(event, ctx):
    data = event["data"]
    action = ApprovalAction.for_callable(issue_refund, version="1", arguments={
        "provider": data["provider"], "effect_key": data["effect_key"], "amount": data["amount"]})
    return ctx.request_effect_approval("refund", action=action, resume=Entry.APPLY, state=None, timeout_ms=300_000)


@workflow.entrypoint(Entry.APPLY)
async def apply(event, ctx):
    if ctx.approval.status != ApprovalStatus.APPROVED:
        return ctx.fail("not_approved", "The fixture requires approval")
    try:
        result = await ctx.approved_effect(issue_refund, lookup=lookup_refund, version="1")
        return ctx.complete({"unexpected_confirmed_result": result})
    except EffectOutcomeUnknown:
        pass
    # The owned acceptance fixture can stop before the *durable* park to test
    # either acceptance ordering without sleeping or manufacturing DB state.
    marker = Path(event["data"]["before_park"])
    marker.write_text("ready")
    release = Path(event["data"]["release_park"])
    while not release.exists():
        await asyncio.sleep(.02)
    return ctx.wait_effect_resolution("operator:0", resume=Entry.RESOLVED, state={"round": 0})


@workflow.entrypoint(Entry.RESOLVED)
def resolved(event, ctx):
    wake = ctx.effect_resolution
    with Path(event["data"]["wakes"]).open("a") as stream:
        stream.write(json.dumps(wake.to_dict()) + "\n")
    if event["data"].get("follow_up") and ctx.state["round"] == 0:
        # A later assessment needs a new logical wait; the prior wake is immutable.
        return ctx.wait_effect_resolution("operator:1", resume=Entry.RESOLVED, state={"round": 1})
    return ctx.complete(wake.to_dict())


handle = workflow.build()
