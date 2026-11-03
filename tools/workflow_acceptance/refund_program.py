"""Reference workflow for the protected-refund acceptance contract (MIT)."""
from enum import StrEnum
from http.client import HTTPConnection, HTTPException
import json
import os
from pathlib import Path
from urllib.parse import urlencode, urlsplit

from ledgence.worker.workflow import ApprovalAction, ApprovalStatus, EffectOutcomeUnknown, Workflow, WorkflowError


class Entry(StrEnum):
    PROPOSE = "propose"
    APPLY = "apply"
    FINISH = "finish"
    UNRESOLVED = "unresolved"


workflow = Workflow(Entry)


def issue_refund(*, provider, effect_key, amount, currency="USD"):
    return _receipt("POST", provider=provider, effect_key=effect_key, amount=amount, currency=currency)


def lookup_refund(*, provider, effect_key, amount, currency="USD"):
    return _receipt("GET", provider=provider, effect_key=effect_key, amount=amount, currency=currency)


def _unique_fields(pairs):
    fields = {}
    for key, value in pairs:
        if key in fields:
            raise ValueError("duplicate provider receipt field")
        fields[key] = value
    return fields


def _invalid_number(_):
    raise ValueError("invalid provider receipt number")


def _receipt(method, *, provider, effect_key, amount, currency="USD"):
    arguments = {"amount": amount, "currency": currency}
    target = urlsplit(provider)
    if target.scheme != "http" or target.netloc != f"127.0.0.1:{target.port}" or target.path or target.query or target.fragment:
        raise ValueError("refund fixture requires its owned loopback destination")
    connection = HTTPConnection(target.hostname, target.port, timeout=120)
    try:
        if method == "GET":
            connection.request("GET", "/refunds?" + urlencode({"effect_key": effect_key}))
        else:
            body = json.dumps({"effect_key": effect_key, "arguments": arguments}).encode()
            connection.request("POST", "/refunds", body, {"Content-Type": "application/json"})
        response = connection.getresponse()
        # This fixture issues complete synchronous receipts only on these statuses.
        # Read a bounded response from this exact destination without following redirects.
        if response.status not in ((200,) if method == "GET" else (200, 201)):
            raise EffectOutcomeUnknown(effect_key)
        length = int(response.getheader("Content-Length", "0"))
        if not 0 < length <= 8192:
            raise EffectOutcomeUnknown(effect_key)
        body = response.read(8193)
        if len(body) != length:
            raise EffectOutcomeUnknown(effect_key)
        receipt = json.loads(body.decode("utf-8"), object_pairs_hook=_unique_fields, parse_constant=_invalid_number)
    except (OSError, HTTPException, ValueError, RecursionError) as exc:
        raise EffectOutcomeUnknown(effect_key) from exc
    finally:
        connection.close()
    if (not isinstance(receipt, dict) or receipt.get("effect_key") != effect_key
            or json.dumps(receipt.get("arguments"), sort_keys=True) != json.dumps(arguments, sort_keys=True)
            or not isinstance(receipt.get("refund_id"), str) or not receipt["refund_id"]):
        raise EffectOutcomeUnknown(effect_key)
    return {"state": "confirmed", "receipt": receipt}


@workflow.entrypoint(Entry.PROPOSE, default=True)
def propose(event, ctx):
    data = event["data"]
    action = ApprovalAction.for_callable(issue_refund, version="1", arguments={
        "provider": data["provider"], "effect_key": data["effect_key"], "amount": data["amount"],
    })
    return ctx.request_effect_approval("refund", action=action, resume=Entry.APPLY, state=None, timeout_ms=300_000)


@workflow.entrypoint(Entry.APPLY)
async def apply(event, ctx):
    if ctx.approval.status != ApprovalStatus.APPROVED:
        return ctx.fail("refund_not_approved", "Approval was required for this refund")
    if event["data"].get("unprotected_helper"):
        # Negative acceptance case. The saved request must reject this call
        # before the callback sends a provider request.
        try:
            result = await ctx.approved_local(issue_refund, version="1")
        except WorkflowError as error:
            return ctx.fail("protected_helper_rejected", str(error))
        return ctx.complete(result)
    # Crash-test observability only. Dispatch authority belongs to Ledgence.
    with Path(event["data"]["marker"]).open("a", encoding="utf-8") as stream:
        stream.write(json.dumps({"tag": event["data"]["effect_key"], "kind": "apply",
            "activation": ctx.activation_id, "attempt": event["ldgattemptno"],
            "attempt_id": event["ldgattemptid"], "pid": os.getpid()}) + "\n")
    try:
        result = await ctx.approved_effect(issue_refund, lookup=lookup_refund, version="1")
    except EffectOutcomeUnknown:
        return park(ctx, {"state": "unknown", "effect_key": ctx.approval.action.arguments["effect_key"]})
    return ctx.continue_(continuation=Entry.FINISH, state=result)


@workflow.entrypoint(Entry.FINISH)
def finish(event, ctx):
    return ctx.complete(ctx.state)


def park(ctx, result):
    return ctx.wait_event("unresolved:" + ctx.activation_id, continuation=Entry.UNRESOLVED, state=result)


@workflow.entrypoint(Entry.UNRESOLVED)
def unresolved(event, ctx):
    # Generic events cannot confirm a refund. Verified operator resolution is
    # a separate part of the proposed production contract.
    return park(ctx, ctx.state)


handle = workflow.build()
