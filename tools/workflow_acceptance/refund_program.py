"""Reference workflow for the protected-refund acceptance contract (MIT)."""
from enum import StrEnum
from http.client import HTTPConnection, HTTPException
import json
import os
from pathlib import Path
from urllib.parse import urlencode, urlsplit

from ledgence.worker.workflow import ApprovalAction, ApprovalStatus, Workflow


class Entry(StrEnum):
    PROPOSE = "propose"
    APPLY = "apply"
    FINISH = "finish"
    UNRESOLVED = "unresolved"


workflow = Workflow(Entry)


class Refund:
    """A replacement attempt can only look up the original approved action."""

    def __init__(self, lookup_only):
        self.lookup_only = lookup_only

    def execute(self, *, provider, effect_key, amount, currency="USD"):
        arguments = {"amount": amount, "currency": currency}
        unknown = {"state": "unknown", "effect_key": effect_key}
        target = urlsplit(provider)
        if target.scheme != "http" or target.netloc != f"127.0.0.1:{target.port}" or target.path or target.query or target.fragment:
            raise ValueError("refund fixture requires its owned loopback destination")
        connection = HTTPConnection(target.hostname, target.port, timeout=120)
        try:
            if self.lookup_only:
                connection.request("GET", "/refunds?" + urlencode({"effect_key": effect_key}))
            else:
                body = json.dumps({"effect_key": effect_key, "arguments": arguments}).encode()
                connection.request("POST", "/refunds", body, {"Content-Type": "application/json"})
            response = connection.getresponse()
            # This fixture issues complete synchronous receipts only on these statuses.
            # Read a bounded response from this exact destination without following redirects.
            if response.status not in ((200,) if self.lookup_only else (200, 201)):
                return unknown
            length = int(response.getheader("Content-Length", "0"))
            if not 0 < length <= 8192:
                return unknown
            body = response.read(8193)
            if len(body) != length:
                return unknown
            receipt = json.loads(body)
        except (OSError, HTTPException, ValueError):
            return unknown
        finally:
            connection.close()
        if (not isinstance(receipt, dict) or receipt.get("effect_key") != effect_key
                or json.dumps(receipt.get("arguments"), sort_keys=True) != json.dumps(arguments, sort_keys=True)
                or not isinstance(receipt.get("refund_id"), str) or not receipt["refund_id"]):
            return unknown
        return {"state": "confirmed", "receipt": receipt}


@workflow.entrypoint(Entry.PROPOSE, default=True)
def propose(event, ctx):
    data = event["data"]
    action = ApprovalAction.for_callable(Refund(False).execute, version="1", arguments={
        "provider": data["provider"], "effect_key": data["effect_key"], "amount": data["amount"],
    })
    return ctx.request_approval("refund", action=action, resume=Entry.APPLY, state=None, timeout_ms=300_000)


@workflow.entrypoint(Entry.APPLY)
async def apply(event, ctx):
    if ctx.approval.status != ApprovalStatus.APPROVED:
        return ctx.fail("refund_not_approved", "Approval was required for this refund")
    attempt_id = event["ldgattemptid"]

    def claim_send(*, action):
        # Bind the immutable action as input. Capture the winning attempt as output,
        # so subsequent attempts replay the same owner rather than changing the binding.
        return attempt_id

    owner = await ctx.local("refund-intent", claim_send, action=ctx.approval.action.to_dict())
    with Path(event["data"]["marker"]).open("a", encoding="utf-8") as stream:
        stream.write(json.dumps({"tag": event["data"]["effect_key"], "kind": "intent",
            "activation": ctx.activation_id, "attempt": event["ldgattemptno"],
            "attempt_id": attempt_id, "owner": owner, "pid": os.getpid()}) + "\n")
    result = await ctx.approved_local(Refund(lookup_only=owner != attempt_id).execute, version="1")
    if result["state"] == "unknown":
        return park(ctx, result)
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
