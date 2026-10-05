"""Offline approval fault fixture; marker is test evidence, not a payment (MIT)."""
import json
from enum import StrEnum
from pathlib import Path
from ledgence.worker.workflow import ApprovalAction, ApprovalStatus, Workflow

class Entry(StrEnum):
    REQUEST = "request"
    APPLY = "apply"

workflow = Workflow(Entry)

def mark(path, **fields):
    with Path(path).open("a") as output:
        output.write(json.dumps(fields) + "\n")

def refund(*, marker, amount, currency="USD"):
    mark(marker, kind="effect", amount=amount, currency=currency)
    return {"simulated": True, "amount": amount, "currency": currency}

@workflow.entrypoint(Entry.REQUEST, default=True)
def request(event, ctx):
    data = event["data"]
    action = ApprovalAction.for_callable(refund, version="1", arguments={
        "marker": data["marker"], "amount": min(data.get("amount", 100), 50)})
    return ctx.request_approval("refund", action=action, proposed_arguments={"amount":data.get("amount",100)},
        timeout_ms=data.get("timeout_ms",120000), resume=Entry.APPLY, state={"requested":True})

@workflow.entrypoint(Entry.APPLY)
async def apply(event, ctx):
    mark(event["data"]["marker"], kind="resume", activation=ctx.activation_id,
         attempt=event["ldgattemptno"], status=ctx.approval.status)
    if ctx.approval.status != ApprovalStatus.APPROVED:
        return ctx.complete({"status":ctx.approval.status.value,"executed":False})
    result = await ctx.approved_local(refund, version="1")
    if event["data"].get("fail_after_commit") and event["ldgattemptno"] == 1:
        raise RuntimeError("intentional failure after the approved local result committed")
    return ctx.complete({"status":"approved","result":result})

handle = workflow.build()
