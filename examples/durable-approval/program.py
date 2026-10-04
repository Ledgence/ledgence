"""Normalize a simulated refund, durably approve it, then execute saved args (MIT)."""
from enum import StrEnum

from ledgence.worker.workflow import ApprovalAction, ApprovalStatus, Workflow


class Entry(StrEnum):
    REQUEST = "request"
    RESOLVE = "resolve"


workflow = Workflow(Entry)


def simulate_refund(amount, currency="USD"):
    """A leaf operation with no payment provider or real financial effect."""
    return {"simulated": True, "amount": amount, "currency": currency}


@workflow.entrypoint(Entry.REQUEST, default=True)
async def request(event, ctx):
    data = event.get("data")
    if type(data) is not dict or type(data.get("amount")) is not int or not 1 <= data["amount"] <= 10_000:
        return ctx.fail("invalid_input", "amount must be an integer between 1 and 10000")
    timeout_ms = data.get("timeout_ms", 86_400_000)
    if type(timeout_ms) is not int or not 0 <= timeout_ms <= 31_536_000_000:
        return ctx.fail("invalid_input", "timeout_ms must be an integer from 0 through 31536000000")
    original = {"amount": data["amount"]}
    # Domain normalization runs before the request is stored or displayed.
    action = ApprovalAction.for_callable(
        simulate_refund, version="1", arguments={"amount": min(original["amount"], 50)},
    )
    return ctx.request_approval("refund", action=action, proposed_arguments=original,
                                resume=Entry.RESOLVE, state=None, timeout_ms=timeout_ms)


@workflow.entrypoint(Entry.RESOLVE)
async def resolve(event, ctx):
    approval = ctx.approval
    if approval is None:
        return ctx.fail("missing_approval", "expected an approval wake")
    if approval.status is ApprovalStatus.APPROVED:
        # No new arguments are accepted here. The helper reads the private,
        # authoritative wake and reuses a durable local result on replay.
        result = await ctx.approved_local(simulate_refund, version="1")
        return ctx.complete({"status": approval.status.value, "result": result,
                             "proposed_arguments": approval.proposed_arguments,
                             "approved_arguments": approval.action.arguments})
    return ctx.complete({"status": approval.status.value, "executed": False})


handle = workflow.build()
