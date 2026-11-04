"""Review two support tickets under a shared allowance (MIT)."""

from enum import StrEnum

from ledgence.worker.workflow import Workflow


class Entry(StrEnum):
    REVIEW = "review"
    COLLECT = "collect"


workflow = Workflow(Entry)


@workflow.entrypoint(Entry.REVIEW, default=True)
def review(event, ctx):
    data = event["data"]
    if (
        not isinstance(data, dict)
        or not isinstance(data.get("queue"), str)
        or not data["queue"]
    ):
        return ctx.fail("invalid_input", "Supply the worker queue and ticket texts.")
    tickets = data.get("tickets")
    if (
        not isinstance(tickets, list)
        or len(tickets) != 2
        or not all(isinstance(ticket, str) and ticket for ticket in tickets)
    ):
        return ctx.fail("invalid_tickets", "Supply two nonempty ticket texts.")
    # Each child uses the existing classifier program. Both inherit this run's budget.
    children = [
        ctx.workflow(
            f"ticket-{index}",
            program="resource-admission",
            version="1.0.0",
            queue=data["queue"],
            data={"endpoint": data["endpoint"], "text": ticket},
        )
        for index, ticket in enumerate(tickets, start=1)
    ]
    return ctx.suspend(continuation=Entry.COLLECT, state={}, until=children)


@workflow.entrypoint(Entry.COLLECT)
def collect(event, ctx):
    # The operator can follow either execution to inspect its result or refusal.
    decisions = {}
    for key, terminal in ctx.inputs.items():
        decisions[key] = {"state": terminal["state"]}
        if terminal["state"] == "succeeded":
            decisions[key]["classification"] = ctx.get_result(key)
    return ctx.complete(decisions)


handle = workflow.build()
