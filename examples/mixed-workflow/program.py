"""One local computation and two owned branches with explicit durable joins (MIT)."""
from enum import StrEnum

from ledgence.worker.workflow import Workflow


class Entry(StrEnum):
    MAIN = "main"
    DOUBLE = "double"
    DOUBLE_READY = "double_ready"
    TRIPLE = "triple"
    COLLECT = "collect"


workflow = Workflow(Entry)


def input_values(data):
    if type(data) is not dict:
        raise ValueError("data must be an object")
    values = data.get("values")
    if (type(values) is not list or not 1 <= len(values) <= 1000
            or any(type(value) is not int or abs(value) > 1_000_000 for value in values)):
        raise ValueError("values must contain 1 to 1000 integers between -1000000 and 1000000")
    return values


def summarize(values):
    return {"count": len(values), "sum": sum(values)}


@workflow.entrypoint(Entry.MAIN, default=True)
async def main(event, ctx):
    data = event.get("data")
    try:
        values = input_values(data)
    except ValueError as error:
        return ctx.fail("invalid_input", str(error))
    queue = data.get("queue")
    if (type(queue) is not str or not 1 <= len(queue) <= 128
            or any(ord(char) < 32 or ord(char) >= 127 for char in queue)):
        return ctx.fail("invalid_input", "queue must contain 1 to 128 printable ASCII characters")
    branch_data = {"values": values}
    branches = await ctx.fork("calculations:0", branches=[
        ctx.branch("double:0", entrypoint=Entry.DOUBLE,
                   queue=queue, data=branch_data),
        ctx.branch("triple:0", entrypoint=Entry.TRIPLE,
                   queue=queue, data=branch_data),
    ])
    # Registration has committed, so workers can execute both branches while
    # this activation does local work. One worker slot is sufficient to finish.
    local = await ctx.local("summary", summarize, values=values)
    return ctx.join(branches, resume=Entry.COLLECT, state={"local": local})


@workflow.entrypoint(Entry.DOUBLE)
def double(event, ctx):
    # This child owns its timer and checkpoint independently of the parent.
    return ctx.sleep("ready:0", 100, continuation=Entry.DOUBLE_READY,
                     state={"sum": sum(event["data"]["values"])})


@workflow.entrypoint(Entry.DOUBLE_READY)
def double_ready(event, ctx):
    return ctx.complete(2 * ctx.state["sum"])


@workflow.entrypoint(Entry.TRIPLE)
def triple(event, ctx):
    return ctx.complete(3 * sum(event["data"]["values"]))


@workflow.entrypoint(Entry.COLLECT)
def collect(event, ctx):
    outcomes = ctx.inputs
    for key in ("double:0", "triple:0"):
        if outcomes[key]["state"] == "failed":
            return ctx.fail("branch_failed", key + " failed; inspect its terminal outcome")
        if outcomes[key]["state"] == "cancelled":
            return ctx.fail("branch_cancelled", key + " was cancelled")
    return ctx.complete({"local": ctx.state["local"],
                         "double": ctx.get_result("double:0"),
                         "triple": ctx.get_result("triple:0")})


handle = workflow.build()
