"""A workflow reuses the greeting task and completes when it succeeds."""
from enum import StrEnum

from ledgence.worker.workflow import Workflow


class Entry(StrEnum):
    START = "start"
    FINISH = "finish"


workflow = Workflow(Entry)


@workflow.entrypoint(Entry.START, default=True)
async def start(event, ctx):
    # Describe a child task. The checkpoint makes its creation durable.
    greeting = ctx.task(
        "greeting",
        program="hello-ledgence",
        version="0.1.0",
        queue="demo",
        data=event["data"],
    )
    # End this activation and resume at FINISH when the child is terminal.
    return ctx.suspend(continuation=Entry.FINISH, state={}, until=[greeting])


@workflow.entrypoint(Entry.FINISH)
async def finish(event, ctx):
    # A terminal child may have failed or been cancelled; check before reading.
    if ctx.inputs["greeting"]["state"] != "succeeded":
        return ctx.fail("greeting_failed", "The greeting task did not succeed.")
    return ctx.complete(ctx.get_result("greeting"))


handle = workflow.build()
