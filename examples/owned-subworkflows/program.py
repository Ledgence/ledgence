"""Join a page-processing subworkflow and an ordinary task at concurrency one."""
from enum import StrEnum

from ledgence.worker.workflow import Workflow


class Entry(StrEnum):
    START = "start"
    COLLECT = "collect"


workflow = Workflow(Entry)


@workflow.entrypoint(Entry.START, default=True)
def start(event, ctx):
    data = event.get("data")
    if type(data) is not dict or type(data.get("urls")) is not list:
        return ctx.fail("invalid_input", "data must contain a urls list and queue")
    if (not 1 <= len(data["urls"]) <= 4
            or any(type(url) is not str or not 1 <= len(url) <= 2048 for url in data["urls"])):
        return ctx.fail("invalid_input", "urls must contain one to four URL strings of at most 2048 characters")
    queue = data.get("queue")
    if (type(queue) is not str or not 1 <= len(queue) <= 128
            or any(ord(char) < 32 or ord(char) >= 127 for char in queue)):
        return ctx.fail("invalid_input", "queue must contain 1 to 128 printable ASCII characters")
    # The page workflow validates individual URLs and owns its HTTP retries.
    pages = ctx.workflow("pages", program="workflow-example", version="1.0.1",
                         queue=queue, data={"urls": data["urls"], "queue": queue})
    metadata = ctx.task("metadata", program="workflow-summary", version="1.0.0",
                        queue=queue, data={"pages": ["owned subworkflow example"]})
    return ctx.suspend(continuation=Entry.COLLECT, state={}, until=[pages, metadata])


@workflow.entrypoint(Entry.COLLECT)
def collect(event, ctx):
    outcomes = ctx.inputs
    for key in ("pages", "metadata"):
        if outcomes[key]["state"] == "failed":
            return ctx.fail("child_failed", key + " failed; inspect its terminal outcome")
        if outcomes[key]["state"] == "cancelled":
            return ctx.fail("child_cancelled", key + " was cancelled")
    return ctx.complete({"pages": ctx.get_result("pages"),
                         "metadata": ctx.get_result("metadata")})


handle = workflow.build()
