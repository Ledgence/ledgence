"""Generate a report, then move its reference to a different worker queue (MIT)."""
from enum import StrEnum
from pathlib import Path
from tempfile import TemporaryDirectory

from ledgence.worker import artifacts
from ledgence.worker.workflow import Workflow


class Entry(StrEnum):
    CREATE = "create"
    TRANSFER = "transfer"
    COMPLETE = "complete"


workflow = Workflow(Entry)


@workflow.entrypoint(Entry.CREATE, default=True)
async def create(event, ctx):
    # Workflow activations may also publish files. The server derives the
    # activation producer and workflow root; applications never supply these IDs.
    with TemporaryDirectory() as directory:
        note = Path(directory) / "note.txt"
        note.write_text("Report generation requested.\n")
        note_ref = await artifacts.put_file(note, key="request-note", media_type="text/plain")
    report = ctx.task("report", program="artifact-producer", version="1.0.0",
                      queue=event["data"]["producer_queue"], data={"size": event["data"].get("size", 300_000)},
                      retry_policy={"max_attempts": 2, "retry_delay_ms": 0})
    return ctx.suspend(continuation=Entry.TRANSFER, state={"note": note_ref}, until=[report])


@workflow.entrypoint(Entry.TRANSFER)
async def transfer(event, ctx):
    if ctx.inputs["report"]["state"] != "succeeded":
        return ctx.fail("report_failed", "Inspect the report task's outcome before retrying.")
    reference = ctx.get_result("report")["artifact"]
    check = ctx.task("check", program="artifact-consumer", version="1.0.0",
                     queue=event["data"]["consumer_queue"], data={"artifact": reference})
    return ctx.suspend(continuation=Entry.COMPLETE,
                       state={"note": ctx.state["note"], "report": reference}, until=[check])


@workflow.entrypoint(Entry.COMPLETE)
async def complete(event, ctx):
    if ctx.inputs["check"]["state"] != "succeeded":
        return ctx.fail("download_failed", "Inspect the consumer task's outcome.")
    return ctx.complete({"note": ctx.state["note"], "report": ctx.state["report"],
                         "verified": ctx.get_result("check")})


handle = workflow.build()
