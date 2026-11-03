"""Record which callback the migrated orchestrator permits; no external service (MIT)."""
from enum import StrEnum
from pathlib import Path

from ledgence.worker.workflow import ApprovalAction, ApprovalStatus, Workflow


class Entry(StrEnum):
    REQUEST = "request"
    APPLY = "apply"


workflow = Workflow(Entry)


def mark(path, callback):
    with Path(path).open("a", encoding="utf-8") as log:
        print(callback, file=log)
    return callback


def write(*, marker):
    return mark(marker, "dispatch")


def lookup(*, marker):
    return mark(marker, "lookup")


@workflow.entrypoint(Entry.REQUEST, default=True)
def request(event, ctx):
    action = ApprovalAction.for_callable(write, version="1", arguments={"marker": event["data"]["marker"]})
    return ctx.request_approval("write", action=action, resume=Entry.APPLY, state=None, timeout_ms=300_000)


@workflow.entrypoint(Entry.APPLY)
async def apply(event, ctx):
    if ctx.approval.status != ApprovalStatus.APPROVED:
        return ctx.fail("not_approved", "The fixture requires an approval")
    return ctx.complete(await ctx.approved_effect(write, version="1", lookup=lookup))


handle = workflow.build()
