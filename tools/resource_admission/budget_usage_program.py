"""Report known fixture token counts through a real governed worker callback (MIT)."""

from enum import StrEnum

from ledgence.worker.admission import (
    AdmissionOutcomeKind,
    AdmittedResult,
    RunBudgetUsage,
)
from ledgence.worker.workflow import OperationKind, Workflow


class Entry(StrEnum):
    REQUEST = "request"
    FINISH = "finish"


workflow = Workflow(Entry)


@workflow.operation(kind=OperationKind.MODEL, version="1")
def summarize(*, provenance):
    usage = (
        RunBudgetUsage.reported
        if provenance == "reported"
        else RunBudgetUsage.estimated
    )(input_tokens=7, output_tokens=3)
    return AdmittedResult({"review": "complete"}, usage=usage)


@workflow.entrypoint(Entry.REQUEST, default=True)
def request(event, ctx):
    return ctx.request_admission(
        "summary",
        resource="support-model",
        operation=summarize.bind(provenance=event["data"]["provenance"]),
        resume=Entry.FINISH,
        state={},
        timeout_ms=60000,
    )


@workflow.entrypoint(Entry.FINISH)
async def finish(event, ctx):
    result = await ctx.admitted_operation(summarize)
    if result.kind == AdmissionOutcomeKind.SUCCEEDED:
        return ctx.complete(result.result)
    if result.kind in (
        AdmissionOutcomeKind.WAIT_REQUIRED,
        AdmissionOutcomeKind.NEEDS_REVIEW,
    ):
        return ctx.wait_admission(resume=Entry.FINISH, state={}, timeout_ms=60000)
    return ctx.fail("summary_closed", "The summary was not authorized")


handle = workflow.build()
