"""Implement a change, test locally while reviewing remotely, then publish evidence."""
from enum import StrEnum

from ledgence.worker.workflow import Workflow

from change_review.config import AGENT_QUEUE, CONTROL_QUEUE, DEFAULT_MODEL, FINALIZE, IMPLEMENT, VERSION
from change_review.inputs import submission
from change_review.steps import review_candidate, run_tests


class Entry(StrEnum):
    START = "start"
    VALIDATE = "validate"
    REVIEW = "review"
    COLLECT = "collect"
    FINISH = "finish"


workflow = Workflow(Entry)


@workflow.entrypoint(Entry.START, default=True)
def start(event, ctx):
    try:
        data = submission(event.get("data"))
    except ValueError as error:
        return ctx.fail("invalid_input", str(error))
    task = ctx.task("implement:0", program=IMPLEMENT, version=VERSION, queue=AGENT_QUEUE,
                    data={"change_id": data["change_id"], "model": data.get("model", DEFAULT_MODEL)},
                    retry_policy={"max_attempts": 1, "retry_delay_ms": 0}, attempt_timeout_ms=180_000)
    return ctx.suspend(continuation=Entry.VALIDATE, state={}, until=[task])


@workflow.entrypoint(Entry.VALIDATE)
async def validate(event, ctx):
    if ctx.inputs["implement:0"]["state"] != "succeeded":
        return ctx.fail("implementation_failed", "Inspect the implementation task's terminal outcome")
    candidate = ctx.get_result("implement:0")
    reviews = await ctx.fork("validate:0", branches=[
        ctx.branch("review:0", entrypoint=Entry.REVIEW, queue=AGENT_QUEUE,
                   data={"candidate": candidate, "model": event["data"].get("model", DEFAULT_MODEL)},
                   retry_policy={"max_attempts": 1, "retry_delay_ms": 0}, attempt_timeout_ms=180_000),
    ])
    tests = await ctx.local("tests:0", run_tests, candidate=candidate)
    return ctx.join(reviews, resume=Entry.COLLECT, state={"candidate": candidate, "tests": tests})


@workflow.entrypoint(Entry.REVIEW)
async def review(event, ctx):
    return ctx.complete(await ctx.local("review:0", review_candidate,
                                       candidate=event["data"]["candidate"], model=event["data"]["model"]))


@workflow.entrypoint(Entry.COLLECT)
def collect(event, ctx):
    if ctx.inputs["review:0"]["state"] != "succeeded":
        return ctx.fail("review_failed", "Inspect the review branch's terminal outcome")
    data = {**ctx.state, "review": ctx.get_result("review:0")}
    if "publication" in event["data"]:
        data["publication"] = event["data"]["publication"]
    task = ctx.task("finalize:0", program=FINALIZE, version=VERSION, queue=CONTROL_QUEUE,
                    data=data, retry_policy={"max_attempts": 2, "retry_delay_ms": 1000},
                    attempt_timeout_ms=180_000)
    return ctx.suspend(continuation=Entry.FINISH, state={}, until=[task])


@workflow.entrypoint(Entry.FINISH)
def finish(event, ctx):
    if ctx.inputs["finalize:0"]["state"] != "succeeded":
        return ctx.fail("finalization_failed", "Inspect the final task's terminal outcome before reconciling")
    return ctx.complete(ctx.get_result("finalize:0"))


handle = workflow.build()
