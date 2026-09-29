"""Real SDK release-review workflow used by Console acceptance (MIT)."""
from enum import StrEnum

from ledgence.worker.workflow import Workflow


class Entry(StrEnum):
    START = "start"
    AFTER_PREPARE = "after_prepare"
    PUBLISH_DRAFT = "publish_draft"
    REVIEW = "review"
    PUBLISH_REPORT = "publish_report"
    FINISH = "finish"
    SECURITY = "security_review"
    TESTS = "test_suite"
    DEPENDENCIES = "dependency_audit"
    DOCS = "docs_review"


workflow = Workflow(Entry)
BRANCHES = (
    ("security:0", Entry.SECURITY, "Security review"),
    ("tests:0", Entry.TESTS, "Test suite"),
    ("dependencies:0", Entry.DEPENDENCIES, "Dependency audit"),
    ("docs:0", Entry.DOCS, "Docs review"),
)
POLICY = {"max_attempts": 1, "retry_delay_ms": 0}


def task(ctx, data, key, operation, **payload):
    return ctx.task(key, program="explorer-release-task", version="1.0.0",
                    queue=data["queue"], data={"operation": operation, **payload},
                    retry_policy=POLICY)


def unsuccessful(ctx, key):
    state = ctx.inputs[key]["state"]
    return None if state == "succeeded" else ctx.fail("task_" + state, key)


@workflow.entrypoint(Entry.START, default=True)
async def start(event, ctx):
    data = event["data"]
    fork = await ctx.fork("release-checks:0", branches=[
        ctx.branch(key, entrypoint=entry, queue=data["queue"], data=data,
                   retry_policy=POLICY)
        for key, entry, _ in BRANCHES
    ])
    prepare = task(ctx, data, "prepare:0", "prepare", release=data["release"],
                   sequence=data["sequence"])
    # Persist only durable identities. ForkRef belongs to this activation and
    # must not be reconstructed or reused by the later after_prepare handler.
    return ctx.suspend(continuation=Entry.AFTER_PREPARE,
                       state={"branch_keys": list(fork.branch_keys)}, until=[prepare])


@workflow.entrypoint(Entry.AFTER_PREPARE)
def after_prepare(event, ctx):
    if "prepared" in ctx.state:
        prepared = ctx.state["prepared"]
    else:
        failed = unsuccessful(ctx, "prepare:0")
        if failed:
            return failed
        prepared = ctx.get_result("prepare:0")
        if event["data"]["mode"] == "join_satisfied":
            # A separate acceptance case releases this real event only after
            # all four branches are terminal. No scheduling-order assumption.
            return ctx.wait_event("join-ready:0", continuation=Entry.AFTER_PREPARE,
                state={**ctx.state, "prepared": prepared})
    return ctx.suspend(continuation=Entry.PUBLISH_DRAFT,
                       state={**ctx.state, "prepared": prepared},
                       until=ctx.state["branch_keys"])


@workflow.entrypoint(Entry.PUBLISH_DRAFT)
def publish_draft(event, ctx):
    for key in ctx.state["branch_keys"]:
        state = ctx.inputs[key]["state"]
        if state != "succeeded":
            return ctx.fail("branch_" + state, key)
    checks = {key: ctx.get_result(key) for key in ctx.state["branch_keys"]}
    if not all(check["passed"] is True for check in checks.values()):
        return ctx.fail("checks_rejected", "A release check did not pass")
    draft = task(ctx, event["data"], "publish-draft:0", "publish_draft",
                 prepared=ctx.state["prepared"], checks=checks)
    return ctx.suspend(continuation=Entry.REVIEW, state={}, until=[draft])


@workflow.entrypoint(Entry.REVIEW)
def review(event, ctx):
    failed = unsuccessful(ctx, "publish-draft:0")
    if failed:
        return failed
    draft = ctx.get_result("publish-draft:0")
    review_task = task(ctx, event["data"], "review-draft:0", "review_draft",
                       draft=draft, approve=event["data"]["mode"] != "rejected")
    return ctx.suspend(continuation=Entry.PUBLISH_REPORT, state={"draft": draft},
                       until=[review_task])


@workflow.entrypoint(Entry.PUBLISH_REPORT)
def publish_report(event, ctx):
    failed = unsuccessful(ctx, "review-draft:0")
    if failed:
        return failed
    review_result = ctx.get_result("review-draft:0")
    if review_result["approved"] is not True:
        return ctx.fail("review_rejected", "Review draft did not approve publication")
    if review_result["draft_id"] != ctx.state["draft"]["draft_id"]:
        return ctx.fail("review_mismatch", "Approval belongs to a different draft")
    final = task(ctx, event["data"], "publish-final:0", "publish_final",
                 draft=ctx.state["draft"], review=review_result)
    return ctx.suspend(continuation=Entry.FINISH, state={}, until=[final])


@workflow.entrypoint(Entry.FINISH)
def finish(event, ctx):
    failed = unsuccessful(ctx, "publish-final:0")
    return failed or ctx.complete(ctx.get_result("publish-final:0"))


def check(ctx, data, name):
    if name == "Security review":
        if data["mode"] == "branch_failed":
            return ctx.fail("security_check_failed", "Synthetic release check failure")
        if data["mode"] == "branch_cancelled":
            # Acceptance cancels this *child* through the real HTTP API once
            # the wait is persisted, leaving the parent to inspect its outcome.
            return ctx.wait_event("cancel-check:0", continuation=Entry.SECURITY, state={})
    return ctx.complete({"check": name, "passed": True, "release": data["release"]})


@workflow.entrypoint(Entry.SECURITY)
def security_review(event, ctx):
    return check(ctx, event["data"], "Security review")


@workflow.entrypoint(Entry.TESTS)
def test_suite(event, ctx):
    return check(ctx, event["data"], "Test suite")


@workflow.entrypoint(Entry.DEPENDENCIES)
def dependency_audit(event, ctx):
    return check(ctx, event["data"], "Dependency audit")


@workflow.entrypoint(Entry.DOCS)
def docs_review(event, ctx):
    return check(ctx, event["data"], "Docs review")


handle = workflow.build()
