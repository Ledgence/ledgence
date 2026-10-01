"""Review one immutable change in parallel, then wait for its human decision (MIT)."""
from enum import StrEnum

from ledgence.worker.workflow import Workflow

from change_review.candidate import validate_candidate
from change_review.config import AGENT_QUEUE, APPROVAL_KEY, CONTROL_QUEUE, FINALIZE, IMPLEMENT, VERSION
from change_review.inputs import submission
from change_review.steps import assemble_bundle, compare_candidate, draft_note, review_candidate, run_tests
from change_review.validation import fields, text


class Entry(StrEnum):
    START = "start"
    CHECK_CANDIDATE = "check_candidate"
    RUN_TESTS = "run_tests"
    REVIEW_CODE = "review_code"
    DRAFT_NOTE = "draft_note"
    PREPARE_REVIEW = "prepare_review"
    AWAIT_DECISION = "await_decision"
    ON_DECISION = "on_decision"
    FINISH = "finish"


workflow = Workflow(Entry)
PACKET_FIELDS = ("workflow_id", "candidate", "comparison", "tests", "review", "note", "decision")
CHECK_KEYS = ("tests:0", "review:0", "note:0")


def pending_packet(value, workflow_id):
    """Rebuild every derived field before trusting a saved review packet."""
    if type(value) is not dict or not set(PACKET_FIELDS) <= value.keys():
        raise ValueError("the accepted review packet is missing")
    data = {key: value[key] for key in PACKET_FIELDS}
    if data["workflow_id"] != workflow_id or data["decision"] is not None:
        raise ValueError("review packet must belong to this undecided workflow")
    bundle = assemble_bundle(data)
    if value != bundle:
        raise ValueError("review packet does not match its validated evidence")
    return bundle


def successful(ctx, key):
    return ctx.inputs.get(key, {}).get("state") == "succeeded"


def finalize(ctx, key, data, continuation):
    task = ctx.task(key, program=FINALIZE, version=VERSION, queue=CONTROL_QUEUE,
                    data=data, retry_policy={"max_attempts": 2, "retry_delay_ms": 1000},
                    attempt_timeout_ms=180_000)
    return ctx.suspend(continuation=continuation, state={}, until=[task])


@workflow.entrypoint(Entry.START, default=True)
def start(event, ctx):
    try:
        data = submission(event.get("data"))
    except ValueError as error:
        return ctx.fail("invalid_input", str(error))
    task = ctx.task("implement:0", program=IMPLEMENT, version=VERSION, queue=AGENT_QUEUE,
                    data={"change_id": data["change_id"], "model": data["model"]},
                    retry_policy={"max_attempts": 1, "retry_delay_ms": 0}, attempt_timeout_ms=180_000)
    return ctx.suspend(continuation=Entry.CHECK_CANDIDATE, state={}, until=[task])


@workflow.entrypoint(Entry.CHECK_CANDIDATE)
async def check_candidate(event, ctx):
    if not successful(ctx, "implement:0"):
        return ctx.fail("implementation_failed", "Inspect the implementation task's terminal outcome")
    try:
        data = submission(event.get("data"))
        candidate = validate_candidate(ctx.get_result("implement:0"))
        if candidate["change_id"] != data["change_id"]:
            raise ValueError("candidate does not match the requested change")
    except ValueError as error:
        return ctx.fail("invalid_candidate", str(error))
    checks = await ctx.fork("checks:0", branches=[
        ctx.branch("tests:0", entrypoint=Entry.RUN_TESTS, queue=CONTROL_QUEUE,
                   data={"candidate": candidate},
                   retry_policy={"max_attempts": 1, "retry_delay_ms": 0}, attempt_timeout_ms=60_000),
        ctx.branch("review:0", entrypoint=Entry.REVIEW_CODE, queue=AGENT_QUEUE,
                   data={"candidate": candidate, "model": data["model"]},
                   retry_policy={"max_attempts": 1, "retry_delay_ms": 0}, attempt_timeout_ms=180_000),
        ctx.branch("note:0", entrypoint=Entry.DRAFT_NOTE, queue=AGENT_QUEUE,
                   data={"candidate": candidate, "model": data["model"]},
                   retry_policy={"max_attempts": 1, "retry_delay_ms": 0}, attempt_timeout_ms=180_000),
    ])
    # The acknowledged fork starts independent work. This local operation stays
    # on the parent, then join checkpoints until every branch is terminal.
    comparison = await ctx.local("compare:0", compare_candidate, candidate=candidate)
    return ctx.join(checks, resume=Entry.PREPARE_REVIEW,
                    state={"candidate": candidate, "comparison": comparison})


@workflow.entrypoint(Entry.RUN_TESTS)
async def test_branch(event, ctx):
    return ctx.complete(await ctx.local("tests:0", run_tests, candidate=event["data"]["candidate"]))


@workflow.entrypoint(Entry.REVIEW_CODE)
async def review_branch(event, ctx):
    return ctx.complete(await ctx.local("review:0", review_candidate,
                                       candidate=event["data"]["candidate"], model=event["data"]["model"]))


@workflow.entrypoint(Entry.DRAFT_NOTE)
async def note_branch(event, ctx):
    return ctx.complete(await ctx.local("note:0", draft_note,
                                       candidate=event["data"]["candidate"], model=event["data"]["model"]))


@workflow.entrypoint(Entry.PREPARE_REVIEW)
def prepare_review(event, ctx):
    for key in CHECK_KEYS:
        if not successful(ctx, key):
            return ctx.fail("check_failed", f"Inspect the {key} branch's terminal outcome")
    try:
        state = ctx.state
        fields(state, {"candidate", "comparison"})
        data = {"workflow_id": ctx.workflow_id, **state, "decision": None,
                "tests": ctx.get_result("tests:0"), "review": ctx.get_result("review:0"),
                "note": ctx.get_result("note:0")}
        # Validation here prevents an unusable packet from reaching the human.
        assemble_bundle(data)
    except ValueError as error:
        return ctx.fail("invalid_packet", str(error))
    # An ordinary task output makes the packet retrievable through the public
    # client API while this workflow is still waiting for a human event.
    return finalize(ctx, "prepare:0", data, Entry.AWAIT_DECISION)


@workflow.entrypoint(Entry.AWAIT_DECISION)
def await_decision(event, ctx):
    if not successful(ctx, "prepare:0"):
        return ctx.fail("preparation_failed", "Inspect the review packet task's terminal outcome")
    try:
        data = submission(event.get("data"))
        bundle = pending_packet(ctx.get_result("prepare:0"), ctx.workflow_id)
        if bundle["change_id"] != data["change_id"]:
            raise ValueError("review packet does not match the requested change")
    except ValueError as error:
        return ctx.fail("invalid_packet", str(error))
    if bundle["status"] == "needs_changes":
        return ctx.complete(bundle)
    if bundle["status"] != "waiting_for_approval":
        return ctx.fail("invalid_packet", "Expected an undecided review packet")
    return ctx.wait_event(APPROVAL_KEY, continuation=Entry.ON_DECISION,
                          state={"bundle": bundle}, timeout_ms=data["approval_timeout_ms"])


@workflow.entrypoint(Entry.ON_DECISION)
def on_decision(event, ctx):
    try:
        data = submission(event.get("data"))
        fields(ctx.state, {"bundle"})
        bundle = pending_packet(ctx.state["bundle"], ctx.workflow_id)
        if bundle["change_id"] != data["change_id"] or bundle["status"] != "waiting_for_approval":
            raise ValueError("only this change's passing review packet can be decided")
    except ValueError as error:
        return ctx.fail("invalid_packet", str(error))
    wake = ctx.wake
    if type(wake) is not dict or wake.get("key") != APPROVAL_KEY:
        return ctx.fail("invalid_decision", "Expected the approval:0 event or timeout")
    decision = {"workflow_id": ctx.workflow_id, "candidate_sha256": bundle["candidate"]["sha256"],
                "outcome": "expired", "event_id": None}
    if wake.get("kind") == "event":
        try:
            envelope = wake["event"]
            approval = envelope.get("data")
            fields(approval, {"workflow_id", "candidate_sha256", "approved"})
            if (approval["workflow_id"] != ctx.workflow_id
                    or approval["candidate_sha256"] != bundle["candidate"]["sha256"]
                    or type(approval["approved"]) is not bool):
                raise ValueError("decision must identify this workflow and exact candidate, with a boolean approved")
            decision.update(outcome="approved" if approval["approved"] else "rejected",
                            event_id=text(envelope.get("id"), 128))
        except ValueError as error:
            return ctx.fail("invalid_decision", str(error))
    elif wake.get("kind") != "timeout":
        return ctx.fail("invalid_decision", "Expected an approval event or timeout")
    final_data = {key: bundle[key] for key in PACKET_FIELDS}
    final_data["decision"] = decision
    if decision["outcome"] == "approved" and "publication" in data:
        final_data["publication"] = data["publication"]
    return finalize(ctx, "finalize:0", final_data, Entry.FINISH)


@workflow.entrypoint(Entry.FINISH)
def finish(event, ctx):
    if not successful(ctx, "finalize:0"):
        return ctx.fail("finalization_failed", "Inspect the final task's terminal outcome before reconciling")
    return ctx.complete(ctx.get_result("finalize:0"))


handle = workflow.build()
