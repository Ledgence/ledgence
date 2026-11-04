"""Single-send fixture for resource load qualification, published as task and workflow (MIT)."""

import asyncio
import http.client
import json
from enum import StrEnum

from ledgence.worker import current_invocation
from ledgence.worker.admission import AdmissionOutcomeKind
from ledgence.worker.workflow import OperationKind, Workflow


def send(data):
    # The test controls this loopback port. There are no network retries or redirects.
    invocation = current_invocation()
    payload = {
        **data,
        "task": invocation.task_id,
        "attempt": invocation.attempt_id,
        "workflow": invocation.workflow_id,
    }
    client = http.client.HTTPConnection("127.0.0.1", data["port"], timeout=45)
    try:
        client.request(
            "POST",
            "/work",
            json.dumps(payload).encode(),
            {"Content-Type": "application/json"},
        )
        reply = client.getresponse()
        body = json.loads(reply.read(4096))
        if reply.status not in (200, 429):
            raise RuntimeError("unexpected fixture response")
        return {
            "key": body["key"],
            "status": reply.status,
            "retry_after": int(reply.getheader("Retry-After", "0")),
        }
    finally:
        client.close()


def task(event):
    return send(event["data"])


class Entry(StrEnum):
    ISSUE = "issue"
    CALL = "call"


program = Workflow(Entry)


@program.operation(kind=OperationKind.MODEL, version="1")
async def call_provider(*, data):
    return await asyncio.to_thread(send, data)


@program.entrypoint(Entry.ISSUE, default=True)
def issue(event, ctx):
    number = (ctx.state or {}).get("number", 1)
    return ctx.request_admission(
        f"send-{number}",
        resource="load-hot",
        operation=call_provider.bind(data=event["data"]),
        resume=Entry.CALL,
        state={"number": number},
        timeout_ms=60_000,
    )


@program.entrypoint(Entry.CALL)
async def call(event, ctx):
    result = await ctx.admitted_operation(call_provider)
    if result.kind in (
        AdmissionOutcomeKind.WAIT_REQUIRED,
        AdmissionOutcomeKind.NEEDS_REVIEW,
    ):
        return ctx.wait_admission(resume=Entry.CALL, state=ctx.state, timeout_ms=60_000)
    if result.kind != AdmissionOutcomeKind.SUCCEEDED:
        return ctx.fail("closed", "The call was not permitted")
    if result.result["status"] == 200:
        return ctx.complete(result.result)
    if ctx.state["number"] == 2:
        return ctx.fail(
            "retry_limit",
            "The local provider remained throttled after one durable retry",
        )
    return ctx.sleep(
        "backoff",
        result.result["retry_after"] * 1000,
        continuation=Entry.ISSUE,
        state={"number": 2},
    )


workflow = program.build()
