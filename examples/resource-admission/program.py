"""Share model-call capacity without keeping a worker idle (MIT)."""
from enum import StrEnum
import asyncio
import http.client
import json
from urllib.parse import urlsplit

from ledgence.worker.admission import AdmissionOutcomeKind
from ledgence.worker.workflow import OperationKind, Workflow


class Entry(StrEnum):
    REQUEST = "request"
    CALL = "call"


workflow = Workflow(Entry)


def exchange(endpoint, text):
    """One bounded local HTTP request, with no automatic retry or redirect."""
    origin = urlsplit(endpoint)
    if (origin.scheme != "http" or origin.hostname != "127.0.0.1" or not origin.port
            or origin.path or origin.query or origin.fragment or origin.username):
        raise ValueError("use the synthetic provider's http://127.0.0.1:PORT origin")
    connection = http.client.HTTPConnection(origin.hostname, origin.port, timeout=10)
    try:
        connection.request("POST", "/classify", json.dumps({"text": text}).encode(),
                           {"Content-Type": "application/json"})
        response = connection.getresponse()
        payload = response.read(8193)
        if response.status != 200 or len(payload) > 8192:
            raise RuntimeError("the provider did not return a bounded completed response")
        result = json.loads(payload)
        if type(result) is not dict or type(result.get("category")) is not str:
            raise ValueError("unexpected classification response")
        return result
    finally:
        connection.close()


@workflow.operation(kind=OperationKind.MODEL, version="1")
async def classify(*, endpoint, text):
    # The event loop remains available while this single HTTP request runs.
    # Replace the transport with your adapter; account for each hidden retry.
    return await asyncio.to_thread(exchange, endpoint, text)


@workflow.entrypoint(Entry.REQUEST, default=True)
async def request(event, ctx):
    data = event["data"]
    return ctx.request_admission(
        "classify:0", resource="support-model",
        operation=classify.bind(endpoint=data["endpoint"], text=data["text"]),
        resume=Entry.CALL, state={}, timeout_ms=60_000,
    )


@workflow.entrypoint(Entry.CALL)
async def call(event, ctx):
    outcome = await ctx.admitted_operation(classify)
    if outcome.kind == AdmissionOutcomeKind.SUCCEEDED:
        return ctx.complete(outcome.result)
    if outcome.kind in (AdmissionOutcomeKind.WAIT_REQUIRED, AdmissionOutcomeKind.NEEDS_REVIEW):
        # Keep the original operation identity. A new request would be new work.
        return ctx.wait_admission(resume=Entry.CALL, state={}, timeout_ms=60_000)
    return ctx.fail("admission_closed", "The admitted operation was closed without a result")


handle = workflow.build()
