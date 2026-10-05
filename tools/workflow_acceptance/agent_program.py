"""Offline per-call recovery fixture; all synthetic effects stay in the owned run (MIT)."""
import asyncio
from enum import StrEnum
import json
import os
from pathlib import Path
import time
import uuid

from ledgence.worker.workflow import OperationKind, Workflow


class Entry(StrEnum):
    RUN = "run"
    FINISH = "finish"


workflow = Workflow(Entry)


def mark(marker, tag, kind, **fields):
    with Path(marker).open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(dict(tag=tag, kind=kind, pid=os.getpid(), **fields)) + "\n")


async def rendezvous(gate):
    # The gate observes a precise phase, kills this process, then releases the
    # retry. Polling checks the handshake; elapsed time never triggers the fault.
    deadline = time.monotonic() + 150
    while not Path(gate).exists():
        if time.monotonic() >= deadline:
            raise RuntimeError("owned agent fault rendezvous was not released")
        await asyncio.sleep(0.02)


def model(*, marker, tag, request):
    token = uuid.uuid4().hex
    mark(marker, tag, "agent_model_called", token=token, request=request)
    return {"token": token, "tool_calls": [{"id": "lookup:0", "name": "lookup", "value": 7}]}


async def tool(*, marker, tag, call, model_token, receipt_path, idempotency_key, fault, gate):
    mark(marker, tag, "agent_tool_called", call=call, model_token=model_token,
         idempotency_key=idempotency_key)
    if fault == "model":
        mark(marker, tag, "agent_tool_unfinished")
        await rendezvous(gate)
    # An exclusive owned-file receipt models a provider accepting an idempotency
    # key. This demonstrates application reconciliation, not an exactly-once
    # guarantee from Ledgence or an external transactional database test.
    receipt = {"idempotency_key": idempotency_key, "model_token": model_token,
               "value": call["value"], "receipt": uuid.uuid4().hex}
    try:
        with Path(receipt_path).open("x", encoding="utf-8") as output:
            output.write(json.dumps(receipt))
            output.flush()
            os.fsync(output.fileno())
        mark(marker, tag, "agent_effect_created", receipt=receipt)
    except FileExistsError:
        receipt = json.loads(Path(receipt_path).read_text())
        if (receipt["idempotency_key"] != idempotency_key
                or receipt["model_token"] != model_token or receipt["value"] != call["value"]):
            raise RuntimeError("synthetic provider rejected changed idempotency binding")
        mark(marker, tag, "agent_effect_reconciled", receipt=receipt)
    if fault == "effect" and not Path(gate).exists():
        mark(marker, tag, "agent_effect_before_commit", receipt=receipt)
        await rendezvous(gate)
    return receipt


@workflow.entrypoint(Entry.RUN, default=True)
async def run(event, ctx):
    data = event["data"]
    evidence = dict(activation=ctx.activation_id, attempt=event["ldgattemptno"],
                    attempt_id=event["ldgattemptid"])
    mark(data["marker"], data["tag"], "agent_activation", **evidence)
    request = {"model": "offline-random-marker-v1", "messages": [{"role": "user", "content": "lookup 7"}],
               "temperature": 1 if data["fault"] == "binding" and event["ldgattemptno"] > 1 else 0}
    response = await ctx.operation("turn:0:model", model, kind=OperationKind.MODEL, version="1",
                                   arguments={"marker": data["marker"], "tag": data["tag"], "request": request})
    mark(data["marker"], data["tag"], "agent_model_ack", token=response["token"], **evidence)
    if data["fault"] == "binding":
        raise RuntimeError("retry with a deliberately changed model request")
    result = await ctx.operation("turn:0:tool:lookup:0", tool, kind=OperationKind.TOOL, version="1",
        arguments={"marker": data["marker"], "tag": data["tag"], "call": response["tool_calls"][0],
                   "model_token": response["token"], "receipt_path": data["receipt"],
                   "idempotency_key": ctx.workflow_id + ":" + ctx.activation_id + ":lookup:0",
                   "fault": data["fault"], "gate": data["gate"]})
    mark(data["marker"], data["tag"], "agent_tool_ack", receipt=result, **evidence)
    if data["fault"] == "tool":
        await rendezvous(data["gate"])
    return ctx.continue_(continuation=Entry.FINISH, state={"model": response, "tool": result})


@workflow.entrypoint(Entry.FINISH)
def finish(event, ctx):
    mark(event["data"]["marker"], event["data"]["tag"], "agent_finish",
         activation=ctx.activation_id, attempt=event["ldgattemptno"], attempt_id=event["ldgattemptid"])
    return ctx.complete(ctx.state)


handle = workflow.build()
