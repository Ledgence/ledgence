"""Real-process fork acceptance fixture. All effects stay in the owned test directory."""
import asyncio
from enum import StrEnum
import json
import os
from pathlib import Path
import time

from ledgence.worker.workflow import Workflow


class Entry(StrEnum):
    ROOT = "root"
    LEFT = "left"
    RIGHT = "right"
    REMOTE_DONE = "remote_done"
    COLLECT = "collect"


workflow = Workflow(Entry)


def mark(data, kind, **fields):
    record = dict(tag=data["tag"], kind=kind, pid=os.getpid(), at=time.monotonic_ns(), **fields)
    with open(data["marker"], "a", encoding="utf-8") as stream:
        stream.write(json.dumps(record, separators=(",", ":")) + "\n")


def remote_starts(marker, tag):
    rows = Path(marker).read_text().splitlines(keepends=True)
    return {record["side"] for line in rows if line.endswith("\n")
            for record in [json.loads(line)]
            if record["tag"] == tag and record["kind"] == "fork_remote_started"}


async def local_work(*, marker, tag, require_overlap):
    data = dict(marker=marker, tag=tag)
    mark(data, "fork_local_started")
    deadline = time.monotonic() + 100
    while require_overlap and remote_starts(marker, tag) != {"left", "right"}:
        if time.monotonic() >= deadline:
            raise RuntimeError("remote branches did not start during local execution")
        await asyncio.sleep(0.02)
    mark(data, "fork_local_finished")
    return {"value": 11, "pid": os.getpid()}


@workflow.entrypoint(Entry.ROOT, default=True)
async def root(event, ctx):
    data = event["data"]
    mark(data, "fork_parent_started", activation=ctx.activation_id,
         attempt=event["ldgattemptno"], workflow=ctx.workflow_id)
    group = await ctx.fork("analysis", branches=[
        ctx.branch(side.value, entrypoint=side, queue=data["queue"], data=data,
                   retry_policy={"max_attempts": 3, "retry_delay_ms": 0})
        for side in (Entry.LEFT, Entry.RIGHT)
    ])
    mark(data, "fork_ack", activation=ctx.activation_id, attempt=event["ldgattemptno"])
    local = await ctx.local("normalize", local_work, marker=data["marker"], tag=data["tag"],
                            require_overlap=data.get("require_overlap", False))
    mark(data, "fork_local_ack", activation=ctx.activation_id, attempt=event["ldgattemptno"])
    if data.get("crash_gate") and event["ldgattemptno"] == 1:
        parent = os.getppid()
        deadline = time.monotonic() + 150
        while not Path(data["crash_gate"]).exists():
            if os.getppid() != parent or time.monotonic() >= deadline:
                raise RuntimeError("owned crash fixture was interrupted")
            await asyncio.sleep(0.02)
    return ctx.join(group, resume=Entry.COLLECT, state={"local": local})


async def branch(event, ctx, side):
    data = event["data"]
    # A test-only rendezvous proves both branch invocations overlap the live
    # parent local step; it is never used by the one-slot recovery scenarios.
    deadline = time.monotonic() + 100
    while data.get("require_overlap"):
        lines = Path(data["marker"]).read_text().splitlines(keepends=True)
        if any(record["tag"] == data["tag"] and record["kind"] == "fork_local_started"
               for line in lines if line.endswith("\n") for record in [json.loads(line)]):
            break
        if time.monotonic() >= deadline:
            raise RuntimeError("parent local step did not start during fork execution")
        await asyncio.sleep(0.02)
    mark(data, "fork_remote_started", side=side, workflow=ctx.workflow_id,
         parent=ctx.parent_workflow_id, root=ctx.root_workflow_id,
         activation=ctx.activation_id, attempt=event["ldgattemptno"])
    if side == "right" and data.get("fail_right"):
        return ctx.fail("synthetic_branch_failure", "Deliberate acceptance fixture failure")
    if data.get("wait_remote"):
        return ctx.wait_event("release", continuation=Entry.REMOTE_DONE,
                              state={"side": side}, timeout_ms=120_000)
    return ctx.complete({"side": side, "value": 7})


@workflow.entrypoint(Entry.LEFT)
async def left(event, ctx):
    return await branch(event, ctx, "left")


@workflow.entrypoint(Entry.RIGHT)
async def right(event, ctx):
    return await branch(event, ctx, "right")


@workflow.entrypoint(Entry.REMOTE_DONE)
def remote_done(event, ctx):
    if ctx.wake["kind"] != "event" or ctx.wake["event"]["data"] != {"approved": True}:
        return ctx.fail("invalid_release", "Expected the owned test release event")
    mark(event["data"], "fork_remote_done", workflow=ctx.workflow_id)
    return ctx.complete({"side": ctx.state["side"], "value": 7})


@workflow.entrypoint(Entry.COLLECT)
def collect(event, ctx):
    mark(event["data"], "fork_join", activation=ctx.activation_id,
         workflow=ctx.workflow_id, attempt=event["ldgattemptno"])
    states = {key: ctx.inputs[key]["state"] for key in ("left", "right")}
    # Recovery is ordinary code over the recorded branch outcomes.
    values = {key: ctx.get_result(key) for key, state in states.items() if state == "succeeded"}
    return ctx.complete({"local": ctx.state["local"], "states": states, "values": values})


handle = workflow.build()
