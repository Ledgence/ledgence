"""Reconcile four feeds, investigate one snapshot, and approve its report (MIT)."""
from enum import StrEnum
import re

from ledgence.worker.workflow import ApprovalAction, ApprovalStatus, OperationKind, Workflow

from fulfillment.agent import MAX_TURNS, TOOLS, model_turn
from fulfillment.config import APPROVAL_KEY, SOURCES, SOURCE_WAIT, TOPICS, submission
from fulfillment.data import analyze_snapshot, assemble_snapshot, ingest_source
from fulfillment.report import build_candidate, publish_report
from fulfillment.storage import read_json, write_json


class Entry(StrEnum):
    INGEST = "ingest"
    SOURCE = "extract_source"
    RECONCILE = "reconcile"
    SOURCE_READY = "source_ready"
    ANALYZE = "analyze_snapshot"
    INVESTIGATE = "investigate"
    AGENT_TURN = "agent_turn"
    PUBLISH = "publish"


workflow = Workflow(Entry)


def checked_results(ctx, keys):
    for key in keys:
        if ctx.inputs.get(key, {}).get("state") != "succeeded":
            raise ValueError("Inspect the failed or cancelled branch: " + key)
    return {key: ctx.get_result(key) for key in keys}


def analyze_to_artifact(*, store, snapshot, topic):
    return write_json(store, analyze_snapshot(store=store, snapshot=snapshot, topic=topic))


def read_evidence(*, store, snapshot_id, evidence_refs, topic):
    if topic not in TOPICS or topic not in evidence_refs:
        raise ValueError("unknown evidence tool topic")
    value = read_json(store, evidence_refs[topic])
    if value["topic"] != topic or value["snapshot_id"] != snapshot_id:
        raise ValueError("evidence belongs to a different topic or snapshot")
    return value


def candidate_from_refs(*, store, snapshot, evidence_refs, answer, mode):
    evidence = [read_evidence(store=store, snapshot_id=snapshot["snapshot_id"],
                              evidence_refs=evidence_refs, topic=topic) for topic in TOPICS]
    return build_candidate(store=store, snapshot=snapshot, evidence=evidence, answer=answer, mode=mode)


async def fork_sources(ctx, data, sources, revision, state):
    branches = await ctx.fork("ingest:" + revision, branches=[
        ctx.branch(f"source:{source}:{revision}", entrypoint=Entry.SOURCE, queue=data["queue"],
                   data={"store": data["store"], "scenario": data["scenario"],
                         "source": source, "revision": revision},
                   retry_policy={"max_attempts": 3, "retry_delay_ms": 100}, attempt_timeout_ms=60_000)
        for source in sources
    ])
    return ctx.join(branches, resume=Entry.RECONCILE,
                    state={**state, "sources": list(sources), "revision": revision})


@workflow.entrypoint(Entry.INGEST, default=True)
async def ingest(event, ctx):
    try:
        data = submission(event.get("data"))
    except ValueError as error:
        return ctx.fail("invalid_input", str(error))
    return await fork_sources(ctx, data, SOURCES, "initial", {"receipts": {}, "initial_quality": None})


@workflow.entrypoint(Entry.SOURCE)
async def source(event, ctx):
    # The local result is a small manifest; source rows remain in external files.
    return ctx.complete(await ctx.local("extract", ingest_source, **event["data"]))


@workflow.entrypoint(Entry.RECONCILE)
async def reconcile(event, ctx):
    data, state = submission(event["data"]), ctx.state
    try:
        keys = [f"source:{source}:{state['revision']}" for source in state["sources"]]
        results = checked_results(ctx, keys)
    except ValueError as error:
        return ctx.fail("source_failed", str(error))
    receipts = dict(state["receipts"])
    for source_name, key in zip(state["sources"], keys):
        receipts[source_name] = results[key]
    assembled = await ctx.local("reconcile", assemble_snapshot, store=data["store"],
                                scenario=data["scenario"], receipts=receipts)
    initial_quality = state["initial_quality"] or assembled["quality"]
    if not assembled["ready"]:
        # Only this known missing fixture partition has a correction path.
        # Invalid records and any other incomplete data remain quarantined.
        if (state["revision"] != "initial" or data["scenario"] != "data-gap"
                or assembled["quality"].get("incomplete_sources") != ["warehouse"]
                or assembled["quality"].get("missing_sources")
                or any(assembled["quality"].get(key) for key in ("orphan_records", "duplicate_relations", "inconsistent_records", "inconsistent_windows", "missing_cohorts", "missing_shipments", "unfinished_shipments"))
                or assembled["quality"].get("quarantined_records", 0)):
            return ctx.complete({"status": "quarantined", "published": False, "quality": assembled["quality"]})
        return ctx.wait_event(SOURCE_WAIT, continuation=Entry.SOURCE_READY,
                              state={"receipts": receipts, "initial_quality": initial_quality},
                              timeout_ms=data["source_timeout_ms"])
    snapshot = assembled["snapshot"]
    branches = await ctx.fork("investigations", branches=[
        ctx.branch("analysis:" + topic, entrypoint=Entry.ANALYZE, queue=data["queue"],
                   data={"store": data["store"], "snapshot": snapshot, "topic": topic},
                   retry_policy={"max_attempts": 3, "retry_delay_ms": 100}, attempt_timeout_ms=60_000)
        for topic in TOPICS
    ])
    return ctx.join(branches, resume=Entry.INVESTIGATE, state={
        "snapshot": snapshot, "initial_quality": initial_quality,
        "quality": assembled["quality"], "corrected_sources": [] if state["revision"] == "initial" else state["sources"],
    })


@workflow.entrypoint(Entry.SOURCE_READY)
async def source_ready(event, ctx):
    data, wake = submission(event["data"]), ctx.wake
    if not wake or wake.get("key") != SOURCE_WAIT:
        return ctx.fail("invalid_source_event", "Expected the warehouse correction event")
    if wake["kind"] == "timeout":
        return ctx.complete({"status": "incomplete", "published": False, "quality": ctx.state["initial_quality"]})
    expected = {"workflow_id": ctx.workflow_id, "source": "warehouse", "revision": "corrected"}
    if wake["kind"] != "event" or wake["event"].get("data") != expected:
        return ctx.fail("invalid_source_event", "The correction must identify this workflow and warehouse revision")
    return await fork_sources(ctx, data, ("warehouse",), "corrected", ctx.state)


@workflow.entrypoint(Entry.ANALYZE)
async def analyze(event, ctx):
    return ctx.complete(await ctx.local("query-evidence", analyze_to_artifact, **event["data"]))


@workflow.entrypoint(Entry.INVESTIGATE)
def investigate(event, ctx):
    try:
        results = checked_results(ctx, ["analysis:" + topic for topic in TOPICS])
    except ValueError as error:
        return ctx.fail("analysis_failed", str(error))
    state = ctx.state
    return ctx.continue_(continuation=Entry.AGENT_TURN, state={
        **state, "evidence_refs": {topic: results["analysis:" + topic] for topic in TOPICS},
        "round": 0, "call_ids": [], "messages": [{"role": "user", "content": {
            "question": "Are deliveries getting slower, or was the apparent spike caused by incomplete data?",
            "snapshot_id": state["snapshot"]["snapshot_id"], "available_topics": list(TOPICS),
        }}],
    })


@workflow.entrypoint(Entry.AGENT_TURN)
async def agent_turn(event, ctx):
    data, state = submission(event["data"]), ctx.state
    round_no = state["round"]
    if round_no >= MAX_TURNS:
        return ctx.fail("agent_turn_limit", "Investigation exceeded its bounded turn budget")
    response = await ctx.operation(
        f"model:{round_no}", model_turn, kind=OperationKind.MODEL, version="1",
        arguments={"mode": data["mode"], "model": data["model"],
                   "snapshot_id": state["snapshot"]["snapshot_id"], "messages": state["messages"], "tools": TOOLS},
    )
    if response.get("kind") == "tool":
        arguments, call_id = response.get("arguments"), response.get("call_id")
        if (response.get("name") != "read_evidence" or type(arguments) is not dict
                or set(arguments) != {"topic"} or arguments["topic"] not in TOPICS
                or type(call_id) is not str or re.fullmatch(r"[A-Za-z0-9:_-]{1,64}", call_id) is None
                or call_id in state["call_ids"]):
            return ctx.fail("invalid_tool_call", "Agent selected an unsupported or repeated tool call identity")
        evidence = await ctx.operation(
            f"tool:{round_no}:{call_id}", read_evidence, kind=OperationKind.TOOL, version="1",
            arguments={"store": data["store"], "snapshot_id": state["snapshot"]["snapshot_id"],
                       "evidence_refs": state["evidence_refs"], "topic": arguments["topic"]},
        )
        return ctx.continue_(continuation=Entry.AGENT_TURN, state={
            **state, "round": round_no + 1, "call_ids": [*state["call_ids"], call_id],
            "messages": [*state["messages"], {"role": "assistant", "content": response},
                         {"role": "tool", "content": evidence}],
        })
    if response.get("kind") != "answer":
        return ctx.fail("invalid_agent_answer", "Agent must select a tool or a supported answer")
    seen = {message["content"]["topic"] for message in state["messages"] if message["role"] == "tool"}
    if seen != set(TOPICS):
        return ctx.fail("incomplete_investigation", "Read all four evidence topics before preparing a report")
    try:
        candidate = await ctx.local("verify-report", candidate_from_refs, store=data["store"],
                                    snapshot=state["snapshot"], evidence_refs=state["evidence_refs"],
                                    answer=response, mode=data["mode"])
    except ValueError as error:
        return ctx.fail("unverified_report", str(error))
    action = ApprovalAction.for_callable(publish_report, version="1", arguments={
        "store": data["store"], "candidate_ref": candidate,
    })
    return ctx.request_approval(APPROVAL_KEY, action=action, resume=Entry.PUBLISH,
                                timeout_ms=data["approval_timeout_ms"],
                                proposed_arguments={"snapshot_id": state["snapshot"]["snapshot_id"],
                                                    "candidate_sha256": candidate["sha256"]},
                                state={"candidate": candidate, "snapshot_id": state["snapshot"]["snapshot_id"],
                                       "corrected_sources": state["corrected_sources"],
                                       "initial_quality": state["initial_quality"], "quality": state["quality"]})


@workflow.entrypoint(Entry.PUBLISH)
async def publish(event, ctx):
    approval = ctx.approval
    if approval is None or approval.key != APPROVAL_KEY:
        return ctx.fail("missing_approval", "Expected a report publication approval")
    common = {"snapshot_id": ctx.state["snapshot_id"], "candidate_ref": ctx.state["candidate"],
              "corrected_sources": ctx.state["corrected_sources"], "quality": ctx.state["quality"],
              "initial_quality": ctx.state["initial_quality"]}
    if approval.status is not ApprovalStatus.APPROVED:
        return ctx.complete({**common, "status": approval.status.value, "published": False})
    # Reads the authoritative saved action arguments; the wake cannot replace the candidate.
    receipt = await ctx.approved_local(publish_report, version="1")
    return ctx.complete({**common, "status": "published", "published": True, "publication": receipt})


handle = workflow.build()
