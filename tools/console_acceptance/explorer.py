"""Real fork/local/join evidence with no model, trace backend, or fabricated API."""

STEP = "def handle(event):\n    return event['data']\n"
WORKFLOW = '''from enum import StrEnum
import asyncio
from ledgence.worker.workflow import Workflow

class Entry(StrEnum):
    START = "start"
    VALIDATE = "validate"
    REVIEW = "review"
    COLLECT = "collect"
    FINISH = "finish"

workflow = Workflow(Entry)

async def tests(candidate):
    await asyncio.sleep(.03)
    return {"passed": True, "candidate": candidate}

async def review(candidate):
    await asyncio.sleep(.03)
    return {"approved": True, "candidate": candidate}

@workflow.entrypoint(Entry.START, default=True)
def start(event, ctx):
    first = ctx.task("implement:0", program="explorer-step", version="1.0.0",
                     queue=event["data"]["queue"], data={"value": 7})
    return ctx.suspend(continuation=Entry.VALIDATE, state={}, until=[first])

@workflow.entrypoint(Entry.VALIDATE)
async def validate(event, ctx):
    candidate = ctx.get_result("implement:0")
    branches = await ctx.fork("validate:0", branches=[ctx.branch("review:0",
        entrypoint=Entry.REVIEW, queue=event["data"]["queue"], data=candidate)])
    checked = await ctx.local("tests:0", tests, candidate=candidate)
    return ctx.join(branches, resume=Entry.COLLECT, state={"tests": checked})

@workflow.entrypoint(Entry.REVIEW)
async def review_branch(event, ctx):
    return ctx.complete(await ctx.local("review:0", review, candidate=event["data"]))

@workflow.entrypoint(Entry.COLLECT)
def collect(event, ctx):
    final = ctx.task("finalize:0", program="explorer-step", version="1.0.0",
        queue=event["data"]["queue"], data={**ctx.state, "review": ctx.get_result("review:0")})
    return ctx.suspend(continuation=Entry.FINISH, state={}, until=[final])

@workflow.entrypoint(Entry.FINISH)
def finish(event, ctx):
    return ctx.complete(ctx.get_result("finalize:0"))

handle = workflow.build()
'''


def run(d, publish, record):
    publish(d, "explorer-step", STEP)
    publish(d, "explorer-workflow", WORKFLOW)
    command = d.console_submission("explorer-native-fork", program="explorer-workflow")
    command["input"]["data"] = {"queue": d.queue}
    command["input"]["correlation_key"] = "explorer-native-fork"
    root = d.api("POST", "workflows", command)["workflow"]["workflow_id"]
    result = d.workflow_result(root)
    assert result["outcome"]["output"]["tests"]["passed"] is True
    assert result["outcome"]["output"]["review"]["approved"] is True
    nodes = d.rows("workflows/explorer", workflow_id=root, limit=2)
    assert len({node["id"] for node in nodes}) == len(nodes), "pagination duplicated a logical node"
    kinds = lambda kind: [node for node in nodes if node["kind"] == kind]
    fork, = kinds("fork")
    local, = kinds("local")
    assert fork["branch_keys"] == ["review:0"]
    assert local["key"] == "tests:0" and local["entrypoint"] == "validate"
    assert local["accepted_at"] is not None
    assert local["observation"]["state"] == "returned"
    assert local["activation_id"] == fork["activation_id"]
    children = kinds("child")
    assert {node["key"] for node in children} == {"implement:0", "review:0", "finalize:0"}
    assert len(children) == 3
    assert all(node["availability"] == "available" and node["state"] == "succeeded" for node in children)
    review, = [node for node in children if node["key"] == "review:0"]
    assert review["fork_key"] == "validate:0"
    assert review["execution"]["kind"] == "workflow"
    assert all(node.get("fork_key") is None for node in children if node["key"] != "review:0")
    joins = kinds("child_wait")
    assert len(joins) == 3
    entrypoints = {node["entrypoint"]: node for node in kinds("entrypoint")}
    assert len(kinds("entrypoint")) == 4 and set(entrypoints) == {"start", "validate", "collect", "finish"}
    for entrypoint, child_key, resume in [("start", "implement:0", "validate"),
                                         ("validate", "review:0", "collect"),
                                         ("collect", "finalize:0", "finish")]:
        join, = [node for node in joins if node["activation_id"] == entrypoints[entrypoint]["activation_id"]]
        assert join["member_keys"] == [child_key] and join["resume"] == resume
        assert join["resumed_activation_id"] == entrypoints[resume]["activation_id"]
        assert entrypoints[entrypoint]["decision_kind"] == "suspend"
        assert entrypoints[entrypoint]["resumed_activation_id"] == entrypoints[resume]["activation_id"]
    assert entrypoints["finish"]["decision_kind"] == "complete"
    assert entrypoints["finish"]["resumed_activation_id"] is None
    ancestry = d.api("GET", "executions/ancestry", kind="workflow", id=review["execution"]["id"])
    assert [node["execution"]["id"] for node in ancestry["path"]] == [root, review["execution"]["id"]]
    nested = d.rows("workflows/explorer", workflow_id=review["execution"]["id"], limit=1)
    assert {node["kind"] for node in nested} == {"entrypoint", "local"} and len(nested) == 2
    nested_local, = [node for node in nested if node["kind"] == "local"]
    assert nested_local["entrypoint"] == "review" and nested_local["key"] == "review:0"
    assert nested_local["accepted_at"] is not None and nested_local["observation"]["state"] == "returned"
    first, = [node for node in children if node["key"] == "implement:0"]
    task_ancestry = d.api("GET", "executions/ancestry", kind="task", id=first["execution"]["id"])
    assert [node["execution"]["id"] for node in task_ancestry["path"]] == [root, first["execution"]["id"]]
    assert d.api("GET", "workflows/input", workflow_id=root)["data"] == {"queue": d.queue}
    attempt_id = local["observation"]["attempt_id"]
    attempt = d.api("GET", "attempts/observations", attempt_id=attempt_id)
    assert attempt["attempt_id"] == attempt_id and attempt["task_id"] == local["activation_id"]
    measured = attempt["observations"]
    assert measured is not None and int(measured["runtime_elapsed_us"]) > 0
    assert len(measured["local_steps"]) == 1 and not measured["local_steps_truncated"]
    observed = measured["local_steps"][0]
    assert observed["key"] == "tests:0" and observed["state"] == "returned"
    assert observed["started_at_ms"] == local["observation"]["started_at"]
    assert observed["elapsed_us"] == local["observation"]["elapsed_us"]
    assert "input" not in observed and "output" not in observed
    # Discovery works for an unregistered program used only as a child.
    discovered = d.rows("executions", program_id="explorer-step", limit=1)
    assert {row["id"] for row in discovered} == {node["execution"]["id"] for node in children if node["execution"]["kind"] == "task"}
    roots = d.rows("executions", correlation_key="explorer-native-fork", limit=1)
    assert [row["id"] for row in roots] == [root]
    record("explorer-real-local-and-distributed-join", {
        "workflow_id": root, "review_id": review["execution"]["id"],
        "nodes": len(nodes), "local_is_fork_member": False,
        "observations_attempt_id": attempt_id, "otel_required": False,
        "unregistered_child_discovery": len(discovered),
    })
    return root
