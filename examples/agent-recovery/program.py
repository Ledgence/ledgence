"""A bounded agent loop with a scripted model and durable call recovery (MIT)."""
from enum import StrEnum
import re

from ledgence.worker.workflow import OperationKind, Workflow


class Entry(StrEnum):
    TURN = "turn"


workflow = Workflow(Entry)
MODEL = "scripted-order-assistant-v1"
MAX_TURNS = 3
TOOLS = [{"name": "lookup_order", "version": "1", "parameters": {
    "type": "object", "properties": {"order_id": {"type": "string"},
        "region": {"type": "string", "default": "us-east"}},
    "required": ["order_id"], "additionalProperties": False}}]


def lookup_order(*, order_id, region="us-east"):
    """An in-process fixture, with no real order service or side effect."""
    return {"order_id": order_id, "region": region, "status": "shipped", "delivery_days": 2}


def scripted_model(*, model, messages, tools, temperature=0):
    """Replace this boundary with a JSON-in/JSON-out model adapter for real AI."""
    if model != MODEL or tools != TOOLS or temperature != 0:
        raise ValueError("unsupported scripted model request")
    if messages[-1]["role"] == "tool":
        order = messages[-1]["result"]
        return {"kind": "answer", "text": (
            f"Order {order['order_id']} is {order['status']}; "
            f"estimated delivery in {order['delivery_days']} days.")}
    return {"kind": "tool", "call_id": "lookup:0", "name": "lookup_order",
            "arguments": {"order_id": messages[0]["order_id"]}}


@workflow.entrypoint(Entry.TURN, default=True)
async def turn(event, ctx):
    data = event.get("data")
    if (type(data) is not dict or type(data.get("order_id")) is not str
            or re.fullmatch(r"[A-Za-z0-9_-]{1,64}", data["order_id"]) is None):
        return ctx.fail("invalid_input", "order_id must be 1-64 letters, digits, underscores or hyphens")
    state = ctx.state
    if state is None:
        state = {"round": 0, "messages": [{"role": "user", "order_id": data["order_id"],
                                         "text": "When will my order arrive?"}]}
    round_no, messages = state["round"], state["messages"]
    if round_no >= MAX_TURNS:
        return ctx.fail("turn_limit", "agent exceeded its bounded turn budget")
    response = await ctx.operation(
        f"model:turn:{round_no}", scripted_model, kind=OperationKind.MODEL, version="1",
        arguments={"model": MODEL, "messages": messages, "tools": TOOLS},
    )
    if response["kind"] == "answer":
        return ctx.complete({"answer": response["text"], "turns": round_no + 1,
                             "model": MODEL, "simulated": True})
    if response["kind"] != "tool" or response["name"] != "lookup_order":
        return ctx.fail("unsupported_model_response", "model selected an unsupported action")
    result = await ctx.operation(
        f"tool:turn:{round_no}:{response['call_id']}", lookup_order,
        kind=OperationKind.TOOL, version="1", arguments=response["arguments"],
    )
    # Both responses have committed before the transcript advances. Carry only
    # the next turn's required state; Python locals are never snapshotted.
    return ctx.continue_(continuation=Entry.TURN, state={
        "round": round_no + 1,
        "messages": [*messages, {"role": "assistant", "response": response},
                     {"role": "tool", "call_id": response["call_id"], "result": result}],
    })


handle = workflow.build()
