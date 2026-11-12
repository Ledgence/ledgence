"""Portable offline real-SDK program for worker/HTTP/recovery acceptance (MIT).

The marker is test instrumentation, not a production effect guarantee. It records
only actual model/tool dispatches; reconstructing accepted operations adds none.
"""
import asyncio
import json
import os
from pathlib import Path

from pydantic_ai import Agent
from pydantic_ai.messages import ModelResponse, TextPart, ToolCallPart
from pydantic_ai.models.function import FunctionModel
from pydantic_ai.usage import RequestUsage

from ledgence.worker.workflow import workflow_context, WorkflowError
from ledgence.worker.integrations import AgentRunSpec
from ledgence.worker.integrations import pydantic_ai


def mark(path, kind, **fields):
    with open(path, "a", encoding="utf-8") as output:
        output.write(json.dumps({"framework": "pydantic_ai", "kind": kind, **fields}, sort_keys=True) + "\n")


async def handle(event):
    data = event["data"]
    marker = data["marker"]
    context = workflow_context()

    def nested_local():
        return "forbidden nested operation"

    async def read_value(x: int) -> dict:
        mark(marker, "tool", x=x)
        if data.get("nested_control"):
            try:
                context.local("forbidden-nested-control", nested_local)
            except WorkflowError:
                # Qualification asserts the retained owner still fails after this catch.
                pass
        if x == 1 and data.get("release_file"):
            while not Path(data["release_file"]).exists():
                await asyncio.sleep(0.01)
        await asyncio.sleep(0)
        return {"x": x}

    async def model(messages, info):
        completed = sum(isinstance(message, ModelResponse) for message in messages)
        mark(marker, "model", turn=completed)
        if data.get("fail_model"):
            raise RuntimeError("controlled model failure")
        if completed < 2:
            return ModelResponse(parts=[
                ToolCallPart("read_value", {"x": completed + 1}, tool_call_id="a"),
                ToolCallPart("read_value", {"x": completed + 1}, tool_call_id="b"),
            ], usage=RequestUsage(input_tokens=5, output_tokens=2))
        return ModelResponse(parts=[TextPart("done")], usage=RequestUsage(input_tokens=7, output_tokens=1))

    def factory(bindings):
        return Agent(FunctionModel(model, model_name="offline-pydantic-acceptance"), name="fixture",
                     tools=[bindings.read_tool("fixture-read", read_value, version="1")],
                     capabilities=[bindings.durability])

    result = await pydantic_ai.run(context,
        spec=AgentRunSpec(data.get("key", "pydantic-acceptance"), "1", {"mode": "offline-read"}),
        prompt="Run two pairs of read tools, then finish.", factory=factory)
    return {"framework": "pydantic_ai", "output": result.output, "status": result.status.value,
            "pid": os.getpid(), "loop_id": id(asyncio.get_running_loop()),
            "transport_closed": result.lifecycle.transport_closed,
            "usage": [{"origin": item.origin.value, "input_tokens": item.input_tokens,
                       "output_tokens": item.output_tokens, "operation_key": item.operation_key}
                      for item in result.usage]}
