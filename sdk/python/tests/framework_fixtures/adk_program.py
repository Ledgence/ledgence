"""Portable offline real-ADK program for worker/HTTP/recovery acceptance (MIT).

Markers are test instrumentation: they count actual model/read dispatches, not
reconstructed accepted operations. No provider, network, or subprocess is used.
"""
import asyncio
import json
import os
from pathlib import Path

from google.adk.agents import LlmAgent
from google.adk.models.base_llm import BaseLlm
from google.adk.models.llm_response import LlmResponse
from google.genai import types

from ledgence.worker.workflow import workflow_context, WorkflowError
from ledgence.worker.integrations import AgentRunSpec
from ledgence.worker.integrations import google_adk


def mark(path, kind, **fields):
    with open(path, "a", encoding="utf-8") as output:
        output.write(json.dumps({"framework": "google_adk", "kind": kind, **fields}, sort_keys=True) + "\n")


async def handle(event):
    data = event["data"]
    marker = data["marker"]
    resource = {}

    async def read_value(x: int) -> dict:
        mark(marker, "tool", x=x)
        if data.get("nested_control"):
            def forbidden_nested_work():
                return None
            try:
                await workflow_context().local("forbidden-adk-nested-control", forbidden_nested_work)
            except WorkflowError:
                # Deliberately swallow: the production owner must remain poisoned.
                mark(marker, "nested_rejected", x=x)
        if data.get("release_file"):
            while not Path(data["release_file"]).exists():
                await asyncio.sleep(0.01)
        await asyncio.sleep(0)
        return {"x": x}

    class Model(BaseLlm):
        async def generate_content_async(self, llm_request, stream=False):
            completed = sum(content.role == "model" for content in llm_request.contents)
            mark(marker, "model", turn=completed)
            if data.get("fail_model"):
                raise RuntimeError("offline ADK model failure")
            if completed < 2:
                yield LlmResponse(content=types.Content(role="model", parts=[
                    types.Part(function_call=types.FunctionCall(name="read_value", args={"x": completed + 1}, id="a")),
                    types.Part(function_call=types.FunctionCall(name="read_value", args={"x": completed + 1}, id="b")),
                ]), usage_metadata=types.GenerateContentResponseUsageMetadata(
                    prompt_token_count=5, candidates_token_count=2, total_token_count=7))
            else:
                yield LlmResponse(content=types.Content(role="model", parts=[types.Part(text="done")]),
                    usage_metadata=types.GenerateContentResponseUsageMetadata(
                        prompt_token_count=7, candidates_token_count=1, total_token_count=8))

    async def close_model():
        resource["close_task_id"] = id(asyncio.current_task())
        resource["closed"] = True

    def factory(bindings):
        native = Model(model="offline-adk-acceptance")
        resource["model_id"] = id(native)
        resource["open_task_id"] = id(asyncio.current_task())
        return LlmAgent(name="fixture", model=bindings.model(native, identity="fixture-model",
            configuration={"mode": "offline-read"}, close=close_model),
            tools=[bindings.read_tool("fixture-read", read_value, version="1")])

    result = await google_adk.run(workflow_context(),
        spec=AgentRunSpec(data.get("key", "adk-acceptance"), "1", {"mode": "offline-read"}),
        prompt="Run two pairs of read tools, then finish.", factory=factory)
    return {"framework": "google_adk", "output": result.output, "status": result.status.value,
            "pid": os.getpid(), "loop_id": id(asyncio.get_running_loop()), "resource": resource,
            "transport_closed": result.lifecycle.transport_closed,
            "usage": [{"origin": item.origin.value, "input_tokens": item.input_tokens,
                       "output_tokens": item.output_tokens, "operation_key": item.operation_key}
                      for item in result.usage]}
