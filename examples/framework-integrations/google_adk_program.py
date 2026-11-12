"""Real Google ADK SDK, deterministic offline model, controller-owned writes (MIT)."""
import json

from ledgence.worker.integrations import AgentRunSpec, AgentStatus
from ledgence.worker.integrations import google_adk
from framework_common import build_controller, read_policy


async def propose(ctx, topic):
    def factory(bindings):
        from google.adk.agents import LlmAgent
        from google.adk.models.base_llm import BaseLlm
        from google.adk.models.llm_response import LlmResponse
        from google.genai import types

        class PolicyModel(BaseLlm):
            async def generate_content_async(self, llm_request, stream=False):
                returns = [part.function_response for content in llm_request.contents for part in content.parts or []
                           if part.function_response is not None]
                if not returns:
                    part = types.Part(function_call=types.FunctionCall(name="read_policy",
                        args={"topic": bindings.configuration["topic"]}))
                else:
                    part = types.Part(text=json.dumps(returns[-1].response))
                yield LlmResponse(content=types.Content(role="model", parts=[part]))

        # Explicit None means this fixture owns no model transport resources.
        model = bindings.model(PolicyModel(model="offline-policy-model"), identity="policy-model",
                               configuration={"mode": "offline"}, close=None)
        return LlmAgent(name="policy_reader", model=model,
                        tools=[bindings.read_tool("policy", read_policy, version="1")])

    result = await google_adk.run(ctx, spec=AgentRunSpec("notice-proposal", "1", {"topic": topic}),
                                 prompt="Read the policy and propose its notice as JSON.", factory=factory)
    if result.status is not AgentStatus.SUCCEEDED:
        raise ValueError("The framework did not produce a successful proposal")
    return json.loads(result.output)


handle = build_controller(propose)
