"""Real PydanticAI SDK, deterministic offline model, controller-owned writes (MIT)."""
import json

from ledgence.worker.integrations import AgentRunSpec, AgentStatus
from ledgence.worker.integrations import pydantic_ai
from framework_common import build_controller, read_policy


async def propose(ctx, topic):
    def factory(bindings):
        from pydantic_ai import Agent
        from pydantic_ai.messages import ModelRequest, ModelResponse, TextPart, ToolCallPart, ToolReturnPart
        from pydantic_ai.models.function import FunctionModel

        async def model(messages, info):
            returns = [part for message in messages if isinstance(message, ModelRequest)
                       for part in message.parts if isinstance(part, ToolReturnPart)]
            if not returns:
                return ModelResponse(parts=[ToolCallPart("read_policy", {"topic": bindings.configuration["topic"]},
                                                        tool_call_id="read-policy")])
            return ModelResponse(parts=[TextPart(json.dumps(returns[-1].content))])

        return Agent(FunctionModel(model, model_name="offline-policy-model"), name="policy_reader",
                     tools=[bindings.read_tool("policy", read_policy, version="1")],
                     capabilities=[bindings.durability])

    result = await pydantic_ai.run(ctx, spec=AgentRunSpec("notice-proposal", "1", {"topic": topic}),
                                  prompt="Read the policy and propose its notice as JSON.", factory=factory)
    if result.status is not AgentStatus.SUCCEEDED:
        raise ValueError("The framework did not produce a successful proposal")
    return json.loads(result.output)


handle = build_controller(propose)
