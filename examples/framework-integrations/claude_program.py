"""Real source-only Claude SDK with an offline custom transport (MIT).

The transport simulates an external runtime's read. Only the entire finished run
is replayable; this does not exercise a Claude CLI, model provider or live MCP.
"""
import asyncio
import json

from ledgence.worker.integrations import AgentRunSpec, AgentStatus
from ledgence.worker.integrations import claude_agent_sdk
from framework_common import build_controller, read_policy


async def propose(ctx, topic):
    def transport_factory():
        from claude_agent_sdk import Transport

        class OfflineTransport(Transport):
            def __init__(self):
                self.messages = asyncio.Queue()
                self.ready = False
            async def connect(self):
                self.ready = True
            async def close(self):
                self.ready = False
                await self.messages.put(None)
            def is_ready(self):
                return self.ready
            async def end_input(self):
                pass
            async def read_messages(self):
                while True:
                    message = await self.messages.get()
                    if message is None:
                        return
                    yield message
            async def write(self, line):
                message = json.loads(line)
                if message["type"] == "control_request":
                    await self.messages.put({"type": "control_response", "response": {
                        "subtype": "success", "request_id": message["request_id"], "response": {}}})
                elif message["type"] == "user":
                    # Simulated external read; no inner Ledgence operation exists.
                    output = read_policy(topic)
                    await self.messages.put({"type": "result", "subtype": "success", "is_error": False,
                        "duration_ms": 1, "duration_api_ms": 0, "num_turns": 1,
                        "session_id": "offline-policy-session", "terminal_reason": "completed",
                        "result": json.dumps(output)})
        return OfflineTransport()

    def factory(bindings):
        return bindings.client()

    result = await claude_agent_sdk.run(ctx,
        spec=AgentRunSpec("notice-proposal", "1", {"topic": topic,
            "options": {"model": "offline-policy-model", "tools": []}}),
        prompt="Read the policy and propose its notice as JSON.", factory=factory,
        runtime=claude_agent_sdk.CustomTransport(transport_factory, "offline-policy-runtime-v1"))
    if result.status is not AgentStatus.SUCCEEDED:
        raise ValueError("The framework did not produce a successful proposal")
    return json.loads(result.output)


handle = build_controller(propose)
