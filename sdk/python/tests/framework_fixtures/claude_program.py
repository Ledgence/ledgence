"""Portable source-only SDK whole-run recovery fixture (MIT).

The CustomTransport simulates an external runtime's inner model/tool work. These
markers measure actual fixture dispatch, not real CLI/provider execution. An
accepted whole-run replay creates no client or transport and adds no markers.
"""
import asyncio
import json
import os
from pathlib import Path
import socket
import subprocess

from ledgence.worker.workflow import workflow_context
from ledgence.worker.integrations import AgentRunSpec
from ledgence.worker.integrations import claude_agent_sdk as integration


def _deny(*args, **kwargs):
    raise AssertionError("network/process activity is forbidden in the offline Claude fixture")


_original_connect = socket.socket.connect
_original_connect_ex = socket.socket.connect_ex
_original_sendto = socket.socket.sendto


def _connect(sock, address):
    if sock.family in (socket.AF_INET, socket.AF_INET6): return _deny()
    return _original_connect(sock, address)


def _connect_ex(sock, address):
    if sock.family in (socket.AF_INET, socket.AF_INET6): return _deny()
    return _original_connect_ex(sock, address)


def _sendto(sock, *args):
    if sock.family in (socket.AF_INET, socket.AF_INET6): return _deny()
    return _original_sendto(sock, *args)


class _NoProcess(subprocess.Popen):
    def __init__(self, *args, **kwargs): _deny()


# This dedicated offline program keeps guards installed through invocation drain
# and subsequent warm invocations, including cancelled observer paths.
socket.socket.connect = _connect
socket.socket.connect_ex = _connect_ex
socket.socket.sendto = _sendto
socket.getaddrinfo = _deny
subprocess.Popen = _NoProcess
os.system = _deny
asyncio.create_subprocess_exec = _deny
asyncio.create_subprocess_shell = _deny
for _name in ("fork", "forkpty", "posix_spawn", "posix_spawnp"):
    if hasattr(os, _name): setattr(os, _name, _deny)


def mark(path, kind, **fields):
    with open(path, "a", encoding="utf-8") as output:
        output.write(json.dumps({"framework": "claude_agent_sdk", "kind": kind,
                                 "pid": os.getpid(), **fields}, sort_keys=True) + "\n")


async def handle(event):
    import anyio
    anyio.open_process = _deny
    from claude_agent_sdk import Transport

    data = event["data"]
    marker = data["marker"]

    class Scripted(Transport):
        def __init__(self):
            self.queue = asyncio.Queue()
            self.ready = False
            self.open_task = None
            mark(marker, "transport_created", loop_id=id(asyncio.get_running_loop()))
        async def connect(self):
            self.open_task = asyncio.current_task()
            self.ready = True
            mark(marker, "transport_open")
        async def close(self):
            self.ready = False
            await self.queue.put(None)
            mark(marker, "transport_close", same_task=asyncio.current_task() is self.open_task)
        def is_ready(self): return self.ready
        async def end_input(self): pass
        async def read_messages(self):
            while True:
                message = await self.queue.get()
                if message is None: return
                yield message
        async def write(self, line):
            message = json.loads(line)
            if message["type"] == "control_request":
                mark(marker, "control", subtype=message["request"]["subtype"])
                await self.queue.put({"type": "control_response", "response": {
                    "subtype": "success", "request_id": message["request_id"], "response": {}}})
            elif message["type"] == "user":
                mark(marker, "query")
                if data.get("fail_model"):
                    raise RuntimeError("fixture native query failure")
                for turn in range(2):
                    identifier = f"call-{turn}"
                    mark(marker, "model", turn=turn)
                    await self.queue.put({"type": "assistant", "session_id": "offline-session", "message": {
                        "id": f"model-{turn}", "model": "offline-model", "content": [
                            {"type": "tool_use", "id": identifier, "name": "fixture_read", "input": {"x": turn}}],
                        "usage": {"input_tokens": 5, "output_tokens": 2}}})
                    # Deterministic work inside the simulated external runtime,
                    # not an SDK host-MCP callback or a per-tool journal entry.
                    answer = {"x": turn}
                    mark(marker, "tool", turn=turn)
                    await self.queue.put({"type": "user", "session_id": "offline-session", "message": {
                        "role": "user", "content": [{"type": "tool_result", "tool_use_id": identifier,
                                                       "content": json.dumps(answer)}]}})
                mark(marker, "model", turn=2)
                await self.queue.put({"type": "assistant", "session_id": "offline-session", "message": {
                    "id": "model-final", "model": "offline-model", "content": [{"type": "text", "text": "done"}],
                    "usage": {"input_tokens": 7, "output_tokens": 1}}})
                mark(marker, "whole_run_ready")
                if data.get("release_file"):
                    while not Path(data["release_file"]).exists():
                        await asyncio.sleep(0.01)
                await self.queue.put({"type": "result", "subtype": "success", "duration_ms": 1,
                    "duration_api_ms": 0, "is_error": False, "num_turns": 3,
                    "session_id": "offline-session", "terminal_reason": "completed", "result": "done",
                    "usage": {"input_tokens": 17, "output_tokens": 5}, "total_cost_usd": 0.001})

    def factory(bindings):
        mark(marker, "factory")
        return bindings.client()

    result = await integration.run(workflow_context(),
        spec=AgentRunSpec(data.get("key", "claude-acceptance"), "1", {
            "options": {"model": "offline-model", "tools": []},
        }), prompt="Run two deterministic inner reads, then finish.", factory=factory,
        runtime=integration.CustomTransport(Scripted, "offline-claude-transport-v1"))
    return {"framework": "claude_agent_sdk", "output": result.output, "status": result.status.value,
            "pid": os.getpid(), "loop_id": id(asyncio.get_running_loop()),
            "transport_closed": result.lifecycle.transport_closed,
            "lifecycle_origin": result.lifecycle.origin.value,
            "process_exit_observed": result.lifecycle.local_process_exit_observed,
            "usage": [{"origin": item.origin.value, "input_tokens": item.input_tokens,
                       "output_tokens": item.output_tokens, "source_kind": item.source_kind.value,
                       "operation_key": item.operation_key} for item in result.usage]}
