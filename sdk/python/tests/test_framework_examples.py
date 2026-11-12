"""Shared controller bridge and optional real-SDK public examples (MIT)."""
import asyncio
from copy import deepcopy
from functools import wraps
from importlib import metadata
import importlib
import json
from pathlib import Path
import socket
import subprocess
import sys
import unittest
from unittest.mock import patch

import test_provider_admission as admission
import test_provider_operations as providers
import test_workflow
from test_workflow_approvals import snapshot
from ledgence.worker.workflow import WorkflowContext, _workflow

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "examples/framework-integrations"))
common = importlib.import_module("framework_common")
MODULES = {name: importlib.import_module(module) for name, module in (
    ("pydantic", "pydantic_program"), ("adk", "google_adk_program"), ("claude", "claude_program"))}
DATA = {"topic": "maintenance", "notice_key": "notice-1", "queue": "workflows"}


def installed(*packages):
    try:
        return all(metadata.version(name) == version for name, version in packages)
    except metadata.PackageNotFoundError:
        return False


class FrameworkControllerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.contexts, self.records, self.requests = [], [], []
    async def asyncTearDown(self):
        for context in self.contexts:
            await context._finish(cancel=True)
    async def invoke(self, handle, *, data=None, rpc=None, **changes):
        async def commit(operation, record):
            self.assertEqual(operation, "local_step.commit")
            self.records.append(deepcopy(record))
            return {"committed": True}
        context = WorkflowContext(test_workflow.payload(**changes), rpc or commit,
            effect_evidence_v2=True, native_effect_resolution=True, provider_operation=True,
            resource_admission=True, provider_admission=True)
        self.contexts.append(context)
        token = _workflow.set(context)
        try:
            result = await handle({"data": DATA if data is None else data})
            await context._drain()
            return context._validate_decision(result)
        finally:
            _workflow.reset(token)

    async def controller(self):
        async def propose(ctx, topic):
            return common.read_policy(topic)
        return common.build_controller(propose)

    async def test_controller_preserves_approved_intent_through_admission_dispatch_and_wait(self):
        handle = await self.controller()
        requested = await self.invoke(handle)
        wait = requested["wait"]
        original = common.read_policy("maintenance")
        self.assertEqual(wait["action"]["arguments"], original)
        self.assertEqual(wait["provider_binding"]["admission"], {"resource": "notice-write"})
        resolved = deepcopy(wait["provider_binding"])
        resolved["program"] = providers.binding(common.notices)["program"]
        approved = snapshot(key="publish-notice", action=wait["action"], proposed_arguments=None,
                            protection="provider_operation", provider_binding=resolved)
        reserve = await self.invoke(handle, continuation="reserve", revision=1,
            wake={"kind": "approval", "approval": approved}, data={"title": "substituted input"})
        self.assertEqual(reserve["wait"]["request"], {"kind": "provider", "approval_key": "publish-notice"})
        self.assertEqual(self.records, [])

        selected = admission.wake(common.notices)
        selected["binding"]["resource"] = "notice-write"
        selected["binding"]["provider"]["binding"] = deepcopy(wait["provider_binding"])
        selected["binding"]["provider"]["action"] = deepcopy(wait["action"])
        selected["binding"]["provider"]["ticket"]["occurrence"]["approval_key"] = "publish-notice"
        async def rpc(operation, payload):
            self.requests.append((operation, deepcopy(payload)))
            if operation == "provider_admission.start.v1":
                return {"kind": "granted", "source": admission.source(selected, **payload)}
            self.assertEqual(operation, "provider_admission.report.v1")
            return admission.report(payload["source"], payload["outcome"]["call_completion"])
        dispatched = await self.invoke(handle, continuation="dispatch", revision=1, wake=selected,
                                       rpc=rpc, data={"body": "unapproved replacement"})
        self.assertEqual([item[0] for item in self.requests],
                         ["provider_admission.start.v1", "provider_admission.report.v1"])
        report = self.requests[-1][1]["outcome"]["report"]
        self.assertEqual(report["knowledge"]["result"], {"simulated": True, "notice": original})
        self.assertEqual(dispatched["wait"]["kind"], "provider_operation")
        self.assertEqual(dispatched["continuation"], "result")

        wake = providers.selected_wake(key="notice-result",
            occurrence=selected["binding"]["provider"]["ticket"]["occurrence"])
        wake["selection"]["result"] = {"simulated": True, "notice": original}
        completed = await self.invoke(handle, continuation="result", revision=2,
                                      activation_id="activation-resumed", wake=wake)
        self.assertEqual(completed["output"], {"status": "succeeded", "receipt": wake["selection"]["result"]})

    async def test_refused_approval_and_invalid_input_never_dispatch(self):
        handle = await self.controller()
        requested = await self.invoke(handle)
        binding = deepcopy(requested["wait"]["provider_binding"])
        binding["program"] = providers.binding(common.notices)["program"]
        for status in ("rejected", "expired"):
            refused = snapshot(status=status, key="publish-notice", action=requested["wait"]["action"],
                proposed_arguments=None, protection="provider_operation", provider_binding=binding)
            decision = await self.invoke(handle, continuation="reserve", revision=1,
                                         wake={"kind": "approval", "approval": refused})
            self.assertEqual(decision["output"], {"status": status, "dispatched": False})
        for data in (None, {}, {**DATA, "topic": "unknown"}, {**DATA, "queue": "../unsafe"}):
            decision = await self.invoke(handle, data={} if data is None else data)
            self.assertEqual(decision["kind"], "fail")
        self.assertEqual(self.records, [])

    async def test_all_three_entry_modules_import_without_optional_sdk_imports(self):
        self.assertEqual(set(MODULES), {"pydantic", "adk", "claude"})
        for module in MODULES.values():
            self.assertTrue(callable(module.handle))
            self.assertEqual(module.handle._ledgence_providers["example.framework-notice"], common.notices)

    async def real_sdk(self, name):
        module = MODULES[name]
        reads, blocked = [], []
        @wraps(module.read_policy)
        def counted(topic: str) -> dict:
            reads.append(topic)
            return common.read_policy(topic)
        def deny(*args, **kwargs):
            blocked.append(True)
            raise AssertionError("offline example attempted network/process activity")
        class NoProcess(subprocess.Popen):
            def __init__(self, *args, **kwargs):
                deny()
        with patch.object(module, "read_policy", counted), patch.object(socket.socket, "connect", deny), \
                patch.object(socket.socket, "connect_ex", deny), patch.object(socket, "getaddrinfo", deny), \
                patch.object(subprocess, "Popen", NoProcess):
            first = await self.invoke(module.handle)
            records = list(self.records)
            replayed = await self.invoke(module.handle, local_steps=records)
        self.assertEqual(first, replayed)
        self.assertEqual(first["wait"]["action"]["arguments"], common.read_policy("maintenance"))
        self.assertEqual(reads, ["maintenance"])
        self.assertEqual(blocked, [])
        self.assertEqual(len(self.records), 2 if name == "claude" else 4)

    @unittest.skipUnless(installed(("pydantic-ai-slim", "2.66.0")), "optional pinned PydanticAI environment")
    async def test_pydantic_sdk_proposal_and_replay(self):
        await self.real_sdk("pydantic")

    @unittest.skipUnless(installed(("google-adk", "2.14.0"), ("google-genai", "2.34.0")), "optional pinned ADK environment")
    async def test_adk_sdk_proposal_and_replay(self):
        await self.real_sdk("adk")

    @unittest.skipUnless(installed(("claude-agent-sdk", "0.2.180")), "optional pinned source-only Claude environment")
    async def test_claude_sdk_proposal_and_replay(self):
        await self.real_sdk("claude")


if __name__ == "__main__":
    unittest.main()
