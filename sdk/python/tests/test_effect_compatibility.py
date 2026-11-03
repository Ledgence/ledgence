"""Execute immutable historical helper bytes against additive host messages (MIT)."""
from contextlib import contextmanager
import gzip
import hashlib
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import patch

import test_bootstrap
import test_workflow
from test_workflow_approvals import snapshot

FIXTURES = Path(__file__).parent / "fixtures/effect-compatibility"
PINS = {
    "stable-0.7.0": "4172603474ea2efd99301096c04bc873a6b89046261647fb218249673eea9c57",
    "effect-v1": "20b65a8271c1b416a14c935ca086e297bbb96950c32f9d47c7025bbf7444a534",
}


class HistoricalEffectCompatibilityTests(unittest.TestCase):
    launch = test_bootstrap.BootstrapTests.launch
    invocation = test_workflow.WorkflowProtocolTests.invocation
    reply = test_workflow.WorkflowProtocolTests.reply

    @contextmanager
    def helper(self, name):
        manifest = next(item for item in json.loads((FIXTURES / "SOURCES.json").read_text())
                        if item["name"] == name)
        compressed = (FIXTURES / (name + ".tar.gz")).read_bytes()
        self.assertEqual(hashlib.sha256(compressed).hexdigest(), manifest["gzip_sha256"])
        raw = gzip.decompress(compressed)
        self.assertEqual(hashlib.sha256(raw).hexdigest(), PINS[name])
        self.assertEqual(manifest["tar_sha256"], PINS[name])
        with tempfile.TemporaryDirectory(prefix="ledgence-historical-helper-") as directory:
            root = Path(directory).resolve()
            with tarfile.open(fileobj=io.BytesIO(raw)) as archive:
                for member in archive.getmembers():
                    relative = Path(member.name)
                    self.assertFalse(relative.is_absolute())
                    self.assertNotIn("..", relative.parts)
                    if member.isdir():
                        continue
                    self.assertTrue(member.isfile(), "fixture must contain only source files")
                    self.assertTrue(member.name == "LICENSE" or member.name.startswith("sdk/python/ledgence/worker/"))
                    target = root / relative
                    target.parent.mkdir(parents=True, exist_ok=True)
                    target.write_bytes(archive.extractfile(member).read())
            self.assertIn("MIT License", (root / "LICENSE").read_text())
            helper = root / "sdk/python/ledgence/worker"
            with patch.object(test_bootstrap, "BOOTSTRAP", helper / "bootstrap.py"):
                yield helper

    def test_original_stable_helper_runs_approved_local_with_additive_capability(self):
        with self.helper("stable-0.7.0") as helper:
            source = f'''from pathlib import Path
import ledgence.worker.workflow as module
from ledgence.worker.workflow import workflow_context
assert Path(module.__file__).parent == Path({str(helper)!r})
assert not hasattr(module.WorkflowContext, "approved_effect")
def dispatch(amount, currency="USD"):
    return {{"receipt": "legacy-local"}}
async def handle(event):
    ctx = workflow_context()
    return ctx.complete(await ctx.approved_local(dispatch, version="1"))
'''
            action = {"name": "program:dispatch", "version": "1", "arguments": {"amount": 50, "currency": "USD"}}
            message = self.invocation(activation=test_workflow.payload(
                revision=1, wake={"kind": "approval", "approval": snapshot(action=action)}))
            message["runtime_capabilities"] = {"approved_effect_evidence_v2": True}
            result, frames = self.launch(source, [message, self.reply()], protocol=3)
        self.assertEqual(result.returncode, 0, result.stderr)
        requests = [frame for frame in frames if frame["type"] == "runtime_request"]
        self.assertEqual([request["operation"] for request in requests], ["local_step.commit"])
        self.assertEqual(frames[-1]["status"], "success")
        self.assertEqual(frames[-1]["output"]["output"], {"receipt": "legacy-local"})

    def test_pre_v2_helper_uses_unchanged_v1_claim_and_confirm(self):
        with self.helper("effect-v1") as helper:
            source = f'''from pathlib import Path
import ledgence.worker.workflow as module
from ledgence.worker.workflow import workflow_context
assert Path(module.__file__).parent == Path({str(helper)!r})
def dispatch(amount, currency="USD"):
    return {{"receipt": "v1"}}
def lookup(amount, currency="USD"):
    raise AssertionError("first dispatch must use dispatch callback")
async def handle(event):
    ctx = workflow_context()
    return ctx.complete(await ctx.approved_effect(dispatch, lookup=lookup, version="1"))
'''
            action = {"name": "program:dispatch", "version": "1", "arguments": {"amount": 50, "currency": "USD"}}
            message = self.invocation(activation=test_workflow.payload(
                revision=1, wake={"kind": "approval", "approval": snapshot(action=action)}))
            message["runtime_capabilities"] = {"approved_effect_evidence_v2": True}
            claim = {"workflow_id": "workflow-1", "activation_id": "activation-1", "approval_key": "refund",
                     "action": action, "lookup": "program:lookup", "disposition": "dispatch"}
            result, frames = self.launch(source, [message, self.reply(result=claim), self.reply(number=2)], protocol=3)
        self.assertEqual(result.returncode, 0, result.stderr)
        requests = [frame for frame in frames if frame["type"] == "runtime_request"]
        self.assertEqual([request["operation"] for request in requests],
                         ["approved_effect.claim", "approved_effect.confirm"])
        self.assertEqual(set(requests[0]["payload"]), {"approval_key", "action", "lookup"})
        self.assertEqual(set(requests[1]["payload"]), {"approval_key", "lookup", "record"})
        self.assertEqual(frames[-1]["status"], "success")
        self.assertEqual(frames[-1]["output"]["output"], {"receipt": "v1"})


if __name__ == "__main__":
    unittest.main()
