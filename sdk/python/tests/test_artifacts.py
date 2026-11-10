"""Invocation-bound artifact helper and protocol regressions (MIT)."""
import contextvars
import unittest
from pathlib import Path
from unittest.mock import AsyncMock

from ledgence.worker import artifacts
from ledgence.worker.bootstrap import _invoke_async


REFERENCE = {"schema": "ledgence.artifact.v1", "id": "art_test", "digest": "sha256:" + "a" * 64,
             "size": 0, "media_type": "application/octet-stream"}


class ArtifactTests(unittest.IsolatedAsyncioTestCase):
    async def test_path_is_resolved_in_child_and_rpc_has_no_identity_or_file_bytes(self):
        rpc = AsyncMock(return_value=REFERENCE)
        binding, token = artifacts._bind(rpc, True)
        try:
            result = await artifacts.put_file("local.bin", key="result")
            self.assertEqual(result, REFERENCE)
            rpc.assert_awaited_once_with("artifacts.put_file", {
                "path": str(Path("local.bin").absolute()), "key": "result",
                "media_type": "application/octet-stream", "filename": None,
            })
            path = str(Path("copy.bin").absolute())
            rpc.return_value = {"path": path}
            self.assertEqual(await artifacts.download(REFERENCE, "copy.bin"), Path(path))
        finally:
            artifacts._unbind(binding, token)

    async def test_unavailable_outside_invocation_and_after_unbind_even_in_copied_context(self):
        with self.assertRaisesRegex(RuntimeError, "no Ledgence invocation"):
            await artifacts.put_file("file", key="a")
        rpc = AsyncMock()
        binding, token = artifacts._bind(rpc, False)
        try:
            with self.assertRaisesRegex(RuntimeError, "unavailable"):
                await artifacts.put_file("file", key="a")
        finally:
            artifacts._unbind(binding, token)
        binding, token = artifacts._bind(rpc, True)
        copied = contextvars.copy_context()
        artifacts._unbind(binding, token)
        with self.assertRaisesRegex(RuntimeError, "no Ledgence invocation"):
            copied.run(artifacts._binding)
        rpc.assert_not_called()

    async def test_invalid_requests_and_changed_ack_are_rejected(self):
        rpc = AsyncMock(return_value={"path": "/wrong"})
        binding, token = artifacts._bind(rpc, True)
        try:
            for ref in [None, dict(REFERENCE, size=True), dict(REFERENCE, size=artifacts.MAX_BYTES + 1),
                        dict(REFERENCE, digest="sha256:" + "A" * 64), dict(REFERENCE, url="secret")]:
                with self.assertRaises(ValueError):
                    await artifacts.download(ref, "file")
            for filename in ["../file", "a/b", "a\\b", ".."]:
                with self.assertRaises(ValueError):
                    await artifacts.put_file("file", key="a", filename=filename)
            rpc.assert_not_called()
            with self.assertRaisesRegex(RuntimeError, "changed destination"):
                await artifacts.download(REFERENCE, "file")
        finally:
            artifacts._unbind(binding, token)

    async def test_plain_task_can_use_artifacts_but_not_workflow_context_and_warm_reset(self):
        from ledgence.worker.workflow import workflow_context, WorkflowError
        rpc = AsyncMock(return_value=REFERENCE)
        async def handler(event):
            with self.assertRaises(WorkflowError):
                workflow_context()
            return await artifacts.put_file("report", key="report")
        envelope = {"v": 3, "type": "result", "event_id": "evt", "attempt_id": "att"}
        result = await _invoke_async(handler, {}, envelope, 4096, None, rpc, execution_artifacts=True)
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["output"], REFERENCE)
        result = await _invoke_async(handler, {}, envelope, 4096, None, rpc)
        self.assertEqual(result["status"], "error")
        self.assertIn("unavailable", result["error"]["message"])
        self.assertEqual(rpc.await_count, 1)
        self.assertIsNone(artifacts._current.get())

    async def test_probe_cannot_enable_artifacts_even_with_advertised_feature(self):
        from unittest.mock import patch
        async def probe(*args):
            await artifacts.put_file("file", key="invalid")
        rpc = AsyncMock()
        envelope = {"v": 3, "type": "result", "event_id": "evt", "attempt_id": "att"}
        with patch("ledgence.worker.providers._invoke_probe", probe):
            result = await _invoke_async(lambda _: None, {}, envelope, 4096,
                                         {"schema": "ledgence.provider.probe.v1", "payload": {}}, rpc,
                                         effect_evidence_v2=True, native_effect_resolution=True,
                                         provider_operation=True, execution_artifacts=True)
        self.assertEqual(result["status"], "runtime_error")
        self.assertIn("unavailable", result["error"]["message"])
        rpc.assert_not_called()

class RecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_explicit_recovery_uses_selection_without_runtime_authority_fields(self):
        selection = {"artifact_id": "art_test", "upload_id": "upload_1", "revision": 1}
        status = {"selection": selection, "selected": True, "stage": "ready", "prepare_expires_at": 120000,
                  "ingress_expires_at": None, "dispatch_expires_at": 120000, "successor": None,
                  "reference": REFERENCE, "retry": "complete"}
        rpc = AsyncMock(side_effect=[{"artifact_error": {"kind": "uncertain", "selection": selection,
                        "status": None, "operation": {"kind": "put_file"}, "message": "uncertain"}}, status, status])
        binding, token = artifacts._bind(rpc, True)
        try:
            with self.assertRaises(artifacts.ArtifactUploadUncertain) as raised:
                await artifacts.put_file("file", key="report")
            self.assertEqual(raised.exception.selection, selection)
            self.assertEqual(await artifacts.upload_status("upload_1"), status)
            self.assertEqual(await artifacts.reconcile(raised.exception.selection), status)
            self.assertEqual([call.args[0] for call in rpc.await_args_list],
                             ["artifacts.put_file", "artifacts.upload_status", "artifacts.reconcile"])
            self.assertEqual(rpc.await_args_list[-1].args[1], {"selection": selection})
            with self.assertRaises(ValueError):
                await artifacts.reconcile(dict(selection, owner="forged"))
        finally:
            artifacts._unbind(binding, token)

    async def test_replacement_receipt_is_stable_after_its_successor_was_replaced(self):
        old = {"artifact_id": "art_test", "upload_id": "upload_1", "revision": 1}
        new = dict(old, upload_id="upload_2", revision=2)
        current = {"selection": new, "selected": False, "stage": "prepared", "prepare_expires_at": 120000,
                   "ingress_expires_at": None, "dispatch_expires_at": None,
                   "successor": dict(new, upload_id="upload_3", revision=3), "reference": None, "retry": "unavailable"}
        receipt = {"replaced": old, "replacement": new, "current": current}
        rpc = AsyncMock(return_value=receipt)
        binding, token = artifacts._bind(rpc, True)
        try:
            result = await artifacts.replace("upload_1", expected_selection_revision=1)
            self.assertEqual(result, receipt)
            self.assertFalse(result["current"]["selected"])
            rpc.assert_awaited_once_with("artifacts.replace", {"upload_id": "upload_1", "expected_selection_revision": 1})
            rpc.return_value = dict(current, retry="upload")
            with self.assertRaises(ValueError):
                await artifacts.upload_status("upload_2")
        finally:
            artifacts._unbind(binding, token)
