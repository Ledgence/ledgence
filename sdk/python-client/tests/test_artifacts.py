"""Installed client artifact transport: real loopback HTTP, files and recovery."""
import asyncio
import base64
import hashlib
import json
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

from aiohttp import web
from ledgence.client import (
    ArtifactRecoveryRequired, ArtifactRef, ArtifactSelection, ArtifactUploadStage,
    ArtifactUploadUncertain, AsyncClient, InputError, ProtocolError, RequestTimeout,
)
from ledgence.client import artifact_files
import test_workflows
from support import SCOPE, response


def reference(data=b"artifact"):
    return {"schema": "ledgence.artifact.v1", "id": "art_test", "digest": "sha256:" + hashlib.sha256(data).hexdigest(),
            "size": len(data), "media_type": "application/octet-stream"}


def selection(revision=1):
    return {"artifact_id": "art_test", "upload_id": "upload_" + str(revision), "revision": revision}


def status(data=b"artifact", stage="prepared", *, revision=1):
    return {"selection": selection(revision), "selected": True, "stage": stage, "prepare_expires_at": 120000,
            "ingress_expires_at": None, "dispatch_expires_at": 120000 if stage in ("writing", "uncertain", "ready") else None,
            "successor": None, "reference": reference(data) if stage == "ready" else None,
            "retry": {"prepared": "upload", "uncertain": "reconcile", "ready": "complete"}[stage]}


def manifest(data=b"artifact"):
    return {"reference": reference(data), "scope": dict(SCOPE), "root": {"kind": "task", "id": "task"},
            "producer": {"kind": "verified_actor", "identity": {"issuer": "issuer", "subject": "user"}},
            "key": "report", "filename": None, "created_at": 1, "accepted_at": 2, "first_attempt_id": None}


def binary(data=b"artifact", *, declared=None):
    value = manifest(data if declared is None else declared)
    return web.Response(body=data, headers={"Content-Type": "application/octet-stream", "Request-Id": "artifact-request",
        "Ledgence-Artifact-Manifest": base64.b64encode(json.dumps(value).encode()).decode()})


class ArtifactTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = test_workflows.WorkflowClientTests.asyncSetUp

    def directory(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        return Path(temp.name)

    async def test_empty_and_multichunk_files_stream_once_with_external_scope(self):
        for data in (b"", b"a" * (256 * 1024 + 3)):
            source = self.directory() / "report.bin"
            source.write_bytes(data)
            self.requests.clear()
            async def handle(request, body):
                if request.path.endswith("/prepare"):
                    payload = json.loads(body)
                    self.assertEqual(payload["requester"], {"kind": "external", "scope": SCOPE})
                    command = payload["command"]
                    self.assertEqual(command["target"], {"kind": "task", "id": "task"})
                    self.assertEqual(command["metadata"]["size"], len(data))
                    self.assertEqual(command["metadata"]["digest"], reference(data)["digest"])
                    self.assertNotIn("producer", command)
                    return response({"upload": status(data)})
                self.assertEqual(request.method, "PUT")
                self.assertEqual(body, data)
                self.assertEqual(request.headers["Content-Type"], "application/octet-stream")
                self.assertEqual(json.loads(base64.b64decode(request.headers["Ledgence-Artifact-Selection"])), selection())
                return response({"upload": status(data, "ready")})
            self.callback = handle
            ref = await self.client.artifacts.put_file(source, task_id="task", key="report")
            self.assertIsInstance(ref, ArtifactRef)
            self.assertEqual(ref.to_dict(), reference(data))
            self.assertEqual(len(self.requests), 2)

    async def test_uncertain_upload_never_resends_and_explicit_recovery_is_available(self):
        source = self.directory() / "report.bin"
        source.write_bytes(b"artifact")
        async def handle(request, body):
            if request.path.endswith("/prepare"):
                return response({"upload": status()})
            return response({"code": "unavailable", "message": "lost acknowledgement"}, code=503)
        self.callback = handle
        with self.assertRaises(ArtifactUploadUncertain) as raised:
            await self.client.artifacts.put_file(source, task_id="task", key="report")
        self.assertEqual(raised.exception.selection, ArtifactSelection(**selection()))
        self.assertEqual(len(self.requests), 2)
        self.callback = lambda request, body: self.pending(request, body)
        with self.assertRaises(ArtifactRecoveryRequired) as pending:
            await self.client.artifacts.put_file(source, task_id="task", key="report")
        self.assertEqual(pending.exception.status.stage, ArtifactUploadStage.UNCERTAIN)
        self.assertEqual(len(self.requests), 3)
        async def recover(request, body):
            if request.path.endswith("/status"):
                return response(status(stage="uncertain"))
            self.assertTrue(request.path.endswith("/reconcile"))
            self.assertEqual(json.loads(body)["command"], selection())
            return response({"upload": status(stage="ready")})
        self.callback = recover
        current = await self.client.artifacts.upload_status("upload_1")
        accepted = await self.client.artifacts.reconcile(current.selection)
        self.assertEqual(accepted.reference, ArtifactRef.from_dict(reference()))
        self.assertEqual(sum(method == "PUT" for method, *_ in self.requests), 1)

    async def pending(self, request, body):
        return response({"upload": status(stage="uncertain")})

    async def test_replace_replays_exact_cas_and_validates_identity(self):
        async def handle(request, body):
            self.assertEqual(json.loads(body)["command"], {"upload_id": "upload_1", "expected_selection_revision": 1})
            return response({"replaced": selection(), "replacement": selection(2), "current": status(revision=2)})
        self.callback = handle
        first = await self.client.artifacts.replace("upload_1", expected_revision=1)
        second = await self.client.artifacts.replace("upload_1", expected_revision=1)
        self.assertEqual(first, second)
        with self.assertRaises(InputError):
            await self.client.artifacts.replace("../foreign", expected_revision=True)
        self.assertEqual(len(self.requests), 2)

    async def test_download_integrity_and_no_overwrite_race(self):
        for corrupt, race in ((False, False), (True, False), (False, True)):
            directory = self.directory()
            destination = directory / "download.bin"
            async def handle(request, body):
                if race:
                    destination.write_bytes(b"other writer")
                return binary(b"corrupt!" if corrupt else b"artifact", declared=b"artifact")
            self.callback = handle
            if corrupt:
                with self.assertRaises(ProtocolError):
                    await self.client.artifacts.download(reference(), destination)
                self.assertFalse(destination.exists())
            elif race:
                with self.assertRaises(FileExistsError):
                    await self.client.artifacts.download(reference(), destination)
                self.assertEqual(destination.read_bytes(), b"other writer")
            else:
                result = await self.client.artifacts.download(reference(), destination)
                self.assertEqual(result, destination)
                self.assertEqual(destination.read_bytes(), b"artifact")
                count = len(self.requests)
                with self.assertRaises(InputError):
                    await self.client.artifacts.download(reference(), destination)
                self.assertEqual(len(self.requests), count)
            self.assertEqual(list(directory.iterdir()), [destination] if destination.exists() else [])

    async def test_binary_budget_is_separate_from_normal_json_timeout(self):
        source = self.directory() / "source"
        source.write_bytes(b"artifact")
        async def handle(request, body):
            if request.path.endswith("/prepare"):
                return response({"upload": status()})
            await asyncio.sleep(.08)
            if request.method == "PUT":
                return response({"upload": status(stage="ready")})
            return response(manifest())
        self.callback = handle
        async with AsyncClient(self.url, tenant="tenant", namespace="tests", request_timeout=.04) as client:
            result = await client.artifacts.put_file(source, task_id="task", key="report", timeout=1)
            self.assertEqual(result.id, "art_test")
            with self.assertRaises(RequestTimeout):
                await client.artifacts.inspect("art_test")
        with self.assertRaises(ArtifactUploadUncertain):
            await self.client.artifacts.put_file(source, task_id="task", key="report", timeout=.04)

    async def test_cancel_during_file_creation_waits_for_io_and_removes_partial_file(self):
        directory = self.directory()
        entered, release = threading.Event(), threading.Event()
        original = artifact_files.temporary
        def delayed(destination, deadline):
            file = original(destination, deadline)
            entered.set()
            release.wait(3)
            return file
        with patch.object(artifact_files, "temporary", delayed):
            task = asyncio.create_task(self.client.artifacts.download(reference(), directory / "file"))
            await asyncio.to_thread(entered.wait, 3)
            self.assertTrue(entered.is_set())
            task.cancel()
            await asyncio.sleep(.02)
            self.assertFalse(task.done(), "filesystem ownership must survive cancellation")
            release.set()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertEqual(list(directory.iterdir()), [])
        self.assertEqual(self.requests, [])

    async def test_manifest_page_scope_and_selection_are_strict(self):
        async def handle(request, body):
            return response({"items": [manifest()], "next_cursor": None})
        self.callback = handle
        page = await self.client.artifacts.list(task_id="task")
        self.assertEqual(page.items[0].producer.subject, "user")
        self.assertIsInstance(page.items, tuple)
        bad = manifest()
        bad["scope"]["tenant_id"] = "foreign"
        async def foreign(request, body):
            return response(bad)
        self.callback = foreign
        with self.assertRaises(ProtocolError):
            await self.client.artifacts.inspect("art_test")
        with self.assertRaises(InputError):
            await self.client.artifacts.list(task_id="task", workflow_id="workflow")

    async def test_pages_reject_mixed_roots_but_allow_nested_target_canonicalization(self):
        for mixed in (False, True):
            first, second = manifest(), manifest()
            first["root"] = {"kind": "workflow", "id": "root-workflow"}
            second["reference"]["id"] = "art_other"
            second["root"] = {"kind": "workflow", "id": "foreign-workflow" if mixed else "root-workflow"}
            async def handle(request, body):
                self.assertEqual(json.loads(body)["target"], {"kind": "task", "id": "nested-task"})
                return response({"items": [first, second], "next_cursor": None})
            self.callback = handle
            if mixed:
                with self.assertRaises(ProtocolError):
                    await self.client.artifacts.list(task_id="nested-task")
            else:
                page = await self.client.artifacts.list(task_id="nested-task")
                self.assertEqual(len(page.items), 2)
                self.assertEqual(page.items[0].root.id, "root-workflow")

    async def test_valid_pending_put_is_recoverable_but_malformed_reply_is_not(self):
        source = self.directory() / "source"
        source.write_bytes(b"artifact")
        async def pending(request, body):
            return response({"upload": status(stage="prepared" if request.path.endswith("prepare") else "uncertain")})
        self.callback = pending
        with self.assertRaises(ArtifactRecoveryRequired) as raised:
            await self.client.artifacts.put_file(source, task_id="task", key="report")
        self.assertEqual(raised.exception.selection, ArtifactSelection(**selection()))
        async def malformed(request, body):
            return response({"upload": status()} if request.path.endswith("prepare") else {"upload": {"invalid": True}})
        self.callback = malformed
        with self.assertRaises(ProtocolError):
            await self.client.artifacts.put_file(source, task_id="task", key="report")
        self.assertEqual(sum(method == "PUT" for method, *_ in self.requests), 2)

    async def test_local_deadline_is_not_reported_as_uncertain_dispatch(self):
        source = self.directory() / "source"
        source.write_bytes(b"artifact")
        with patch.object(artifact_files, "prepare", side_effect=RequestTimeout(dispatched=False)):
            with self.assertRaises(RequestTimeout) as raised:
                await self.client.artifacts.put_file(source, task_id="task", key="report")
        self.assertFalse(raised.exception.dispatched)
        self.assertEqual(self.requests, [])

    async def test_replace_replay_keeps_original_successor_when_it_is_no_longer_selected(self):
        current = status(revision=2)
        current.update(selected=False, retry="unavailable", successor=selection(3))
        async def replay(request, body):
            return response({"replaced": selection(), "replacement": selection(2), "current": current})
        self.callback = replay
        receipt = await self.client.artifacts.replace("upload_1", expected_revision=1)
        self.assertEqual(receipt.replacement.revision, 2)
        self.assertFalse(receipt.current.selected)
        self.assertEqual(receipt.current.successor.revision, 3)
        async def unknown(request, body):
            return response({"code": "unavailable", "message": "lost acknowledgement"}, code=503)
        self.callback = unknown
        with self.assertRaises(ArtifactUploadUncertain) as raised:
            await self.client.artifacts.replace("upload_1", expected_revision=1)
        self.assertEqual(raised.exception.operation, {"kind": "replace", "upload_id": "upload_1", "expected_selection_revision": 1})
