"""Artifact downloads remain queryable in the ordinary root-scoped audit history."""
import unittest
from unittest.mock import AsyncMock

from ledgence.client import AuditArtifact, AuditFilter, AuditReadKind, ProtocolError
from support import response
from test_audit import event, page
import test_workflows


class ArtifactAuditTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = test_workflows.WorkflowClientTests.asyncSetUp

    async def test_download_resource_is_typed_and_filters_by_canonical_root(self):
        for kind in ("task", "workflow"):
            resource = {"kind": "artifact", "artifact_id": "art_one", "root": {"kind": kind, "id": "root"}}
            value = event(operation={"kind": "sensitive_response", "read": "artifact_download", "resource": resource})
            self.callback = AsyncMock(side_effect=lambda request, body, value=value: response(page(value)))
            result = await self.client.audit.list(filter=AuditFilter.resource(kind, "root"))
            operation = result.items[0].operation
            self.assertEqual(operation.read, AuditReadKind.ARTIFACT_DOWNLOAD)
            self.assertIsInstance(operation.resource, AuditArtifact)
            self.assertEqual(operation.resource.root.kind, kind)
            self.assertEqual(operation.resource.artifact_id, "art_one")
            with self.assertRaises(ProtocolError):
                await self.client.audit.list(filter=AuditFilter.resource(kind, "other"))

    async def test_malformed_or_mismatched_artifact_audit_is_rejected(self):
        base = {"kind": "artifact", "artifact_id": "art_one", "root": {"kind": "workflow", "id": "wf_one"}}
        bad = [base | {"artifact_id": ""}, base | {"root": {"kind": "queue", "id": "q"}},
               base | {"root": {"kind": "workflow", "id": ""}}, base | {"url": "https://example.com"}]
        for resource in bad:
            value = event(operation={"kind": "sensitive_response", "read": "artifact_download", "resource": resource})
            self.callback = AsyncMock(side_effect=lambda request, body, value=value: response(page(value)))
            with self.assertRaises(ProtocolError):
                await self.client.audit.list()
        for read, resource in [("task_result", base), ("artifact_download", {"kind":"task","task_id":"t"})]:
            value = event(operation={"kind": "sensitive_response", "read": read, "resource": resource})
            self.callback = AsyncMock(side_effect=lambda request, body, value=value: response(page(value)))
            with self.assertRaises(ProtocolError):
                await self.client.audit.list()
