"""Execution artifacts: bounded streaming files and explicit upload recovery."""
from __future__ import annotations

import asyncio
import base64
from dataclasses import dataclass, field
import hashlib
import re

from . import artifact_files as files, codec
from .artifact_models import (
    ArtifactManifest, ArtifactPage, ArtifactRef, ArtifactReplacementReceipt, ArtifactRecoveryRequired,
    ArtifactRetryGuidance, ArtifactSelection, ArtifactTarget, ArtifactUploadStage, ArtifactUploadUncertain,
    CHUNK_BYTES, TRANSFER_TIMEOUT, filename, media_type, parse_manifest, parse_status, parse_upload,
)
from .errors import InputError, ProtocolError, RequestTimeout, TransportError


def _scope(client):
    return {"tenant_id": client.scope.tenant_id, "namespace": client.scope.namespace}


def _target(task_id, workflow_id):
    if (task_id is None) == (workflow_id is None):
        raise InputError("choose exactly one of task_id or workflow_id")
    return ArtifactTarget("task" if task_id is not None else "workflow", task_id if task_id is not None else workflow_id)


def _upload_id(value):
    codec.text(value, "upload_id")
    if re.fullmatch(r"[a-zA-Z0-9_-]+", value) is None:
        raise InputError("invalid upload identifier")
    return value


def _reference(value):
    return value if type(value) is ArtifactRef else ArtifactRef.from_dict(value)


def _accepted(status, metadata):
    if status.reference is None:
        return None
    ref = status.reference
    if (status.stage != ArtifactUploadStage.READY or not status.selected
            or status.retry != ArtifactRetryGuidance.COMPLETE
            or (ref.digest, ref.size, ref.media_type) != (metadata["digest"], metadata["size"], metadata["media_type"])):
        raise ProtocolError("artifact acceptance disagrees with prepared content")
    return ref


class Artifacts:
    def __init__(self, client):
        self._client = client

    def handle(self, artifact_id):
        return ArtifactHandle(self._client, codec.text(artifact_id, "artifact_id"))

    def _requester(self):
        return {"kind": "external", "scope": _scope(self._client)}

    async def _json(self, route, body, parser, *, deadline=None, limit=4096, transfer=False):
        transport = self._client._require_transport()
        now = asyncio.get_running_loop().time()
        budget = TRANSFER_TIMEOUT if transfer else self._client.request_timeout
        deadline = min(deadline if deadline is not None else now + budget, now + budget)
        return await transport.exchange("POST", route, body=codec.encode(body, 16384), deadline=deadline,
                                        parser=parser, limit=limit)

    async def inspect(self, artifact_id) -> ArtifactManifest:
        return await self._inspect(artifact_id)

    async def _inspect(self, artifact_id, deadline=None) -> ArtifactManifest:
        codec.text(artifact_id, "artifact_id")
        def parse(raw):
            value = parse_manifest(raw, self._client.scope)
            if value.reference.id != artifact_id:
                raise ProtocolError("artifact inspection identity mismatch")
            return value
        return await self._json("/v1/artifacts/inspect", {"scope": _scope(self._client), "artifact_id": artifact_id}, parse, deadline=deadline)

    async def list(self, *, task_id=None, workflow_id=None, cursor=None, limit=100) -> ArtifactPage:
        target = _target(task_id, workflow_id)
        codec.integer(limit, "limit", 1, 100)
        if cursor is not None:
            codec.text(cursor, "cursor", 4096)
        def parse(raw):
            codec.fields(raw, {"items", "next_cursor"})
            if type(raw["items"]) is not list or len(raw["items"]) > limit:
                raise InputError("invalid artifact page length")
            items = tuple(parse_manifest(item, self._client.scope) for item in raw["items"])
            if len({item.reference.id for item in items}) != len(items):
                raise InputError("duplicate artifact reference")
            if items and any(item.root != items[0].root for item in items[1:]):
                raise InputError("artifact page contains mixed canonical roots")
            next_cursor = raw["next_cursor"]
            if next_cursor is not None:
                codec.text(next_cursor, "next_cursor", 4096)
                if not items or next_cursor == cursor:
                    raise InputError("artifact cursor did not advance")
            return ArtifactPage(items, next_cursor)
        return await self._json("/v1/artifacts/list", {"scope": _scope(self._client), "target": target.to_dict(),
                                                       "cursor": cursor, "limit": limit}, parse, limit=512 * 1024)

    async def upload_status(self, upload_id):
        _upload_id(upload_id)
        def parse(raw):
            result = parse_status(raw)
            if result.selection.upload_id != upload_id:
                raise ProtocolError("upload status identity mismatch")
            return result
        return await self._json(f"/v1/artifacts/uploads/{upload_id}/status", {"requester": self._requester()}, parse)

    async def reconcile(self, selection: ArtifactSelection, *, timeout=TRANSFER_TIMEOUT):
        if type(selection) is not ArtifactSelection:
            raise InputError("selection must be an ArtifactSelection")
        _upload_id(selection.upload_id)
        deadline = asyncio.get_running_loop().time() + codec.duration(timeout, "timeout", TRANSFER_TIMEOUT)
        def parse(raw):
            result = parse_upload(raw)
            if result.selection != selection:
                raise ProtocolError("reconciliation selection mismatch")
            return result
        try:
            return await self._json(f"/v1/artifacts/uploads/{selection.upload_id}/reconcile",
                                    {"requester": self._requester(), "command": selection.to_dict()}, parse,
                                    deadline=deadline, transfer=True)
        except (ProtocolError, RequestTimeout) as exc:
            if isinstance(exc, ProtocolError) or not exc.dispatched:
                raise
            raise ArtifactUploadUncertain(selection, exc, operation={"kind": "reconcile", "selection": selection.to_dict()}) from exc
        except TransportError as exc:
            raise ArtifactUploadUncertain(selection, exc, operation={"kind": "reconcile", "selection": selection.to_dict()}) from exc

    async def replace(self, upload_id, *, expected_revision) -> ArtifactReplacementReceipt:
        _upload_id(upload_id)
        codec.integer(expected_revision, "expected_revision", 1, (1 << 63) - 2)
        def parse(raw):
            codec.fields(raw, {"replaced", "replacement", "current"})
            old = ArtifactSelection.from_dict(raw["replaced"])
            new = ArtifactSelection.from_dict(raw["replacement"])
            current = parse_status(raw["current"])
            if (old.upload_id != upload_id or old.revision != expected_revision
                    or new.upload_id == old.upload_id or new.artifact_id != old.artifact_id
                    or new.revision != old.revision + 1 or current.selection != new):
                raise ProtocolError("replacement receipt identity mismatch")
            return ArtifactReplacementReceipt(old, new, current)
        command = {"upload_id": upload_id, "expected_selection_revision": expected_revision}
        try:
            return await self._json(f"/v1/artifacts/uploads/{upload_id}/replace",
                                    {"requester": self._requester(), "command": command}, parse)
        except (ProtocolError, RequestTimeout) as exc:
            if isinstance(exc, ProtocolError) or not exc.dispatched:
                raise
            raise ArtifactUploadUncertain(None, exc, operation={"kind": "replace", **command}) from exc
        except TransportError as exc:
            raise ArtifactUploadUncertain(None, exc, operation={"kind": "replace", **command}) from exc

    async def put_file(self, path, *, key, task_id=None, workflow_id=None,
                       media_type="application/octet-stream", filename=None, timeout=TRANSFER_TIMEOUT) -> ArtifactRef:
        """Publish once; same key/content replays. Never automatically replace an uncertain upload."""
        transport = self._client._require_transport()
        target = _target(task_id, workflow_id)
        codec.text(key, "key")
        source = files.path(path)
        from .artifact_models import media_type as validate_type, filename as validate_filename
        validate_type(media_type)
        display = validate_filename(source.name if filename is None else filename)
        deadline = asyncio.get_running_loop().time() + codec.duration(timeout, "timeout", TRANSFER_TIMEOUT)
        return await transport.owned(self._put_file(transport, source, target, key, media_type, display, deadline))

    async def _put_file(self, transport, path, target, key, media_type, filename, deadline):
        source = None
        selection = None
        try:
            source, size, digest = await files.io_job(transport, lambda: files.prepare(path, deadline), cancel_cleanup=lambda result: result[0].close())
            metadata = {"digest": digest, "size": size, "media_type": media_type, "filename": filename}
            status = await self._json("/v1/artifacts/prepare", {"requester": self._requester(), "command": {
                "scope": _scope(self._client), "target": target.to_dict(), "key": key, "metadata": metadata,
            }}, parse_upload, deadline=deadline)
            selection = status.selection
            reference = _accepted(status, metadata)
            if reference is not None:
                return reference
            if not status.selected or status.stage != ArtifactUploadStage.PREPARED or status.retry != ArtifactRetryGuidance.UPLOAD:
                raise ArtifactRecoveryRequired(status)
            def parse(raw):
                result = parse_upload(raw)
                if result.selection != selection:
                    raise ProtocolError("upload selection mismatch")
                reference = _accepted(result, metadata)
                if reference is None:
                    raise ArtifactRecoveryRequired(result)
                return reference
            encoded = lambda value: base64.b64encode(codec.encode(value, 4096)).decode("ascii")
            return await transport.exchange("PUT", f"/v1/artifacts/uploads/{_upload_id(selection.upload_id)}/content",
                body=files.chunks(transport, source, size, deadline), deadline=deadline, parser=parse, limit=4096,
                extra_headers={"Content-Type": "application/octet-stream", "Ledgence-Artifact-Requester": encoded(self._requester()),
                               "Ledgence-Artifact-Selection": encoded(selection.to_dict())})
        except (ProtocolError, RequestTimeout) as exc:
            if isinstance(exc, ProtocolError) or not exc.dispatched:
                raise
            raise ArtifactUploadUncertain(selection, exc) from exc
        except TransportError as exc:
            raise ArtifactUploadUncertain(selection, exc) from exc
        finally:
            if source is not None:
                await files.io_job(transport, source.close)

    async def download(self, reference, destination, *, timeout=TRANSFER_TIMEOUT):
        """Stream, verify and atomically publish without overwriting an existing destination."""
        transport = self._client._require_transport()
        reference = _reference(reference)
        destination = files.path(destination)
        deadline = asyncio.get_running_loop().time() + codec.duration(timeout, "timeout", TRANSFER_TIMEOUT)
        return await transport.owned(self._download(transport, reference, destination, deadline))

    async def _download(self, transport, reference, destination, deadline):
        temp = None
        try:
            temp = await files.io_job(transport, lambda: files.temporary(destination, deadline), cancel_cleanup=files.remove_temporary)
            async def read(response, actual_deadline, request_id):
                if (response.headers.getall("Content-Type", []) != ["application/octet-stream"]
                        or response.headers.getall("Content-Encoding", []) or response.content_length != reference.size):
                    raise ProtocolError("invalid artifact download framing", request_id=request_id)
                headers = response.headers.getall("Ledgence-Artifact-Manifest", [])
                if len(headers) != 1 or len(headers[0]) > 8192:
                    raise ProtocolError("missing or oversized artifact manifest")
                try:
                    manifest = parse_manifest(codec.decode(base64.b64decode(headers[0], validate=True), 4096), self._client.scope)
                except (ValueError, InputError) as exc:
                    raise ProtocolError("invalid artifact manifest") from exc
                if manifest.reference != reference:
                    raise ProtocolError("artifact download changed reference")
                size = 0
                digest = hashlib.sha256()
                async for chunk in response.content.iter_chunked(CHUNK_BYTES):
                    size += len(chunk)
                    if size > reference.size:
                        raise ProtocolError("download exceeds declared size")
                    digest.update(chunk)
                    await files.io_job(transport, lambda chunk=chunk: files.append(temp, chunk, actual_deadline))
                if size != reference.size or "sha256:" + digest.hexdigest() != reference.digest:
                    raise ProtocolError("artifact download integrity check failed")
                return manifest
            await transport.exchange("POST", "/v1/artifacts/download", deadline=deadline,
                body=codec.encode({"requester": self._requester(), "command": reference.to_dict()}, 4096),
                parser=lambda _: None, response_reader=read, limit=4096)
            await files.io_job(transport, lambda: files.publish(temp, destination, deadline))
            return destination
        finally:
            if temp is not None:
                await files.io_job(transport, lambda: files.remove_temporary(temp))


@dataclass(frozen=True)
class ArtifactHandle:
    _client: object = field(repr=False, compare=False)
    id: str

    def __post_init__(self):
        codec.text(self.id, "artifact_id")

    async def inspect(self):
        return await self._client.artifacts.inspect(self.id)

    async def download(self, destination, *, timeout=TRANSFER_TIMEOUT):
        deadline = asyncio.get_running_loop().time() + codec.duration(timeout, "timeout", TRANSFER_TIMEOUT)
        manifest = await self._client.artifacts._inspect(self.id, deadline)
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            from .errors import RequestTimeout
            raise RequestTimeout(dispatched=True)
        return await self._client.artifacts.download(manifest.reference, destination, timeout=remaining)
