"""Explicit resource management and audited admission inspection/release."""

import asyncio
from dataclasses import asdict

from . import codec
from .admission_models import (
    LIMIT,
    SAFE,
    AdmissionOccurrence,
    AdmissionReleaseReceipt,
    AdmissionSnapshot,
    ResourcePage,
    ResourcePolicy,
    ResourcePutReceipt,
    ResourceSnapshot,
    _occurrence,
    _record,
    _resource,
    _snapshot,
    boolean,
    integer,
    parsed,
    timestamp,
)
from .admission_models import (
    name as resource_name,
)
from .errors import InputError
from .resource_decisions import ResourceDecisionPage, parse_page


async def _exchange(client, path, value, parser, *, limit=LIMIT):
    return await client._require_transport().exchange(
        "POST",
        path,
        body=codec.encode(value, 160 * 1024, max_depth=96),
        deadline=asyncio.get_running_loop().time() + client.request_timeout,
        parser=lambda raw: parsed(raw, parser),
        limit=limit,
    )


class Resources:
    def __init__(self, client):
        self._client = client

    async def put(
        self,
        name: str,
        *,
        policy: ResourcePolicy,
        request_id: str,
        reason: str,
        expected_revision: int | None = None,
    ) -> ResourcePutReceipt:
        """Create/CAS a policy once. Reconcile uncertainty by resending identical arguments."""
        if type(policy) is not ResourcePolicy:
            raise InputError("policy must be ResourcePolicy")
        resource_name(name)
        codec.text(request_id, "request_id")
        codec.text(reason, "reason", 1024)
        if expected_revision is not None:
            codec.integer(expected_revision, "expected_revision", 1, SAFE - 1)
        scope = self._client.scope
        value = {
            "scope": asdict(scope),
            "name": name,
            "policy": policy.to_dict(),
            "request_id": request_id,
            "reason": reason,
            "expected_revision": expected_revision,
        }

        def parse(raw):
            codec.fields(raw, {"resource", "already_accepted"})
            record = _record(raw["resource"], scope)
            if (
                record.name != name
                or record.policy != policy
                or record.revision != (expected_revision or 0) + 1
            ):
                raise InputError("resource receipt differs from command")
            return ResourcePutReceipt(record, boolean(raw["already_accepted"]))

        return await _exchange(self._client, "/v1/resources/put", value, parse)

    async def get(self, name: str) -> ResourceSnapshot:
        resource_name(name)
        scope = self._client.scope

        def parse(raw):
            result = _resource(raw, scope)
            if result.resource.name != name:
                raise InputError("foreign resource")
            return result

        return await _exchange(
            self._client,
            "/v1/resources/query",
            {"scope": asdict(scope), "name": name},
            parse,
        )

    async def list(self, *, after: str | None = None, limit: int = 50) -> ResourcePage:
        codec.integer(limit, "limit", 1, 100)
        if after is not None:
            resource_name(after)
        scope = self._client.scope

        def parse(raw):
            codec.fields(raw, {"items", "next"})
            if type(raw["items"]) is not list or len(raw["items"]) > limit:
                raise InputError("invalid resource page")
            items = tuple(_resource(item, scope) for item in raw["items"])
            previous = after
            for item in items:
                if previous is not None and item.resource.name <= previous:
                    raise InputError("resource page order")
                previous = item.resource.name
            if raw["next"] is not None:
                resource_name(raw["next"])
                if not items or raw["next"] != previous:
                    raise InputError("invalid resource cursor")
            return ResourcePage(items, raw["next"])

        return await _exchange(
            self._client,
            "/v1/resources/list",
            {"scope": asdict(scope), "after": after, "limit": limit},
            parse,
        )

    async def decisions(
        self, name: str, *, before: int | None = None, limit: int = 50
    ) -> ResourceDecisionPage:
        """Read committed weighted offers newest first. Pass page.next to continue."""

        resource_name(name)
        codec.integer(limit, "limit", 1, 100)
        if before is not None:
            codec.integer(before, "before", 1, (1 << 64) - 1)
        scope = self._client.scope
        return await _exchange(
            self._client,
            "/v1/resources/decisions",
            {
                "scope": asdict(scope),
                "name": name,
                "before": str(before) if before is not None else None,
                "limit": limit,
            },
            lambda raw: parse_page(raw, scope, name, before, limit),
        )


class Admissions:
    def __init__(self, client):
        self._client = client

    def _check(self, occurrence):
        if (
            type(occurrence) is not AdmissionOccurrence
            or occurrence.scope != self._client.scope
        ):
            raise InputError("admission belongs to another scope")

    async def inspect(self, occurrence: AdmissionOccurrence) -> AdmissionSnapshot:
        """Read sensitive binding/results through the audited inspection endpoint."""
        self._check(occurrence)
        return await _exchange(
            self._client,
            "/v1/admissions/inspect",
            {"occurrence": occurrence.to_dict()},
            lambda raw: _snapshot(raw, occurrence),
        )

    async def release(
        self,
        occurrence: AdmissionOccurrence,
        *,
        source_id: str,
        expected_revision: int,
        request_id: str,
        reason: str,
    ) -> AdmissionReleaseReceipt:
        """Explicit operator override. Releases occupancy; does not assert success.

        No automatic retry. Preserve all arguments, including request_id, when
        reconciling an uncertain acknowledgement. Inspect current evidence first.
        """
        self._check(occurrence)
        for key, value in (("source_id", source_id), ("request_id", request_id)):
            codec.text(value, key)
        codec.text(reason, "reason", 1024)
        codec.integer(expected_revision, "expected_revision", 1, SAFE - 1)
        value = {
            "occurrence": occurrence.to_dict(),
            "source_id": source_id,
            "expected_revision": expected_revision,
            "request_id": request_id,
            "reason": reason,
        }

        def parse(raw):
            codec.fields(
                raw,
                {
                    "occurrence",
                    "source_id",
                    "revision",
                    "accepted_at",
                    "already_accepted",
                },
            )
            _occurrence(raw["occurrence"], occurrence)
            if (
                raw["source_id"] != source_id
                or integer(raw["revision"], 1) != expected_revision + 1
            ):
                raise InputError("release receipt differs from command")
            return AdmissionReleaseReceipt(
                occurrence,
                source_id,
                raw["revision"],
                timestamp(raw["accepted_at"]),
                boolean(raw["already_accepted"]),
            )

        return await _exchange(self._client, "/v1/admissions/release", value, parse)
