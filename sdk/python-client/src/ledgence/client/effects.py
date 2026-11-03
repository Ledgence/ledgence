"""Bounded inspection of retained approved-effect evidence (MIT)."""
from __future__ import annotations

import asyncio

from . import codec
from .effect_models import (
    EFFECT_PAGE_LIMIT, EffectEvidenceDetail, EffectFilter, EffectInboxPage, EffectOccurrence,
    inbox_position, inspect_sequence, parse_effect_detail, parse_effect_inbox,
)
from .errors import InputError


class Effects:
    def __init__(self, client):
        self._client = client

    async def inbox(self, *, filter: EffectFilter | str = EffectFilter.ALL,
                    workflow_id: str | None = None, cursor: str | None = None,
                    limit: int = 50) -> EffectInboxPage:
        """Read metadata summaries, preserving filters when following next_cursor."""
        try:
            selected = EffectFilter(filter)
        except (ValueError, TypeError) as exc:
            raise InputError("invalid effect filter") from exc
        if workflow_id is not None:
            codec.text(workflow_id, "workflow_id")
        codec.integer(limit, "limit", 1, 100)
        scope = self._client.scope
        inbox_position(cursor, scope, selected, workflow_id)
        body = codec.encode({"scope": {"tenant_id": scope.tenant_id, "namespace": scope.namespace},
            "filter": selected.value, "workflow_id": workflow_id, "cursor": cursor, "limit": limit})
        return await self._client._require_transport().exchange(
            "POST", "/v1/workflows/approved-effects/inbox", body=body,
            deadline=asyncio.get_running_loop().time() + self._client.request_timeout,
            parser=lambda raw: parse_effect_inbox(raw, scope, selected, workflow_id, cursor, limit),
            limit=EFFECT_PAGE_LIMIT)

    async def inspect(self, *, workflow_id: str, activation_id: str, approval_key: str,
                      cursor: str | None = None, limit: int = 4) -> EffectEvidenceDetail:
        """Read the immutable binding and a page of reported evidence.

        Sensitive inspection is audited by the server. A short page can have a
        next_cursor when the byte budget was reached. Follow that cursor to read
        the next whole entry. Reports describe their recorded sources.
        """
        for name, value in (("workflow_id", workflow_id), ("activation_id", activation_id),
                            ("approval_key", approval_key)):
            codec.text(value, name)
        codec.integer(limit, "limit", 1, 4)
        occurrence = EffectOccurrence(self._client.scope, workflow_id, activation_id, approval_key)
        inspect_sequence(cursor, occurrence)
        body = codec.encode({"occurrence": occurrence.to_dict(), "cursor": cursor, "limit": limit})
        return await self._client._require_transport().exchange(
            "POST", "/v1/workflows/approved-effects/inspect", body=body,
            deadline=asyncio.get_running_loop().time() + self._client.request_timeout,
            parser=lambda raw: parse_effect_detail(raw, occurrence, cursor, limit, self._client.base_url),
            limit=EFFECT_PAGE_LIMIT)
