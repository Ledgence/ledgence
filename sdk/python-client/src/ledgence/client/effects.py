"""Bounded inspection of retained approved-effect evidence (MIT)."""
from __future__ import annotations

import asyncio

from . import codec
from .effect_models import (
    EFFECT_PAGE_LIMIT, EffectEvidenceDetail, EffectFilter, EffectInboxPage, EffectOccurrence,
    inbox_position, inspect_sequence, parse_effect_detail, parse_effect_inbox,
)
from .errors import EffectResolutionUncertain, InputError, RequestTimeout, TransportError
from .effect_resolution_models import (
    EFFECT_RESOLUTION_COMMAND_MAX_BYTES, EFFECT_RESOLUTION_RECEIPT_MAX_BYTES,
    EffectResolutionAssessment, EffectResolutionCommand, EffectResolutionReceipt,
    parse_resolution_receipt, restore_resolution, validate_resolution_command,
)


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

    def prepare_resolution(self, detail: EffectEvidenceDetail, *, resolution_id: str,
                           assessment: EffectResolutionAssessment, reason: str,
                           evidence_references=()) -> EffectResolutionCommand:
        """Freeze an assessment of the inspected occurrence at its reviewed revision."""
        scope, endpoint = self._client.scope, self._client.base_url
        if (type(detail) is not EffectEvidenceDetail or detail._base_url != endpoint
                or detail.summary.occurrence.scope != scope
                or detail.binding.occurrence != detail.summary.occurrence):
            raise InputError("effect detail belongs to another endpoint or scope")
        if type(assessment) is not EffectResolutionAssessment:
            raise InputError("assessment must be an EffectResolutionAssessment")
        current = detail.summary.operator_resolution
        value = {"occurrence": detail.summary.occurrence.to_dict(), "resolution_id": resolution_id,
            "expected_effect_revision": detail.summary.effect_revision,
            "supersedes_resolution_id": current.reference.resolution_id if current else None,
            "action": detail.binding.approval.action.to_dict(), "assessment": assessment.to_dict(),
            "reason": reason, "evidence_references": list(evidence_references)
                if type(evidence_references) in (list, tuple) else evidence_references}
        validate_resolution_command(value, scope)
        body = codec.encode(value, EFFECT_RESOLUTION_COMMAND_MAX_BYTES, max_depth=96)
        return EffectResolutionCommand._create(endpoint, scope, detail.summary.occurrence, body)

    def restore_resolution(self, saved: str | bytes) -> EffectResolutionCommand:
        """Restore an application-owned saved envelope without rebinding or sending it."""
        return restore_resolution(saved, self._client.base_url, self._client.scope)

    async def resolve(self, command: EffectResolutionCommand) -> EffectResolutionReceipt:
        """Send once. Explicitly resend the same command to reconcile uncertainty."""
        if (type(command) is not EffectResolutionCommand or command.base_url != self._client.base_url
                or command.scope != self._client.scope):
            raise InputError("resolution command belongs to another endpoint or scope")
        try:
            return await self._client._require_transport().exchange(
                "POST", "/v1/workflows/approved-effects/resolve", body=command._body,
                deadline=asyncio.get_running_loop().time() + self._client.request_timeout,
                parser=lambda raw: parse_resolution_receipt(raw, command),
                limit=EFFECT_RESOLUTION_RECEIPT_MAX_BYTES)
        except RequestTimeout as exc:
            if not exc.dispatched: raise
            raise EffectResolutionUncertain(command, exc) from exc
        except TransportError as exc:
            raise EffectResolutionUncertain(command, exc) from exc
