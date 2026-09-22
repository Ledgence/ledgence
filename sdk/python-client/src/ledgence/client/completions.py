"""Durable completion subscriptions; no caller-side background waiters."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from . import codec
from .completion_models import (
    COMPLETION_COMMAND_LIMIT, COMPLETION_STATUS_LIMIT, CompletionRetryCommand,
    CompletionSubscribeCommand, CompletionSubscription, CompletionTarget,
    parse_completion_subscription,
)
from .errors import (
    CompletionRetryUncertain, CompletionSubscriptionUncertain, InputError,
    RequestTimeout, TransportError,
)
from .models import Scope

if TYPE_CHECKING:
    from .client import AsyncClient


class Completions:
    def __init__(self, client: AsyncClient):
        self._client = client

    def prepare(self, *, target: CompletionTarget, destination: str,
                idempotency_key: str) -> CompletionSubscribeCommand:
        """Freeze a subscription to an operator-configured destination alias."""
        if type(target) is not CompletionTarget:
            raise InputError("target must be a CompletionTarget")
        codec.text(destination, "destination")
        codec.text(idempotency_key, "idempotency_key")
        body = codec.encode({"scope": {"tenant_id": self._client.scope.tenant_id,
                                        "namespace": self._client.scope.namespace},
                             "target": {"kind": target.kind, "id": target.id},
                             "destination": destination, "idempotency_key": idempotency_key},
                            COMPLETION_COMMAND_LIMIT)
        return CompletionSubscribeCommand._create(self._client.base_url, self._client.scope,
                                                  target, destination, idempotency_key, body)

    async def subscribe(self, command: CompletionSubscribeCommand | None = None,
                        **kwargs) -> CompletionSubscriptionHandle:
        """Accept a durable subscription once; explicitly reconcile uncertainty."""
        deadline = asyncio.get_running_loop().time() + self._client.request_timeout
        transport = self._client._require_transport()
        if command is None:
            try:
                command = self.prepare(**kwargs)
            except TypeError as exc:
                raise InputError("invalid subscription arguments") from exc
        elif type(command) is not CompletionSubscribeCommand or kwargs:
            raise InputError("supply one subscription command or subscription arguments")
        elif command.scope != self._client.scope or command.base_url != self._client.base_url:
            raise InputError("subscription command belongs to another endpoint or scope")
        try:
            accepted = await transport.exchange(
                "POST", "/v1/completion-subscriptions", body=command._body, deadline=deadline,
                parser=lambda raw: parse_completion_subscription(
                    raw, self._client.base_url, self._client.scope, expected_command=command),
                limit=COMPLETION_STATUS_LIMIT,
            )
        except RequestTimeout as exc:
            if not exc.dispatched:
                raise
            raise CompletionSubscriptionUncertain(command, exc) from exc
        except TransportError as exc:
            raise CompletionSubscriptionUncertain(command, exc) from exc
        return CompletionSubscriptionHandle(self._client, accepted.subscription_id, command)

    def handle(self, subscription_id: str) -> CompletionSubscriptionHandle:
        """Create a local reference; existence is checked by status or retry."""
        return CompletionSubscriptionHandle(self._client, subscription_id)


@dataclass(frozen=True)
class CompletionSubscriptionHandle:
    _client: AsyncClient = field(repr=False, compare=False)
    id: str
    _command: CompletionSubscribeCommand | None = field(default=None, repr=False, compare=False)
    _scope: Scope = field(init=False, repr=False)
    _base_url: str = field(init=False, repr=False)

    def __post_init__(self):
        codec.text(self.id, "subscription_id")
        object.__setattr__(self, "_scope", self._client.scope)
        object.__setattr__(self, "_base_url", self._client.base_url)

    @property
    def scope(self) -> Scope:
        return self._scope

    def _parse(self, raw, *, expected_generation=None):
        return parse_completion_subscription(
            raw, self._base_url, self.scope, subscription_id=self.id,
            expected_command=self._command, expected_generation=expected_generation,
        )

    async def status(self) -> CompletionSubscription:
        """Read one bounded observation; this never advances delivery."""
        deadline = asyncio.get_running_loop().time() + self._client.request_timeout
        return await self._client._require_transport().exchange(
            "GET", "/v1/completion-subscriptions/status",
            query={"tenant_id": self.scope.tenant_id, "namespace": self.scope.namespace,
                   "subscription_id": self.id}, deadline=deadline,
            parser=self._parse, limit=COMPLETION_STATUS_LIMIT,
        )

    def prepare_retry(self, *, expected_generation: int) -> CompletionRetryCommand:
        """Freeze rearming one exhausted generation; reuse this command on uncertainty."""
        codec.integer(expected_generation, "expected_generation", 1, 999)
        body = codec.encode({"scope": {"tenant_id": self.scope.tenant_id,
                                        "namespace": self.scope.namespace},
                             "subscription_id": self.id,
                             "expected_generation": expected_generation}, COMPLETION_COMMAND_LIMIT)
        return CompletionRetryCommand._create(self._base_url, self.scope, self.id,
                                              expected_generation, body)

    async def retry(self, command: CompletionRetryCommand | None = None, *,
                    expected_generation: int | None = None) -> CompletionSubscription:
        """Rearm notification delivery only; never retry the underlying execution."""
        deadline = asyncio.get_running_loop().time() + self._client.request_timeout
        transport = self._client._require_transport()
        if command is None:
            command = self.prepare_retry(expected_generation=expected_generation)
        elif type(command) is not CompletionRetryCommand or expected_generation is not None:
            raise InputError("supply one retry command or expected_generation")
        elif (command.base_url != self._base_url or command.scope != self.scope
              or command.subscription_id != self.id):
            raise InputError("retry command belongs to another endpoint, scope, or subscription")
        try:
            return await transport.exchange(
                "POST", "/v1/completion-subscriptions/retry", body=command._body,
                deadline=deadline, parser=lambda raw: self._parse(
                    raw, expected_generation=command.expected_generation),
                limit=COMPLETION_STATUS_LIMIT,
            )
        except RequestTimeout as exc:
            if not exc.dispatched:
                raise
            raise CompletionRetryUncertain(command, exc) from exc
        except TransportError as exc:
            raise CompletionRetryUncertain(command, exc) from exc


def prepare_subscription(handle, kind, *, destination, idempotency_key):
    return handle._client.completions.prepare(
        target=CompletionTarget(kind, handle.id), destination=destination,
        idempotency_key=idempotency_key,
    )


async def subscribe_handle(handle, kind, command=None, **kwargs):
    if command is not None:
        if (type(command) is not CompletionSubscribeCommand
                or command.target != CompletionTarget(kind, handle.id)):
            raise InputError("subscription command belongs to another execution")
        return await handle._client.completions.subscribe(command, **kwargs)
    try:
        prepared = prepare_subscription(handle, kind, **kwargs)
    except TypeError as exc:
        raise InputError("invalid subscription arguments") from exc
    return await handle._client.completions.subscribe(prepared)
