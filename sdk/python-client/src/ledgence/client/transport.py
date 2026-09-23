"""Bounded HTTP/JSON exchange ownership, separate from task execution."""
from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
import functools
import re
from typing import Callable, TypeVar

import aiohttp

from . import codec, otel
from .errors import (
    ClientClosed, Conflict, InputError, NotFound, ProtocolError, RequestTimeout,
    ServiceError, TransportError, Unavailable,
)

T = TypeVar("T")
_MAX_EXCHANGES = 8
_CODEC_JOBS = 2


@dataclass
class _ExchangeState:
    dispatched: bool = False
    request_id: str | None = None


class _CodecJobs:
    """Cancellation releases the waiter, not the running job's admission slot."""

    def __init__(self):
        self._loop = asyncio.get_running_loop()
        self._permits = asyncio.Semaphore(_CODEC_JOBS)
        self._pool = ThreadPoolExecutor(max_workers=_CODEC_JOBS,
                                        thread_name_prefix="ledgence-json")
        self._pending: set[asyncio.Future] = set()
        self._closing = False

    async def run(self, function: Callable[[], T]) -> T:
        await self._permits.acquire()
        if self._closing:
            self._permits.release()
            raise ClientClosed("client is closing")
        try:
            raw = self._pool.submit(function)
        except BaseException:
            self._permits.release()
            raise
        future = asyncio.wrap_future(raw, loop=self._loop)
        self._pending.add(future)

        def finished(done):
            self._pending.discard(done)
            self._permits.release()
            # Consume detached waiter exceptions while retaining ordinary awaits.
            if not done.cancelled():
                done.exception()

        future.add_done_callback(finished)
        return await asyncio.shield(future)

    async def close(self):
        self._closing = True
        if self._pending:
            await asyncio.gather(*self._pending, return_exceptions=True)
        self._pool.shutdown(wait=False, cancel_futures=False)


def _decode_response(raw: bytes, status: int, request_id: str | None,
                     parser: Callable, limit: int):
    try:
        value = codec.decode(raw, limit)
        if status == 200:
            return parser(value)
        codec.fields(value, {"code"}, {"message"})
        code = value["code"]
        statuses = {
            "invalid_input": {400, 413, 415}, "not_found": {404}, "unknown_session": {404},
            "conflict": {409}, "ownership_lost": {409}, "obsolete_operation": {409},
            "out_of_order": {409}, "busy": {409}, "session_expired": {410},
            "unavailable": {503},
        }
        if type(code) is not str or status not in statuses.get(code, set()):
            raise ProtocolError("unknown or mismatched HTTP error response")
        message = value.get("message")
        if code in {"invalid_input", "unavailable"}:
            if type(message) is not str:
                raise ProtocolError("missing HTTP error message")
        elif "message" in value:
            raise ProtocolError("unexpected HTTP error message")
        if code == "unavailable":
            raise Unavailable("service unavailable", request_id=request_id)
        error_class = NotFound if code == "not_found" else Conflict if code == "conflict" else ServiceError
        raise error_class(message or code, code=code, request_id=request_id)
    except InputError as exc:
        raise ProtocolError("invalid response fields", request_id=request_id) from exc
    except ProtocolError as exc:
        exc.request_id = request_id
        raise


class _Transport:
    def __init__(self, base_url: str):
        self.base_url = base_url
        self._loop = asyncio.get_running_loop()
        self._slots = asyncio.Semaphore(_MAX_EXCHANGES)
        self._codec = _CodecJobs()
        self._operations: set[asyncio.Task] = set()
        self._close_task: asyncio.Task | None = None
        self._closing = False
        self._session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=_MAX_EXCHANGES,
                                          resolver=aiohttp.ThreadedResolver()),
            cookie_jar=aiohttp.DummyCookieJar(), auto_decompress=False, trust_env=False,
            timeout=aiohttp.ClientTimeout(total=30),
        )

    def check_open(self):
        if self._closing:
            raise ClientClosed("client is closing or closed")
        if asyncio.get_running_loop() is not self._loop:
            raise InputError("client must be used on the event loop where it was opened")

    async def exchange(self, method: str, route: str, *, body: bytes | None = None,
                       query: dict | None = None, deadline: float,
                       parser: Callable, limit: int = codec.RESPONSE_LIMIT):
        self.check_open()
        if self._loop.time() >= deadline:
            raise RequestTimeout(dispatched=False)
        state = _ExchangeState()
        operation = self._loop.create_task(self._run(
            method, route, body=body, query=query, deadline=deadline, parser=parser, limit=limit, state=state,
        ))
        self._operations.add(operation)
        try:
            result, request_id = await operation
            if self._loop.time() >= deadline:
                raise RequestTimeout(dispatched=True, request_id=request_id)
            return result
        except asyncio.CancelledError:
            if self._closing and not asyncio.current_task().cancelling():
                if state.dispatched:
                    raise TransportError("client closed during a dispatched exchange",
                                         request_id=state.request_id) from None
                raise ClientClosed("client closed before dispatch") from None
            raise
        finally:
            self._operations.discard(operation)

    async def _run(self, method, route, *, body, query, deadline, parser, limit, state):
        dispatched = False
        request_id = None
        try:
            async with asyncio.timeout_at(deadline):
                async with self._slots:
                    self.check_open()
                    if self._loop.time() >= deadline:
                        raise RequestTimeout(dispatched=False)
                    operation = route.rsplit("/", 1)[-1]
                    with otel._exchange(method, operation, deadline=deadline) as (carrier, span):
                        headers = {"Accept": "application/json", "Accept-Encoding": "identity",
                                   **carrier}
                        if body is not None:
                            headers["Content-Type"] = "application/json"
                        if self._loop.time() >= deadline:
                            raise RequestTimeout(dispatched=False)
                        dispatched = True
                        state.dispatched = True
                        async with self._session.request(
                            method, self.base_url + route, data=body, params=query,
                            headers=headers, allow_redirects=False,
                            timeout=aiohttp.ClientTimeout(total=max(deadline - self._loop.time(), 1e-9)),
                        ) as response:
                            ids = response.headers.getall("Request-Id", [])
                            if (len(ids) == 1 and len(ids[0]) <= 256
                                    and all("!" <= char <= "~" for char in ids[0])):
                                request_id = ids[0]
                            state.request_id = request_id
                            if span is not None:
                                span.set_attribute("http.response.status_code", response.status)
                                if response.status != 200:
                                    span.record_error(f"http_{response.status}")
                                if request_id is not None:
                                    span.set_attribute("ledgence.request.id", request_id)
                            types = response.headers.getall("Content-Type", [])
                            if len(types) != 1 or not re.fullmatch(
                                r'application/json(?:\s*;\s*charset\s*=\s*(?:utf-8|"utf-8"))?',
                                types[0].strip(), re.IGNORECASE,
                            ):
                                raise ProtocolError("response content type must be UTF-8 JSON")
                            encodings = response.headers.getall("Content-Encoding", [])
                            if encodings and (len(encodings) != 1 or encodings[0].strip().lower() != "identity"):
                                raise ProtocolError("compressed responses are unsupported")
                            effective_limit = limit if response.status == 200 else 64 * 1024
                            if response.content_length is not None and response.content_length > effective_limit:
                                raise ProtocolError("response exceeds its byte limit")
                            raw = bytearray()
                            async for chunk in response.content.iter_chunked(64 * 1024):
                                if len(raw) + len(chunk) > effective_limit:
                                    raise ProtocolError("response exceeds its byte limit")
                                raw.extend(chunk)
                            status = response.status
                        result = await self._codec.run(functools.partial(
                            _decode_response, bytes(raw), status, request_id, parser, effective_limit,
                        ))
                        if self._loop.time() >= deadline:
                            raise RequestTimeout(dispatched=True, request_id=request_id)
                        return result, request_id
        except RequestTimeout:
            raise
        except TimeoutError as exc:
            raise RequestTimeout(dispatched=dispatched, request_id=request_id) from exc
        except ProtocolError as exc:
            exc.request_id = request_id
            raise
        except (aiohttp.ClientError, OSError) as exc:
            raise TransportError("HTTP exchange failed", request_id=request_id) from exc

    async def close(self):
        if self._close_task is None:
            self._closing = True
            operations = list(self._operations)
            for operation in operations:
                operation.cancel()
            self._close_task = self._loop.create_task(self._finish_close(operations))
        await asyncio.shield(self._close_task)

    async def _finish_close(self, operations):
        try:
            await asyncio.gather(*operations, return_exceptions=True)
            await self._session.close()
        finally:
            await self._codec.close()
