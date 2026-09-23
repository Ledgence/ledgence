"""One explicitly owned asynchronous client for a scoped Ledgence endpoint."""
from __future__ import annotations

from urllib.parse import urlsplit, urlunsplit

from . import codec
from .errors import ClientClosed, InputError
from .models import Scope
from .transport import _Transport


def _endpoint(value: str) -> str:
    if type(value) is not str or any(ord(c) <= 0x20 or ord(c) == 0x7F for c in value):
        raise InputError("base_url must be an HTTP(S) endpoint")
    try:
        parsed = urlsplit(value)
        if parsed.scheme not in ("http", "https") or not parsed.hostname or parsed.username is not None:
            raise ValueError("invalid endpoint")
        if parsed.query or parsed.fragment:
            raise ValueError("endpoint cannot contain query or fragment")
        host = parsed.hostname.encode("idna").decode("ascii").lower()
        if ":" in host:
            host = f"[{host}]"
        port = parsed.port
        if port is not None and port != (80 if parsed.scheme == "http" else 443):
            host += f":{port}"
        return urlunsplit((parsed.scheme, host, parsed.path.rstrip("/"), "", ""))
    except (ValueError, UnicodeError) as exc:
        raise InputError("invalid base_url") from exc


class AsyncClient:
    """Async task and workflow client. Open with async with; all use stays on one event loop."""

    def __init__(self, base_url: str, *, tenant: str, namespace: str,
                 request_timeout: float = 30.0):
        from .tasks import Tasks
        from .workflows import Workflows
        from .completions import Completions
        self._base_url = _endpoint(base_url)
        self._scope = Scope(tenant, namespace)
        self._request_timeout = codec.duration(request_timeout, "request_timeout", 30.0)
        self.tasks = Tasks(self)
        self.workflows = Workflows(self)
        self.completions = Completions(self)
        self._transport: _Transport | None = None
        self._closed = False

    @property
    def base_url(self) -> str:
        return self._base_url

    @property
    def scope(self) -> Scope:
        return self._scope

    @property
    def request_timeout(self) -> float:
        return self._request_timeout

    async def __aenter__(self) -> AsyncClient:
        if self._closed or self._transport is not None:
            raise ClientClosed("client cannot be reopened or entered twice")
        self._transport = _Transport(self.base_url)
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        await self.close()

    async def close(self) -> None:
        """Release owned operations; remote tasks continue independently.

        Running codec jobs retain ownership until complete. Cancellation of this
        await leaves one owned close operation; await close() again to join it.
        """
        self._closed = True
        if self._transport is not None:
            await self._transport.close()

    def _require_transport(self) -> _Transport:
        if self._closed or self._transport is None:
            raise ClientClosed("use the client inside an async with block")
        self._transport.check_open()
        return self._transport
