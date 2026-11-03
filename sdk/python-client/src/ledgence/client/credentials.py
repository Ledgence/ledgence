"""Private, endpoint-bound credentials. No credential contents enter diagnostics."""
from __future__ import annotations

import ipaddress
import os
import re
import stat
from urllib.parse import urlsplit

from . import codec
from .errors import InputError, ProtocolError

_CREDENTIAL_FILE_LIMIT = 16 * 1024


def validate_token(token: str) -> str:
    if type(token) is not str or not 1 <= len(token) <= 4096 or not re.fullmatch(r"[A-Za-z0-9\-._~+/]+=*", token):
        raise InputError("token must be a bounded bearer credential")
    return token


def require_secure_endpoint(endpoint: str) -> None:
    parsed = urlsplit(endpoint)
    if parsed.scheme == "https":
        return
    host = parsed.hostname
    if host == "localhost":
        return
    try:
        if host and ipaddress.ip_address(host).is_loopback:
            return
    except ValueError:
        pass
    raise InputError("credentials require HTTPS, except trusted loopback HTTP development")


def load_token(path: os.PathLike[str] | str, endpoint: str, normalize) -> str:
    try:
        # Open without following a final symlink; inspect the same descriptor we read.
        fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0))
        with os.fdopen(fd, "rb") as source:
            info = os.fstat(source.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_size > _CREDENTIAL_FILE_LIMIT:
                raise InputError("credential file must be a bounded regular file")
            if os.name == "posix" and (info.st_mode & 0o077 or info.st_uid != os.getuid()):
                raise InputError("credential file must be owned by you and private (mode 0600)")
            raw = source.read(_CREDENTIAL_FILE_LIMIT + 1)
        value = codec.decode(raw, _CREDENTIAL_FILE_LIMIT)
        codec.fields(value, {"server", "token"})
        if normalize(value["server"]) != endpoint:
            raise InputError("credential file belongs to a different server")
        return validate_token(value["token"])
    except (OSError, TypeError, ValueError, InputError, ProtocolError):
        raise InputError("invalid credential file: use a private, endpoint-bound {server, token} file") from None
