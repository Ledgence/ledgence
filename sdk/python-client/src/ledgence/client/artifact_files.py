"""Owned file operations for artifact transfers; no whole-file buffering."""
from __future__ import annotations

import asyncio
import hashlib
import os
from pathlib import Path
import stat
import tempfile
import time

from .artifact_models import CHUNK_BYTES, MAX_BYTES
from .errors import InputError, ProtocolError, RequestTimeout


def check(deadline):
    if time.monotonic() >= deadline:
        raise RequestTimeout(dispatched=False)


async def io_job(transport, function, *, cancel_cleanup=None):
    # Cancellation cannot stop a running filesystem syscall. Retain its waiter
    # until the job ends, including repeated cancellation during client.close().
    job = asyncio.create_task(transport._codec.run(function))
    cancelled = False
    while True:
        try:
            result = await asyncio.shield(job)
            break
        except asyncio.CancelledError:
            cancelled = True
        except BaseException:
            if cancelled:
                raise asyncio.CancelledError from None
            raise
    if cancelled:
        if cancel_cleanup is not None:
            await io_job(transport, lambda: cancel_cleanup(result))
        raise asyncio.CancelledError
    return result


def path(value):
    try:
        value = Path(value).absolute()
    except (ValueError, TypeError) as exc:
        raise InputError("invalid artifact path") from exc
    if "\x00" in str(value):
        raise InputError("invalid artifact path")
    return value


def prepare(source, deadline):
    check(deadline)
    if not stat.S_ISREG(source.lstat().st_mode):
        raise InputError("artifact source must be a regular file")
    file = source.open("rb", buffering=0)
    try:
        if not stat.S_ISREG(os.fstat(file.fileno()).st_mode):
            raise InputError("artifact source must be a regular file")
        digest = hashlib.sha256()
        size = 0
        while True:
            check(deadline)
            chunk = file.read(CHUNK_BYTES)
            if not chunk:
                break
            size += len(chunk)
            if size > MAX_BYTES:
                raise InputError("artifact exceeds 64 MiB")
            digest.update(chunk)
        file.seek(0)
        return file, size, "sha256:" + digest.hexdigest()
    except BaseException:
        file.close()
        raise


async def chunks(transport, source, size, deadline):
    remaining = size
    while True:
        check(deadline)
        chunk = await io_job(transport, lambda: source.read(CHUNK_BYTES))
        if len(chunk) > remaining:
            raise ProtocolError("artifact source grew after preparation")
        remaining -= len(chunk)
        if not chunk:
            if remaining:
                raise ProtocolError("artifact source shrank after preparation")
            return
        yield chunk


def temporary(destination, deadline):
    check(deadline)
    if destination.exists() or destination.is_symlink():
        raise InputError("artifact destination already exists")
    return tempfile.NamedTemporaryFile(mode="w+b", buffering=0, prefix=".ledgence-artifact-",
                                       dir=destination.parent, delete=False)


def publish(file, destination, deadline):
    check(deadline)
    file.flush()
    os.fsync(file.fileno())
    check(deadline)
    # Atomic no-replace publication on the same filesystem. A concurrent file or
    # symlink at the destination makes link fail without changing that entry.
    os.link(file.name, destination)


def remove_temporary(file):
    try:
        file.close()
    finally:
        try:
            os.unlink(file.name)
        except FileNotFoundError:
            pass


def append(file, chunk, deadline):
    pending = memoryview(chunk)
    while pending:
        check(deadline)
        count = file.write(pending)
        if count is None or count <= 0:
            raise OSError("artifact destination write made no progress")
        pending = pending[count:]
