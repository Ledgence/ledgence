"""Fetch concurrently in one workflow process, then stage a distributed child."""
import asyncio
from contextlib import suppress
from enum import StrEnum
from urllib.parse import urlsplit

from ledgence.worker.workflow import Workflow


MAX_PAGES = 4
MAX_PAGE_BYTES = 8 * 1024
MAX_URL_LENGTH = 2048
FETCH_TIMEOUT_SECONDS = 10


class Entry(StrEnum):
    START = "start"
    COLLECT = "collect"


workflow = Workflow(Entry)


def parse_url(url):
    if (type(url) is not str or not url or len(url) > MAX_URL_LENGTH
            or any(ord(char) <= 32 or ord(char) >= 127 for char in url)):
        raise ValueError("URLs must be ASCII without whitespace, at most 2048 characters")
    parsed = urlsplit(url)
    if (parsed.scheme != "http" or not parsed.hostname or parsed.username is not None
            or parsed.password is not None or parsed.fragment or parsed.port == 0):
        raise ValueError("URLs must use http with a host and no credentials or fragment")
    return parsed


def input_data(event):
    data = event.get("data")
    if type(data) is not dict:
        raise ValueError("data must be an object")
    urls, queue = data.get("urls"), data.get("queue")
    if type(urls) is not list or not 1 <= len(urls) <= MAX_PAGES:
        raise ValueError("urls must contain one to four URLs")
    for url in urls:
        parse_url(url)
    if (type(queue) is not str or not 1 <= len(queue) <= 128
            or any(ord(char) < 32 or ord(char) >= 127 for char in queue)):
        raise ValueError("queue must contain 1 to 128 printable ASCII characters")
    return urls, queue


async def fetch(url):
    # Small plain-HTTP example. The bounds leave room for JSON escaping and
    # record bindings across all four local results and the child command.
    parsed = parse_url(url)
    async with asyncio.timeout(FETCH_TIMEOUT_SECONDS):
        reader, writer = await asyncio.open_connection(
            parsed.hostname, parsed.port or 80, limit=16 * 1024)
        try:
            path = parsed.path or "/"
            if parsed.query:
                path += "?" + parsed.query
            writer.write(f"GET {path} HTTP/1.1\r\nHost: {parsed.netloc}\r\nConnection: close\r\n\r\n".encode("ascii"))
            await writer.drain()
            header = await reader.readuntil(b"\r\n\r\n")
            if len(header) > 16 * 1024:
                raise ValueError("example response headers exceed 16 KiB")
            lines = header.split(b"\r\n")
            status = lines[0].split()
            if len(status) < 2 or status[0] not in (b"HTTP/1.0", b"HTTP/1.1") or status[1] != b"200":
                raise RuntimeError("example HTTP request failed")
            headers = {}
            for line in lines[1:]:
                if not line:
                    continue
                name, separator, value = line.decode("ascii").partition(":")
                name = name.lower()
                if not separator or not name or name != name.strip():
                    raise ValueError("invalid HTTP response header")
                if name == "content-length" and name in headers:
                    raise ValueError("duplicate Content-Length")
                headers[name] = value.strip()
            if ("transfer-encoding" in headers or "content-length" not in headers
                    or headers.get("content-encoding", "identity").lower() != "identity"):
                raise ValueError("example requires an uncompressed Content-Length response")
            length = headers["content-length"]
            if not length.isdecimal() or not 0 <= int(length) <= MAX_PAGE_BYTES:
                raise ValueError("example response exceeds 8 KiB or has an invalid length")
            return (await reader.readexactly(int(length))).decode("utf-8")
        finally:
            writer.close()
            with suppress(ConnectionError):
                await writer.wait_closed()


@workflow.entrypoint(Entry.START, default=True)
async def start(event, ctx):
    try:
        urls, queue = input_data(event)
    except ValueError as error:
        return ctx.fail("invalid_input", str(error))
    # Network/protocol exceptions remain activation failures and use its retry
    # policy. Previously acknowledged page results are reused on that retry.
    pages = await ctx.gather(*[
        ctx.local(f"page-{index}", fetch, url=url)
        for index, url in enumerate(urls)
    ])
    child = ctx.task("summarize", program="workflow-summary", version="1.0.0",
                     queue=queue, data={"pages": pages})
    return ctx.suspend(continuation=Entry.COLLECT,
                       state={"page_count": len(pages)}, until=[child])


@workflow.entrypoint(Entry.COLLECT)
def collect(event, ctx):
    outcome = ctx.inputs["summarize"]
    if outcome["state"] == "failed":
        return ctx.fail("summary_failed", "The summary task failed; inspect its terminal outcome")
    if outcome["state"] == "cancelled":
        return ctx.fail("summary_cancelled", "The summary task was cancelled")
    return ctx.complete({"page_count": ctx.state["page_count"],
                         "summary": ctx.get_result("summarize")})


handle = workflow.build()
