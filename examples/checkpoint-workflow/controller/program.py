"""Fetch concurrently in one workflow process, then stage a distributed child."""
import asyncio
from urllib.parse import urlsplit

from ledgence.worker.workflow import workflow_context


async def fetch(url):
    # This small example supports plain HTTP; applications may package their
    # preferred async HTTP client and prepared dependencies instead.
    parsed = urlsplit(url)
    if parsed.scheme != "http":
        raise ValueError("example URLs must use http")
    reader, writer = await asyncio.open_connection(parsed.hostname, parsed.port or 80)
    try:
        path = parsed.path or "/"
        if parsed.query:
            path += "?" + parsed.query
        writer.write(f"GET {path} HTTP/1.1\r\nHost: {parsed.netloc}\r\nConnection: close\r\n\r\n".encode("ascii"))
        await writer.drain()
        header = await reader.readuntil(b"\r\n\r\n")
        if not header.startswith(b"HTTP/1.1 200 ") and not header.startswith(b"HTTP/1.0 200 "):
            raise RuntimeError("example HTTP request failed")
        headers = dict(line.decode("ascii").split(":", 1) for line in header.split(b"\r\n")[1:] if line)
        headers = {key.lower(): value.strip() for key, value in headers.items()}
        if "transfer-encoding" in headers or "content-length" not in headers:
            raise ValueError("example requires a Content-Length response")
        length = int(headers["content-length"])
        if not 0 <= length <= 64 * 1024:
            raise ValueError("example response exceeds 64 KiB")
        return (await reader.readexactly(length)).decode("utf-8")
    finally:
        writer.close()
        await writer.wait_closed()


async def handle(event):
    ctx = workflow_context()
    if ctx.continuation == "start":
        pages = await ctx.gather(*[
            ctx.local(f"page-{index}", fetch, url=url)
            for index, url in enumerate(event["data"]["urls"])
        ])
        child = ctx.task("summarize", program="workflow-summary", version="1.0.0",
                         queue=event["data"]["queue"], data={"pages": pages})
        return ctx.suspend(continuation="collect", state={"page_count": len(pages)}, until=[child])
    return ctx.complete({"page_count": ctx.state["page_count"], "summary": ctx.get_result("summarize")})
