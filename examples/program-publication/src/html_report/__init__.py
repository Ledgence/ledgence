"""Render a small report through an explicitly recorded workflow step."""
from enum import StrEnum
import platform

from ledgence.worker.workflow import Workflow
from markupsafe import escape
# This example intentionally requires the native wheel, rather than silently
# accepting MarkupSafe's pure-Python fallback. It exercises the target ABI.
from markupsafe import _speedups


class Entry(StrEnum):
    START = "start"


workflow = Workflow(Entry)


def render(title):
    if type(title) is not str or not 1 <= len(title) <= 200:
        raise ValueError("title must contain 1 to 200 characters")
    return {"html": f"<h1>{escape(title)}</h1>",
            "platform": platform.system().lower(),
            "architecture": platform.machine(),
            "native_extension": _speedups.__file__.split("/")[-1]}


@workflow.entrypoint(Entry.START, default=True)
async def start(event, ctx):
    data = event["data"]
    if type(data) is not dict:
        return ctx.fail("invalid_input", "data must be an object with a title")
    title = data.get("title")
    if type(title) is not str or not 1 <= len(title) <= 200:
        return ctx.fail("invalid_input", "title must contain 1 to 200 characters")
    report = await ctx.local("render-report", render, title=title)
    return ctx.complete(report)


handle = workflow.build()
