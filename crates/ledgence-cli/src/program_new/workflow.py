"""An enum-addressed workflow with a single default entrypoint."""
from enum import StrEnum

from ledgence.worker.workflow import Workflow


class Entry(StrEnum):
    START = "start"


workflow = Workflow(Entry)


def greet(name: str) -> dict[str, str]:
    """Keep business logic independent of the execution envelope."""
    if not isinstance(name, str) or not name.strip() or len(name) > 200:
        raise ValueError("name must contain 1 to 200 characters and not be blank")
    return {"message": f"Hello, {name}!"}


@workflow.entrypoint(Entry.START, default=True)
async def start(event, ctx):
    # This pure computation belongs to this entrypoint. Use ctx.local only when
    # an operation needs its own durable result and explicit execution node.
    return ctx.complete(greet(event["data"]["name"]))


handle = workflow.build()
