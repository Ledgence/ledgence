"""Inspect a run using the installed SDK in an isolated interpreter (MIT)."""

import asyncio
import sys
from pathlib import Path

from ledgence.client import AsyncClient


async def main(server, credentials, tenant, namespace, root):
    async with AsyncClient(
        server, tenant=tenant, namespace=namespace, credentials_file=Path(credentials)
    ) as client:
        snapshot = await client.budgets.inspect(workflow_id=root)
        assert snapshot.calls == 1 and snapshot.delegations == 2
        page = await client.budgets.entries(workflow_id=root, limit=1)
        assert page.next is not None and len(page.items) == 1
        older = await client.budgets.entries(
            workflow_id=root, before=page.next, limit=1
        )
        assert (
            len(older.items) == 1 and older.items[0].sequence < page.items[0].sequence
        )


if __name__ == "__main__":
    asyncio.run(main(*sys.argv[1:]))
