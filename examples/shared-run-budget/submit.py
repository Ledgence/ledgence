"""Submit a review with one classifier call shared by two owned workflows (MIT)."""

import argparse
import asyncio
from pathlib import Path

from ledgence.client import AsyncClient, RunBudgetPolicy


async def review(args):
    async with AsyncClient(
        args.server,
        tenant=args.tenant,
        namespace=args.namespace,
        credentials_file=args.credentials,
    ) as client:
        run = await client.workflows.submit(
            program="shared-run-budget",
            version="1.0.0",
            queue=args.queue,
            data={
                "queue": args.queue,
                "endpoint": args.classifier,
                "tickets": ["invoice total looks wrong", "how do I change my address?"],
            },
            idempotency_key=args.request_id,
            budget=RunBudgetPolicy(max_calls=1, max_delegations=2),
        )
        print("Workflow:", run.id)
        print("Review:", await run.result(timeout=90))
        print("Budget:", await client.budgets.inspect(workflow_id=run.id))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", default="http://127.0.0.1:8080")
    parser.add_argument("--tenant", default="acme")
    parser.add_argument("--namespace", default="demo")
    parser.add_argument("--queue", default="demo")
    parser.add_argument("--classifier", required=True)
    parser.add_argument("--credentials", required=True, type=Path)
    parser.add_argument("--request-id", required=True)
    asyncio.run(review(parser.parse_args()))
