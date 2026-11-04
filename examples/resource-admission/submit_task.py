"""Run the Hello Ledgence task under an existing named resource (development source)."""
import argparse
import asyncio
from pathlib import Path

from ledgence.client import AsyncClient, TaskAdmission


async def submit(args):
    async with AsyncClient(args.server, tenant=args.tenant, namespace=args.namespace,
                           credentials_file=args.credentials_file) as client:
        task = await client.tasks.submit(
            program="hello-ledgence", version="0.1.0", queue=args.queue,
            data={"name": "Ada"}, idempotency_key=args.key,
            admission=TaskAdmission(resource=args.resource),
        )
        print("Task:", task.id)
        print("Status:", (await task.status()).state.value)
        # A client timeout only ends this wait. The accepted task continues.
        print("Result:", await task.result(timeout=60))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", default="http://127.0.0.1:8080")
    parser.add_argument("--tenant", default="acme")
    parser.add_argument("--namespace", default="demo")
    parser.add_argument("--queue", default="demo")
    parser.add_argument("--credentials-file", type=Path, required=True)
    parser.add_argument("--resource", default="support-model")
    parser.add_argument("--key", required=True, help="Stable submission ID; reuse it only for identical input")
    asyncio.run(submit(parser.parse_args()))
