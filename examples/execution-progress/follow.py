"""Follow an existing execution; keep its cursor after processing each page."""
from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from ledgence.client import AsyncClient, ProgressTimeout


async def follow(args):
    checkpoint = Path(args.checkpoint)
    cursor = checkpoint.read_text(encoding="utf-8").strip() if checkpoint.exists() else None
    async with AsyncClient(
        args.server, tenant=args.tenant, namespace=args.namespace,
        credentials_file=args.credentials_file,
    ) as client:
        collection = client.tasks if args.kind == "task" else client.workflows
        execution = collection.handle(args.execution)
        try:
            async for page in execution.follow_progress(cursor=cursor, timeout=args.timeout):
                for event in page.events:
                    print(event.sequence, event.reason, event.at, flush=True)
                print("State:", page.state.value, flush=True)
                # A real consumer deduplicates using scope + target + sequence.
                # Save only AFTER processing the complete page. A crash between
                # processing and saving may display the same events again.
                temporary = checkpoint.with_suffix(checkpoint.suffix + ".tmp")
                temporary.write_text(page.next_cursor, encoding="utf-8")
                temporary.replace(checkpoint)
        except ProgressTimeout:
            print("Observation timed out. The execution continues; rerun to resume.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("kind", choices=("task", "workflow"))
    parser.add_argument("execution", help="existing execution ID; this script submits no work")
    parser.add_argument("--server", default="http://127.0.0.1:8080")
    parser.add_argument("--tenant", default="acme")
    parser.add_argument("--namespace", default="demo")
    parser.add_argument("--checkpoint", required=True, help="one cursor file per endpoint/scope/execution")
    parser.add_argument("--credentials-file")
    parser.add_argument("--timeout", type=float, default=60.0)
    asyncio.run(follow(parser.parse_args()))


if __name__ == "__main__":
    main()
