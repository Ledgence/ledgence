"""Submit and explicitly review one synthetic export (MIT)."""

import argparse
import asyncio
import json
import time
import uuid
from pathlib import Path

from ledgence.client import AsyncClient, ApprovalStatus


async def main(args):
    async with AsyncClient(
        args.server,
        tenant=args.tenant,
        namespace=args.namespace,
        credentials_file=args.credentials,
    ) as client:
        if args.decision_file.exists():
            raise FileExistsError(
                "Choose a new --decision-file; an existing decision is never overwritten"
            )
        run_key = "example-export:" + uuid.uuid4().hex
        workflow = await client.workflows.submit(
            program="provider-recovery",
            version="1.0.0",
            queue=args.queue,
            data={
                "dataset": "customers",
                "export_key": run_key,
                "provider_url": args.provider,
                "probe_queue": args.probe_queue,
            },
            idempotency_key=run_key,
            correlation_key=run_key,
        )
        print("Workflow:", workflow.id)
        deadline = time.monotonic() + 60
        while True:
            approvals = await workflow.approvals(limit=10)
            if approvals.items:
                break
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    "The workflow has not requested approval; inspect its execution before resubmitting"
                )
            await asyncio.sleep(0.2)
        approval = approvals.items[0]
        if approval.status is not ApprovalStatus.PENDING:
            raise RuntimeError("approval is no longer pending")
        print(json.dumps(approval.to_dict(), indent=2))
        if input("Approve this one local CSV export? Type yes: ").strip() != "yes":
            print("Left pending; no export was approved.")
            return
        command = workflow.prepare_approval_decision(
            approval,
            decision_id="approve:" + uuid.uuid4().hex,
            decision="approve",
            reviewer="local-example",
            reason="Reviewed the synthetic CSV export",
        )
        # Retain the same decision if a reply is lost; do not create another ID.
        with args.decision_file.open("x", encoding="utf-8") as saved:
            saved.write(json.dumps(command.to_dict(), indent=2) + "\n")
        await workflow.decide_approval(command)
        print(json.dumps(await workflow.result(timeout=90), indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", default="http://127.0.0.1:8080")
    parser.add_argument("--credentials", type=Path, required=True)
    parser.add_argument("--tenant", default="acme")
    parser.add_argument("--namespace", default="demo")
    parser.add_argument("--queue", default="demo")
    parser.add_argument("--probe-queue", default="provider-checks")
    parser.add_argument("--provider", default="http://127.0.0.1:8092")
    parser.add_argument(
        "--decision-file", type=Path, default=Path("export-approval.json")
    )
    asyncio.run(main(parser.parse_args()))
