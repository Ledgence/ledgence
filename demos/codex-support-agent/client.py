#!/usr/bin/env python3
"""Submit and review the support demo through Ledgence's public Python client."""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import asdict
import json
from pathlib import Path
import sys


def read_ticket(path: Path) -> dict:
    if path.stat().st_size > 16 * 1024:
        raise ValueError("ticket file exceeds 16 KiB")
    ticket = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(ticket, dict) or set(ticket) != {"ticket_id", "question"}:
        raise ValueError("ticket must contain only ticket_id and question")
    for name, limit in (("ticket_id", 128), ("question", 8192)):
        value = ticket[name]
        if not isinstance(value, str) or not value.strip() or len(value.encode("utf-8")) > limit:
            raise ValueError(f"invalid {name}")
    return ticket


def review_event(ticket_id: str, draft_task_id: str, approved: bool, event_id: str) -> dict:
    return {
        "specversion": "1.0",
        "id": event_id,
        "source": "urn:ledgence:demo:support-review",
        "type": "com.ledgence.demo.support.reviewed.v1",
        "datacontenttype": "application/json",
        "data": {"ticket_id": ticket_id, "draft_task_id": draft_task_id, "approved": approved},
    }


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--server", default="http://127.0.0.1:8080")
    result.add_argument("--tenant", default="acme")
    result.add_argument("--namespace", default="demo")
    commands = result.add_subparsers(dest="command", required=True)
    submit = commands.add_parser("submit", help="submit once; the same key and input reconcile")
    submit.add_argument("--ticket", type=Path, required=True)
    submit.add_argument("--idempotency-key", required=True)
    submit.add_argument("--queue", default="codex-support-demo")
    submit.add_argument("--model", default="gpt-6-luna")
    submit.add_argument("--approval-timeout-ms", type=int, default=3_600_000)
    for name in ("status", "result", "cancel"):
        command = commands.add_parser(name)
        command.add_argument("--workflow", required=True)
        if name == "result":
            command.add_argument("--timeout", type=float, default=60)
    draft = commands.add_parser("draft", help="read the task ID shown under Console workflow Children")
    draft.add_argument("--task", required=True)
    review = commands.add_parser("review", help="approve or reject the specific draft already inspected")
    review.add_argument("--workflow", required=True)
    review.add_argument("--ticket-id", required=True)
    review.add_argument("--draft-task", required=True)
    review.add_argument("--decision", choices=("approve", "reject"), required=True)
    review.add_argument("--event-id", required=True, help="retain this ID and all arguments on uncertain delivery")
    return result


async def execute(args) -> dict | list | str | None:
    from ledgence.client import AsyncClient, RetryPolicy

    async with AsyncClient(args.server, tenant=args.tenant, namespace=args.namespace) as client:
        if args.command == "submit":
            data = read_ticket(args.ticket)
            data.update(queue=args.queue, model=args.model, approval_timeout_ms=args.approval_timeout_ms)
            submission = client.workflows.prepare(
                program="codex-support-workflow", version="1.0.1", queue=args.queue,
                data=data, idempotency_key=args.idempotency_key,
                correlation_key=data["ticket_id"],
                retry_policy=RetryPolicy(max_attempts=3, retry_delay_ms=1000),
                attempt_timeout_ms=60_000, origin_trace=None,
            )
            workflow = await client.workflows.submit(submission)
            return {"workflow_id": workflow.id, "ticket_id": data["ticket_id"]}
        if args.command == "draft":
            return await client.tasks.handle(args.task).result(timeout=1)
        workflow = client.workflows.handle(args.workflow)
        if args.command == "status":
            return asdict(await workflow.status())
        if args.command == "cancel":
            return asdict(await workflow.cancel())
        if args.command == "result":
            return await workflow.result(timeout=args.timeout)
        event = review_event(args.ticket_id, args.draft_task, args.decision == "approve", args.event_id)
        command = workflow.prepare_event("approval:1", event=event)
        return asdict(await workflow.send_event(command))


def main(argv=None) -> int:
    args = parser().parse_args(argv)
    try:
        from ledgence.client import (
            LedgenceError, SubmissionUncertain, WaitTimeout,
            WorkflowCancellationUncertain, WorkflowEventUncertain,
        )
    except ImportError:
        print("Install the Ledgence Python client in this interpreter; see the demo README.", file=sys.stderr)
        return 1
    try:
        result = asyncio.run(execute(args))
    except (SubmissionUncertain, WorkflowEventUncertain, WorkflowCancellationUncertain):
        print("Outcome uncertain. Repeat the same command with unchanged IDs, arguments and ticket file to reconcile.", file=sys.stderr)
        return 2
    except WaitTimeout:
        print("Observation timed out. The remote work continues; reuse its saved ID to observe again.", file=sys.stderr)
        return 3
    except (LedgenceError, ValueError, OSError) as error:
        # Provider errors live in the agent's redacted task result; never dump environment or credentials.
        print(f"Demo command failed ({type(error).__name__}); inspect the execution in Console.", file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
