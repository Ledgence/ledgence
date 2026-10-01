#!/usr/bin/env python3
"""Submit a change and export its review bundle through ledgence.client (MIT)."""
import argparse
import asyncio
from dataclasses import asdict
import json
import re
from pathlib import Path
import sys

from change_review.config import APPROVAL_KEY, CONTROL_QUEUE, DEFAULT_MODEL, VERSION, WORKFLOW
from change_review.inputs import submission


def export_bundle(bundle, output):
    from change_review.report import render_report
    from change_review.steps import validate_bundle
    bundle = validate_bundle(bundle)
    html = render_report(bundle)
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    for name, content in {
        "review.html": html,
        "shipping.py": bundle["candidate"]["source"],
        "change.patch": bundle["candidate"]["patch"],
        "review.json": json.dumps(bundle, indent=2, ensure_ascii=False) + "\n",
        "pull-request.md": bundle["pull_request"]["title"] + "\n\n" + bundle["pull_request"]["body"] + "\n",
    }.items():
        (output / name).write_text(content, encoding="utf-8")
    return {"workflow_id": bundle["workflow_id"], "status": bundle["status"],
            "directory": str(output.resolve()), "report": str((output / "review.html").resolve()),
            "candidate_sha256": bundle["candidate"]["sha256"], "pull_request": bundle["pull_request"]}


def decision_event(workflow_id, candidate_sha256, approved, event_id):
    """Bind an explicit decision to one workflow and the inspected candidate."""
    from change_review.validation import text
    text(workflow_id, 128, identifier=True)
    text(event_id, 128)
    if type(candidate_sha256) is not str or not re.fullmatch(r"[0-9a-f]{64}", candidate_sha256):
        raise ValueError("candidate SHA-256 must contain 64 lowercase hexadecimal characters")
    if type(approved) is not bool:
        raise ValueError("decision must be a boolean")
    return {"specversion": "1.0", "id": event_id,
            "source": "urn:ledgence:demo:change-review",
            "type": "com.ledgence.demo.change.reviewed.v1", "datacontenttype": "application/json",
            "data": {"workflow_id": workflow_id, "candidate_sha256": candidate_sha256, "approved": approved}}


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--server", default="http://127.0.0.1:8084")
    result.add_argument("--tenant", default="acme")
    result.add_argument("--namespace", default="demo")
    commands = result.add_subparsers(dest="command", required=True)
    submit = commands.add_parser("submit")
    submit.add_argument("--change-id", default="shipping-100")
    submit.add_argument("--idempotency-key", required=True)
    submit.add_argument("--model", default=DEFAULT_MODEL)
    submit.add_argument("--approval-timeout-ms", type=int, default=3_600_000)
    submit.add_argument("--publication", type=Path, help="explicit GitHub target JSON; omit for a local bundle")
    for name in ("status", "result", "cancel"):
        command = commands.add_parser(name)
        command.add_argument("--workflow", required=True)
        if name == "result":
            command.add_argument("--timeout", type=float, default=60)
            command.add_argument("--output", required=True, type=Path, help="new output directory")
    review = commands.add_parser("review", help="export the prepare:0 task result while approval is pending")
    review.add_argument("--task", required=True, help="prepare:0 task ID from the workflow's Console Children tab")
    review.add_argument("--timeout", type=float, default=60)
    review.add_argument("--output", required=True, type=Path, help="new output directory")
    for name in ("approve", "reject"):
        decision = commands.add_parser(name, help=f"{name} the exact candidate already inspected")
        decision.add_argument("--workflow", required=True)
        decision.add_argument("--candidate-sha256", required=True)
        decision.add_argument("--event-id", required=True, help="retain this ID and all arguments on uncertain delivery")
    return result


async def execute(args):
    from ledgence.client import AsyncClient, RetryPolicy

    async with AsyncClient(args.server, tenant=args.tenant, namespace=args.namespace) as client:
        if args.command == "submit":
            data = {"change_id": args.change_id, "model": args.model,
                    "approval_timeout_ms": args.approval_timeout_ms}
            if args.publication:
                if args.publication.stat().st_size > 4096:
                    raise ValueError("publication configuration exceeds 4 KiB")
                data["publication"] = json.loads(args.publication.read_text())
            data = submission(data)
            prepared = client.workflows.prepare(
                program=WORKFLOW, version=VERSION, queue=CONTROL_QUEUE, data=data,
                idempotency_key=args.idempotency_key, correlation_key=data["change_id"],
                retry_policy=RetryPolicy(max_attempts=3, retry_delay_ms=1000), attempt_timeout_ms=60_000,
            )
            workflow = await client.workflows.submit(prepared)
            return {"workflow_id": workflow.id, "change_id": data["change_id"]}
        if args.command == "review":
            return export_bundle(await client.tasks.handle(args.task).result(timeout=args.timeout), args.output)
        workflow = client.workflows.handle(args.workflow)
        if args.command == "status":
            return asdict(await workflow.status())
        if args.command == "cancel":
            return asdict(await workflow.cancel())
        if args.command in ("approve", "reject"):
            event = decision_event(args.workflow, args.candidate_sha256, args.command == "approve", args.event_id)
            command = workflow.prepare_event(APPROVAL_KEY, event=event)
            return asdict(await workflow.send_event(command))
        return export_bundle(await workflow.result(timeout=args.timeout), args.output)


def main(argv=None):
    args = parser().parse_args(argv)
    try:
        from ledgence.client import (
            LedgenceError, SubmissionUncertain, WaitTimeout,
            WorkflowCancellationUncertain, WorkflowEventUncertain,
        )
    except ImportError:
        print("Install the Ledgence client in this interpreter; see README.md.", file=sys.stderr)
        return 1
    try:
        output = asyncio.run(execute(args))
    except (SubmissionUncertain, WorkflowCancellationUncertain, WorkflowEventUncertain):
        print("Outcome uncertain. Repeat the exact command with unchanged identifiers and publication file.", file=sys.stderr)
        return 2
    except WaitTimeout:
        print("Observation timed out. The workflow continues; use the same workflow ID to observe again.", file=sys.stderr)
        return 3
    except (LedgenceError, OSError, ValueError, KeyError) as error:
        print(f"Command failed ({type(error).__name__}); inspect the workflow and use a fresh output directory.", file=sys.stderr)
        return 1
    print(json.dumps(output, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
