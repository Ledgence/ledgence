#!/usr/bin/env python3
"""Run and review the fulfillment example through the public Python client (MIT)."""
import argparse
import asyncio
from dataclasses import asdict
import json
from pathlib import Path
import re
import sys

from fulfillment.config import APPROVAL_KEY, DEFAULT_MODEL, PROGRAM, QUEUE, SOURCE_WAIT, VERSION, submission


def source_event(workflow_id, event_id):
    for value in (workflow_id, event_id):
        if type(value) is not str or re.fullmatch(r"[A-Za-z0-9:_.-]{1,128}", value) is None:
            raise ValueError("workflow and event IDs must be bounded identifiers")
    return {"specversion": "1.0", "id": event_id, "source": "urn:ledgence:example:fulfillment",
            "type": "com.ledgence.fulfillment.source-ready.v1", "datacontenttype": "application/json",
            "data": {"workflow_id": workflow_id, "source": "warehouse", "revision": "corrected"}}


def encode(value):
    return json.dumps(value, indent=2, ensure_ascii=False) + "\n"


def review_bundle(approval, output):
    from fulfillment.report import load_candidate, render_report
    from fulfillment.storage import resolve_reference
    arguments = approval.action.arguments
    candidate = load_candidate(store=arguments["store"], candidate_ref=arguments["candidate_ref"])
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    (output / "review.json").write_text(encode(candidate), encoding="utf-8")
    (output / "review.html").write_text(render_report(candidate, published=False), encoding="utf-8")
    return {"workflow_id": approval.workflow_id, "approval_status": approval.status.value,
            "candidate_sha256": arguments["candidate_ref"]["sha256"],
            "candidate_path": str(resolve_reference(arguments["store"], arguments["candidate_ref"])),
            "review": str((output / "review.html").resolve())}


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--server", default="http://127.0.0.1:8087")
    result.add_argument("--tenant", default="acme")
    result.add_argument("--namespace", default="demo")
    commands = result.add_subparsers(dest="command", required=True)
    submit = commands.add_parser("submit")
    submit.add_argument("--store", required=True, help="absolute shared data directory from prepare.py")
    submit.add_argument("--scenario", choices=("data-gap", "real-delay"), default="data-gap")
    submit.add_argument("--idempotency-key", required=True)
    submit.add_argument("--mode", choices=("fixture", "codex"), default="fixture")
    submit.add_argument("--model", default=DEFAULT_MODEL)
    submit.add_argument("--queue", default=QUEUE)
    submit.add_argument("--source-timeout-ms", type=int, default=3_600_000)
    submit.add_argument("--approval-timeout-ms", type=int, default=3_600_000)
    for name in ("status", "result", "cancel", "source-ready", "review", "decide"):
        command = commands.add_parser(name)
        command.add_argument("--workflow", required=True)
        if name == "result":
            command.add_argument("--timeout", type=float, default=120)
        elif name == "source-ready":
            command.add_argument("--event-id", required=True)
        elif name == "review":
            command.add_argument("--output", required=True, type=Path, help="new local review directory")
        elif name == "decide":
            command.add_argument("--decision", required=True, choices=("approve", "reject"))
            command.add_argument("--decision-id", required=True)
            command.add_argument("--reviewer", required=True)
            command.add_argument("--candidate-sha256", required=True, help="digest printed by review")
            command.add_argument("--reason")
            command.add_argument("--file", required=True, type=Path,
                                 help="persist the exact decision before delivery; reuse on uncertainty")
    return result


async def decide(handle, args):
    if re.fullmatch(r"[0-9a-f]{64}", args.candidate_sha256) is None:
        raise ValueError("candidate digest must be SHA-256 from the reviewed packet")
    if args.file.exists():
        if args.file.stat().st_size > 32 * 1024:
            raise ValueError("decision command exceeds 32 KiB")
        command = handle.restore_approval_decision(json.loads(args.file.read_text(encoding="utf-8")))
    else:
        approval = await handle.approval(APPROVAL_KEY)
        if approval.action.arguments["candidate_ref"]["sha256"] != args.candidate_sha256:
            raise ValueError("the pending action does not match the reviewed candidate")
        command = handle.prepare_approval_decision(approval, decision_id=args.decision_id,
            decision=args.decision, reviewer=args.reviewer, reason=args.reason)
        # Persist before any network write. An existing command must never be replaced.
        with args.file.open("x", encoding="utf-8") as stream:
            stream.write(encode(command.to_dict()))
            stream.flush()
            import os
            os.fsync(stream.fileno())
    saved = command.to_dict()
    if (saved["key"] != APPROVAL_KEY or saved["decision_id"] != args.decision_id
            or saved["decision"] != args.decision or saved["reviewer"] != args.reviewer
            or saved.get("reason") != args.reason
            or saved["action"]["arguments"]["candidate_ref"]["sha256"] != args.candidate_sha256):
        raise ValueError("saved decision differs from this command; preserve all original arguments on retry")
    receipt = await handle.decide_approval(command)
    return {"approval": receipt.approval.to_dict(), "already_accepted": receipt.already_accepted}


async def execute(args):
    from ledgence.client import AsyncClient, RetryPolicy
    async with AsyncClient(args.server, tenant=args.tenant, namespace=args.namespace) as client:
        if args.command == "submit":
            data = submission({name: getattr(args, name) for name in (
                "store", "scenario", "mode", "model", "queue", "source_timeout_ms", "approval_timeout_ms")})
            handle = await client.workflows.submit(program=PROGRAM, version=VERSION, queue=data["queue"],
                data=data, idempotency_key=args.idempotency_key, correlation_key=data["scenario"],
                retry_policy=RetryPolicy(max_attempts=3, retry_delay_ms=1000), attempt_timeout_ms=180_000)
            return {"workflow_id": handle.id, "scenario": data["scenario"], "mode": data["mode"],
                    "console": args.server.rstrip("/") + "/console/workflows/" + handle.id}
        handle = client.workflows.handle(args.workflow)
        if args.command == "source-ready":
            command = handle.prepare_event(SOURCE_WAIT, event=source_event(handle.id, args.event_id))
            return asdict(await handle.send_event(command))
        if args.command == "review":
            return review_bundle(await handle.approval(APPROVAL_KEY), args.output)
        if args.command == "decide":
            return await decide(handle, args)
        if args.command == "status":
            return asdict(await handle.status())
        if args.command == "cancel":
            return asdict(await handle.cancel())
        output = await handle.result(timeout=args.timeout)
        if output.get("published"):
            from fulfillment.storage import resolve_reference
            approval = await handle.approval(APPROVAL_KEY)
            store = approval.action.arguments["store"]
            publication = output["publication"]
            for kind in ("report", "html"):
                publication[kind + "_path"] = str(resolve_reference(store, publication[kind + "_ref"]))
        return output


def main(argv=None):
    args = parser().parse_args(argv)
    try:
        from ledgence.client import (ApprovalDecisionUncertain, LedgenceError, SubmissionUncertain,
            WaitTimeout, WorkflowCancellationUncertain, WorkflowEventUncertain)
    except ImportError:
        print("Install the Python client from this checkout; see README.md.", file=sys.stderr)
        return 1
    try:
        value = asyncio.run(execute(args))
    except (SubmissionUncertain, ApprovalDecisionUncertain, WorkflowEventUncertain, WorkflowCancellationUncertain):
        print("Delivery outcome is uncertain. Retry the identical command and retain its decision file.", file=sys.stderr)
        return 2
    except WaitTimeout:
        print("Observation timed out; the workflow continues. Observe the same workflow ID again.", file=sys.stderr)
        return 3
    except (LedgenceError, OSError, ValueError, KeyError) as error:
        print(f"Command failed ({type(error).__name__}): {error}", file=sys.stderr)
        return 1
    print(encode(value), end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
