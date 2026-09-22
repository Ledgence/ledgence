#!/usr/bin/env python3
"""Run the published Compose programs through an installed ledgence.client wheel.

Start/publish the local stack first; see docs/local-deployment.md. The worker
must have exactly one slot for this example's warm-process reuse assertion.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import importlib.metadata
import json
from pathlib import Path
import sys
import sysconfig
import uuid

import ledgence
import ledgence.client
from ledgence.client import AsyncClient


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def installed_sdk():
    """Reject editable/source-shadowed imports and record the actual installed bytes."""
    distribution = importlib.metadata.distribution("ledgence-client")
    files = {str(item): item for item in distribution.files or ()}
    module = Path(ledgence.client.__file__).resolve()
    expected = distribution.locate_file("ledgence/client/__init__.py").resolve()
    sites = [Path(sysconfig.get_path(name)).resolve() for name in ("purelib", "platlib")]
    require(ledgence.__spec__.origin is None, "ledgence must be a native namespace")
    require("ledgence/client/__init__.py" in files and module == expected
            and module.is_relative_to(Path(sys.prefix).resolve())
            and any(module.is_relative_to(site) for site in sites),
            f"install the client wheel in this interpreter; source-shadowed SDK: {module}")
    direct = json.loads(distribution.read_text("direct_url.json") or "{}")
    require(not direct.get("dir_info", {}).get("editable"), "editable SDK installs are not accepted")
    record = distribution.read_text("RECORD")
    require(record is not None, "installed wheel RECORD is missing")
    hashes = {name: hashlib.sha256(distribution.locate_file(item).read_bytes()).hexdigest()
              for name, item in sorted(files.items())
              if name.startswith("ledgence/client/") and name.endswith(".py")}
    return {"distribution": distribution.metadata["Name"], "version": distribution.version,
            "python": sys.version, "interpreter": sys.executable, "prefix": sys.prefix,
            "module": str(module), "record_sha256": hashlib.sha256(record.encode()).hexdigest(),
            "module_sha256": hashes}


async def delivered(subscription, kind, identity, correlation):
    # This wait observes persisted delivery. It never retries the mutation or
    # executes a local callback; the caller may disconnect after registration.
    async with asyncio.timeout(90):
        while True:
            status = await subscription.status()
            require(status.state != "exhausted", f"callback exhausted: {status.last_failure}")
            if status.state == "delivered":
                require(status.command.target.kind == kind and status.command.target.id == identity,
                        "subscription target differs")
                event = status.event
                require(event is not None and event["ldgstate"] == "succeeded"
                        and event.get("ldgcorrelationkey") == correlation and "data" not in event,
                        "callback does not describe the expected reference-only completion")
                return event
            await asyncio.sleep(0.2)


async def run(server, sdk):
    prefix = "sdk-demo-" + uuid.uuid4().hex
    async with AsyncClient(server, tenant="acme", namespace="demo", request_timeout=10) as client:
        first = await client.tasks.submit(
            program="invoice-issuer", version="1.0.0", queue="demo",
            data={"invoice_id": prefix + "-1"}, idempotency_key=prefix + "-task-1",
            correlation_key=prefix)
        one = await first.result(timeout=90)
        second = await client.tasks.submit(
            program="invoice-issuer", version="1.0.0", queue="demo",
            data={"invoice_id": prefix + "-2"}, idempotency_key=prefix + "-task-2",
            correlation_key=prefix)
        two = await second.result(timeout=90)
        require(one["invoice_id"] == prefix + "-1" and two["invoice_id"] == prefix + "-2",
                "invoice outputs differ")
        require(type(one["pid"]) is int and one["pid"] > 0 and one["pid"] == two["pid"]
                and two["invocation"] == one["invocation"] + 1, "healthy process was not reused")
        # Deliberately register only after successful completion.
        task_subscription = await second.subscribe(
            destination="demo-callback", idempotency_key=prefix + "-task-callback")
        task_event = await delivered(task_subscription, "task", second.id, prefix)
        workflow = await client.workflows.submit(
            program="workflow-example", version="1.0.0", queue="demo",
            data={"urls": ["http://receiver:8091/page.txt"] * 4, "queue": "demo"},
            idempotency_key=prefix + "-workflow", correlation_key=prefix)
        output = await workflow.result(timeout=90)
        require(output == {"page_count": 4, "summary": {"pages": 4, "characters": 84}},
                f"workflow output differs: {output!r}")
        workflow_subscription = await workflow.subscribe(
            destination="demo-callback", idempotency_key=prefix + "-workflow-callback")
        workflow_event = await delivered(workflow_subscription, "workflow", workflow.id, prefix)
    return {"passed": True, "sdk": sdk, "task_ids": [first.id, second.id],
            "workflow_id": workflow.id,
            "subscriptions": [task_subscription.id, workflow_subscription.id],
            "callback_events": [task_event, workflow_event],
            "reused_process_id": one["pid"], "workflow_output": output}


async def bounded_run(server, sdk):
    async with asyncio.timeout(300):
        return await run(server, sdk)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", default="http://127.0.0.1:8080")
    parser.add_argument("--check-install", action="store_true", help="check installed SDK without contacting a server")
    args = parser.parse_args()
    sdk = installed_sdk()
    result = {"sdk": sdk} if args.check_install else asyncio.run(bounded_run(args.server, sdk))
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
