"""Exercise the installed Python client against real Rust/PostgreSQL/Python processes.

Run with a wheel-installed Python environment, LEDGENCE_POSTGRES_URL pointing at
an owned disposable server, and built Rust binaries. Creates/drops its own DB.
"""
import argparse
import asyncio
import contextlib
import json
import math
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import urllib.parse
import uuid

from http_acceptance.harness import Deployment


def equivalent(left, right):
    """Keep bool/int/float and negative zero distinct in the cross-language oracle."""
    assert type(left) is type(right), (type(left), type(right))
    if isinstance(left, dict):
        assert left.keys() == right.keys()
        for key in left:
            equivalent(left[key], right[key])
    elif isinstance(left, list):
        assert len(left) == len(right)
        for first, second in zip(left, right):
            equivalent(first, second)
    else:
        assert left == right, (left, right)
        if isinstance(left, float) and left == 0:
            assert math.copysign(1, left) == math.copysign(1, right)


async def scenarios(d, record):
    import ledgence.client as package
    from ledgence.client import (AsyncClient, Conflict, NotFound, RetryPolicy,
                                SubmissionUncertain, CancellationUncertain,
                                TaskFailed, TaskCancelled, WaitTimeout)

    location = Path(package.__file__).resolve()
    assert not location.is_relative_to(d.root), "client must come from the installed wheel"
    assert "ledgence_worker" not in sys.modules, "caller client must not import runtime helper"
    options = dict(tenant=d.scope["tenant_id"], namespace=d.scope["namespace"])
    proxy = d.proxy()
    settlement_lost = proxy.lose_once("/v1/settlements")
    worker = d.start_worker(server=proxy.url, concurrency=2)
    async with AsyncClient(proxy.url, **options) as client:
        def prepare(key, data, **kwargs):
            return client.tasks.prepare(program="echo", version="1.0.0", queue=d.queue,
                                        data=data, idempotency_key=key, **kwargs)

        async def send(key, data, **kwargs):
            return await client.tasks.submit(prepare(key, data, **kwargs))

        # A committed submission with a lost acknowledgement must be retried
        # byte-for-byte, despite subsequent mutation of the caller's dictionary.
        data = {"value": {"invoice": 42, "negative_zero": -0.0}}
        frozen = prepare("uncertain-frozen", data)
        lost = proxy.lose_once("/v1/tasks")
        try:
            await client.tasks.submit(frozen)
            raise AssertionError("lost acknowledgement must be uncertain")
        except SubmissionUncertain as error:
            assert error.submission is frozen
        assert lost.is_set()
        data["value"]["invoice"] = 99
        task = await client.tasks.submit(frozen)
        equivalent(await task.result(timeout=30), {"invoice": 42, "negative_zero": -0.0})
        replay = await client.tasks.submit(frozen)
        assert replay.id == task.id
        calls = proxy.commands("/v1/tasks", lambda body: body["idempotency_key"] == "uncertain-frozen")
        assert len(calls) == 3 and all(call["body"] == calls[0]["body"] for call in calls)
        assert await asyncio.to_thread(d.sql, "SELECT count(*) FROM tasks WHERE idempotency_key = 'uncertain-frozen'") == "1"
        try:
            await send("uncertain-frozen", data)
            raise AssertionError("changed input must conflict")
        except Conflict:
            pass
        record("frozen_submission_reconciliation", task.id)
        unavailable = prepare("unavailable-after-commit", {"value": "accepted"})
        proxy.unavailable_once("/v1/tasks")
        try:
            await client.tasks.submit(unavailable)
            raise AssertionError("post-commit Unavailable must be uncertain")
        except SubmissionUncertain as error:
            assert error.submission is unavailable
            assert error.request_id is not None
        accepted = await client.tasks.submit(unavailable)
        assert await accepted.result(timeout=20) == "accepted"
        record("unavailable_after_submission_commit", accepted.id)

        # Exercise values through package download, a pooled Python process,
        # CloudEvent input, Rust settlement bytes, PostgreSQL and HTTP result.
        values = [None, False, 0, 1, 1.0, -0.0, "é\u0000𝄞", -9223372036854775808,
                  18446744073709551615, 5e-324, 1.7976931348623157e308,
                  [None, False, 0], {"integer": 1, "float": 1.0, "negative": -0.0}]
        values.append("x" * (1024 * 1024 - len('{"value":""}')))
        nested = None
        for _ in range(63):
            nested = [nested]
        values.append(nested)  # outer data object is the 64th container
        for index, value in enumerate(values):
            handle = await send(f"value-{index}", {"value": value})
            result = await handle.wait(timeout=30)
            assert result.task.state == "succeeded"
            assert result.outcome.kind == "succeeded"
            equivalent(await handle.result(timeout=5), value)
        assert settlement_lost.is_set()
        settlements = proxy.commands("/v1/settlements", lambda body: body["owner"]["task_id"] == task.id)
        assert len(settlements) >= 2
        assert settlements[0]["body"] == settlements[1]["body"]
        record("cross_language_values_and_settlement_replay", len(values))

        # Pending reads and timeout never become success-null or remote cancel.
        gate = d.directory / "result-gate"
        d.gates.add(gate)
        held = await send("held", {"value": None, "gate": str(gate)})
        assert (await held.outcome()).outcome is None
        cli_scope = ["--server", d.server_url, "--tenant", options["tenant"],
                     "--namespace", options["namespace"], "--task", held.id]
        cli_pending = await asyncio.to_thread(d.command, "ledgence", ["task", "result", *cli_scope])
        assert cli_pending["outcome"] is None
        cli_status = await asyncio.to_thread(d.command, "ledgence", ["task", "status", *cli_scope])
        assert cli_status["state"] in ("queued", "active")
        assert not {"input", "output", "descriptor", "settlement"} & cli_status.keys()
        try:
            await held.wait(timeout=0.1)
            raise AssertionError("held task should exceed local wait")
        except WaitTimeout as error:
            assert error.task.id == held.id
            assert error.last_status.state in ("queued", "active")
        observation = asyncio.create_task(held.wait(timeout=30))
        await asyncio.sleep(0.05)
        observation.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await observation
        assert not proxy.commands("/v1/tasks/cancel")
        # A held observation cannot block an independent task on the same client.
        other = await send("concurrent", {"value": "ready"})
        assert await other.result(timeout=15) == "ready"
        gate.touch()
        assert await held.result(timeout=20) is None
        final = await held.outcome()
        assert final.outcome.kind == "succeeded" and final.outcome.output is None
        cli_terminal = await asyncio.to_thread(d.command, "ledgence", ["task", "result", *cli_scope])
        assert cli_terminal["outcome"]["kind"] == "succeeded"
        assert cli_terminal["outcome"]["output"] is None
        record("wait_cancel_and_concurrent_handles", held.id)

        failed = await send("application-error", {"fail": True},
                            retry_policy=RetryPolicy(max_attempts=1, retry_delay_ms=0))
        failed_result = await failed.wait(timeout=20)
        assert failed_result.outcome.kind == "failed"
        assert failed_result.outcome.failure.kind == "application"
        try:
            await failed.result(timeout=5)
            raise AssertionError("failed result should raise TaskFailed")
        except TaskFailed as error:
            assert error.result == failed_result
            assert "deliberate" in error.failure.error.message
        record("typed_application_failure", failed.id)

        # Queue without a consumer for cancellation before first attempt.
        cancelled = await client.tasks.submit(program="echo", version="1.0.0", queue="no-consumer",
                                               data=None, idempotency_key="cancel-before-attempt")
        proxy.lose_once("/v1/tasks/cancel")
        try:
            await cancelled.cancel()
            raise AssertionError("lost cancellation acknowledgement must be uncertain")
        except CancellationUncertain as error:
            assert error.task.id == cancelled.id
        assert await cancelled.cancel() == "cancelled"
        cancelled_result = await cancelled.wait(timeout=5)
        assert cancelled_result.outcome.kind == "cancelled"
        assert cancelled_result.task.latest_attempt_id is None
        try:
            await cancelled.result(timeout=5)
            raise AssertionError("cancelled result should raise TaskCancelled")
        except TaskCancelled as error:
            assert error.result == cancelled_result
        record("cancellation_reconciliation", cancelled.id)
        unavailable_cancel = await client.tasks.submit(program="echo", version="1.0.0", queue="no-consumer",
                                                         data=None, idempotency_key="cancel-unavailable")
        proxy.unavailable_once("/v1/tasks/cancel")
        try:
            await unavailable_cancel.cancel()
            raise AssertionError("post-commit cancellation Unavailable must be uncertain")
        except CancellationUncertain as error:
            assert error.task.id == unavailable_cancel.id
        assert (await unavailable_cancel.outcome()).outcome.kind == "cancelled"
        record("unavailable_after_cancellation_commit", unavailable_cancel.id)

        # Restart the actual Rust server. Saving task ID/scope is sufficient to
        # resume reads without serializing a local Python handle.
        await asyncio.to_thread(d.server.stop)
        d.server, _ = await asyncio.to_thread(d.start_server)
        saved_id = held.id
    async with AsyncClient(d.server_url, **options) as resumed:
        assert await resumed.tasks.handle(saved_id).result(timeout=5) is None
        async with AsyncClient(d.server_url, tenant=options["tenant"], namespace="wrong") as wrong:
            try:
                await wrong.tasks.handle(saved_id).status()
                raise AssertionError("wrong scope must be NotFound")
            except NotFound:
                pass
    record("restart_and_saved_identity", saved_id)
    await asyncio.to_thread(worker.stop)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--psql", default="psql")
    parser.add_argument("--binaries", type=Path, required=True)
    parser.add_argument("--evidence", type=Path)
    args = parser.parse_args()
    parent_url = os.environ.get("LEDGENCE_POSTGRES_URL")
    if not parent_url:
        parser.error("LEDGENCE_POSTGRES_URL must name a disposable test server")
    root = Path(__file__).resolve().parents[1]
    directory = args.evidence or Path(tempfile.mkdtemp(prefix="ledgence-python-client-e2e-"))
    if args.evidence:
        directory.mkdir(parents=True, exist_ok=False)
    directory = directory.resolve()
    database = "ledgence_client_" + uuid.uuid4().hex
    url = urllib.parse.urlunsplit(urllib.parse.urlsplit(parent_url)._replace(path="/" + database))
    def admin(sql):
        subprocess.run([args.psql, "--dbname", parent_url, "-X", "--set", "ON_ERROR_STOP=1",
                        "--command", sql], check=True, capture_output=True, timeout=40)
    deployment = None
    created = False
    succeeded = False
    results = []
    def record(name, detail):
        results.append(dict(scenario=name, result="passed", detail=detail))
        (directory / "results.json").write_text(json.dumps(results, indent=2) + "\n")
        print(f"PASS {name}", flush=True)
    try:
        admin(f'CREATE DATABASE "{database}"')
        created = True
        deployment = Deployment(root, directory, args.binaries.resolve(),
                                os.environ.get("LEDGENCE_PYTHON", sys.executable), url, args.psql)
        deployment.publish("echo", "1.0.0", program_source='''import time
from pathlib import Path
def handle(event):
    data = event['data']
    if data.get('fail'):
        raise RuntimeError('deliberate application failure')
    if data.get('gate'):
        deadline = time.monotonic() + 90
        while not Path(data['gate']).exists():
            if time.monotonic() > deadline:
                raise RuntimeError('test gate timeout')
            time.sleep(0.02)
    return data['value']
''')
        subprocess.run([str(deployment.binaries / "ledgence-orchestrator"), "migrate"],
                       env=deployment.environment, check=True, capture_output=True, timeout=40)
        deployment.server, _ = deployment.start_server()
        asyncio.run(scenarios(deployment, record))
        succeeded = True
        print(f"Installed Python client acceptance passed: {len(results)} scenarios")
        return 0
    finally:
        try:
            if deployment:
                deployment.close()
        finally:
            if created:
                admin(f'DROP DATABASE "{database}" WITH (FORCE)')
            if succeeded and not args.evidence:
                shutil.rmtree(directory)
            elif not succeeded:
                print(f"Failure evidence: {directory}", file=sys.stderr)


if __name__ == "__main__":
    sys.exit(main())
