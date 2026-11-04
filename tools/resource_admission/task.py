"""Native task admission qualification with an independently recorded handler marker (MIT)."""

import json
import threading
from pathlib import Path

from http_acceptance.harness import PROGRAM, eventually
from workflow_acceptance.owned_scenarios import quote
from .harness import AdmissionDeployment

START = "/v1/task-admissions/start"
CLOSE = "/v1/task-admissions/close"
INSPECT = "/v1/task-admissions/inspect"
RELEASE = "/v1/task-admissions/release"
SESSIONS = "/v1/task-admissions/sessions"


class TaskAdmissionDeployment(AdmissionDeployment):
    """Uses the real SDK helper, artifact store, subprocess pool and governed CLI submission."""

    def provision(self):
        super().provision()
        bundle = self.directory / "another-worker"
        self.bundle_directories.append(bundle)
        self.command(
            "ledgence",
            [
                "access",
                "issue",
                "--server",
                self.server_url,
                "--directory",
                str(bundle),
                "--subject",
                "another-worker",
                "--class",
                "worker",
                "--expires-at",
                self.expiry,
                "--config",
                str(self.config),
                "--capabilities",
                "worker_execute",
            ],
        )
        self.config = self.track_bundle(bundle)
        self.other_worker = bundle / "credential.json"

    def publish_example(self):
        # Qualify A1/A3 competition against the actual published workflow example.
        super().publish_example()
        self.publish(
            "hello-ledgence",
            "0.1.0",
            program_source=(
                self.root / "examples/hello-ledgence/task/program.py"
            ).read_text(),
            runtime_protocol=3,
        )
        # Both immutable packages are published before provenance is frozen.
        # A gate in module import controls preparation; PROGRAM writes a separate
        # JSONL record only inside the invoked handler.
        self.cold_gates = {
            name: self.directory / name for name in ("cold-ready", "cold-revoke")
        }
        for name, gate in self.cold_gates.items():
            self.publish(name, "1.0.0", startup_gate=gate, program_source=PROGRAM)

    def begin_task(
        self,
        key,
        *,
        program="invoice",
        mode="success",
        attempts=1,
        governed=True,
        **data,
    ):
        command = self.submission(key, mode=mode, program=program, **data)
        command["input"]["attempt_timeout_ms"] = 60_000
        command["input"]["retry_policy"]["max_attempts"] = attempts
        if governed:
            command["input"]["admission"] = {"resource": self.resource_name}
        return self.submit(command)["task_id"]

    def occurrence(self, task_id, generation=1):
        return {"scope": self.scope, "task_id": task_id, "generation": generation}

    def ledger(self, task_id, generation=1):
        occurrence = self.occurrence(task_id, generation)
        snapshot = self.checked("POST", INSPECT, {"occurrence": occurrence})
        assert snapshot["occurrence"] == occurrence
        return snapshot

    def ledger_state(self, task_id, state, generation=1, timeout=40):
        return eventually(
            lambda: (
                s if (s := self.ledger(task_id, generation))["state"] == state else None
            ),
            timeout=timeout,
            description=f"actual task admission {generation} becomes {state}",
        )

    def status(self, task_id):
        return self.checked("GET", self.query("/v1/tasks/status", task_id=task_id))

    def outcome(self, task_id, state="succeeded", timeout=45):
        def terminal():
            result = self.checked(
                "GET", self.query("/v1/tasks/result", task_id=task_id)
            )
            actual = result["task"]["state"]
            if actual not in {"succeeded", "failed", "cancelled"}:
                return None
            assert actual == state, f"task ended as {actual}, expected {state}"
            return result

        return eventually(
            terminal, timeout=timeout, description=f"task {task_id} reaches {state}"
        )

    def cancel_task(self, task_id):
        return self.checked(
            "POST", "/v1/tasks/cancel", {"scope": self.scope, "task_id": task_id}
        )

    def release_source(self, snapshot):
        command = {
            "occurrence": snapshot["occurrence"],
            "source_id": snapshot["source"]["source_id"],
            "expected_revision": snapshot["revision"],
            "request_id": "release-" + snapshot["source"]["source_id"],
            "reason": "The fixture retained the original worker's confirmed invocation cleanup.",
        }
        accepted = self.checked("POST", RELEASE, command)
        replay = self.checked("POST", RELEASE, command)
        assert not accepted["already_accepted"] and replay["already_accepted"]
        assert accepted["snapshot"] == replay["snapshot"]
        assert accepted["snapshot"]["state"] == "administratively_released"
        return command

    def assert_marker(self, task_id, expected):
        records = self.invocations(task_id)
        assert [row["number"] for row in records] == expected, (
            "handler entries disagree with admitted attempts"
        )
        return records

    def claims(self, task_id):
        return json.loads(
            self.sql(
                "SELECT coalesce(json_agg(convert_from(reply_bytes,'UTF8')::json), '[]'::json) "
                "FROM dispatch_claim_receipts WHERE task_id=" + quote(task_id)
            )
        )

    def hold_original_close(self):
        """Capture a real worker cleanup receipt *before* forwarding it; no credential is saved."""
        proxy = self.authenticated_proxy()
        captured = threading.Event()
        commands = []
        parent = proxy.server.RequestHandlerClass

        class Barrier(parent):
            def do_POST(self):
                if self.path != CLOSE:
                    return super().do_POST()
                size = int(self.headers.get("Content-Length", "0"))
                if not 0 < size <= 16 * 1024:
                    self.send_error(400)
                    return
                body = self.rfile.read(size)
                command = json.loads(body)
                if command.get("outcome") != "quiescent":
                    self.send_error(500)
                    return
                with proxy.lock:
                    commands.append(body)
                    # Drop only this capacity Close; ordinary settlement must
                    # independently reach the server and preserve a good result.
                    captured.set()
                self.close_connection = True

        proxy.server.RequestHandlerClass = Barrier
        return proxy, captured, commands

    def collect_task(self, task_id, *, retained):
        # Exercise the production collector using only the owned database. Age the
        # eligibility timestamps; never alter the source, close, or accounting rows.
        # A simultaneously eligible ordinary task proves the collector ran;
        # merely observing that the retained task still exists is not evidence.
        control = None
        if retained:
            control = self.begin_task("retention-ordinary-control", governed=False)
            self.cancel_task(control)
            self.outcome(control, "cancelled")
        self.server.stop()
        task = quote(task_id)
        original_terminal = int(
            self.sql("SELECT terminal_at_ms FROM tasks WHERE task_id=" + task)
        )
        candidates = ", ".join(
            quote(item) for item in (task_id, control) if item is not None
        )
        age = "(extract(epoch FROM clock_timestamp())*1000)::bigint-91*86400000::bigint"
        self.sql(
            "UPDATE tasks SET terminal_at_ms="
            + age
            + " WHERE task_id IN ("
            + candidates
            + ") AND terminal_at_ms IS NOT NULL"
        )
        self.sql(
            "UPDATE worker_sessions SET expires_at_ms=" + age + " WHERE NOT EXISTS "
            "(SELECT 1 FROM attempts WHERE worker_session_id=worker_sessions.session_id AND state='active')"
        )
        for _ in range(3):
            self.command(
                "ledgence",
                [
                    "orchestrator",
                    "retain",
                    "--tenant",
                    self.scope["tenant_id"],
                    "--namespace",
                    self.scope["namespace"],
                    "--retain-days",
                    "90",
                    "--batch-size",
                    "128",
                    "--batches",
                    "100",
                    "--apply",
                ],
            )
            collector_target = quote(control) if control is not None else task
            if (
                self.sql("SELECT count(*) FROM tasks WHERE task_id=" + collector_target)
                == "0"
            ):
                break
        if retained:
            assert (
                self.sql("SELECT count(*) FROM tasks WHERE task_id=" + quote(control))
                == "0"
            ), "the collector never removed its ordinary control"
            assert (
                self.sql(
                    "SELECT count(*) FROM tasks WHERE task_id="
                    + task
                    + " AND retiring_at_ms IS NULL"
                )
                == "1"
            ), "collector hid an unresolved occupied task from discovery"
            # Restore the real completion timestamp before exercising public
            # observation; it must agree with the immutable attempt result.
            self.sql(
                "UPDATE tasks SET terminal_at_ms="
                + str(original_terminal)
                + " WHERE task_id="
                + task
            )
        else:
            assert (
                self.sql("SELECT count(*) FROM tasks WHERE task_id=" + task) == "0"
            ), "collector retained released terminal task"
        self.start_server()
        if retained:
            assert self.status(task_id)["state"] == "succeeded"
            assert self.resource()["occupied"] == 1
