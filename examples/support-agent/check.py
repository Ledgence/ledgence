#!/usr/bin/env python3
"""Opt-in real Gemini acceptance: one draft, durable approval, and an N=1 slot probe."""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import http.client
import importlib.util
import json
import os
from pathlib import Path
import platform
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import unicodedata
import urllib.parse
import uuid
import zipfile

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
sys.path.insert(0, str(ROOT / "tools"))
from http_acceptance.harness import ArtifactServer, Process, eventually, exchange, unused_port

SCOPE = {"tenant_id": "acme", "namespace": "demo"}
QUEUE = "support-demo"
PROGRAM_VERSIONS = {"support-agent": "1.0.2", "support-workflow": "1.0.3",
                    "support-demo-slot-probe": "1.0.1"}


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class CheckFailure(Exception):
    """Only static check descriptions belong in this exception."""


def require(condition, description):
    if not condition:
        raise CheckFailure(description)


def database_url(parent, database):
    require(re.fullmatch(r"ldg_support_demo_[0-9a-f]{32}", database), "invalid owned database name")
    parsed = urllib.parse.urlsplit(parent)
    require(parsed.scheme in ("postgres", "postgresql") and parsed.hostname,
            "database parent must be a PostgreSQL URI")
    query = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
    require(not any(key in ("dbname", "service") for key, _ in query),
            "database URI must not override its database through query parameters")
    return urllib.parse.urlunsplit(parsed._replace(path="/" + database))


def server_environment(inherited, secret):
    # Credentials are loaded separately into the worker. Never pass the Gemini
    # key (including aliases containing its value) to PostgreSQL or the server.
    return {name: value for name, value in inherited.items()
            if not name.startswith(("GOOGLE_", "GEMINI_")) and secret not in value}


class Evidence:
    def __init__(self, directory, secret):
        self.directory = directory
        self.secret_forms = {secret.encode(), json.dumps(secret)[1:-1].encode(),
                             urllib.parse.quote(secret, safe="").encode()}
        self.secret_detected = False

    def write_bytes(self, name, content):
        for secret in sorted(self.secret_forms, key=len, reverse=True):
            if secret and secret in content:
                self.secret_detected = True
                content = content.replace(secret, b"[REDACTED]")
        (self.directory / name).write_bytes(content)

    def write(self, name, value):
        self.write_bytes(name, (json.dumps(value, ensure_ascii=False, indent=2) + "\n").encode())

    def scan(self):
        return all(not any(secret in path.read_bytes() for secret in self.secret_forms if secret)
                   for path in self.directory.iterdir() if path.is_file())


def timeout_answer_review_hints(draft):
    """Non-authoritative wording hints; a human assesses the retained answer."""
    text = "".join(char for char in unicodedata.normalize("NFD", draft["reply"].lower())
                   if unicodedata.category(char) != "Mn")
    return {
        "cites_task_results": any(source["id"] == "task-results" for source in draft["sources"]),
        "explains_observation_timeout": "timeout" in text or "tiempo de espera" in text,
        "does_not_cancel": bool(re.search(r"\bno\b[^.!?\n]{0,200}\b(?:cancel\w*|detien\w*|interrump\w*)", text)),
        "does_not_resubmit": bool(re.search(r"\bno\b[^.!?\n]{0,200}\b(?:reenvi\w*|reintent\w*|repite\w*)", text)),
        "reuses_identity": bool(re.search(r"(?:mism[oa]|reutiliz\w*|conserv\w*|guard\w*)[^.!?\n]{0,140}(?:\bid\b|task_id|identificador|handle)", text)),
    }


def link_or_copy(source, destination):
    try:
        os.link(source, destination)
    except OSError:
        shutil.copy2(source, destination)
    return destination


def validate_prepared_sources(directory):
    """A new helper or corpus change requires a newly published package, too."""
    prepared = json.loads((directory / "prepared.json").read_text())
    descriptors = prepared.get("packages")
    require(type(descriptors) is dict and descriptors.keys() == {"agent", "workflow"},
            "prepared package descriptors are missing")
    for kind, program in (("agent", "support-agent"), ("workflow", "support-workflow")):
        source = HERE / kind
        package = directory / "packages" / kind
        expected = {path.relative_to(source).as_posix(): path for path in source.rglob("*")
                    if path.is_file() and "__pycache__" not in path.parts
                    and path.suffix != ".pyc" and not path.name.startswith(".env")}
        require("program.py" in expected, "checked demo program source is missing")
        actual = {path.relative_to(package).as_posix() for path in package.rglob("*")
                  if path.is_file() and path.name not in ("ledgence-program.json", "LEDGENCE-LICENSE")}
        # The ADK package also contains the reviewed vendored application graph.
        # Its owned corpus must be exact, and the store ZIP must match every
        # staged file, including dependencies and their retained notices.
        require(set(expected) <= actual, "prepared source files differ from checked source")
        if kind == "workflow":
            require(actual == set(expected), "prepared workflow files differ from checked source")
        require({name for name in actual if name.startswith("corpus/")} ==
                {name for name in expected if name.startswith("corpus/")},
                "prepared corpus files differ from checked source")
        if kind == "agent":
            for name in ("NOTICE.md", "inventory.json", "SUPPLEMENTAL-LICENSES.txt"):
                require((package / "third_party" / name).read_bytes() ==
                        (HERE / "third_party" / name).read_bytes(),
                        "prepared dependency review differs from checked source")
        for relative, path in expected.items():
            require((package / relative).read_bytes() == path.read_bytes(),
                    "prepared source differs from checked source; prepare fresh packages")
        manifest = json.loads((package / "ledgence-program.json").read_text())
        version = PROGRAM_VERSIONS[program]
        require(manifest.get("program") == {"id": program, "version": version},
                "prepared program identity differs from this demo version")
        require((package / "LEDGENCE-LICENSE").read_bytes() == (ROOT / "LICENSE").read_bytes(),
                "prepared package license differs from checked source")
        descriptor = descriptors[kind]
        require(type(descriptor) is dict and descriptor.keys() == {"program", "digest", "size"}
                and descriptor["program"] == manifest["program"], "prepared package descriptor is invalid")
        digest = descriptor["digest"]
        require(type(digest) is str and re.fullmatch(r"sha256:[0-9a-f]{64}", digest),
                "prepared package digest is invalid")
        require(type(descriptor["size"]) is int and 0 < descriptor["size"] <= 256 * 1024 * 1024,
                "prepared package size is invalid")
        stored = directory / "store/programs" / program / version / "descriptor.json"
        require(json.loads(stored.read_text()) == descriptor, "program store descriptor differs from prepared evidence")
        blob = directory / "store/blobs" / (digest.removeprefix("sha256:") + ".zip")
        require(blob.is_file() and not blob.is_symlink() and blob.stat().st_size == descriptor["size"],
                "program store blob size differs from prepared descriptor")
        with blob.open("rb") as stream:
            require("sha256:" + hashlib.file_digest(stream, "sha256").hexdigest() == digest,
                    "program store blob digest differs from prepared descriptor")
        packaged = {path.relative_to(package).as_posix(): path for path in package.rglob("*")
                    if path.is_file()}
        with zipfile.ZipFile(blob) as archive:
            entries = archive.infolist()
            files = {entry.filename: entry for entry in entries if not entry.is_dir()}
            require(len(entries) <= 4096 and len({entry.filename for entry in entries}) == len(entries)
                    and files.keys() == packaged.keys(), "program store blob files differ from prepared package")
            for name, path in packaged.items():
                content = path.read_bytes()
                require(files[name].file_size == len(content) and archive.read(name) == content,
                        "program store blob content differs from checked source")


class Deployment:
    def __init__(self, args, scratch, environment, worker_environment, database, evidence):
        self.args, self.scratch, self.evidence = args, scratch, evidence
        self.environment = dict(environment, DATABASE_URL=database, RUST_LOG="info", PYTHONDONTWRITEBYTECODE="1")
        self.worker_environment = dict(worker_environment, RUST_LOG="info", PYTHONDONTWRITEBYTECODE="1")
        self.worker_environment.pop("DATABASE_URL", None)
        self.worker_environment.pop("LEDGENCE_POSTGRES_URL", None)
        self.processes = []
        self.counter = 0
        self.artifacts = None
        self.store = scratch / "store"
        shutil.copytree(args.directory / "store", self.store, copy_function=link_or_copy)
        self.instance = scratch / "instance.json"
        self.instance.write_text(json.dumps({"instance_id": "support-demo-check", "name": "Support demo check",
                                            "scope": SCOPE, "suggested_queues": [QUEUE]}))
        self.server_url = f"http://127.0.0.1:{unused_port()}"
        self.server = self.worker = None

    def command(self, binary, arguments, label):
        completed = subprocess.run([str(self.args.binaries / binary), *arguments],
                                   env=self.environment, capture_output=True, timeout=90)
        # Save sanitized logs, never subprocess argv or an exception containing it.
        self.evidence.write_bytes(f"{label}.stdout", completed.stdout)
        self.evidence.write_bytes(f"{label}.stderr", completed.stderr)
        require(completed.returncode == 0, f"{label} command failed")
        return json.loads(completed.stdout) if completed.stdout.strip() else None

    def prepare(self):
        self.command("ledgence-orchestrator", ["migrate"], "migrate")
        probe = self.scratch / "probe-package"
        probe.mkdir()
        manifest = json.loads((self.args.directory / "packages/workflow/ledgence-program.json").read_text())
        manifest["program"] = {"id": "support-demo-slot-probe", "version": PROGRAM_VERSIONS["support-demo-slot-probe"]}
        manifest["runtime"]["protocol"] = 1
        manifest["handler"] = "program:handle"
        (probe / "ledgence-program.json").write_text(json.dumps(manifest))
        (probe / "program.py").write_text(
            'def handle(event):\n    return {"probe": "slot-released", "token": event["data"]["token"]}\n'
        )
        self.command("ledgence-worker", ["publish", "--source", str(probe), "--store", str(self.store)], "publish-probe")
        self.artifacts = ArtifactServer(self.store)

    def request(self, path, body=None, **query):
        url = path + ("?" + urllib.parse.urlencode(query) if query else "")
        status, _, response = exchange(self.server_url, "POST" if body is not None else "GET", url, body)
        require(status == 200, "Console request failed")
        return response

    def start_server(self):
        self.counter += 1
        address = urllib.parse.urlsplit(self.server_url).netloc
        self.server = Process([str(self.args.binaries / "ledgence-orchestrator"), "serve",
                               "--bind", address, "--store", self.artifacts.url,
                               "--instance-config", str(self.instance)],
                              self.scratch, f"server-{self.counter}", self.environment)
        self.processes.append(self.server)

        def ready():
            require(self.server.process.poll() is None, "orchestrator exited before readiness")
            try:
                return exchange(self.server_url, "GET", "/health/ready", timeout=1)[0] == 200
            except (OSError, http.client.HTTPException):
                return False
        eventually(ready, timeout=45, description="orchestrator readiness")

    def start_worker(self):
        self.counter += 1
        self.worker = Process([
            str(self.args.binaries / "ledgence-worker"), "connect", "--server", self.server_url,
            "--tenant", SCOPE["tenant_id"], "--namespace", SCOPE["namespace"], "--queue", QUEUE,
            "--store", self.artifacts.url, "--cache", str(self.scratch / "cache"),
            "--python", sys.executable, "--runner", str(ROOT / "sdk/python/ledgence/worker/bootstrap.py"),
            "--concurrency", "1",
        ], self.scratch, f"worker-{self.counter}", self.worker_environment)
        self.processes.append(self.worker)

    def register(self):
        for program, kind in (("support-agent", "task"), ("support-workflow", "workflow"),
                              ("support-demo-slot-probe", "task")):
            receipt = self.request("/v1/console/programs/register", {
                "program": {"id": program, "version": PROGRAM_VERSIONS[program]},
                "metadata": {"display_name": program, "description": None, "kind": kind},
                "update_metadata": False,
            })
            self.evidence.write(f"registration-{program}.json", receipt)

    def workflow_wait(self, workflow_id):
        result = self.request("/v1/console/workflows/inspect", workflow_id=workflow_id)
        state = result["summary"]["workflow"]["state"]
        if state in ("succeeded", "failed", "cancelled"):
            # The owned database is removed during cleanup. Retain the child
            # failure now; its parent's draft_failed outcome hides the cause.
            # Diagnostics must never replace the original acceptance failure.
            with contextlib.suppress(Exception):
                self.capture_workflow_failure(workflow_id, result)
            raise CheckFailure("workflow became terminal before approval")
        if state == "waiting" and result["external_wait_key"] == "approval:1":
            require(result["continuation"] == "finish", "wrong approval continuation")
            return result
        require(self.worker.process.poll() is None, "worker exited before approval wait")
        return None

    def capture_workflow_failure(self, workflow_id, inspection):
        report = {"workflow_inspection": inspection, "tasks": [], "errors": []}

        def fetch(stage, path, **query):
            try:
                return self.request(path, **query)
            except Exception as error:
                # Exception text may include a provider body, URL or credential.
                report["errors"].append({"stage": stage, "failure_type": type(error).__name__})
                return None

        def pages(stage, path, **query):
            retained, seen, cursor = [], set(), None
            # A healthy demo has one child and one attempt. Bound collection if
            # a broken endpoint repeats a cursor or produces unexpected pages.
            for _ in range(5):
                fields = dict(query, limit=20)
                if cursor is not None:
                    fields["cursor"] = cursor
                page = fetch(stage, path, **fields)
                if page is None:
                    return retained
                retained.append(page)
                if not isinstance(page, dict) or not isinstance(page.get("items"), list) or "next_cursor" not in page:
                    report["errors"].append({"stage": stage, "failure_type": "InvalidPage"})
                    return retained
                cursor = page["next_cursor"]
                if cursor is None:
                    return retained
                if not isinstance(cursor, str) or not cursor or cursor in seen:
                    report["errors"].append({"stage": stage, "failure_type": "InvalidCursor"})
                    return retained
                seen.add(cursor)
            report["errors"].append({"stage": stage, "failure_type": "PageLimitExceeded"})
            return retained

        try:
            report["workflow_result"] = fetch("workflow_result", "/v1/console/workflows/result",
                                               workflow_id=workflow_id)
            report["children"] = pages("children", "/v1/console/workflows/children", workflow_id=workflow_id)
            seen_tasks = set()
            for page in report["children"]:
                if not isinstance(page, dict) or not isinstance(page.get("items"), list):
                    continue
                for child in page["items"]:
                    if not isinstance(child, dict) or child.get("kind") != "task":
                        continue
                    task_id = child.get("target_id")
                    if not isinstance(task_id, str) or not task_id or task_id in seen_tasks:
                        continue
                    if len(seen_tasks) == 20:
                        report["errors"].append({"stage": "tasks", "failure_type": "TaskLimitExceeded"})
                        return
                    seen_tasks.add(task_id)
                    report["tasks"].append({
                        "task_id": task_id,
                        "result": fetch("task_result", "/v1/console/tasks/result", task_id=task_id),
                        "attempts": pages("task_attempts", "/v1/console/tasks/attempts", task_id=task_id),
                    })
        finally:
            self.evidence.write("failure-diagnostics.json", report)
            if report.get("workflow_result") is not None:
                self.evidence.write("unexpected-workflow-result.json", report["workflow_result"])

    def task_result(self, task_id):
        result = self.request("/v1/console/tasks/result", task_id=task_id)
        if result["task"]["state"] in ("succeeded", "failed", "cancelled"):
            self.evidence.write(f"task-{task_id}.json", result)
            require(result["task"]["state"] == "succeeded", "task did not succeed")
            return result
        return None

    def stop_services(self):
        self.worker.stop()
        self.server.stop()

    def close(self):
        failures = []
        for process in reversed(self.processes):
            try:
                if process.process.poll() is None:
                    process.stop()
            except Exception:
                failures.append(process.label)
            try:
                process.cleanup()
            except Exception:
                failures.append(process.label + "-cleanup")
            for log in (process.stdout_path, process.stderr_path):
                try:
                    self.evidence.write_bytes(log.name, log.read_bytes())
                except OSError:
                    failures.append(process.label + "-logs")
        # A forced worker exit must not leave its separately grouped helper.
        # Match the helper option and this unique fixture's package/cache path.
        try:
            listing = subprocess.check_output(["ps", "-axo", "pid=,args="], text=True)
            for line in listing.splitlines():
                parts = line.strip().split(None, 1)
                if len(parts) == 2 and "--package-root" in parts[1] and str(self.scratch) + "/" in parts[1]:
                    with contextlib.suppress(ProcessLookupError):
                        os.kill(int(parts[0]), signal.SIGKILL)
        except (OSError, subprocess.SubprocessError):
            failures.append("helper-cleanup")
        if self.artifacts:
            try:
                self.artifacts.close()
            except Exception:
                failures.append("artifact-server-cleanup")
        return failures


def submission(program, key, data):
    return {"idempotency_key": key, "origin_trace": None, "input": {
        "program": {"id": program, "version": PROGRAM_VERSIONS[program]}, "queue": QUEUE,
        "data": data, "retry_policy": {"max_attempts": 1, "retry_delay_ms": 0},
        "attempt_timeout_ms": 60_000,
    }}


def scenario(deployment, model):
    evidence = deployment.evidence
    ticket = json.loads((HERE / "tickets/observation-timeout.json").read_text())
    ticket.update(queue=QUEUE, model=model, approval_timeout_ms=3_600_000)
    sys.path.insert(0, str(ROOT / "sdk/python"))
    controller = load("support_check_controller", HERE / "workflow/program.py")
    controller._ticket({"data": ticket})
    print("RUN one real support draft (no automatic workflow resubmission)", flush=True)
    command = submission("support-workflow", "support-demo-live-draft", ticket)
    evidence.write("submission.json", command)
    submitted = deployment.request("/v1/console/workflows", command)
    workflow_id = submitted["workflow"]["workflow_id"]
    evidence.write("workflow-submitted.json", submitted)
    waiting = eventually(lambda: deployment.workflow_wait(workflow_id), timeout=240,
                         description="real draft followed by durable approval wait")
    evidence.write("workflow-waiting.json", waiting)
    children = deployment.request("/v1/console/workflows/children", workflow_id=workflow_id)
    require(len(children["items"]) == 1 and children["next_cursor"] is None,
            "workflow must have exactly one draft child")
    child = children["items"][0]
    require(child["kind"] == "task" and child["command_key"] == "draft", "wrong draft child binding")
    task_id = child["target_id"]
    task = deployment.task_result(task_id)
    require(task is not None and task["task"]["attempt_count"] == 1, "draft must have one attempt")
    output = controller._draft(task["outcome"]["output"], ticket)
    evidence.write("draft.json", output)
    evidence.write("answer-review-hints.json", timeout_answer_review_hints(output))
    waits = deployment.request("/v1/console/workflows/waits", workflow_id=workflow_id)
    evidence.write("waits-before-restart.json", waits)
    evidence.write("children-before-restart.json", children)

    print("RUN N=1 slot probe while the same workflow waits", flush=True)
    token = uuid.uuid4().hex
    probe = deployment.request("/v1/console/tasks", submission(
        "support-demo-slot-probe", "support-demo-slot-probe", {"token": token}))
    result = eventually(lambda: deployment.task_result(probe["task_id"]), timeout=45,
                        description="N=1 worker to run independent slot probe")
    require(result["outcome"]["output"] == {"probe": "slot-released", "token": token}, "slot probe failed")
    require(deployment.workflow_wait(workflow_id), "workflow stopped waiting during slot probe")
    evidence.write("slot-probe.json", result)

    print("RUN graceful worker and orchestrator restart during approval", flush=True)
    original_pids = [deployment.server.process.pid, deployment.worker.process.pid]
    deployment.stop_services()
    deployment.start_server()
    deployment.start_worker()
    require(deployment.workflow_wait(workflow_id), "approval wait did not survive restart")
    restored_waits = deployment.request("/v1/console/workflows/waits", workflow_id=workflow_id)
    require(restored_waits["page"]["items"] == waits["page"]["items"], "approval deadline changed across restart")
    evidence.write("waits-after-restart.json", restored_waits)
    restored_task = deployment.task_result(task_id)
    require(restored_task is not None and restored_task["task"]["attempt_count"] == 1
            and restored_task["outcome"] == task["outcome"], "accepted draft changed across restart")

    client = load("support_check_client", HERE / "client.py")
    event = client.review_event(ticket["ticket_id"], task_id, True, "support-demo-approval-1")
    approval = {"workflow_id": workflow_id, "key": "approval:1", "event": event}
    evidence.write("approval-command.json", approval)
    receipt = deployment.request("/v1/console/workflows/events", approval)
    duplicate = deployment.request("/v1/console/workflows/events", approval)
    require(receipt["already_accepted"] is False and duplicate["already_accepted"] is True,
            "duplicate approval did not reconcile")
    require({**receipt, "already_accepted": True} == duplicate, "approval receipt identity changed")
    evidence.write("approval-receipts.json", [receipt, duplicate])

    def finished():
        current = deployment.request("/v1/console/workflows/result", workflow_id=workflow_id)
        if current["workflow"]["state"] in ("succeeded", "failed", "cancelled"):
            return current
        return None
    final = eventually(finished, timeout=60, description="approved workflow result")
    evidence.write("workflow-result.json", final)
    require(final["workflow"]["state"] == "succeeded", "approved workflow did not succeed")
    require(final["outcome"]["output"] == {"ticket_id": ticket["ticket_id"], "status": "approved",
                                         "draft_task_id": task_id, "draft": output}, "final accepted draft changed")
    final_children = deployment.request("/v1/console/workflows/children", workflow_id=workflow_id)
    require(final_children["items"] == children["items"], "draft child was relaunched")
    final_task = deployment.task_result(task_id)
    require(final_task["task"]["attempt_count"] == 1 and final_task["outcome"] == task["outcome"],
            "draft attempt or accepted result changed")
    attempts = deployment.request("/v1/console/tasks/attempts", task_id=task_id)
    require(len(attempts["items"]) == 1 and attempts["next_cursor"] is None, "draft attempts were repeated")
    evidence.write("draft-attempts.json", attempts)
    evidence.write("children-final.json", final_children)
    return {"workflow_id": workflow_id, "draft_task_id": task_id, "probe_task_id": probe["task_id"],
            "concurrency": 1, "draft_attempt_count": 1, "model": model,
            "model_calls": output["model_calls"], "tool_calls": output["tool_calls"],
            "http_attempts": output["http_attempts"], "http_retries": output["http_retries"],
            "retry_wait_ms": output["retry_wait_ms"],
            "original_pids": original_pids,
            "restarted_pids": [deployment.server.process.pid, deployment.worker.process.pid],
            "server_url": deployment.server_url, "artifact_url": deployment.artifacts.url,
            "checks": ["real_gemini_draft", "validated_structured_draft", "timeout_answer_retained_for_review", "single_slot_released",
                       "approval_survives_service_restart", "deadline_preserved", "duplicate_event_reconciled",
                       "accepted_draft_preserved", "one_draft_attempt"]}


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--directory", type=Path, required=True)
    result.add_argument("--binaries", type=Path, required=True)
    result.add_argument("--env-file", type=Path, help="private credential file outside the repository")
    result.add_argument("--psql", default="psql")
    result.add_argument("--evidence", type=Path, required=True, help="new directory outside the repository")
    result.add_argument("--model", default="gemini-3.8-flash")
    result.add_argument("--live-gemini", action="store_true", required=True,
                        help="explicitly authorize this run's paid Gemini requests (at most six HTTP sends including retries)")
    return result


def main(argv=None):
    args = parser().parse_args(argv)
    evidence = None
    summary = {"passed": False, "phase": "configuration"}
    cleanup_ok = True
    try:
        require(platform.python_implementation() == "CPython" and sys.version_info[:2] == (3, 13),
                "checker requires CPython 3.13")
        args.directory, args.binaries, args.evidence = (path.resolve() for path in
                                                       (args.directory, args.binaries, args.evidence))
        require(not args.evidence.is_relative_to(ROOT), "evidence must stay outside the repository")
        require(not args.evidence.exists(), "evidence directory must be new")
        validate_prepared_sources(args.directory)
        prepared = json.loads((args.directory / "prepared.json").read_text())
        for binary in ("ledgence-worker", "ledgence-orchestrator"):
            require((args.binaries / binary).is_file(), "required binary is missing")
        parent = os.environ.get("LEDGENCE_POSTGRES_URL") or os.environ.get("DATABASE_URL")
        require(parent, "set DATABASE_URL or LEDGENCE_POSTGRES_URL for a disposable PostgreSQL parent")
        database = "ldg_support_demo_" + uuid.uuid4().hex
        owned_url = database_url(parent, database)
        worker_config = load("support_check_worker", HERE / "run_worker.py")
        if args.env_file is not None:
            require(not args.env_file.resolve().is_relative_to(ROOT), "credential file must be outside the repository")
        try:
            credentials = worker_config.worker_environment(args.env_file)
        except (OSError, ValueError):
            raise CheckFailure("could not load worker credentials; configure GOOGLE_API_KEY or a valid --env-file "
                               "and set GOOGLE_GENAI_USE_VERTEXAI=FALSE") from None
        secret = credentials["GOOGLE_API_KEY"]
        require(secret not in args.model, "model configuration must not contain the credential")
        safe_environment = server_environment(os.environ, secret)
        worker_environment = dict(safe_environment, GOOGLE_API_KEY=secret, GOOGLE_GENAI_USE_VERTEXAI="FALSE")
        args.evidence.mkdir(parents=True, exist_ok=False)
        evidence = Evidence(args.evidence, secret)
        evidence.write("prepared.json", prepared)
        evidence.write("program-sha256.json", {kind: hashlib.sha256((HERE / kind / "program.py").read_bytes()).hexdigest()
                                                for kind in ("agent", "workflow")})
        summary.update(database=database, scope=SCOPE, phase="database_setup")

        def admin(statement):
            result = subprocess.run([args.psql, "--dbname", parent, "-X", "--set", "ON_ERROR_STOP=1",
                                     "--command", statement], env=safe_environment, capture_output=True, timeout=40)
            require(result.returncode == 0, "owned database setup or cleanup failed")

        attempted_create = False
        deployment = None
        with tempfile.TemporaryDirectory(prefix="ledgence-support-check-") as scratch:
            try:
                attempted_create = True
                admin(f'CREATE DATABASE "{database}"')
                summary["phase"] = "service_setup"
                deployment = Deployment(args, Path(scratch), safe_environment, worker_environment, owned_url, evidence)
                deployment.prepare()
                deployment.start_server()
                deployment.register()
                deployment.start_worker()
                summary["phase"] = "live_scenario"
                summary.update(scenario(deployment, args.model), phase="shutdown")
                deployment.stop_services()
                summary.update(passed=True, phase="complete")
            finally:
                if deployment:
                    try:
                        cleanup_ok = not deployment.close()
                    except Exception:
                        cleanup_ok = False
                if attempted_create:
                    try:
                        admin(f'DROP DATABASE IF EXISTS "{database}" WITH (FORCE)')
                    except Exception:
                        cleanup_ok = False
    except (Exception, KeyboardInterrupt) as error:
        summary.update(passed=False, failure_type=type(error).__name__)
        if isinstance(error, CheckFailure):
            summary["failed_check"] = str(error)
        # Never print a traceback, arbitrary provider body, credential file, or argv.
    if evidence:
        summary["cleanup_complete"] = cleanup_ok
        summary["credential_scan_clean"] = not evidence.secret_detected and evidence.scan()
        summary["passed"] = summary["passed"] and cleanup_ok and summary["credential_scan_clean"]
        evidence.write("summary.json", summary)
    public = {"passed": summary["passed"], "phase": summary["phase"],
              "cleanup_complete": cleanup_ok,
              "credential_scan_clean": summary.get("credential_scan_clean"),
              "failure_type": summary.get("failure_type")}
    if "failed_check" in summary:
        public["failed_check"] = summary["failed_check"]
    if summary.get("failed_check") == "workflow became terminal before approval":
        public["next_step"] = ("Inspect failure-diagnostics.json in the --evidence directory for the draft task "
                               "outcome and attempts. Review retained service logs if diagnostics are incomplete.")
    print(json.dumps(public), flush=True)
    return 0 if summary["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
