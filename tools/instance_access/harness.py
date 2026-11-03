"""Owned credential bundles and processes. Never record bearer credentials."""
import datetime
import hashlib
import http.client
import json
import os
from pathlib import Path
import re
import socket
import subprocess
import threading
import urllib.parse

from http_acceptance.harness import Deployment, Process, eventually


class AccessDeployment(Deployment):
    def __init__(self, *args):
        self.secret_files, self.secrets, self.sessions, self.bundle_directories = [], [], [], []
        super().__init__(*args)
        # Parent helpers use these only for database commands. Avoid inherited
        # telemetry/provider settings changing this entirely local fixture.
        for name in list(self.environment):
            if name.startswith("OTEL_") or name == "LEDGENCE_POSTGRES_URL":
                self.environment.pop(name)
        self.instance = self.directory / "instance.json"
        self.instance.write_text(json.dumps({
            "instance_id": "access-acceptance", "name": "Access acceptance",
            "scope": self.scope, "allowed_origins": [self.server_url],
            "suggested_queues": [self.queue],
        }))
        self.expiry = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=1)).isoformat()
        self.config = None
        self.operator = self.worker_credentials = self.reader = None
        self.artifact_credentials_seen = threading.Event()
        original = self.artifacts.server.RequestHandlerClass
        owner = self

        class Handler(original):
            def do_GET(self):
                if self.headers.get("Authorization") or self.headers.get("Cookie"):
                    owner.artifact_credentials_seen.set()
                super().do_GET()

        self.artifacts.server.RequestHandlerClass = Handler

    def command(self, binary, args, expected=0):
        # Persist bounded process outputs, but never echo error values (which
        # could contain credentials after a regression). The final gate scans
        # every retained file before exposing diagnostics.
        self.counter += 1
        environment = {key: value for key, value in self.environment.items()
                       if not key.startswith("OTEL_") and key != "LEDGENCE_POSTGRES_URL"}
        result = subprocess.run([str(self.binaries / binary)] + args,
                                env=environment, capture_output=True, timeout=60)
        stem = self.directory / f"command-{self.counter}"
        stem.with_suffix(".stdout").write_bytes(result.stdout)
        stem.with_suffix(".stderr").write_bytes(result.stderr)
        assert result.returncode == expected, f"command-{self.counter} unexpected exit"
        return json.loads(result.stdout) if result.stdout.strip() else None

    def track_bundle(self, bundle):
        for name in ("operator.json", "worker.json", "credential.json"):
            path = bundle / name
            if path.exists():
                self.secret_files.append(path)
                self.secrets.append(json.loads(path.read_text())["token"].encode())
        return bundle / "access.json"

    def provision(self):
        bundle = self.directory / "initial-credentials"
        self.bundle_directories.append(bundle)
        self.command("ledgence", ["access", "init", "--server", self.server_url,
            "--directory", str(bundle), "--subject", "operator", "--expires-at", self.expiry,
            "--instance-config", str(self.instance)])
        self.config = self.track_bundle(bundle)
        self.operator, self.worker_credentials = bundle / "operator.json", bundle / "worker.json"
        self.reader, self.config, _ = self.issue("metadata-credentials", "metadata", ["metadata_read"])

    def issue(self, name, subject, capabilities):
        bundle = self.directory / name
        self.bundle_directories.append(bundle)
        report = self.command("ledgence", ["access", "issue", "--server", self.server_url,
            "--directory", str(bundle), "--subject", subject, "--expires-at", self.expiry,
            "--config", str(self.config), "--class", "operator",
            "--capabilities", ",".join(capabilities)])
        config = self.track_bundle(bundle)
        return bundle / "credential.json", config, report["credential_id"]

    def request(self, method, path, body=None, *, credentials=None, headers=None):
        headers = dict(headers or {}, Accept="application/json")
        if credentials is not None:
            headers["Authorization"] = "Bearer " + json.loads(Path(credentials).read_text())["token"]
        if body is not None:
            body = body if isinstance(body, bytes) else json.dumps(body).encode()
            headers["Content-Type"] = "application/json"
        connection = http.client.HTTPConnection("127.0.0.1", self.server_port, timeout=35)
        try:
            connection.request(method, path, body=body, headers=headers)
            response = connection.getresponse()
            data = response.read(16 * 1024 * 1024 + 1)
            assert len(data) <= 16 * 1024 * 1024, "oversized response"
            return response.status, dict(response.getheaders()), json.loads(data)
        finally:
            connection.close()

    def checked(self, method, path, body=None, *, credentials=None, expected=200):
        status, _, value = self.request(method, path, body, credentials=credentials or self.operator)
        assert status == expected, f"{method} {path.split('?')[0]} unexpected status {status}"
        return value

    def query(self, path, **values):
        return path + "?" + urllib.parse.urlencode(dict(self.scope, **values))

    def server_args(self, config):
        args = [str(self.binaries / "ledgence"), "orchestrator", "serve", "--bind",
                f"127.0.0.1:{self.server_port}", "--store", str(self.store),
                "--instance-config", str(self.instance), "--allow-program-publication"]
        if config is not None:
            args += ["--access-config", str(config), "--allow-insecure-console-access"]
        return args

    def start_server(self):
        self.counter += 1
        process = Process(self.server_args(self.config), self.directory,
                          f"server-{self.counter}", self.environment)
        self.processes.append(process)

        def ready():
            assert process.process.poll() is None, "orchestrator exited before readiness"
            try:
                return self.request("GET", "/health/ready")[0] == 200
            except (OSError, http.client.HTTPException):
                return False

        eventually(ready, description="credential-enabled headless orchestrator readiness")
        self.server = process
        return process

    def rejected_start(self, config):
        self.counter += 1
        process = Process(self.server_args(config), self.directory,
                          f"rejected-server-{self.counter}", self.environment)
        self.processes.append(process)

        def exited():
            with socket.socket() as probe:
                probe.settimeout(0.1)
                assert probe.connect_ex(("127.0.0.1", self.server_port)) != 0, "rejected startup opened API listener"
            return process.process.poll() is not None

        eventually(exited, timeout=20, description="startup authority rejection")
        assert process.process.returncode != 0, "invalid startup succeeded"

    def start_worker(self):
        self.counter += 1
        args = [str(self.binaries / "ledgence"), "worker", "connect", "--server", self.server_url,
            "--credentials-file", str(self.worker_credentials), "--tenant", self.scope["tenant_id"],
            "--namespace", self.scope["namespace"], "--queue", self.queue, "--store", self.artifacts.url,
            "--cache", str(self.directory / "cache"), "--python", self.python,
            "--runner", str(self.root / "sdk/python/ledgence/worker/bootstrap.py"), "--concurrency", "1"]
        process = Process(args, self.directory, f"worker-{self.counter}", self.environment)
        self.processes.append(process)
        return process

    def submit(self, command, *, kind="task"):
        self.counter += 1
        path = self.directory / f"submit-{self.counter}.json"
        path.write_text(json.dumps(command))
        return self.command("ledgence", [kind, "submit", "--server", self.server_url,
            "--credentials-file", str(self.operator), "--file", str(path)])

    def task(self, task_id):
        return self.checked("GET", self.query("/v1/tasks/inspect", task_id=task_id))

    def row_counts(self):
        tables = ("tasks", "attempts", "worker_sessions", "worker_observations", "workflow_runs",
                  "workflow_approvals", "console_programs", "console_program_versions")
        return self.sql("SELECT " + ",".join(f"(SELECT count(*) FROM {table})" for table in tables))

    def store_hashes(self):
        return {str(path.relative_to(self.store)): hashlib.sha256(path.read_bytes()).hexdigest()
                for path in self.store.rglob("*") if path.is_file()}

    def close(self):
        try:
            for session in self.sessions:
                session.cleanup()
        finally:
            super().close()

    def remove_credentials_and_scan(self):
        clean = True
        # Register directories before invoking the CLI, then recover secrets
        # even if command parsing/exit fails after the bundle was published.
        # SIGKILL can leave only a CLI-owned staging directory in this fixture.
        directories = self.bundle_directories + [
            path / "bundle" for path in self.directory.glob(".ledgence-access-*")
            if path.is_dir() and not path.is_symlink()]
        for directory in directories:
            for name in ("operator.json", "worker.json", "credential.json"):
                path = directory / name
                if path.is_symlink():
                    path.unlink()
                    clean = False
                elif path.is_file():
                    data = path.read_bytes()
                    self.secrets.extend(re.findall(rb"ldg1_[a-f0-9]{32}_[a-f0-9]{64}", data))
                    try:
                        token = json.loads(data)["token"]
                        assert isinstance(token, str) and token
                        self.secrets.append(token.encode())
                    except (ValueError, KeyError, TypeError, AssertionError):
                        clean = False
                    path.unlink()
        for path in self.secret_files:
            path.unlink(missing_ok=True)
        for path in self.directory.rglob("*"):
            if not path.is_file() or path.is_symlink():
                continue
            data = path.read_bytes()
            original = data
            # Report a failed scan without retaining the leaked secret.
            for secret in self.secrets:
                if secret in data:
                    clean = False
                    data = data.replace(secret, b"[REDACTED CREDENTIAL]")
            if data != original:
                path.chmod(0o600)
                path.write_bytes(data)
        return clean
