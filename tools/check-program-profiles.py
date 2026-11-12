#!/usr/bin/env python3
"""Exercise package-profile compatibility using actual candidate and 0.9 binaries.

Uses only explicitly supplied binaries and an owned disposable PostgreSQL database.
It does not download releases, call providers, publish images or change shared data.
"""
import argparse
import hashlib
import http.client
import http.server
import json
import os
import signal
import time
from pathlib import Path
import subprocess
import sys
import threading
import traceback
import urllib.parse
import uuid

from instance_access.harness import AccessDeployment
from postgres_fixture import create_owned_database, owned_database_url
from program_fixture import prepare_package
from release.smoke import task_fixture


class CountedPublication:
    """A transparent local observer: count methods, retain no headers or bodies."""
    def __init__(self, upstream):
        origin = urllib.parse.urlsplit(upstream)
        self.puts = 0
        lock = threading.Lock()
        owner = self
        class Handler(http.server.BaseHTTPRequestHandler):
            def handle_request(self):
                length = int(self.headers.get("Content-Length", "0"))
                assert not self.headers.get("Transfer-Encoding") and 0 <= length <= 64 * 1024 * 1024
                body = self.rfile.read(length)
                assert len(body) == length
                headers = {k: v for k, v in self.headers.items() if k.lower() not in ("host", "connection")}
                connection = http.client.HTTPConnection(origin.hostname, origin.port, timeout=130)
                try:
                    with lock:
                        if self.command == "PUT": owner.puts += 1
                    connection.request(self.command, self.path, body, headers)
                    response = connection.getresponse()
                    data = response.read(64 * 1024 + 1)
                    assert len(data) <= 64 * 1024
                    self.send_response(response.status)
                    for key, value in response.getheaders():
                        if key.lower() not in ("content-length", "transfer-encoding", "connection"):
                            self.send_header(key, value)
                    self.send_header("Content-Length", str(len(data)))
                    self.end_headers()
                    self.wfile.write(data)
                finally:
                    connection.close()
            do_GET = do_PUT = do_POST = handle_request
            def log_message(self, *_): pass
        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f"http://127.0.0.1:{self.server.server_port}"
    def close(self):
        self.server.shutdown(); self.server.server_close(); self.thread.join(timeout=5)
        assert not self.thread.is_alive()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binaries", required=True, type=Path)
    parser.add_argument("--legacy-binaries", required=True, type=Path)
    parser.add_argument("--runner", required=True, type=Path)
    parser.add_argument("--legacy-runner", required=True, type=Path)
    parser.add_argument("--psql", default="psql")
    parser.add_argument("--evidence", required=True, type=Path)
    parser.add_argument("--prepared-package", action="append", type=Path, default=[])
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    parent = os.environ.get("LEDGENCE_POSTGRES_URL")
    if not parent: parser.error("LEDGENCE_POSTGRES_URL must name a disposable test server")
    python = os.environ.get("LEDGENCE_PYTHON", sys.executable)
    args.evidence.mkdir(parents=True, exist_ok=False)
    directory = args.evidence.resolve()
    directory.chmod(0o700)
    binaries, legacy = args.binaries.resolve(), args.legacy_binaries.resolve()
    digests = {label: hashlib.sha256((path / "ledgence").read_bytes()).hexdigest()
               for label, path in (("candidate", binaries), ("legacy", legacy))}
    results, deployments, databases = [], [], []
    passed = False
    cleanup = True
    failure = None
    def admin(statement):
        process = subprocess.run([args.psql, "--dbname", parent, "-X", "--set", "ON_ERROR_STOP=1", "--command", statement],
                                 capture_output=True, timeout=40)
        if process.returncode: raise RuntimeError("owned database operation failed")
    def child_environment(extra=None):
        # Keep the same isolated environment for publication and direct workers.
        env = {key: value for key, value in os.environ.items()
               if key in ("PATH", "HOME", "TMPDIR", "LANG", "LC_ALL", "SYSTEMROOT")}
        env.update(extra or {})
        return env
    def owned_runtime_pids():
        listing = subprocess.check_output(["ps", "-axo", "pid=,args="], text=True)
        return [int(fields[0]) for line in listing.splitlines()
                if len(fields := line.strip().split(None, 1)) == 2
                and "--package-root" in fields[1] and str(directory) + "/" in fields[1]]
    counter = 0
    def command(cli, arguments, *, environment=None, expected=0):
        nonlocal counter
        counter += 1
        env = child_environment(environment)
        process = subprocess.run([str(cli / "ledgence"), *map(str, arguments)], cwd=directory,
                                 env=env, capture_output=True, timeout=180)
        (directory / f"command-{counter}.stdout").write_bytes(process.stdout)
        (directory / f"command-{counter}.stderr").write_bytes(process.stderr)
        assert process.returncode == expected, f"command-{counter} failed"
        return json.loads(process.stdout)
    try:
        assert "0.9.0" in subprocess.check_output([legacy / "ledgence", "--version"], text=True)
        small = prepare_package(directory / "small", "profile-small", "1.0.0",
                                "def handle(event): return {'message': 'Hello, ' + event['data']['name'] + '!'}\n",
                                python=python, protocol=3)
        large = prepare_package(directory / "large", "profile-large", "1.0.0",
                                (small / "program.py").read_text(), python=python, protocol=3)
        for index in range(4095): (large / f"member-{index:04}").write_bytes(b"")
        for label, server_binary in (("candidate", binaries), ("legacy", legacy)):
            db = "ledgence_profiles_" + uuid.uuid4().hex
            # Retain the attempted name even when CREATE's reply is uncertain.
            databases.append(db)
            create_owned_database(admin, db)
            work = directory / label; work.mkdir()
            deployment = AccessDeployment(root, work, server_binary, python, owned_database_url(parent, db), args.psql)
            deployments.append(deployment)
            deployment.command("ledgence", ["orchestrator", "migrate"])
            deployment.start_server()
            proxy = CountedPublication(deployment.server_url)
            deployment.proxies.append(proxy)
            client = legacy if label == "candidate" else binaries
            report = command(client, ["program", "publish", "--source", small, "--server", proxy.url,
                                     "--register", "--kind", "task"])
            assert report["phase"] == "registered" and proxy.puts == 1
            results.append({"scenario": f"{('legacy' if label == 'candidate' else 'candidate')}-client-to-{label}-server-small", "passed": True})
            if label == "candidate":
                report = command(binaries, ["program", "publish", "--source", large, "--server", proxy.url,
                                            "--register", "--kind", "task"])
                assert report["phase"] == "registered" and proxy.puts == 2
                for prepared in args.prepared_package:
                    report = command(binaries, ["program", "publish", "--source", prepared.resolve(), "--server", proxy.url,
                                                "--register", "--kind", "task"])
                    assert report["phase"] == "registered"
                    results.append({"scenario": "complete-prepared-package-register", "program": report["descriptor"]["program"], "passed": True})
                results.append({"scenario": "expanded-package-publish-register", "passed": True})
            else:
                report = command(binaries, ["program", "publish", "--source", large, "--server", proxy.url], expected=1)
                assert report["phase"] == "prepared" and proxy.puts == 1
                results.append({"scenario": "expanded-package-to-legacy-rejected-before-put", "passed": True, "additional_puts": 0})
            deployment.server.stop()
        store = directory / "worker-store"
        for source in (small, large): command(binaries, ["program", "publish", "--source", source, "--store", store])
        for label, cli, runner in (("candidate", binaries, args.runner), ("legacy", legacy, args.legacy_runner)):
            for source in (small, large):
                program = json.loads((source / "ledgence-program.json").read_text())["program"]
                tasks = directory / f"{label}-{source.name}-tasks.json"
                tasks.write_text(json.dumps(task_fixture(program)))
                for phase in ("cold", "reopen"):
                    worker_args = ["worker", "run", "--tasks", tasks, "--store", store,
                                    "--cache", directory / f"cache-{label}-{source.name}", "--python", python,
                                    "--runner", runner.resolve(), "--concurrency", "1"]
                    counter += 1
                    process = subprocess.run([str(cli / "ledgence"), *map(str, worker_args)], cwd=directory,
                                             capture_output=True, timeout=180, env=child_environment())
                    (directory / f"worker-{label}-{source.name}-{phase}.stdout").write_bytes(process.stdout)
                    (directory / f"worker-{label}-{source.name}-{phase}.stderr").write_bytes(process.stderr)
                    rejected = label == "legacy" and source == large
                    assert process.returncode == (1 if rejected else 0)
                    reports = [json.loads(line) for line in process.stdout.splitlines()]
                    assert len(reports) == 2
                    if rejected:
                        assert all(item["failure"]["phase"] == "preparation"
                                   and item["failure"]["execution_may_have_started"] is False
                                   and item["failure"]["error"]["kind"] == "invalid_input"
                                   and "ZIP entry count" in item["failure"]["error"]["message"]
                                   for item in reports)
                    else:
                        assert all(item["report"]["outcome"]["status"] == "success" for item in reports)
                        assert reports[0]["report"]["process_id"] == reports[1]["report"]["process_id"]
                        assert reports[1]["report"]["reused_process"] is True
                    results.append({"scenario": f"{label}-worker-{source.name}-{phase}", "passed": True})
        assert digests == {label: hashlib.sha256((path / "ledgence").read_bytes()).hexdigest()
                           for label, path in (("candidate", binaries), ("legacy", legacy))}
        passed = True
    except Exception as error:
        failure = {"type": type(error).__name__, "frames": [
            {"file": Path(frame.filename).name, "line": frame.lineno, "function": frame.name}
            for frame in traceback.extract_tb(error.__traceback__)]}
    finally:
        # A timed-out direct worker may leave its separately grouped runtime.
        # The fresh evidence directory is the ownership boundary, never a name.
        try:
            for pid in owned_runtime_pids():
                try: os.kill(pid, signal.SIGKILL)
                except ProcessLookupError: pass
            deadline = time.monotonic() + 5
            while owned_runtime_pids() and time.monotonic() < deadline: time.sleep(0.05)
            if owned_runtime_pids(): cleanup = False
        except Exception: cleanup = False
        for deployment in reversed(deployments):
            try: deployment.close()
            except Exception: cleanup = False
        for database in databases:
            try: admin(f'DROP DATABASE IF EXISTS "{database}" WITH (FORCE)')
            except Exception: cleanup = False
        result = {"passed": passed and cleanup, "cleanup_complete": cleanup, "binary_sha256": digests,
                  "scenarios": results, "failure": failure}
        (directory / "summary.json").write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps({"passed": result["passed"], "cleanup_complete": cleanup, "scenarios": len(results), "failure": failure}))
    return 0 if passed and cleanup else 1


if __name__ == "__main__":
    sys.exit(main())
