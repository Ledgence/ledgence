#!/usr/bin/env python3
"""Real Rust HTTP/PostgreSQL/Python acceptance for Ledgence Console.

Requires an explicitly prepared --console-dist, the ledgence executable, CPython >=3.11,
psql and LEDGENCE_POSTGRES_URL naming a disposable server that permits database
creation. Creates and drops only a unique test database; never restarts that
server. Browser behavior, container recreation and relocated release bundles have
separate gates. Optional evidence retains scenario results and subprocess logs.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import runpy
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import traceback
import urllib.parse
import uuid

from postgres_fixture import owned_database_url
from console_acceptance.harness import ConsoleDeployment
from console_acceptance.scenarios import run


def run_browser_command(command, root, environment, directory):
    """Retain and clean the entire explicitly launched browser process group."""
    with (directory / "browser-command.stdout").open("wb") as stdout, \
         (directory / "browser-command.stderr").open("wb") as stderr:
        process = subprocess.Popen(command, cwd=root, env=environment, stdout=stdout,
                                   stderr=stderr, start_new_session=True)
        try:
            code = process.wait(timeout=900)
            if code:
                raise subprocess.CalledProcessError(code, command)
        finally:
            # Clean children even when their launcher already exited. This
            # group belongs exclusively to this invocation; never discover or
            # terminate browser processes belonging to another task.
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            if process.poll() is None:
                process.wait(timeout=10)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--console-dist", type=Path, required=True,
                        help="existing compiled Console directory; no Node build is implicit")
    parser.add_argument("--binaries", type=Path, required=True,
                        help="directory containing the ledgence executable")
    parser.add_argument("--psql", default="psql")
    parser.add_argument("--evidence", type=Path, help="new directory for logs, resources and results")
    parser.add_argument("--hold-seconds", type=int, default=0,
                        help="keep the owned seeded server/worker for browser tests, maximum 1800 seconds")
    parser.add_argument("--browser-command", nargs=argparse.REMAINDER,
                        help="run exact argv without a shell against the seeded live stack, bounded to 900 seconds")
    args = parser.parse_args()
    parent_url = os.environ.get("LEDGENCE_POSTGRES_URL")
    if not parent_url:
        parser.error("LEDGENCE_POSTGRES_URL must identify a disposable PostgreSQL server")
    database = "ledgence_console_" + uuid.uuid4().hex
    try:
        database_url = owned_database_url(parent_url, database)
    except ValueError as error:
        parser.error(str(error))
    if not 0 <= args.hold_seconds <= 1800:
        parser.error("--hold-seconds must be between 0 and 1800")
    if args.browser_command is not None and not args.browser_command:
        parser.error("--browser-command requires an executable and optional arguments")
    if args.browser_command and args.hold_seconds:
        parser.error("use either --browser-command or --hold-seconds")
    root = Path(__file__).resolve().parents[1]
    binaries, dist = args.binaries.resolve(), args.console_dist.resolve()
    for name in ("ledgence",):
        if not (binaries / name).is_file():
            parser.error(f"missing executable: {binaries / name}")
    if not (dist / "index.html").is_file() or not (dist / "assets").is_dir():
        parser.error("--console-dist must contain a compiled index.html and assets directory")
    python = os.environ.get("LEDGENCE_PYTHON", sys.executable)
    python_version = subprocess.check_output([python, "-c", "import sys; assert sys.version_info >= (3, 11); print(sys.version)"]).decode().strip()
    directory = args.evidence.resolve() if args.evidence else Path(tempfile.mkdtemp(prefix="ledgence-console-acceptance-"))
    if args.evidence:
        directory.mkdir(parents=True, exist_ok=False)
    deployment = delay = None
    created = succeeded = False
    results = []

    def admin(statement):
        command = [args.psql, "--dbname", parent_url, "-X", "--set", "ON_ERROR_STOP=1", "--command", statement]
        result = subprocess.run(command, capture_output=True, timeout=40)
        if result.returncode:
            raise RuntimeError("owned Console database administration failed")

    def record(scenario, detail):
        results.append({"scenario": scenario, "result": "passed", "detail": detail})
        (directory / "results.json").write_text(json.dumps(results, indent=2) + "\n")
        print(f"PASS {scenario}: {json.dumps(detail)}", flush=True)

    try:
        admin(f'CREATE DATABASE "{database}"')
        created = True
        # Retain the caller's exact prepared build while browser developers may
        # independently rebuild their checkout during a long fixture run.
        prepared_dist = directory / "console-dist"
        shutil.copytree(dist, prepared_dist)
        deployment = ConsoleDeployment(root, directory, binaries, python, database_url, args.psql, console_dist=prepared_dist)
        workflow_gate = runpy.run_path(str(root / "tools/check-workflows.py"))
        delay = workflow_gate["DelayServer"]()
        migration = subprocess.run([str(binaries / "ledgence"), "orchestrator", "migrate"],
                                   env=deployment.environment, capture_output=True, timeout=60)
        assert migration.returncode == 0, migration.stderr.decode(errors="replace")[-3000:]
        deployment.server, _ = deployment.start_server()
        resources = {
            "platform": platform.platform(), "machine": platform.machine(), "python": python_version,
            "database": database, "delivery": "integrated_postgresql", "real_aws": False,
            "git_head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root).decode().strip(),
            "working_tree": subprocess.check_output(["git", "status", "--short"], cwd=root).decode(),
            "binaries": {name: hashlib.sha256((binaries / name).read_bytes()).hexdigest()
                         for name in ("ledgence",)},
            "console_assets": {str(path.relative_to(prepared_dist)): hashlib.sha256(path.read_bytes()).hexdigest()
                               for path in sorted(prepared_dist.rglob("*")) if path.is_file()},
            "source_fixtures": ["tools/http_acceptance/harness.py", "tools/check-workflows.py",
                                "tools/workflow_acceptance/owned_program.py", "examples/checkpoint-workflow/controller/program.py",
                                "examples/durable-approval/program.py"],
            "started_at": time.time(),
        }
        resources["fixture_sha256"] = {
            name: hashlib.sha256((root / name).read_bytes()).hexdigest()
            for name in ["tools/check-console.py", "tools/console_acceptance/harness.py",
                         "tools/console_acceptance/scenarios.py", "tools/console_acceptance/workflows.py",
                         "tools/console_acceptance/explorer.py",
                         "tools/console_acceptance/fork4.py", "tools/console_acceptance/fork4_program.py",
                         "tools/console_acceptance/fork4_task.py",
                         *resources["source_fixtures"]]
        }
        (directory / "resources.json").write_text(json.dumps(resources, indent=2) + "\n")
        run(deployment, delay, workflow_gate, record)
        if args.hold_seconds or args.browser_command:
            worker = deployment.start_worker()
            stack = {"server": deployment.server_url, "console": deployment.server_url + "/console/",
                     "instance_config": str(deployment.instance_file), "database": database,
                     "worker_pid": worker.process.pid, "hold_seconds": args.hold_seconds}
            (directory / "stack.json").write_text(json.dumps(stack, indent=2) + "\n")
            print("BROWSER_STACK " + json.dumps(stack), flush=True)
            if args.browser_command:
                browser_environment = dict(os.environ,
                    LEDGENCE_CONSOLE_STACK=str(directory / "stack.json"),
                    LEDGENCE_CONSOLE_URL=stack["console"])
                run_browser_command(args.browser_command, root, browser_environment, directory)
                record("real-browser-command", {"argv": args.browser_command,
                                                "stdout": "browser-command.stdout",
                                                "stderr": "browser-command.stderr"})
            else:
                until = time.monotonic() + args.hold_seconds
                while time.monotonic() < until:
                    assert deployment.server.process.poll() is None, "browser fixture server exited"
                    assert worker.process.poll() is None, "browser fixture worker exited"
                    time.sleep(max(0, min(1, until - time.monotonic())))
            worker.stop()
        deployment.server.stop()
        succeeded = True
        print(f"Console native acceptance passed: {len(results)} scenarios; evidence {directory}", flush=True)
        return 0
    except Exception as error:
        diagnostic = traceback.format_exc()
        for url in (parent_url, database_url):
            diagnostic = diagnostic.replace(url, "[redacted PostgreSQL URL]")
        (directory / "failure.txt").write_text(diagnostic)
        print(f"Console acceptance failed: {type(error).__name__}; evidence {directory}\n{diagnostic}", file=sys.stderr)
        return 1
    finally:
        try:
            if deployment:
                deployment.close()
            if delay:
                delay.close()
        finally:
            if created:
                admin(f'DROP DATABASE "{database}" WITH (FORCE)')
            if succeeded and not args.evidence:
                shutil.rmtree(directory)


if __name__ == "__main__":
    sys.exit(main())
