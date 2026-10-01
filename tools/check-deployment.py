#!/usr/bin/env python3
"""Qualify only owned Compose projects: fresh start, demo and persistent restart.

Requires Docker Compose v2 with --wait. Uses random project/image names and an
ephemeral loopback port; cleanup removes only the projects created by this run.
"""
from __future__ import annotations
import argparse
import json
import os
import socket
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", choices=("integrated", "elasticmq", "both"), default="both")
    parser.add_argument("--evidence", required=True, type=Path, help="NEW evidence directory")
    parser.add_argument("--image", help="reuse this locally built image instead of building")
    parser.add_argument("--client-python", type=Path, help="interpreter with an installed client wheel; also run the SDK companion")
    args = parser.parse_args()
    evidence = args.evidence.resolve()
    evidence.mkdir(parents=True, exist_ok=False)
    # Preserve the venv interpreter path: resolving its symlink would select the
    # base interpreter and lose the installed client. -I ignores source PYTHONPATH.
    client_python = Path(os.path.abspath(args.client_python.expanduser())) if args.client_python else None
    companion = ROOT / "examples/local-compose-client.py"
    client_install = None
    if client_python:
        with tempfile.TemporaryDirectory(prefix="ledgence-installed-client-") as temporary:
            check = subprocess.run([str(client_python), "-I", "-B", str(companion), "--check-install"],
                                   cwd=temporary, text=True, capture_output=True, timeout=30)
        (evidence / "installed-sdk.log").write_text(check.stdout + check.stderr)
        if check.returncode:
            raise RuntimeError(f"installed SDK check failed; see {evidence / 'installed-sdk.log'}")
        client_install = json.loads(check.stdout)["sdk"]
    token = uuid.uuid4().hex[:12]
    image = args.image or "ledgence:qualification-" + token
    with socket.socket() as probe:
        probe.bind(('127.0.0.1', 0))
        http_port = probe.getsockname()[1]
    env = dict(os.environ, LEDGENCE_HTTP_PORT=str(http_port), LEDGENCE_CONCURRENCY="1", LEDGENCE_LOCAL_IMAGE=image)
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    dirty = bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True).strip())
    env["LEDGENCE_SOURCE_REVISION"] = commit
    env["LEDGENCE_SOURCE_DIRTY"] = "true" if dirty else "false"
    results = []
    backends = ("integrated", "elasticmq") if args.backend == "both" else (args.backend,)
    for index, backend in enumerate(backends):
        project = f"ledgence-qualification-{token}-{backend}"
        compose = ["docker", "compose", "-p", project, "-f", str(ROOT / "deploy/local/compose.yaml")]
        if backend == "elasticmq":
            compose += ["-f", str(ROOT / "deploy/local/compose.elasticmq.yaml")]
        log = evidence / (backend + ".log")
        def run(arguments, timeout=180, cwd=ROOT):
            print("+", " ".join(map(str, arguments)), flush=True)
            result = subprocess.run(list(map(str, arguments)), cwd=cwd, env=env, text=True,
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)
            with log.open("a") as stream:
                stream.write("$ " + " ".join(map(str, arguments)) + "\n" + result.stdout + result.stderr)
            if result.returncode:
                raise RuntimeError(f"command failed ({result.returncode}); see {log}")
            return result.stdout
        def api(path):
            with urllib.request.urlopen(base + path, timeout=10) as response:
                return json.load(response)
        def console_check():
            config = api('/v1/console/config')
            assert config['instance_id'] == 'ledgence-local'
            assert 'scope' not in config and config['capabilities']['workers']
            for route in ['/console/', '/console/executions/deep-link', '/console/agents/invoice-issuer']:
                with urllib.request.urlopen(base + route, timeout=10) as response:
                    assert response.headers['Content-Type'].startswith('text/html')
                    html = response.read().decode()
                    assert '/console/assets/' in html
            import re
            for asset in re.findall(r'(?:src|href)="(/console/assets/[^"]+)"', html):
                with urllib.request.urlopen(base + asset, timeout=10) as response:
                    assert response.status == 200 and 'text/html' not in response.headers['Content-Type']
            with urllib.request.urlopen(base + '/console/notices/index.html', timeout=10) as response:
                assert response.headers['Content-Type'].startswith('text/html')
                assert response.read()
            # Exercise the deployed browser-origin policy, including a harmless
            # idempotent replay through the same HTTP origin a browser uses.
            registration = json.dumps({'program': {'id': 'invoice-issuer', 'version': '1.0.0'},
                                       'metadata': {'kind': 'task', 'display_name': None, 'description': None}}).encode()
            request = urllib.request.Request(base + '/v1/console/programs/register', data=registration,
                                             headers={'Content-Type': 'application/json', 'Origin': base})
            with urllib.request.urlopen(request, timeout=10) as response:
                assert json.load(response)['already_registered'] is True
            request.add_header('Origin', 'https://unconfigured.example')
            try:
                urllib.request.urlopen(request, timeout=10)
                raise AssertionError('unconfigured browser origin accepted')
            except urllib.error.HTTPError as error:
                assert error.code == 400
            catalog = api('/v1/console/programs')['items']
            assert {item['program_id'] for item in catalog} == {'invoice-issuer', 'workflow-example', 'workflow-summary'}
            assert all(item['registered_versions'] == '1' for item in catalog)
            deadline = time.monotonic() + 20
            while True:
                workers = api('/v1/console/workers')['items']
                reporting = [item for item in workers if item['freshness'] == 'fresh' and item['accepting']]
                if reporting:
                    break
                if time.monotonic() >= deadline:
                    raise AssertionError('no fresh accepting worker observation')
                time.sleep(.2)
            worker = reporting[-1]
            detail = api('/v1/console/workers/inspect?' + urllib.parse.urlencode({'worker_session_id': worker['worker_session_id']}))
            assert len(detail['slots']['items']) == 1 and detail['worker']['capacity'] == 1
            assert detail['slots']['items'][0]['slot_id'] == 0
            assert api('/v1/console/tasks')['observed_at'] > 0
            assert api('/v1/console/workflows')['observed_at'] > 0
            return {'instance_id': config['instance_id'], 'catalog': catalog,
                    'worker_session_id': worker['worker_session_id'], 'worker_sequence': worker['snapshot_sequence']}
        def sdk_demo():
            if not client_python:
                return None
            with tempfile.TemporaryDirectory(prefix="ledgence-sdk-demo-") as temporary:
                result = json.loads(run([client_python, "-I", "-B", companion, "--server", base],
                                        timeout=330, cwd=temporary))
            if result.get("passed") is not True or result.get("sdk") != client_install:
                raise AssertionError("installed SDK demo failed or installation changed")
            return result
        def receiver_events(expected):
            # The receiver stays private on the Compose network. Query its
            # existing bounded endpoint inside that container, not a new host port.
            code = """import json, sys, urllib.request
with urllib.request.urlopen('http://127.0.0.1:8091/events', timeout=10) as response:
    body = response.read(4 * 1024 * 1024 + 1)
if len(body) > 4 * 1024 * 1024:
    raise RuntimeError('receiver events exceed demo bounds')
events = json.loads(body)
expected = json.loads(sys.argv[1])
for event in expected:
    matches = [item for item in events if (item['source'], item['id']) == (event['source'], event['id'])]
    if matches != [event]:
        raise RuntimeError('receiver did not persist exactly the delivered event')
print(json.dumps({'verified_events': len(expected)}))
"""
            verified = json.loads(run(compose + ["exec", "-T", "receiver", "python3", "-c", code,
                                                  json.dumps(expected)], timeout=30))
            if verified != {"verified_events": len(expected)}:
                raise AssertionError("receiver event verification failed")
        try:
            run(compose + ["config", "--quiet"])
            if index == 0 and not args.image:
                run(compose + ["build"], timeout=1800)
            run(compose + ["up", "-d", "--wait", "--wait-timeout", "120"])
            address = run(compose + ["port", "orchestrator", "8080"]).strip()
            base = "http://" + address
            run(compose + ["run", "--rm", "--no-deps", "publish"])
            # Republishing identical immutable content is safe and repeatable.
            run(compose + ["run", "--rm", "--no-deps", "publish"])
            console_first = console_check()
            first = json.loads(run(compose + ["run", "--rm", "--no-deps", "demo"], timeout=300))
            if not first.get("passed"):
                raise AssertionError(first)
            sdk_first = sdk_demo()
            if sdk_first:
                receiver_events(sdk_first["callback_events"])
            preserved = [first] + ([sdk_first] if sdk_first else [])
            before = {item["workflow_id"]: api("/v1/workflows/result?" + urllib.parse.urlencode({
                "tenant_id": "acme", "namespace": "demo", "workflow_id": item["workflow_id"]}))
                      for item in preserved}
            run(compose + ["down", "--timeout", "65"])
            # Existing volumes survive; migration reruns and schema verification
            # precedes service readiness. Process reuse assertions run fresh again.
            run(compose + ["up", "-d", "--wait", "--wait-timeout", "120"])
            base = "http://" + run(compose + ["port", "orchestrator", "8080"]).strip()
            for item in preserved:
                after = api("/v1/workflows/result?" + urllib.parse.urlencode({
                    "tenant_id": "acme", "namespace": "demo", "workflow_id": item["workflow_id"]}))
                if after != before[item["workflow_id"]]:
                    raise AssertionError("completed workflow changed across persisted restart")
                for position, identity in enumerate(item["subscriptions"]):
                    status = api("/v1/completion-subscriptions/status?" + urllib.parse.urlencode({
                        "tenant_id": "acme", "namespace": "demo", "subscription_id": identity}))
                    if status["state"] != "delivered":
                        raise AssertionError("callback delivery state lost on restart")
                    if "callback_events" in item and status["event"] != item["callback_events"][position]:
                        raise AssertionError("persisted SDK callback event changed on restart")
            if sdk_first:
                receiver_events(sdk_first["callback_events"])
            console_second = console_check()
            if console_second['catalog'] != console_first['catalog']:
                raise AssertionError('registered program catalog changed on recreation')
            if console_second['worker_session_id'] == console_first['worker_session_id']:
                raise AssertionError('recreated worker did not establish a new session')
            second = json.loads(run(compose + ["run", "--rm", "--no-deps", "demo"], timeout=300))
            if second.get("passed") is not True:
                raise AssertionError(second)
            sdk_second = sdk_demo()
            if sdk_second:
                receiver_events(sdk_second["callback_events"])
            details = json.loads(run(["docker", "image", "inspect", image]))[0]
            results.append({"backend": backend, "image_id": details["Id"],
                            "os": details["Os"], "architecture": details["Architecture"],
                            "image_source_revision": (details["Config"].get("Labels") or {}).get("org.opencontainers.image.revision", "unrecorded"),
                            "fresh": first, "after_restart": second,
                            "console": {"fresh": console_first, "after_restart": console_second},
                            "installed_sdk": {"requested": client_python is not None,
                                              "installation": client_install,
                                              "fresh": sdk_first, "after_restart": sdk_second,
                                              "receiver_event_verification_observations": 6 if sdk_first else 0,
                                              "unique_receiver_events_verified": 4 if sdk_first else 0},
                            "checks": ["Console assets/deep links/notices and real API", "registered catalog survives recreation", "fresh worker ownership report before/after recreation", "explicit migrations before readiness", "dynamic immutable publication and replay",
                                       "task output and warm process reuse with concurrency one", "checkpoint and distributed child task",
                                       "late task/workflow callback delivery", "preserved workflow and callback state across restart"]
                                      + (["installed SDK task/workflow/callback demo before and after restart",
                                          "complete delivered SDK events match receiver persistence before and after restart"]
                                         if sdk_first else [])})
        finally:
            try:
                run(compose + ["logs", "--no-color"], timeout=30)
            finally:
                run(compose + ["down", "--volumes", "--remove-orphans", "--timeout", "65"])
    report = {"passed": True, "executed_at_unix": int(time.time()),
              "source_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
              "source_dirty": bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True).strip()),
              "results": results, "scope": "Local Linux containers and optional installed host SDK; no AWS acceptance or capacity claim."}
    (evidence / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
