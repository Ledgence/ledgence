#!/usr/bin/env python3
"""Qualify only owned Compose projects: fresh start, demo and persistent restart.

Requires Docker Compose v2 with --wait. Uses random project/image names and an
ephemeral loopback port; cleanup removes only the projects created by this run.
"""
from __future__ import annotations
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
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
    env = dict(os.environ, LEDGENCE_HTTP_PORT="0", LEDGENCE_CONCURRENCY="1", LEDGENCE_LOCAL_IMAGE=image)
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    dirty = bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True).strip())
    env["LEDGENCE_SOURCE_REVISION"] = commit + ("-dirty" if dirty else "")
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
                            "installed_sdk": {"requested": client_python is not None,
                                              "installation": client_install,
                                              "fresh": sdk_first, "after_restart": sdk_second,
                                              "receiver_event_verification_observations": 6 if sdk_first else 0,
                                              "unique_receiver_events_verified": 4 if sdk_first else 0},
                            "checks": ["explicit migrations before readiness", "dynamic immutable publication and replay",
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
