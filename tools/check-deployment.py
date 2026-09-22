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
    args = parser.parse_args()
    evidence = args.evidence.resolve()
    evidence.mkdir(parents=True, exist_ok=False)
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
        def run(arguments, timeout=180):
            print("+", " ".join(map(str, arguments)), flush=True)
            result = subprocess.run(list(map(str, arguments)), cwd=ROOT, env=env, text=True,
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)
            with log.open("a") as stream:
                stream.write("$ " + " ".join(map(str, arguments)) + "\n" + result.stdout + result.stderr)
            if result.returncode:
                raise RuntimeError(f"command failed ({result.returncode}); see {log}")
            return result.stdout
        def api(path):
            with urllib.request.urlopen(base + path, timeout=10) as response:
                return json.load(response)
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
            before = api("/v1/workflows/result?" + urllib.parse.urlencode({
                "tenant_id": "acme", "namespace": "demo", "workflow_id": first["workflow_id"]}))
            run(compose + ["down", "--timeout", "65"])
            # Existing volumes survive; migration reruns and schema verification
            # precedes service readiness. Process reuse assertions run fresh again.
            run(compose + ["up", "-d", "--wait", "--wait-timeout", "120"])
            base = "http://" + run(compose + ["port", "orchestrator", "8080"]).strip()
            after = api("/v1/workflows/result?" + urllib.parse.urlencode({
                "tenant_id": "acme", "namespace": "demo", "workflow_id": first["workflow_id"]}))
            if after != before:
                raise AssertionError("completed workflow changed across persisted restart")
            for identity in first["subscriptions"]:
                status = api("/v1/completion-subscriptions/status?" + urllib.parse.urlencode({
                    "tenant_id": "acme", "namespace": "demo", "subscription_id": identity}))
                if status["state"] != "delivered":
                    raise AssertionError("callback delivery state lost on restart")
            second = json.loads(run(compose + ["run", "--rm", "--no-deps", "demo"], timeout=300))
            details = json.loads(run(["docker", "image", "inspect", image]))[0]
            results.append({"backend": backend, "image_id": details["Id"],
                            "os": details["Os"], "architecture": details["Architecture"],
                            "image_source_revision": (details["Config"].get("Labels") or {}).get("org.opencontainers.image.revision", "unrecorded"),
                            "fresh": first, "after_restart": second,
                            "checks": ["explicit migrations before readiness", "dynamic immutable publication and replay",
                                       "task output and warm process reuse with concurrency one", "checkpoint and distributed child workflow",
                                       "late task/workflow callback delivery", "preserved workflow and callback state across restart"]})
        finally:
            try:
                run(compose + ["logs", "--no-color"], timeout=30)
            finally:
                run(compose + ["down", "--volumes", "--remove-orphans", "--timeout", "65"])
    report = {"passed": True, "executed_at_unix": int(time.time()),
              "source_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
              "source_dirty": bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True).strip()),
              "results": results, "scope": "Local Linux containers only; no AWS acceptance or capacity claim."}
    (evidence / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
