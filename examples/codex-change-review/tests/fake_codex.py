#!/usr/bin/env python3
"""Explicit offline Codex CLI protocol fixture; never calls a provider (MIT)."""

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time


def wait_for(path, timeout=45):
    deadline = time.monotonic() + timeout
    while not path.exists():
        if time.monotonic() >= deadline:
            raise RuntimeError("offline acceptance rendezvous was not released: " + path.name)
        time.sleep(0.01)


def main():
    if sys.argv[1:] == ["--version"]:
        print("codex-cli 0.0.0-offline-fixture")
        return 0
    if not sys.argv[1:] or sys.argv[1] != "exec":
        raise RuntimeError("offline fixture only supports --version and exec")
    config_path = globals().get("FIXTURE_CONFIG_PATH")
    if not config_path:
        raise RuntimeError("launch through the acceptance harness's explicit fixture executable")
    config = json.loads(Path(config_path).read_text())
    schema_path = Path(sys.argv[sys.argv.index("--output-schema") + 1])
    properties = json.loads(schema_path.read_text())["properties"]
    phase = "implement" if "source" in properties else ("note" if "body" in properties else "review")
    prompt = sys.stdin.read()
    if phase.upper() not in prompt:
        raise RuntimeError("expected an explicit phase in the Codex prompt")
    marker = Path(config["markers"])
    marker.mkdir(parents=True, exist_ok=True)
    started = {"phase": phase, "pid": os.getpid(), "at_ms": time.time_ns() // 1_000_000,
               "offline_fixture": True, "prompt_sha256": hashlib.sha256(prompt.encode()).hexdigest()}
    with (marker / "codex.jsonl").open("a") as stream:
        stream.write(json.dumps(started) + "\n")
    if phase == "implement":
        value = {"source": config["source"], "summary": "Offline fixture candidate for the shipping change."}
    else:
        source = config["source"].rstrip("\n") + "\n"
        digest = hashlib.sha256(source.encode()).hexdigest()
        if ("CANDIDATE_SHA256: " + digest not in prompt
                or "CANDIDATE_SOURCE:\n" + source + "\nCANONICAL_PATCH:\n" not in prompt):
            raise RuntimeError("branch did not receive the exact immutable implementation candidate")
        if config.get("synchronize"):
            wait_for(marker / "tests-started")
        (marker / (phase + "-started")).write_text(json.dumps(started))
        if config.get("synchronize"):
            wait_for(marker / ("note-started" if phase == "review" else "review-started"))
            wait_for(marker / "tests-finished")
        if phase == "review" and config.get("hold_review"):
            wait_for(marker / "release-review")
        if phase == "review" and config.get("review_failure"):
            # A deliberately abandoned descendant stays in the helper's process
            # group. The real Rust worker must drain it when this turn fails.
            descendant = marker / "descendant.json"
            source = ("import json,os,signal,time; from pathlib import Path; "
                      "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
                      f"Path({str(descendant)!r}).write_text(json.dumps({{'pid':os.getpid()}})); "
                      "time.sleep(120)")
            subprocess.Popen([sys.executable, "-B", "-c", source], stdin=subprocess.DEVNULL,
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            wait_for(descendant)
            print("Deliberate offline review infrastructure failure.", file=sys.stderr)
            return 17
        findings = ([{"severity": "medium", "line": 1,
                      "message": "Offline fixture requests a follow-up clarification."}]
                    if config.get("findings") else [])
        value = ({"title": "Free shipping at $100", "body": "Orders of $100 or more now qualify for free shipping. Offline fixture note; no provider called."}
                 if phase == "note" else
                 {"verdict": "request_changes" if findings else "approve",
                  "summary": "Offline fixture review; no model or provider was called.",
                  "findings": findings})
    events = [
        {"type": "thread.started", "thread_id": "offline-fixture-" + phase},
        {"type": "turn.started"},
        {"type": "item.completed", "item": {"id": "offline-final", "type": "agent_message",
                                               "text": json.dumps(value)}},
        {"type": "turn.completed", "usage": {"input_tokens": 0, "cached_input_tokens": 0,
                                               "output_tokens": 0}},
    ]
    for event in events:
        print(json.dumps(event), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
