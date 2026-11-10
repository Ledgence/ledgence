#!/usr/bin/env python3
"""Qualify the production Rust artifact adapter against a pinned external RustFS.

The fixture is an unchanged third-party test service, never a Ledgence package
or runtime dependency. Owned mode isolates its network and replaces its server
between phases. An explicit external loopback endpoint runs conformance only;
it cannot establish container-restart persistence and reports that distinction.
"""
from __future__ import annotations

import argparse
import contextlib
import datetime
import hashlib
import hmac
import http.client
import ipaddress
import json
import os
import re
from pathlib import Path
import socket
import socketserver
import subprocess
import threading
import time
import urllib.parse
import uuid

IMAGE = "docker.io/rustfs/rustfs:1.0.4@sha256:ed9210d6411d77ffceb70d94b964a3cb7d225983368bc25bebe01e995293dcc2"
REPO = Path(__file__).resolve().parents[1]


def docker(*args: str, check: bool = True, timeout: int = 60) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["docker", *args], text=True, capture_output=True, check=check, timeout=timeout)


class Bridge(socketserver.ThreadingTCPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, container: str):
        self.container = container
        self.children: set[subprocess.Popen] = set()
        self.lock = threading.Lock()
        super().__init__(("127.0.0.1", 0), Connection)

    def close(self):
        self.shutdown()
        with self.lock:
            children = list(self.children)
        for process in children:
            if process.poll() is None:
                process.kill()
            process.wait(timeout=5)
        self.server_close()


class Connection(socketserver.BaseRequestHandler):
    def handle(self):
        process = subprocess.Popen(
            ["docker", "exec", "-i", self.server.container, "nc", "127.0.0.1", "9000"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, bufsize=0,
        )
        with self.server.lock:
            self.server.children.add(process)

        def forward():
            try:
                while data := self.request.recv(65536):
                    process.stdin.write(data)
            except (OSError, ValueError):
                pass
            finally:
                with contextlib.suppress(OSError, ValueError):
                    process.stdin.close()

        sender = threading.Thread(target=forward, daemon=True)
        sender.start()
        try:
            while data := os.read(process.stdout.fileno(), 65536):
                self.request.sendall(data)
        except OSError:
            pass
        finally:
            with contextlib.suppress(OSError):
                self.request.shutdown(socket.SHUT_RDWR)
            if process.poll() is None:
                process.kill()
            process.wait(timeout=5)
            sender.join(timeout=2)
            with self.server.lock:
                self.server.children.discard(process)


class Fixture:
    def __init__(self, evidence: Path):
        self.owner = "ledgence-artifact-s3-" + uuid.uuid4().hex[:12]
        self.network = self.owner + "-net"
        self.volume = self.owner + "-data"
        self.evidence = evidence
        self.resources: list[tuple[str, str]] = []
        self.running = False
        self.bridge = None
        self.generation = 0

    def initialize(self):
        if docker("image", "inspect", IMAGE, check=False).returncode:
            docker("pull", IMAGE, timeout=300)
        docker("network", "create", "--internal", "--label", "ledgence.fixture=" + self.owner, self.network)
        self.resources.append(("network", self.network))
        docker("volume", "create", "--label", "ledgence.fixture=" + self.owner, self.volume)
        self.resources.append(("volume", self.volume))
        self.start()
        self.bridge = Bridge(self.owner)
        threading.Thread(target=self.bridge.serve_forever, daemon=True).start()
        return "http://127.0.0.1:" + str(self.bridge.server_address[1])

    def start(self):
        docker("run", "--detach", "--name", self.owner, "--label", "ledgence.fixture=" + self.owner,
               "--network", self.network, "--memory", "768m", "--cpus", "1",
               "--mount", f"type=volume,source={self.volume},target=/data",
               "--env", "RUSTFS_ACCESS_KEY=ledgence-local", "--env", "RUSTFS_SECRET_KEY=ledgence-local",
               "--env", "RUSTFS_CONSOLE_ENABLE=false", "--env", "RUSTFS_OBS_LOGGER_LEVEL=error",
               "--env", "RUSTFS_ADDRESS=:9000", IMAGE, "/data")
        self.running = True
        self.generation += 1
        version = docker("exec", self.owner, "/usr/bin/rustfs", "--version").stdout
        if "1.0.4" not in version:
            raise RuntimeError("fixture binary version does not match pin")
        (self.evidence / f"rustfs-version-{self.generation}.txt").write_text(version)
        (self.evidence / f"rustfs-inspect-{self.generation}.json").write_text(docker("inspect", self.owner).stdout)
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            if docker("exec", self.owner, "nc", "-z", "127.0.0.1", "9000", check=False, timeout=5).returncode == 0:
                return
            time.sleep(0.2)
        raise RuntimeError("fixture startup timed out")

    def stop(self):
        if self.running:
            docker("stop", "--time", "8", self.owner, check=False)
            logs = docker("logs", self.owner, check=False)
            (self.evidence / f"rustfs-server-{self.generation}.log").write_text(logs.stdout + logs.stderr)
            docker("rm", "-f", self.owner)
            self.running = False

    def close(self):
        errors = []
        if self.bridge is not None:
            try:
                self.bridge.close()
            except Exception as error:
                errors.append(type(error).__name__)
        try:
            self.stop()
        except Exception as error:
            errors.append(type(error).__name__)
        for kind, name in reversed(self.resources):
            try:
                docker(kind, "rm", name)
            except Exception as error:
                errors.append(type(error).__name__)
        return errors



def create_bucket(endpoint: str, bucket: str):
    """Create a fresh disposable bucket in the literal-loopback fixture only.

    This deliberately small SigV4 fixture-admin request uses fixed fake keys,
    no credential resolution, proxies, redirects, or automatic retries. Product
    requests remain the real Rust adapter's responsibility.
    """
    origin = urllib.parse.urlsplit(endpoint)
    if (not origin.hostname or not ipaddress.ip_address(origin.hostname).is_loopback
            or origin.scheme != "http" or origin.username or origin.password
            or origin.query or origin.fragment or origin.path not in ("", "/")
            or not re.fullmatch(r"ledgence-artifact-[a-z0-9-]{1,40}", bucket)):
        raise ValueError("bucket creation requires an owned literal-loopback artifact fixture")
    now = datetime.datetime.now(datetime.timezone.utc)
    stamp, day = now.strftime("%Y%m%dT%H%M%SZ"), now.strftime("%Y%m%d")
    payload_hash = hashlib.sha256(b"").hexdigest()
    signed_headers = "host;x-amz-content-sha256;x-amz-date"
    canonical = (f"PUT\n/{bucket}\n\nhost:{origin.netloc}\n"
                 f"x-amz-content-sha256:{payload_hash}\nx-amz-date:{stamp}\n\n"
                 f"{signed_headers}\n{payload_hash}")
    scope = f"{day}/us-east-1/s3/aws4_request"
    message = f"AWS4-HMAC-SHA256\n{stamp}\n{scope}\n{hashlib.sha256(canonical.encode()).hexdigest()}"
    key = b"AWS4ledgence-local"
    for value in [day, "us-east-1", "s3", "aws4_request"]:
        key = hmac.new(key, value.encode(), hashlib.sha256).digest()
    signature = hmac.new(key, message.encode(), hashlib.sha256).hexdigest()
    headers = {"Host": origin.netloc, "X-Amz-Date": stamp,
               "X-Amz-Content-Sha256": payload_hash,
               "Authorization": f"AWS4-HMAC-SHA256 Credential=ledgence-local/{scope}, SignedHeaders={signed_headers}, Signature={signature}"}
    connection = http.client.HTTPConnection(origin.hostname, origin.port or 80, timeout=15)
    try:
        connection.request("PUT", "/" + bucket, body=b"", headers=headers)
        response = connection.getresponse()
        body = response.read(65537)
        if response.status not in (200, 204) or len(body) > 65536:
            raise RuntimeError(f"local fixture bucket creation failed with status {response.status}")
    finally:
        connection.close()


def phase(endpoint: str, bucket: str, name: str, evidence: Path):
    env = {key: value for key, value in os.environ.items() if not key.startswith("AWS_")}
    env.update(LEDGENCE_S3_TEST_ENDPOINT=endpoint, LEDGENCE_S3_TEST_BUCKET=bucket,
               LEDGENCE_S3_TEST_PHASE=name, AWS_EC2_METADATA_DISABLED="true", AWS_MAX_ATTEMPTS="7")
    command = ["cargo", "test", "-p", "ledgence-adapter-blob", "--features", "s3", "--locked",
               "--test", "s3_acceptance", "--", "--ignored", "--exact", "local_s3_acceptance", "--nocapture"]
    with (evidence / f"adapter-{name}.log").open("w") as log:
        result = subprocess.run(command, cwd=REPO, env=env, stdout=log, stderr=subprocess.STDOUT, timeout=900)
    text = (evidence / f"adapter-{name}.log").read_text()
    if result.returncode or f"LEDGENCE_S3_ACCEPTANCE phase={name} passed=true" not in text:
        raise RuntimeError(f"production adapter {name} acceptance failed; inspect adapter-{name}.log")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--external-endpoint", help="explicit disposable loopback fixture; conformance only, no restart proof")
    parser.add_argument("--bucket", default="ledgence-artifact-" + uuid.uuid4().hex[:16])
    args = parser.parse_args()
    args.evidence.mkdir(parents=True, exist_ok=True)
    report = {"passed": False, "fixture_image": IMAGE if not args.external_endpoint else None,
              "scope": "local S3-compatible production adapter; no AWS qualification", "restart_validated": False,
              "phases": [], "cleanup_complete": False}
    fixture = None
    try:
        if args.external_endpoint:
            endpoint = urllib.parse.urlsplit(args.external_endpoint)
            if not endpoint.hostname or not ipaddress.ip_address(endpoint.hostname).is_loopback or endpoint.scheme != "http" or endpoint.username or endpoint.password or endpoint.query or endpoint.fragment or endpoint.path not in ("", "/"):
                raise ValueError("external fixture must be an HTTP literal-loopback origin")
            address = args.external_endpoint
        else:
            fixture = Fixture(args.evidence)
            address = fixture.initialize()
        phase(address, args.bucket, "write", args.evidence)
        report["phases"].append("write")
        if fixture:
            fixture.stop()
            fixture.start()
        phase(address, args.bucket, "read", args.evidence)
        report["phases"].append("read")
        report["restart_validated"] = fixture is not None
        report["passed"] = True
    except Exception as error:
        report["failure"] = str(error)
    finally:
        errors = fixture.close() if fixture else []
        report["cleanup_complete"] = not errors
        if errors:
            report["cleanup_errors"] = errors
            report["passed"] = False
        report["harness_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
        report["source_sha"] = subprocess.run(["git", "rev-parse", "HEAD"], cwd=REPO, text=True, capture_output=True, check=True).stdout.strip()
        report["source_dirty"] = bool(subprocess.run(["git", "status", "--porcelain"], cwd=REPO, text=True, capture_output=True, check=True).stdout)
        (args.evidence / "summary.json").write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps(report, sort_keys=True))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
