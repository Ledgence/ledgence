"""Credential files are private, bounded, endpoint-bound and never diagnostics."""
import json
import os
from pathlib import Path
import tempfile
import unittest

from aiohttp import web
from ledgence.client import AsyncClient, Forbidden, InputError, Unauthenticated
from ledgence.client.approval_models import parse_approval
from support import response, status
from test_approvals import snapshot

TOKEN = "test-only.opaque_credential-01"

class CredentialTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.requests = []
        self.failure = None
        async def handle(request):
            self.requests.append((request.path, request.headers.get("Authorization")))
            await request.read()
            if self.failure:
                code, http = self.failure
                return response({"code": code}, code=http)
            return response(status())
        app = web.Application()
        app.router.add_route("*", "/{path:.*}", handle)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        self.url = f"http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}"
        self.addAsyncCleanup(runner.cleanup)

    def client(self, **kwargs):
        return AsyncClient(self.url, tenant="tenant", namespace="tests", **kwargs)

    async def test_programmatic_and_file_credentials_reach_only_bound_server(self):
        async with self.client(token=TOKEN) as client:
            self.assertNotIn(TOKEN, repr(client))
            await client.tasks.handle("task").status()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "credential.json"
            path.write_text(json.dumps({"server": self.url + "/", "token": TOKEN}))
            path.chmod(0o600)
            async with self.client(credentials_file=path) as client:
                await client.tasks.handle("task").status()
        self.assertEqual(self.requests, [("/v1/tasks/status", "Bearer " + TOKEN)] * 2)

    async def test_invalid_credentials_fail_before_dispatch_and_hide_secret(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "credential.json"
            cases = [
                {"server": "https://other.example", "token": TOKEN},
                {"server": self.url, "token": TOKEN, "unexpected": True},
                {"server": self.url, "token": TOKEN + "\r\nheader: leaked"},
                {"server": self.url, "token": "x" * 4097},
            ]
            for value in cases:
                path.write_text(json.dumps(value)); path.chmod(0o600)
                with self.assertRaises(InputError) as caught:
                    self.client(credentials_file=path)
                self.assertNotIn(TOKEN, str(caught.exception))
            for raw in ["{broken", "[]", '{"server":"x","server":"y","token":"z"}', "x" * 16385]:
                path.write_text(raw); path.chmod(0o600)
                with self.assertRaises(InputError): self.client(credentials_file=path)
            path.write_text(json.dumps({"server": self.url, "token": TOKEN})); path.chmod(0o644)
            if os.name == "posix":
                with self.assertRaises(InputError): self.client(credentials_file=path)
                path.chmod(0o600)
                link = Path(directory) / "link"; link.symlink_to(path)
                with self.assertRaises(InputError): self.client(credentials_file=link)
            with self.assertRaises(InputError): self.client(token=TOKEN, credentials_file=path)
            with self.assertRaises(InputError): self.client(credentials_file=Path(directory) / "missing")
        for token in ["", "x y", "é", "a\n", 42]:
            with self.assertRaises(InputError): self.client(token=token)
        self.assertEqual(self.requests, [])

    async def test_remote_cleartext_endpoint_cannot_receive_credential(self):
        for host in ["http://example.com", "http://localhost.example", "http://192.168.0.1"]:
            with self.assertRaises(InputError):
                AsyncClient(host, tenant="tenant", namespace="tests", token=TOKEN)
        for host in ["https://example.com", "http://localhost:8080", "http://[::1]:8080"]:
            client = AsyncClient(host, tenant="tenant", namespace="tests", token=TOKEN)
            self.assertNotIn(TOKEN, repr(client))
        self.assertEqual(self.requests, [])

    async def test_authentication_denials_are_definitive_for_reads_submission_and_approval(self):
        for code, http, error in [("unauthenticated", 401, Unauthenticated), ("forbidden", 403, Forbidden)]:
            self.failure = (code, http)
            async with self.client(token=TOKEN) as client:
                with self.assertRaises(error): await client.tasks.handle("task").status()
                with self.assertRaises(error):
                    await client.tasks.submit(program="echo", version="1", queue="queue", data={}, idempotency_key="one")
                workflow = client.workflows.handle("workflow")
                approval = parse_approval(snapshot(), client.scope, "workflow", "refund", self.url)
                command = workflow.prepare_approval_decision(approval, decision_id="decision", decision="approve", reviewer="operator")
                with self.assertRaises(error): await workflow.decide_approval(command)
        self.assertEqual(len(self.requests), 6)
