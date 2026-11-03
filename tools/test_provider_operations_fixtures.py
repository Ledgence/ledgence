"""Check fault evidence and runnable generated programs before native qualification (MIT)."""

from pathlib import Path
import tempfile
import unittest

from provider_operations.harness import fixture_source, provider_class

ROOT = Path(__file__).resolve().parents[1]


class ProviderFixtureTests(unittest.TestCase):
    def test_instrumented_packages_compile_without_losing_registered_entrypoints(self):
        with tempfile.TemporaryDirectory() as temporary:
            variants = [
                fixture_source(ROOT),
                fixture_source(ROOT, lookup=False),
                fixture_source(
                    ROOT,
                    retained_report=Path(temporary) / "actual.json",
                    release_report=Path(temporary) / "release",
                ),
            ]
            for source in variants:
                compile(source, "provider-program.py", "exec")
                self.assertIn("ctx.approved_provider_operation()", source)
                self.assertIn("ctx.wait_provider_operation(", source)
                self.assertIn("ctx.provider_operation_result", source)
            self.assertNotIn("@exports.probe", variants[1])
            self.assertIn("ProviderLookup.UNAVAILABLE", variants[1])
            self.assertIn("report.to_dict()", variants[2])

    def test_database_backup_crosses_process_boundary_using_binary_streams(self):
        import sys
        from provider_operations.scenarios import snapshot_database, restore_database

        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory)
            dump, restore = folder / "pg_dump", folder / "pg_restore"
            # Emulate a remote container: it cannot open the host's snapshot path.
            dump.write_text(
                f"#!{sys.executable}\nimport sys\n"
                "assert sys.argv[1:] == ['--dbname', 'owned-db', '--format=custom']\n"
                "sys.stdout.buffer.write(b'PGDMP\\x00\\xff')\n"
            )
            restore.write_text(
                f"#!{sys.executable}\nimport sys\n"
                "assert sys.argv[1:] == ['--dbname', 'owned-db', '--clean', '--if-exists', '--exit-on-error']\n"
                "assert sys.stdin.buffer.read() == b'PGDMP\\x00\\xff'\n"
            )
            dump.chmod(0o700)
            restore.chmod(0o700)
            snapshot = folder / "host-only.dump"
            snapshot_database(dump, "owned-db", snapshot)
            self.assertEqual(snapshot.read_bytes(), b"PGDMP\x00\xff")
            restore_database(restore, "owned-db", snapshot)

    def test_historical_helper_inventory_detects_source_drift(self):
        import importlib.util

        spec = importlib.util.spec_from_file_location(
            "provider_native_gate", ROOT / "tools/check-provider-operations.py"
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory)
            runner = folder / "bootstrap.py"
            runner.write_text("# prior runtime\n")
            helper = folder / "workflow.py"
            helper.write_text("# prior workflow\n")
            before = module.helper_sources(runner)
            self.assertEqual(set(before), {"bootstrap.py", "workflow.py"})
            helper.write_text("# changed runtime\n")
            self.assertNotEqual(module.helper_sources(runner), before)

    def test_sqs_requires_local_explicit_endpoint_and_both_route_bindings(self):
        from provider_operations.sqs import loopback_endpoint, delivery_config

        self.assertEqual(
            loopback_endpoint("http://127.0.0.1:64324/"), "http://127.0.0.1:64324"
        )
        for endpoint in (
            "https://sqs.us-east-1.amazonaws.com",
            "http://localhost:bad",
            "http://user@127.0.0.1",
            "http://@127.0.0.1",
            "http://127.0.0.1?x=y",
            "http://127.0.0.1/#fragment",
            "http://127.0.0.1/queue",
        ):
            with self.assertRaises(ValueError):
                loopback_endpoint(endpoint)
        scope = {"tenant_id": "owned", "namespace": "fixture"}
        configs = [
            delivery_config(
                scope, queue, "dest-" + queue, "http://localhost/q", "http://localhost"
            )
            for queue in ("workflow", "provider-checks")
        ]
        self.assertNotEqual(configs[0]["route"], configs[1]["route"])
        for config in configs:
            self.assertEqual(config["route"]["scope"], scope)
            self.assertTrue(config["sqs"]["local_credentials"])
            self.assertEqual(config["sqs"]["visibility_timeout_seconds"], 60)

    def test_sqs_census_cannot_pass_with_integrated_tasks_or_missing_claims(self):
        import json
        from unittest.mock import patch
        from provider_operations.sqs import ProviderSqsDeployment

        deployment = object.__new__(ProviderSqsDeployment)
        deployment.queue = "workflow"
        deployment.destinations = {
            "workflow": "external-main",
            "provider-checks": "external-probe",
        }
        tasks = [
            {
                "queue": queue,
                "destination": deployment.destinations[queue],
                "state": "succeeded",
                "attempts": 1,
                "claims": 1,
            }
            for queue in ["workflow"] * 3 + ["provider-checks"]
        ]
        value = {"tasks": tasks, "pending": 0, "active": 0, "unconfirmed": 0}
        occurrence = {"workflow_id": "owned-workflow"}
        with patch.object(deployment, "sql", return_value=json.dumps(value)):
            self.assertEqual(deployment.external_census(occurrence), value)
        for field, invalid in (("destination", None), ("claims", 0), ("attempts", 2)):
            changed = dict(
                value, tasks=tasks[:-1] + [dict(tasks[-1], **{field: invalid})]
            )
            with patch.object(deployment, "sql", return_value=json.dumps(changed)):
                with self.assertRaises(AssertionError):
                    deployment.external_census(occurrence)

    def test_report_uses_an_explicit_public_whitelist(self):
        import json
        from provider_operations.scenarios import (
            public_cancelled,
            CANCELLED_PUBLIC_FIELDS,
        )

        record = {name: None for name in CANCELLED_PUBLIC_FIELDS}
        record.update(
            command={"owner": {"lease_id": "private"}},
            subject="private-subject",
            paths={"file": "private"},
            future={"token": "private"},
        )
        value = public_cancelled(record)
        self.assertEqual(set(value), set(CANCELLED_PUBLIC_FIELDS))
        self.assertNotIn("private", json.dumps(value))

    def test_provider_has_no_remote_fault_controls_and_counts_requests_independently(
        self,
    ):
        import http.client, json
        from urllib.parse import urlsplit

        Provider = provider_class(ROOT)
        with (
            tempfile.TemporaryDirectory() as directory,
            Provider(directory, completion_delay=0.01) as provider,
        ):
            origin = urlsplit(provider.url)
            connection = http.client.HTTPConnection(
                origin.hostname, origin.port, timeout=2
            )
            connection.request(
                "POST",
                "/faults",
                body=json.dumps({"dataset": "customers"}),
                headers={"Idempotency-Key": "x", "X-Provider-Account": "demo"},
            )
            response = connection.getresponse()
            self.assertEqual(response.status, 400)
            response.read()
            connection.close()
            counts = provider.census()
            self.assertEqual(counts["mutation_http_requests"], 1)
            self.assertEqual(counts["committed_effects"], 0)
            self.assertEqual(provider.faults, {})


if __name__ == "__main__":
    unittest.main()
