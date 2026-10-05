"""Database-isolation regressions; never open a DB, socket, or subprocess."""
from contextlib import ExitStack, redirect_stderr
import importlib.util
import io
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from postgres_fixture import create_owned_database, owned_database_url

ROOT = Path(__file__).resolve().parents[1]
OVERRIDES = ("dbname=operator_database", "db%6eame=operator_database", "dbname=",
             "dbname", "DBNAME=operator_database", "database=operator_database",
             "service=operator_service", "service=", "sslmode=require&dbname=operator_database")


def load(relative):
    path = ROOT / relative
    spec = importlib.util.spec_from_file_location("fixture_test_" + path.stem.replace("-", "_"), path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def no_resources():
    stack = ExitStack()
    for module, names in ((subprocess, ("run", "Popen", "check_output")),
                          (socket, ("socket", "create_connection")),
                          (tempfile, ("mkdtemp", "TemporaryFile")),
                          (Path, ("mkdir",))):
        for name in names:
            stack.enter_context(patch.object(module, name, side_effect=AssertionError("resource creation before URL rejection")))
    return stack


class OwnedDatabaseUrlTests(unittest.TestCase):
    def test_preserves_connection_options_and_replaces_only_database(self):
        for parent, expected in (
            ("postgres://user:p%40ss@localhost:5433/original?sslmode=require&application_name=fixture",
             "postgres://user:p%40ss@localhost:5433/owned_fixture?sslmode=require&application_name=fixture"),
            ("postgresql://user@[::1]:5433/original", "postgresql://user@[::1]:5433/owned_fixture"),
            ("postgres:///original?host=%2Ftmp", "postgres:///owned_fixture?host=%2Ftmp"),
        ):
            with self.subTest(parent=parent):
                self.assertEqual(owned_database_url(parent, "owned_fixture"), expected)

    def test_rejects_database_and_service_overrides_without_echoing_credentials(self):
        for query in OVERRIDES:
            with self.subTest(query=query), self.assertRaisesRegex(ValueError, "must not override") as raised:
                owned_database_url("postgres://user:private-password@localhost/original?" + query, "owned_fixture")
            self.assertNotIn("private-password", str(raised.exception))

    def test_rejects_malformed_urls_and_invalid_generated_names(self):
        for parent in ("https://localhost/original", "localhost/original", "postgres:///original", "postgres://localhost:wrong/db",
                       "postgres://localhost/db#fragment", " postgres://localhost/db", "postgres://local\nhost/db"):
            with self.subTest(parent=parent), self.assertRaises(ValueError):
                owned_database_url(parent, "owned_fixture")
        for database in ("", "../original", "original?dbname=other", "original/db", "a" * 64):
            with self.subTest(database=database), self.assertRaises(ValueError):
                owned_database_url("postgres://localhost/original", database)

    def test_all_acceptance_entrypoints_reject_before_resource_creation(self):
        cases = (
            ("check-http.py", []),
            ("check-observability.py", ["--binaries", "/unused", "--capture", "/unused", "--evidence", "/unused"]),
            ("check-console.py", ["--binaries", "/unused", "--console-dist", "/unused"]),
            ("check-python-client-e2e.py", ["--binaries", "/unused"]),
            ("check-completions.py", ["--binaries", "/unused"]),
            ("check-workflows.py", []),
            ("check-sqs.py", ["--endpoint", "http://127.0.0.1:9324"]),
            ("check-performance.py", ["--disposable-postgres"]),
            ("check-workload-soak.py", ["--disposable-postgres", "--binaries", "/unused"]),
        )
        for name, arguments in cases:
            module = load("tools/" + name)
            for query in OVERRIDES:
                diagnostic = io.StringIO()
                with self.subTest(tool=name, query=query), \
                     patch.dict(os.environ, {"LEDGENCE_POSTGRES_URL": "postgres://localhost/original?" + query}), \
                     patch.object(sys, "argv", [name, *arguments]), redirect_stderr(diagnostic), no_resources(), \
                     self.assertRaises(SystemExit) as raised:
                    module.main()
                self.assertEqual(raised.exception.code, 2)
                self.assertIn("must not override", diagnostic.getvalue())

    def test_release_smoke_rejects_before_admin_or_listener_setup(self):
        module = load("tools/release/console_bundle.py")
        for query in OVERRIDES:
            with self.subTest(query=query), \
                 patch.dict(os.environ, {"LEDGENCE_POSTGRES_URL": "postgres://localhost/original?" + query}), \
                 patch.object(module, "verify_bundle", return_value={"assets": []}), no_resources(), \
                 self.assertRaisesRegex(ValueError, "must not override"):
                module.smoke(Path("/unused"), Path("/unused"), Path("/unused"))

    def test_administrative_connection_preserves_the_owned_server_settings(self):
        from http_acceptance.harness import Deployment
        fixture = object.__new__(Deployment)
        fixture.psql = "fixture-psql"
        fixture.database_url = "postgres:///owned_fixture?host=%2Ftmp&sslmode=disable"
        result = subprocess.CompletedProcess([], 0, stdout=b"ok\n")
        with patch.object(subprocess, "run", return_value=result) as command:
            self.assertEqual(fixture.sql("SELECT 1", administrative=True), "ok")
        self.assertEqual(command.call_args.args[0][2], "postgres:///postgres?host=%2Ftmp&sslmode=disable")
        fixture.database_url += "&dbname=operator_database"
        with no_resources(), self.assertRaisesRegex(ValueError, "must not override"):
            fixture.sql("SELECT 1", administrative=True)

    def test_example_checks_use_the_same_isolation_rules(self):
        for example in ("support-agent", "codex-support-agent"):
            module = load(f"examples/{example}/check.py")
            for query in OVERRIDES:
                with self.subTest(example=example, query=query), no_resources(), \
                     self.assertRaisesRegex(module.CheckFailure, "must not override"):
                    module.database_url("postgres://localhost/original?" + query, "ldg_support_demo_" + "a" * 32)
        module = load("examples/codex-change-review/check.py")
        for query in OVERRIDES:
            with self.subTest(example="codex-change-review", query=query), \
                 patch.dict(os.environ, {"LEDGENCE_POSTGRES_URL": "postgres://localhost/original?" + query}), \
                 no_resources(), self.assertRaisesRegex(ValueError, "must not override"):
                with module.deployment(None, Path("/unused"), None, None, "unused"):
                    self.fail("unexpected deployment")


class CreationCleanupTests(unittest.TestCase):
    def test_lost_create_reply_and_interruption_clean_exact_owned_database(self):
        for failure in (subprocess.TimeoutExpired('psql', 1), OSError('connection lost'), KeyboardInterrupt()):
            with self.subTest(failure=type(failure).__name__):
                databases = {'operator_database'}
                statements = []
                def admin(statement):
                    statements.append(statement)
                    if statement.startswith('CREATE'):
                        databases.add('owned_fixture')
                        raise failure
                    databases.discard('owned_fixture')
                with self.assertRaises(type(failure)):
                    create_owned_database(admin, 'owned_fixture')
                self.assertEqual(databases, {'operator_database'})
                self.assertEqual(statements, ['CREATE DATABASE "owned_fixture"',
                    'DROP DATABASE IF EXISTS "owned_fixture" WITH (FORCE)'])

    def test_success_retains_database_for_the_callers_normal_cleanup(self):
        statements = []
        create_owned_database(statements.append, 'owned_fixture')
        self.assertEqual(statements, ['CREATE DATABASE "owned_fixture"'])
        with self.assertRaisesRegex(ValueError, 'invalid owned'):
            create_owned_database(statements.append, 'unsafe"name')
        self.assertEqual(len(statements), 1)

    def test_relocated_console_cleans_when_create_commits_without_reply(self):
        module = load('tools/release/console_bundle.py')
        statements = []
        def admin(*args, **kwargs):
            statements.append(kwargs['input'])
            if statements[-1].startswith('CREATE'):
                raise subprocess.TimeoutExpired('psql', 30)
            return subprocess.CompletedProcess([], 0)
        with tempfile.TemporaryDirectory() as directory, \
             patch.dict(os.environ, {'LEDGENCE_POSTGRES_URL': 'postgres://localhost/parent'}), \
             patch.object(module, 'verify_bundle', return_value={'assets': []}), \
             patch.object(module.socket, 'socket') as sock, \
             patch.object(module.subprocess, 'run', side_effect=admin):
            sock.return_value.__enter__.return_value.getsockname.return_value = ('127.0.0.1', 12345)
            with self.assertRaises(subprocess.TimeoutExpired):
                module.smoke(Path(directory)/'bundle', Path(directory)/'store', Path(directory))
        self.assertEqual(len(statements), 2)
        created_name = statements[0].removeprefix('CREATE DATABASE ')
        self.assertEqual(statements[1], f'DROP DATABASE IF EXISTS {created_name} WITH (FORCE)')


if __name__ == "__main__":
    unittest.main()
