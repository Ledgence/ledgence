"""Offline checks for acceptance timing evidence and opt-in SQS acquisition waits."""

import asyncio
import contextlib
import importlib.util
import io
import json
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from http_acceptance import harness, sqs


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location('check_workflows', ROOT / 'tools/check-workflows.py')
WORKFLOWS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(WORKFLOWS)


def rows(directory):
    return [json.loads(line) for line in (directory / 'timings.jsonl').read_text().splitlines()]


class TimingTests(unittest.TestCase):
    def test_success_and_failure_append_elapsed_without_exception_contents(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            clock = SimpleNamespace(monotonic=Mock(side_effect=[10, 12.5, 20, 23]))
            output = io.StringIO()
            with patch.object(harness, 'time', clock), contextlib.redirect_stdout(output):
                with harness.timed(directory, 'scenario', 'success'):
                    pass
                with self.assertRaisesRegex(RuntimeError, 'private credential'):
                    with harness.timed(directory, 'scenario', 'failure'):
                        raise RuntimeError('private credential')
            self.assertEqual(rows(directory), [
                dict(kind='scenario', name='success', result='passed', elapsed_seconds=2.5),
                dict(kind='scenario', name='failure', result='failed', elapsed_seconds=3,
                     error_type='RuntimeError'),
            ])
            self.assertNotIn('private credential', (directory / 'timings.jsonl').read_text())
            self.assertNotIn('private credential', output.getvalue())

    def test_stop_records_drain_success_timeout_and_invalid_worker_report(self):
        for outcome in ('success', 'timeout', 'unfinished'):
            with self.subTest(outcome=outcome), tempfile.TemporaryDirectory() as temporary:
                directory = Path(temporary)
                process = harness.Process.__new__(harness.Process)
                process.directory = directory
                process.label = 'worker-1'
                process.stderr_path = directory / 'stderr'
                process.stdout_path = directory / 'stdout'
                process.stdout_path.write_text(json.dumps({'delivery': {'finished': outcome != 'unfinished'}}))
                process.process = Mock()
                process.process.poll.return_value = None
                process.process.wait.return_value = 1
                if outcome == 'timeout':
                    process.process.wait.side_effect = subprocess.TimeoutExpired(['private argument'], 7)
                with contextlib.redirect_stdout(io.StringIO()):
                    if outcome == 'success':
                        self.assertEqual(process.stop(timeout=7), 1)
                    else:
                        with self.assertRaises(AssertionError):
                            process.stop(timeout=7)
                process.process.send_signal.assert_called_once_with(signal.SIGTERM)
                process.process.wait.assert_called_once_with(timeout=7)
                result = rows(directory)[0]
                self.assertEqual((result['kind'], result['name']), ('process-stop', 'worker-1'))
                self.assertEqual(result['result'], 'passed' if outcome == 'success' else 'failed')
                self.assertGreaterEqual(result['elapsed_seconds'], 0)
                self.assertNotIn('private argument', (directory / 'timings.jsonl').read_text())

    def test_workflow_timing_includes_drain_and_stops_at_the_failed_case(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            deployment = SimpleNamespace(directory=directory)
            called = []

            async def scenario(d, delay, names, record, iterations, capture):
                called.append((names, iterations, capture))
                record(names[0], {'verified': True})
                # Result assertions may pass before the scenario's drain fails.
                if names == ['resume']:
                    raise RuntimeError('drain failed')
                with harness.timed(d.directory, 'process-stop', 'worker-1'):
                    await asyncio.sleep(0)

            record = Mock()
            with patch.object(WORKFLOWS, 'scenarios', scenario), contextlib.redirect_stdout(io.StringIO()):
                with self.assertRaisesRegex(RuntimeError, 'drain failed'):
                    asyncio.run(WORKFLOWS.timed_scenarios(deployment, None,
                        ['lost-ack', 'resume', 'examples'], record, 2, 'capture'))
            self.assertEqual(called, [(['examples'], 2, 'capture'), (['resume'], 2, 'capture')])
            timings = rows(directory)
            self.assertEqual([(row['kind'], row['name'], row['result']) for row in timings], [
                ('process-stop', 'worker-1', 'passed'),
                ('scenario', 'examples', 'passed'),
                ('scenario', 'resume', 'failed'),
            ])
            self.assertGreaterEqual(timings[1]['elapsed_seconds'], timings[0]['elapsed_seconds'])


class AcquisitionWaitTests(unittest.TestCase):
    def test_worker_omits_option_by_default_and_forwards_explicit_values(self):
        for wait in (None, 0, 1000, 20000):
            with self.subTest(wait=wait), tempfile.TemporaryDirectory() as temporary:
                directory = Path(temporary)
                with patch.object(harness.Deployment, '__init__'):
                    deployment = sqs.SqsDeployment.__new__(sqs.SqsDeployment)
                    deployment.directory = directory
                    deployment.scope = {'tenant_id': 'test', 'namespace': 'test'}
                    deployment.queue = 'test'
                    sqs.SqsDeployment.__init__(deployment, queue_url='http://localhost/queue',
                        endpoint='http://localhost', region='local', acquire_wait_ms=wait)
                deployment.root = ROOT
                deployment.binaries = directory
                deployment.python = sys.executable
                deployment.counter = 0
                deployment.server_url = 'http://localhost/server'
                deployment.artifacts = SimpleNamespace(url='http://localhost/artifacts')
                deployment.environment = {}
                deployment.processes = []
                with patch.object(sqs, 'Process') as process:
                    self.assertIs(deployment.start_worker(), process.return_value)
                arguments = process.call_args.args[0]
                if wait is None:
                    self.assertNotIn('--acquire-wait-ms', arguments)
                else:
                    self.assertEqual(arguments[-2:], ['--acquire-wait-ms', str(wait)])
                self.assertEqual(json.loads(deployment.delivery_config.read_text())['sqs']['visibility_timeout_seconds'], 60)

    def test_invalid_wait_rejected_before_fixture_resources(self):
        for wait in (-1, 20001, 'invalid'):
            with self.subTest(wait=wait), patch.object(harness.Deployment, '__init__') as setup:
                with self.assertRaises(ValueError):
                    sqs.SqsDeployment(acquire_wait_ms=wait)
                setup.assert_not_called()

    def test_cli_rejects_range_and_integrated_use_before_resources(self):
        for arguments, message in (
            (['--acquire-wait-ms', '-1', '--endpoint', 'http://localhost'], 'invalid acquisition_wait'),
            (['--acquire-wait-ms', '20001', '--endpoint', 'http://localhost'], 'invalid acquisition_wait'),
            (['--acquire-wait-ms', '1000'], 'requires an ElasticMQ --endpoint'),
            (['--self-test', '--acquire-wait-ms', '1000', '--endpoint', 'http://localhost'], 'requires an ElasticMQ --endpoint'),
        ):
            with self.subTest(arguments=arguments), patch.object(sys, 'argv', ['check-workflows.py', *arguments]), \
                    patch.object(WORKFLOWS, 'create_owned_database') as create, \
                    contextlib.redirect_stderr(io.StringIO()) as stderr:
                with self.assertRaises(SystemExit) as error:
                    WORKFLOWS.main()
                self.assertEqual(error.exception.code, 2)
                self.assertIn(message, stderr.getvalue())
                create.assert_not_called()


if __name__ == '__main__':
    unittest.main(verbosity=2)
