"""Offline distribution readiness regressions; no Docker, network or real sleeps."""
import importlib.util
import io
import itertools
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, call, patch

SPEC = importlib.util.spec_from_file_location(
    'check_distribution', Path(__file__).resolve().parents[2] / 'tools/check-distribution.py')
distribution = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(distribution)


def worker(identity, *, freshness='fresh', accepting=True, session_expired=False):
    return dict(worker_session_id=identity, freshness=freshness, accepting=accepting,
                session_expired=session_expired)


def page(*workers, cursor=None):
    return dict(items=list(workers), next_cursor=cursor)


class Clock:
    def __init__(self):
        self.now = 0
        self.sleeps = []

    def monotonic(self):
        return self.now

    def sleep(self, duration):
        self.sleeps.append(duration)
        self.now += duration


class WorkerReadinessTests(unittest.TestCase):
    def setUp(self):
        self.acceptance = distribution.Acceptance.__new__(distribution.Acceptance)
        self.acceptance.base = 'http://owned.test'
        self.clock = Clock()
        self.time = patch.object(distribution, 'time', self.clock)
        self.time.start()
        self.addCleanup(self.time.stop)

    def test_old_fresh_accepting_sessions_do_not_satisfy_restart_readiness(self):
        self.acceptance.api = Mock(side_effect=[
            page(worker('old-base'), worker('old-examples')),
            page(worker('new', freshness='unavailable', accepting=None), worker('old-examples')),
            page(worker('new', accepting=False), worker('old-base')),
            page(worker('new'), worker('old-base'), worker('old-examples')),
        ])
        result = self.acceptance.wait_for_worker(excluded_sessions={'old-base', 'old-examples'})
        self.assertEqual(result['worker_session_id'], 'new')
        self.assertEqual(self.acceptance.api.call_count, 4)
        self.assertEqual(self.clock.sleeps, [.2, .2, .2])

    def test_new_fresh_accepting_but_expired_session_cannot_satisfy_readiness(self):
        self.acceptance.api = Mock(side_effect=[
            page(worker('old'), worker('expired-new', session_expired=True)),
            page(worker('expired-new', session_expired=True), worker('live-new')),
        ])
        result = self.acceptance.wait_for_worker(excluded_sessions={'old'})
        self.assertEqual(result['worker_session_id'], 'live-new')
        self.assertEqual(self.clock.sleeps, [.2])

    def test_no_new_session_times_out_even_if_old_workers_remain_ready(self):
        self.acceptance.api = Mock(return_value=page(worker('old-base'), worker('old-examples')))
        with self.assertRaisesRegex(AssertionError, 'no fresh accepting worker with a new session'):
            self.acceptance.wait_for_worker(excluded_sessions={'old-base', 'old-examples'}, timeout=.5)
        self.assertEqual(self.clock.now, .5)
        self.assertEqual(self.acceptance.api.call_count, 4)

    def test_multiple_old_and_new_sessions_are_independent_of_list_order(self):
        workers = [worker(name) for name in ('old-a', 'old-z', 'new-a', 'new-z')]
        for ordering in itertools.permutations(workers):
            with self.subTest(order=[item['worker_session_id'] for item in ordering]):
                self.acceptance.api = Mock(return_value=page(*ordering))
                result = self.acceptance.wait_for_worker(excluded_sessions={'old-a', 'old-z'})
                self.assertEqual(result['worker_session_id'], 'new-a')
        self.assertEqual(self.clock.sleeps, [])

    def test_initial_readiness_still_waits_for_fresh_accepting_observation(self):
        self.acceptance.api = Mock(side_effect=[
            page(worker('unready', freshness='stale'), worker('unreported', freshness='unavailable', accepting=None)),
            page(worker('ready')),
        ])
        self.assertEqual(self.acceptance.wait_for_worker()['worker_session_id'], 'ready')
        self.assertEqual(self.clock.sleeps, [.2])

    def test_restart_snapshot_includes_every_page_and_unready_session(self):
        self.acceptance.api = Mock(side_effect=[
            page(worker('old-base'), cursor='next page+token'),
            page(worker('old-unreported', freshness='unavailable', accepting=None)),
        ])
        self.assertEqual(self.acceptance.worker_session_ids(base='http://cli.test'), {'old-base', 'old-unreported'})
        self.assertEqual(self.acceptance.api.call_args_list, [
            call('/v1/console/workers', base='http://cli.test'),
            call('/v1/console/workers?cursor=next+page%2Btoken', base='http://cli.test'),
        ])

    def test_repeated_pagination_cursor_fails_instead_of_looping(self):
        self.acceptance.api = Mock(return_value=page(worker('old'), cursor='repeat'))
        with self.assertRaisesRegex(AssertionError, 'repeated a cursor'):
            self.acceptance.worker_session_ids()
        self.assertEqual(self.acceptance.api.call_count, 2)

    def test_console_retains_catalog_capacity_and_slot_assertions_for_new_worker(self):
        catalog = [{'program_id': 'example'}]
        self.acceptance.wait_for_worker = Mock(return_value=worker('new'))
        for capacity, slots, valid in ((1, [{}], True), (2, [{}], False), (1, [], False)):
            with self.subTest(capacity=capacity, slots=slots):
                self.acceptance.api = Mock(side_effect=[
                    {'instance_id': 'ledgence-local'}, {'items': catalog},
                    {'worker': {'capacity': capacity}, 'slots': {'items': slots}},
                ])

                def response(*args, **kwargs):
                    result = io.BytesIO(b'<script src="/console/assets/main.js"></script>')
                    result.status = 200
                    result.headers = {'Content-Type': 'text/javascript' if '/assets/' in args[0] else 'text/html'}
                    return result

                with patch.object(distribution.urllib.request, 'urlopen', side_effect=response):
                    if valid:
                        result = self.acceptance.console(['example'], excluded_sessions={'old'})
                        self.assertEqual(result, {'catalog': catalog, 'worker_session_id': 'new'})
                    else:
                        with self.assertRaises(AssertionError):
                            self.acceptance.console(['example'], excluded_sessions={'old'})
                self.acceptance.wait_for_worker.assert_called_with(base='http://owned.test', excluded_sessions={'old'})
                self.assertEqual(self.acceptance.api.call_args,
                    call('/v1/console/workers/inspect?worker_session_id=new', base='http://owned.test'))


class RestartChecksTests(unittest.TestCase):
    def test_compose_restart_excludes_all_preexisting_sessions_at_each_restart(self):
        acceptance = distribution.Acceptance.__new__(distribution.Acceptance)
        acceptance.compose, acceptance.examples = ['compose'], ['compose', 'examples']
        acceptance.manifest = {'image': 'qualified-image'}
        acceptance.image_id, acceptance.report = 'image-id', {}
        events = []
        catalog = [{'program_id': 'example'}]
        acceptance.console = Mock(side_effect=[
            {'catalog': [], 'worker_session_id': 'base'},
            {'catalog': catalog, 'worker_session_id': 'examples'},
            {'catalog': catalog, 'worker_session_id': 'new'},
        ])
        snapshots = iter([{'base', 'earlier'}, {'base', 'earlier', 'examples', 'other'}])

        def sessions():
            result = next(snapshots)
            events.append(('snapshot', result))
            return result

        acceptance.worker_session_ids = Mock(side_effect=sessions)
        acceptance.preserved = Mock(return_value={'workflow': 'retained', 'subscriptions': ['delivered']})

        def run(args, **kwargs):
            events.append(('run', args))
            if 'config' in args:
                return json.dumps({'services': {
                    'postgres': {}, 'worker': {'image': 'qualified-image'},
                    'migrate': {'image': 'qualified-image'},
                    'orchestrator': {'image': 'qualified-image', 'ports': [{'host_ip': '127.0.0.1'}]},
                }})
            if 'inspect' in args:
                return json.dumps([{'Config': {'Labels': {'com.docker.compose.service': 'worker'}}, 'Image': 'image-id'}])
            if 'ps' in args:
                return 'container'
            if args[-1] == 'demo':
                return json.dumps({'passed': True})
            return ''

        acceptance.run = Mock(side_effect=run)
        acceptance.verify_compose()
        expected = ['invoice-issuer', 'workflow-example', 'workflow-summary']
        self.assertEqual(acceptance.console.call_args_list, [
            call([]), call(expected, excluded_sessions={'base', 'earlier'}),
            call(expected, excluded_sessions={'base', 'earlier', 'examples', 'other'}),
        ])
        for index, event in enumerate(events):
            if event[0] == 'snapshot':
                self.assertEqual(events[index + 1][0], 'run')
                self.assertIn('down', events[index + 1][1])
        self.assertEqual(acceptance.preserved.call_count, 2)
        self.assertTrue(acceptance.report['examples']['catalog_preserved'])
        self.assertEqual(acceptance.report['examples']['new_worker_session'], 'new')

    def test_cli_restart_excludes_all_sessions_from_its_own_instance(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            acceptance = distribution.Acceptance.__new__(distribution.Acceptance)
            acceptance.evidence = directory
            acceptance.cwd = directory / 'cwd'
            acceptance.cwd.mkdir()
            acceptance.cli = directory / 'source-cli'
            acceptance.cli.write_bytes(b'fixture-native-executable')
            acceptance.kit = directory / 'kit'
            acceptance.manifest, acceptance.report, acceptance.owned = {}, {'projects': []}, []
            acceptance.free_port = Mock(return_value=12345)
            acceptance.console = Mock(side_effect=[{'worker_session_id': 'old'}, {'worker_session_id': 'new'}])
            acceptance.worker_session_ids = Mock(return_value={'old', 'earlier'})
            state_dir = directory / 'cli-state'

            def run(args, **kwargs):
                if args == ['docker', 'context', 'show']:
                    return 'fixture-context'
                if 'up' in args:
                    state_dir.mkdir(exist_ok=True)
                    (state_dir / '.ledgence-state.json').write_text(json.dumps({
                        'project': 'ledgence-' + 'a' * 24, 'context': 'fixture-context',
                        'directory': str(state_dir), 'distribution': {},
                    }))
                return ''

            acceptance.run = Mock(side_effect=run)
            acceptance.verify_cli()
            acceptance.worker_session_ids.assert_called_once_with(base='http://127.0.0.1:12345')
            self.assertEqual(acceptance.console.call_args_list, [
                call([], base='http://127.0.0.1:12345'),
                call([], base='http://127.0.0.1:12345', excluded_sessions={'old', 'earlier'}),
            ])
            self.assertTrue(acceptance.report['cli']['restart_preserved_kit_and_settings'])


if __name__ == '__main__':
    unittest.main(verbosity=2)
