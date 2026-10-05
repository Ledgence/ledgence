"""Exercise durable demo callback acknowledgment and conflicting redelivery."""
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('local_receiver', ROOT / 'deploy/local/receiver.py')
receiver = importlib.util.module_from_spec(spec)
spec.loader.exec_module(receiver)


class ReceiverTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.store = Path(self.directory.name) / 'events.json'
        for name, value in [('STORE', self.store), ('EVENTS', {})]:
            override = patch.object(receiver, name, value)
            override.start()
            self.addCleanup(override.stop)

    def deliver(self, event):
        encoded = json.dumps(event).encode()
        handler = object.__new__(receiver.Handler)
        handler.connection = type('Connection', (), {'settimeout': lambda *args: None})()
        handler.path = '/completion'
        handler.headers = {'Content-Length': str(len(encoded))}
        handler.rfile = io.BytesIO(encoded)
        responses = []
        handler.reply = lambda status, *args: responses.append(status)
        handler.do_POST()
        self.assertEqual(len(responses), 1)
        return responses[0]

    def event(self, data):
        return {'specversion': '1.0', 'source': 'urn:ledgence:orchestrator',
                'id': 'event-1', 'type': 'task.completed', 'data': data}

    def test_identical_redelivery_is_acknowledged_without_replacing_saved_event(self):
        event = self.event({'invoice': '1042', 'amount': 10})
        self.assertEqual(self.deliver(event), 204)
        saved = self.store.read_bytes()
        self.assertEqual(self.deliver(dict(reversed(list(event.items())))), 204)
        self.assertEqual(self.store.read_bytes(), saved)
        self.assertEqual(len(receiver.EVENTS), 1)

    def test_changed_payload_and_json_type_are_rejected_with_original_preserved(self):
        event = self.event({'value': 1})
        self.assertEqual(self.deliver(event), 204)
        saved = self.store.read_bytes()
        for value in (2, True, 1.0):
            with self.subTest(value=value):
                self.assertEqual(self.deliver(self.event({'value': value})), 409)
                self.assertEqual(self.store.read_bytes(), saved)
                self.assertEqual(next(iter(receiver.EVENTS.values())), event)

    def test_storage_failure_is_retryable_and_never_acknowledged(self):
        with patch.object(receiver.os, 'fsync', side_effect=OSError('storage unavailable')):
            self.assertEqual(self.deliver(self.event({'value': 1})), 503)
        self.assertEqual(receiver.EVENTS, {})
        self.assertEqual(self.deliver(self.event({'value': 1})), 204)

    def test_rename_without_directory_sync_retains_binding_until_durable_retry(self):
        event = self.event({'value': 1})
        with patch.object(receiver.os, 'fsync', side_effect=[None, OSError('directory sync failed')]):
            self.assertEqual(self.deliver(event), 503)
        saved = self.store.read_bytes()
        self.assertEqual(self.deliver(self.event({'value': 2})), 409)
        self.assertEqual(self.store.read_bytes(), saved)
        with patch.object(receiver.os, 'fsync', side_effect=OSError('still unavailable')):
            self.assertEqual(self.deliver(event), 503)
        self.assertEqual(self.deliver(event), 204)
        self.assertEqual(self.store.read_bytes(), saved)

    def test_nonfinite_data_is_invalid(self):
        self.assertEqual(self.deliver(self.event(float('nan'))), 400)
        self.assertFalse(self.store.exists())


if __name__ == '__main__':
    unittest.main()
