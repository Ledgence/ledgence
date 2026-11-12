"""Real CLI/Compose shutdown with an in-flight artifact HTTP upload (MIT)."""
import base64
from concurrent.futures import ThreadPoolExecutor
import hashlib
import http.client
import json
import subprocess
import threading
import time
import urllib.parse
import urllib.request


def eventually(check, *, seconds, description):
    deadline = time.monotonic() + seconds
    while True:
        value = check()
        if value:
            return value
        assert time.monotonic() < deadline, description
        time.sleep(.1)


def event_proof(events):
    """Only retain shutdown facts, never container logs, labels or environment."""
    signals = [str(event.get('Actor', {}).get('Attributes', {}).get('signal'))
               for event in events if event.get('Action', event.get('status')) == 'kill']
    exits = [event.get('Actor', {}).get('Attributes', {}).get('exitCode')
             for event in events if event.get('Action', event.get('status')) == 'die']
    assert any(signal in ('15', 'SIGTERM') for signal in signals), 'Compose did not send SIGTERM'
    assert not any(signal in ('9', 'SIGKILL') for signal in signals), 'Compose forced SIGKILL'
    assert exits == ['0'], 'orchestrator did not exit cleanly exactly once'
    return {'sigterm_observed': True, 'sigkill_absent': True, 'orchestrator_exit_code': 0}


def shutdown_upload(acceptance, down, compose, base, workflow):
    scope = {'tenant_id': 'acme', 'namespace': 'demo'}
    requester = {'kind': 'external', 'scope': scope}
    payload = bytes(range(256)) * 4097
    digest = hashlib.sha256(payload).hexdigest()
    command = {'scope': scope, 'target': {'kind': 'workflow', 'id': workflow},
               'key': 'cli-shutdown-transfer', 'metadata': {'digest': 'sha256:' + digest,
               'size': len(payload), 'media_type': 'application/octet-stream', 'filename': None}}
    prepared = acceptance.api('/v1/artifacts/prepare', base=base,
                              body={'requester': requester, 'command': command})['upload']
    assert prepared['stage'] == 'prepared'
    selection = prepared['selection']
    upload_path = '/v1/artifacts/uploads/' + selection['upload_id']
    container = acceptance.run(compose + ['ps', '--quiet', 'orchestrator']).strip()
    assert container and '\n' not in container
    # Match the saved CLI Docker context. This stream provides the actual
    # service signal and exit code even though Compose down removes containers.
    docker = compose[:compose.index('compose')]
    events = []
    errors = []
    event_process = subprocess.Popen(docker + ['events', '--since', str(int(time.time()) - 1),
        '--filter', 'container=' + container, '--format', '{{json .}}'],
        cwd=acceptance.cwd, env=acceptance.env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    def read_events():
        try:
            for line in event_process.stdout:
                events.append(json.loads(line))
        except Exception as error:
            errors.append(type(error).__name__)
    reader = threading.Thread(target=read_events, daemon=True)
    reader.start()
    origin = urllib.parse.urlsplit(base)
    connection = http.client.HTTPConnection(origin.hostname, origin.port, timeout=125)
    started = time.monotonic()
    try:
        connection.putrequest('PUT', upload_path + '/content')
        connection.putheader('Content-Type', 'application/octet-stream')
        connection.putheader('Content-Length', str(len(payload)))
        for name, value in (('requester', requester), ('selection', selection)):
            connection.putheader('Ledgence-Artifact-' + name, base64.b64encode(json.dumps(value).encode()).decode())
        connection.endheaders()
        connection.send(payload[:1024])
        def receiving():
            status = acceptance.api(upload_path + '/status', base=base, body={'requester': requester})
            assert status['selection'] == selection
            return status['stage'] == 'receiving'
        eventually(receiving, seconds=10, description='artifact did not enter receiving before shutdown')
        with ThreadPoolExecutor(max_workers=1) as executor:
            stopped = executor.submit(acceptance.run, down, timeout=420)
            def signalled():
                assert not stopped.done(), 'local down finished while upload body was incomplete'
                assert event_process.poll() is None and not errors, 'Docker event observer failed'
                return any(event.get('Action', event.get('status')) == 'kill'
                    and str(event.get('Actor', {}).get('Attributes', {}).get('signal')) in ('15', 'SIGTERM')
                    for event in events)
            eventually(signalled, seconds=30, description='orchestrator did not enter Compose shutdown')
            signalled_at = time.monotonic()
            # The old global 65s override kills this request. Keep the body
            # incomplete for 70s after the actual orchestrator SIGTERM.
            while time.monotonic() - signalled_at < 70:
                assert not stopped.done(), 'CLI shutdown ended before the in-flight artifact completed'
                assert not any(event.get('Action', event.get('status')) == 'die' for event in events), 'orchestrator exited during upload'
                time.sleep(.2)
            held = time.monotonic() - signalled_at
            assert time.monotonic() - started < 115, 'upload exceeded its absolute transfer budget'
            connection.send(payload[1024:])
            response = connection.getresponse()
            body = response.read(64 * 1024)
            assert response.status == 200, 'in-flight artifact upload was not accepted'
            accepted = json.loads(body)['upload']
            assert accepted['stage'] == 'ready' and accepted['selection'] == selection
            reference = accepted['reference']
            assert reference['digest'] == 'sha256:' + digest and reference['size'] == len(payload)
            stopped.result(timeout=330)
        eventually(lambda: any(event.get('Action', event.get('status')) == 'die' for event in events),
                   seconds=10, description='missing orchestrator exit event')
        assert not errors
        return {**event_proof(events), 'receiving_before_shutdown': True, 'incomplete_after_sigterm_seconds': held,
                'http_status': 200, 'down_succeeded': True, 'reference': reference, 'selection': selection,
                'payload_sha256': digest, 'prepare_command': command}
    finally:
        connection.close()
        event_process.terminate()
        try:
            event_process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            event_process.kill(); event_process.wait(timeout=10)
        reader.join(timeout=10)
        event_process.stdout.close(); event_process.stderr.close()


def verify_restarted_upload(acceptance, base, proof):
    requester = {'kind': 'external', 'scope': proof['prepare_command']['scope']}
    replay = acceptance.api('/v1/artifacts/prepare', base=base,
                            body={'requester': requester, 'command': proof['prepare_command']})['upload']
    assert replay['stage'] == 'ready' and replay['reference'] == proof['reference']
    assert replay['selection'] == proof['selection'], 'restart minted another artifact generation'
    request = urllib.request.Request(base + '/v1/artifacts/download',
        data=json.dumps({'requester': requester, 'command': proof['reference']}).encode(),
        headers={'Content-Type': 'application/json'})
    with urllib.request.urlopen(request, timeout=30) as response:
        payload = response.read(proof['reference']['size'] + 1)
        manifest = json.loads(base64.b64decode(response.headers['Ledgence-Artifact-Manifest']))
    assert manifest['reference'] == proof['reference']
    assert payload == bytes(range(256)) * 4097
    assert hashlib.sha256(payload).hexdigest() == proof['payload_sha256']
    proof.update(restart_verified_bytes=True, restart_same_reference=True, replay_same_generation=True)
