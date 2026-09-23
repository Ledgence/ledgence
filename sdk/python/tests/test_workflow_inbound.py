"""Bounded reading/replay of Rust-accepted JSON at float formatting boundaries."""
import json
import math
from pathlib import Path
import unittest

import test_workflow
from ledgence.worker.workflow import (
    MAX_CONTEXT_BYTES, MAX_DECISION_BYTES, MAX_EVENT_BYTES, MAX_RECORD_BYTES,
    MAX_RECORDS_BYTES, MAX_STATE_BYTES, WorkflowContext, WorkflowError, _encode, _freeze,
)

# These tokens are fixed parity vectors, not a replacement Rust formatter.
FLOAT = 1e-8
PYTHON_TOKEN = '1e-08'
RUST_TOKEN = '1e-8'


def rust_bytes(value):
    return json.dumps(value, ensure_ascii=False, separators=(',', ':'), sort_keys=True).replace(
        PYTHON_TOKEN, RUST_TOKEN).encode()


def full_array(builder, limit):
    overhead = len(rust_bytes(builder([])))
    count = (limit - overhead + 1) // (len(RUST_TOKEN) + 1)
    result = builder([FLOAT] * count)
    assert len(rust_bytes(result)) <= limit
    return result


def full_event():
    value = {"specversion": "1.0", "id": "boundary", "source": "/boundary",
             "type": "boundary.event", "datacontenttype": "application/json",
             "data": {"number": 2.9802322387695312e-8, "padding": ""}}
    raw = json.dumps(value, separators=(',', ':')).replace('2.9802322387695312e-08', '2.9802322387695312e-8')
    value['data']['padding'] = 'x' * (MAX_EVENT_BYTES - len(raw.encode()))
    return value


class InboundWorkflowTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.contexts = []

    async def asyncTearDown(self):
        for context in self.contexts:
            await context._finish(cancel=True)

    def context(self, **changes):
        async def no_rpc(*args): self.fail('reading accepted values must not create a commit')
        result = WorkflowContext(test_workflow.payload(**changes), no_rpc)
        self.contexts.append(result)
        return result

    async def test_rust_boundary_event_is_readable_but_new_admission_remains_strict(self):
        event = full_event()
        self.assertEqual(len(json.dumps(event, separators=(',', ':')).encode()), MAX_EVENT_BYTES + 1)
        wake = {'kind': 'event', 'key': 'boundary', 'event': event, 'accepted_at': 1}
        context = self.context(wake=wake)
        self.assertEqual(context.wake, wake)
        context.wake['event']['data']['padding'] = 'mutated'
        self.assertEqual(context.wake, wake)
        with self.assertRaises(WorkflowError): _encode(event, MAX_EVENT_BYTES, 96)

    async def test_all_authoritative_fields_and_replay_copies_accept_bounded_expansion(self):
        def input_step(value): self.fail('committed input step executed again')
        def output_step(): self.fail('committed output step executed again')
        record_input = full_array(lambda value: {
            'key': 'input', 'callable': input_step.__module__ + ':' + input_step.__qualname__,
            'input': {'value': value}, 'output': None}, MAX_RECORD_BYTES - 2)
        record_output = full_array(lambda value: {
            'key': 'output', 'callable': output_step.__module__ + ':' + output_step.__qualname__,
            'input': {}, 'output': value}, MAX_RECORD_BYTES - 2)
        records = [record_input, record_output]
        self.assertLessEqual(len(rust_bytes(records)), MAX_RECORDS_BYTES)
        state = full_array(lambda value: value, MAX_STATE_BYTES)
        inputs = full_array(lambda value: {'child': {
            'task_id': 'child-task', 'state': 'succeeded', 'outcome': {
                'kind': 'succeeded', 'attempt_id': 'child-attempt', 'quiescence': 'confirmed',
                'execution_may_have_started': True, 'output': value}}}, MAX_DECISION_BYTES)
        payload = test_workflow.payload(state=state, inputs=inputs, local_steps=records)
        self.assertLess(len(rust_bytes(payload)), MAX_CONTEXT_BYTES)
        with self.assertRaises(WorkflowError): _encode(payload, MAX_CONTEXT_BYTES, 96)
        context = self.context(state=state, inputs=inputs, local_steps=records)
        self.assertEqual(context.state, state)
        self.assertEqual(context.inputs, inputs)
        self.assertEqual(context.get_result('child'), inputs['child']['outcome']['output'])
        self.assertIsNone(await context.local('input', input_step, **record_input['input']))
        copied = await context.local('output', output_step)
        self.assertEqual(copied, record_output['output'])
        copied[0] = 'mutated'
        self.assertEqual((await context.local('output', output_step))[0], FLOAT)
        with self.assertRaises(WorkflowError):
            context.local('input', input_step, value=[FLOAT, 2.0])
        with self.assertRaises(WorkflowError):
            context.local('new', input_step, **record_input['input'])
        with self.assertRaises(WorkflowError):
            context.continue_(continuation='again', state=context.state)
        with self.assertRaises(WorkflowError):
            context.complete(context.get_result('child'))

    async def test_combined_input_and_wake_budget_uses_authoritative_accounting(self):
        wake = {'kind': 'timer', 'key': 'timer', 'deadline': 1}
        combined = full_array(lambda value: {'inputs': {'child': {
            'task_id': 'child-task', 'state': 'succeeded', 'outcome': {
                'kind': 'succeeded', 'attempt_id': 'attempt', 'quiescence': 'confirmed',
                'execution_may_have_started': True, 'output': value}}}, 'wake': wake}, MAX_DECISION_BYTES)
        with self.assertRaises(WorkflowError): _encode(combined, MAX_DECISION_BYTES, 96)
        context = self.context(**combined)
        self.assertEqual(context.inputs, combined['inputs'])
        self.assertEqual(context.wake, wake)

    async def test_authoritative_allowance_retains_structural_depth_and_finite_bounds(self):
        deep = None
        for _ in range(65): deep = [deep]
        cycle = []; cycle.append(cycle)
        for value in (deep, cycle, {'number': FLOAT, 'padding': 'x' * 1024},
                      [math.inf], [math.nan], [1 << 64], [object()]):
            with self.assertRaises(WorkflowError): _freeze(value, 1024, authoritative=True)
        data = [FLOAT] * 20
        encoded = _encode(data, len(rust_bytes(data)), authoritative=True)
        self.assertEqual(json.loads(encoded), data)
        self.assertLessEqual(len(encoded), len(rust_bytes(data)) +
                             sum(max(0, len(repr(value)) - 3) for value in data))
