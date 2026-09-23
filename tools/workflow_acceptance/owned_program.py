"""Owned-tree acceptance fixture, packaged dynamically by check-workflows.py."""
import json
import os

from ledgence.worker.workflow import workflow_context


def mark(event, ctx, kind, **fields):
    data = event['data']
    with open(data['marker'], 'a', encoding='utf-8') as stream:
        stream.write(json.dumps(dict(tag=data['tag'], kind=kind, role=data['role'], pid=os.getpid(),
            workflow=ctx.workflow_id, activation=ctx.activation_id, attempt=event['ldgattemptno'],
            continuation=ctx.continuation, parent=ctx.parent_workflow_id, root=ctx.root_workflow_id,
            event=event, **fields))+'\n')


def branch(ctx, data, key, role):
    return ctx.workflow(key, program='owned-controller', version='1.0.0', queue=data['queue'],
        data=dict(data, role=role), retry_policy={'max_attempts': 3, 'retry_delay_ms': 0})


def ordinary(ctx, data, key='ordinary', gate=None):
    return ctx.task(key, program='workflow-io', version='1.0.0', queue=data['queue'],
        data=dict(url=data['url'], marker=data['marker'], tag=data['tag'], index=7, gate=gate),
        retry_policy={'max_attempts': 3, 'retry_delay_ms': 0})


async def handle(event):
    ctx = workflow_context()
    data = event['data']
    mode, role = data['mode'], data['role']
    mark(event, ctx, 'owned_activation')
    if role == 'parent':
        if ctx.continuation == 'start':
            child = branch(ctx, data, 'branch', 'child')
            if mode in ('tree', 'replay'):
                task = ordinary(ctx, data)
                if mode == 'replay':
                    return ctx.wait_event('join', continuation='join', state={})
                return ctx.suspend(continuation='collect', state={}, until=[child, task])
            if mode == 'handled-failure':
                return ctx.suspend(continuation='handled', state={}, until=[child])
            return ctx.wait_event('close', continuation='close', state={})
        if ctx.continuation == 'join':
            child = branch(ctx, data, 'branch', 'child')
            task = ordinary(ctx, data)
            if event['ldgattemptno'] == 1:
                raise RuntimeError('retry parent after replaying the already committed child key')
            return ctx.suspend(continuation='collect', state={}, until=[child, task])
        if ctx.continuation == 'close':
            if mode == 'premature-complete':
                return ctx.complete({'must_not_complete': True})
            return ctx.fail('parent_failed', 'close the owned tree')
        if ctx.continuation == 'handled':
            child = ctx.inputs['branch']
            assert child['kind']=='workflow' and child['state']=='failed'
            assert child['outcome']['error']['kind']=='child_failed'
            return ctx.complete({'handled_child_failure': True, 'child_id': child['workflow_id']})
        mark(event, ctx, 'owned_join', inputs=ctx.inputs)
        if mode == 'replay' and event['ldgattemptno'] == 1:
            raise RuntimeError('retry controller with frozen mixed child inputs')
        return ctx.complete({'branch': ctx.get_result('branch'), 'ordinary': ctx.get_result('ordinary')})
    if role == 'child':
        if mode == 'handled-failure':
            return ctx.fail('child_failed', 'parent may handle this terminal outcome')
        if ctx.continuation == 'start':
            leaf = branch(ctx, data, 'leaf', 'grandchild')
            if mode in ('cancel', 'failure'):
                return ctx.wait_event('block-task', continuation='block-task', state={})
            if mode in ('tree', 'replay'):
                nested = ordinary(ctx, data, 'nested')
                return ctx.suspend(continuation='collect', state={}, until=[leaf, nested])
            return ctx.suspend(continuation='collect', state={}, until=[leaf])
        if ctx.continuation == 'block-task':
            blocking = ordinary(ctx, data, 'blocking', gate=data['gate'])
            return ctx.suspend(continuation='collect', state={}, until=['leaf', blocking])
        output = {'leaf': ctx.get_result('leaf')}
        if mode in ('tree', 'replay'):
            output['nested'] = ctx.get_result('nested')
        return ctx.complete(output)
    if role == 'grandchild':
        if ctx.continuation == 'start':
            if mode == 'replay':
                return ctx.sleep('settle', 0, continuation='settled', state={'approved': True})
            return ctx.wait_event('approve', continuation='approved', state={})
        if ctx.continuation == 'approved':
            mark(event, ctx, 'owned_wake', wake=ctx.wake)
            return ctx.sleep('settle', 3000, continuation='settled', state={'approved': ctx.wake['event']['data']['approved']})
        return ctx.complete({'approved': ctx.state['approved'], 'value': 7})
    raise AssertionError('unknown fixture role')
