"""One digest-bound package exercises reusable processes across the mixed workload."""

MODES = ('task', 'retry', 'local', 'fanout', 'timer', 'event')

PROGRAM = '''import asyncio, os
from ledgence.worker.workflow import workflow_context

async def local_value(*, value, delay_ms):
    await asyncio.sleep(delay_ms / 1000)
    return value

async def handle(event):
    data = event['data']
    mode, index = data['mode'], data['index']
    output = dict(index=index, mode=mode)
    if mode in ('task', 'retry'):
        await asyncio.sleep(data['work_ms'] / 1000)
        if mode == 'retry' and event['ldgattemptno'] == 1:
            os._exit(17)  # Retryable process loss; business exceptions are terminal.
        return output
    ctx = workflow_context()
    if mode == 'local':
        values = await ctx.gather(
            ctx.local('left', local_value, value=index, delay_ms=data['work_ms']),
            ctx.local('right', local_value, value=index + 1, delay_ms=data['work_ms']))
        assert values == [index, index + 1], values
        return ctx.complete(output)
    if ctx.continuation == 'start':
        if mode == 'timer':
            return ctx.sleep('timer', 100, continuation='finish', state=output)
        if mode == 'event':
            return ctx.wait_event('finish', continuation='finish', state=output, timeout_ms=120000)
        if mode == 'fanout':
            children = [ctx.task(str(i), program='soak', version='1.0.0', queue=data['queue'],
                data=dict(data, mode='task', index=index+i)) for i in range(2)]
            children.append(ctx.workflow('subflow', program='soak', version='1.0.0', queue=data['queue'],
                data=dict(data, mode='local')))
            return ctx.suspend(continuation='finish', state=output, until=children)
        raise RuntimeError('unknown soak mode')
    assert ctx.state == output, ctx.state
    if mode == 'fanout':
        assert ctx.get_result('0') == dict(index=index, mode='task')
        assert ctx.get_result('1') == dict(index=index+1, mode='task')
        assert ctx.get_result('subflow') == dict(index=index, mode='local')
    if mode == 'event':
        assert ctx.wake['event']['data'] == dict(index=index), ctx.wake
    return ctx.complete(output)
'''


def expected_counts(indices):
    counts = dict(tasks=0, attempts=0, workflows=0, local_results=0, callbacks=0)
    for index in indices:
        mode = MODES[index % len(MODES)]
        tasks = {'task': 1, 'retry': 1, 'local': 1, 'fanout': 5, 'timer': 2, 'event': 2}[mode]
        counts['tasks'] += tasks
        counts['attempts'] += tasks + (mode == 'retry')
        counts['workflows'] += 2 if mode == 'fanout' else int(mode not in ('task', 'retry'))
        counts['local_results'] += 2 if mode in ('local', 'fanout') else 0
        counts['callbacks'] += index % 7 == 0
    return counts
