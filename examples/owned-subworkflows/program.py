"""Join a page-processing subworkflow and an ordinary task at concurrency one."""
from ledgence_worker.workflow import workflow_context


async def handle(event):
    ctx = workflow_context()
    data = event['data']
    if ctx.continuation == 'start':
        pages = ctx.workflow('pages', program='workflow-example', version='1.0.0',
            queue=data['queue'], data={'urls': data['urls'], 'queue': data['queue']})
        metadata = ctx.task('metadata', program='workflow-summary', version='1.0.0',
            queue=data['queue'], data={'pages': ['owned subworkflow example']})
        return ctx.suspend(continuation='collect', state={}, until=[pages, metadata])
    return ctx.complete({'pages': ctx.get_result('pages'), 'metadata': ctx.get_result('metadata')})
