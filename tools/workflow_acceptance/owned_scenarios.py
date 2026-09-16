"""Separate-process owned-workflow lifecycle and trace acceptance scenarios."""
import asyncio
import json
import urllib.parse

from http_acceptance.harness import eventually, exchange


SCENARIOS = ('owned-example', 'owned-tree', 'owned-replay', 'owned-cancellation', 'owned-failure')


def quote(value):
    return "'" + value.replace("'", "''") + "'"


def tree_rows(d, root):
    return json.loads(d.sql("SELECT coalesce(json_agg(row_to_json(r)), '[]'::json) FROM "
        "(SELECT workflow_id,parent_workflow_id,root_workflow_id,state,terminal_at_ms,"
        "convert_from(controller_bytes,'UTF8')::json AS controller "
        f"FROM workflow_runs WHERE workflow_id={quote(root)} OR root_workflow_id={quote(root)} "
        "ORDER BY nesting_depth,workflow_id) r"))


def external(d, workflow_id, key, identifier, **payload):
    return dict(scope=d.scope,workflow_id=workflow_id,key=key,event=dict(
        specversion='1.0',id=identifier,source='urn:owned-workflow-acceptance',
        type='approval.granted.v1',datacontenttype='application/json',
        traceparent='00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00',
        data=payload))


async def send(d, workflow_id, key, identifier, **payload):
    status,_,reply = await asyncio.to_thread(exchange,d.server_url,'POST','/v1/workflows/events',
        external(d,workflow_id,key,identifier,**payload))
    assert status==200,reply
    return reply


async def run(d, delay, names, record, records, snapshot):
    from ledgence.client import AsyncClient, RetryPolicy
    options = dict(tenant=d.scope['tenant_id'],namespace=d.scope['namespace'])
    # An aborted SQS receive may have hidden a publication for the configured
    # 60-second visibility period. Match the shared transport acceptance budget.
    wait_timeout=max(30,getattr(d,'terminal_timeout_floor',30))

    async def submit(client, tag, mode, **extra):
        return await client.workflows.submit(program='owned-controller',version='1.0.0',queue=d.queue,
            data=dict(tag=tag,mode=mode,role='parent',queue=d.queue,url=delay.url,
                marker=str(d.directory/'workflow-markers.jsonl'),**extra),
            idempotency_key=tag,retry_policy=RetryPolicy(max_attempts=3,retry_delay_ms=0))

    async def wait_for_tree(tag):
        rows = await asyncio.to_thread(eventually,lambda: records(d,tag,'owned_activation')
            if {row['role'] for row in records(d,tag,'owned_activation')}=={'parent','child','grandchild'} else None,
            timeout=wait_timeout,description='parent, child and grandchild activations')
        ids = {role:next(row['workflow'] for row in rows if row['role']==role)
            for role in ('parent','child','grandchild')}
        assert len(set(ids.values()))==3,ids
        return ids

    async def waiting(workflow_id):
        return await asyncio.to_thread(eventually,lambda: snapshot(d,workflow_id)['state']=='waiting',
            timeout=wait_timeout,description='durably suspended owned workflow')

    def joined(tag, expected):
        joins = records(d,tag,'owned_join')
        assert len(joins)==expected,joins
        child = joins[0]['inputs']['branch']
        task = joins[0]['inputs']['ordinary']
        assert child['kind']=='workflow' and 'task_id' not in child,child
        assert 'kind' not in task and 'workflow_id' not in task,task
        assert child['state']==task['state']=='succeeded'
        return joins

    if 'owned-example' in names:
        worker = d.start_worker(concurrency=1)
        async with AsyncClient(d.server_url,**options) as client:
            handle = await client.workflows.submit(program='owned-example',version='1.0.0',queue=d.queue,
                data={'urls':[delay.url+'?owned='+str(index) for index in range(4)],'queue':d.queue},
                idempotency_key='owned-public-example')
            result = await handle.result(timeout=110)
            expected = {'pages':{'page_count':4,'summary':{'characters':4*len('local network I/O'),'pages':4}},
                        'metadata':{'characters':len('owned subworkflow example'),'pages':1}}
            assert result==expected,result
            rows = await asyncio.to_thread(tree_rows,d,handle.id)
            assert len(rows)==2 and all(row['state']=='succeeded' for row in rows),rows
            assert rows[1]['parent_workflow_id']==rows[1]['root_workflow_id']==handle.id,rows
            record('owned-example',dict(workflow_id=handle.id,child_workflow_id=rows[1]['workflow_id'],
                concurrency=1,output=result,source='examples/owned-subworkflows/program.py'))
        await asyncio.to_thread(worker.stop)

    if 'owned-tree' in names:
        worker = d.start_worker(concurrency=1)
        tag='owned-tree'
        async with AsyncClient(d.server_url,**options) as client:
            handle=await submit(client,tag,'tree')
            ids=await wait_for_tree(tag)
            await waiting(ids['grandchild'])
            await waiting(ids['child'])
            await waiting(ids['parent'])
            assert len([r for r in records(d,tag,'owned_activation') if r['role']=='parent'])==1
            for role in ('child','grandchild'):
                status=await client.workflows.handle(ids[role]).status()
                expected_parent=ids['parent' if role=='child' else 'child']
                assert status.parent_workflow_id==expected_parent and status.root_workflow_id==handle.id,status
            await send(d,ids['grandchild'],'approve','evt-owned-tree',approved=True)
            await asyncio.to_thread(eventually,lambda: records(d,tag,'owned_wake'),timeout=wait_timeout,description='grandchild event wake')
            await waiting(ids['grandchild'])
            assert not records(d,tag,'owned_join'),'parent resumed after child controller checkpoint'
            assert (await asyncio.to_thread(snapshot,d,handle.id))['state']=='waiting'
            result=await handle.result(timeout=110)
            assert result['branch']['leaf']=={'approved':True,'value':7},result
            assert result['branch']['nested']['index']==7 and result['ordinary']['index']==7,result
            assert result['branch']['nested']['workflow']==ids['child'],result
            joins=joined(tag,1)
            rows=await asyncio.to_thread(tree_rows,d,handle.id)
            assert all(row['state']=='succeeded' for row in rows),rows
            assert joins[0]['inputs']['branch']['workflow_id']==ids['child']
            record('owned-tree',dict(ids=ids,concurrency=1,mixed_join=True,nested_ordinary_task=True,
                no_parent_wake_from_controller_checkpoint=True,grandchild_event_and_timer=True,
                parent_resume_count=1,output=result))
        await asyncio.to_thread(worker.stop)

    if 'owned-replay' in names:
        tag='owned-replay'
        worker=d.start_worker(concurrency=1)
        async with AsyncClient(d.server_url,**options) as client:
            handle=await submit(client,tag,'replay')
            ids=await wait_for_tree(tag)
            await asyncio.to_thread(eventually,lambda: snapshot(d,ids['child'])['state']=='succeeded',
                timeout=wait_timeout,description='owned child completes before its parent waits for it')
            await waiting(handle.id)
            before=await asyncio.to_thread(tree_rows,d,handle.id)
            await asyncio.to_thread(worker.stop)
            # Hide only this deployment's mutable release references. Accepted
            # bindings must replay from their persisted descriptors, even after
            # restarting both orchestrator and worker with an empty cache.
            hidden=[]
            try:
                for program in ('owned-controller','workflow-io'):
                    path=d.store/'programs'/program/'1.0.0'/'descriptor.json'
                    hidden.append((path,path.read_bytes()))
                    path.unlink()
                await asyncio.to_thread(d.server.kill)
                d.server,_=await asyncio.to_thread(d.start_server)
                worker=d.start_worker(concurrency=1,cache='owned-replay-fresh-cache')
                await send(d,handle.id,'join','evt-owned-join')
                result=await handle.result(timeout=110)
            finally:
                for path,content in hidden:
                    path.write_bytes(content)
            after=await asyncio.to_thread(tree_rows,d,handle.id)
            assert [(r['workflow_id'],r['controller']) for r in before]==[(r['workflow_id'],r['controller']) for r in after]
            assert len(after)==3 and all(r['state']=='succeeded' for r in after),after
            activations=records(d,tag,'owned_activation')
            replay=[r for r in activations if r['role']=='parent' and r['continuation']=='join']
            assert len(replay)==2 and [r['attempt'] for r in replay]==[1,2],replay
            assert len({r['activation'] for r in replay})==1,replay
            joins=joined(tag,2)
            assert joins[0]['activation']==joins[1]['activation']
            assert joins[0]['inputs']==joins[1]['inputs'],'resumed controller retry changed frozen mixed inputs'
            assert len([r for r in activations if r['role']=='child' and r['continuation']=='start'])==1
            assert len(records(d,tag,'child'))==2,'stable keys repeated an ordinary child'
            record('owned-replay',dict(ids=ids,concurrency=1,child_completed_before_wait=True,
                same_child_ids_and_pinned_descriptors=True,release_references_unavailable=True,
                orchestrator_and_worker_restarted=True,fresh_worker_cache=True,
                replay_activation_attempts=2,frozen_input_attempts=2,output=result))
        await asyncio.to_thread(worker.stop)

    for name,mode,expected in [('owned-cancellation','cancel','cancelled'),
                               ('owned-failure','failure','failed')]:
        if name not in names:
            continue
        # Failure executes a parent continuation while another slot is blocked
        # inside an ordinary descendant. Cancellation needs only one slot.
        concurrency=1 if mode=='cancel' else 2
        worker=d.start_worker(concurrency=concurrency)
        gate=d.directory/(name+'-blocked-task')
        d.gates.add(gate)
        async with AsyncClient(d.server_url,**options) as client:
            handle=await submit(client,name,mode,gate=str(gate))
            ids=await wait_for_tree(name)
            await waiting(ids['grandchild'])
            await waiting(ids['child'])
            await waiting(handle.id)
            await send(d,ids['child'],'block-task','evt-'+name+'-block')
            ordinary=await asyncio.to_thread(eventually,lambda: records(d,name,'child'),
                timeout=wait_timeout,description='running ordinary descendant before parent closure')
            task_id=ordinary[0]['task']
            if mode=='cancel':
                await handle.cancel()
            else:
                await send(d,handle.id,'close','evt-'+name+'-close')
            await asyncio.to_thread(eventually,lambda: snapshot(d,handle.id)['state']==expected,
                timeout=max(90,wait_timeout),description='parent closure drains running and waiting descendants')
            rows=await asyncio.to_thread(tree_rows,d,handle.id)
            assert len(rows)==3 and rows[0]['state']==expected,rows
            assert all(row['state']=='cancelled' for row in rows[1:]),rows
            assert rows[0]['terminal_at_ms']>=max(row['terminal_at_ms'] for row in rows[1:]),rows
            task=await asyncio.to_thread(d.task,task_id)
            assert task['state']=='cancelled' and task['terminal_at']<=rows[0]['terminal_at_ms'],task
            query=urllib.parse.urlencode(dict(d.scope,task_id=task_id,attempt_id=ordinary[0]['attempt_id']))
            status,_,attempt=await asyncio.to_thread(exchange,d.server_url,'GET','/v1/attempts/inspect?'+query)
            assert status==200 and attempt['quiescence']=='confirmed',attempt
            assert not gate.exists(),'blocked descendant must be terminated, not released by the test'
            before=len(records(d,name,'owned_activation'))
            for workflow_id,key in ((ids['grandchild'],'approve'),(ids['child'],'block-task'),(handle.id,'close')):
                status,_,error=await asyncio.to_thread(exchange,d.server_url,'POST','/v1/workflows/events',
                    external(d,workflow_id,key,'evt-'+name+'-late-'+key))
                # Exact event receipts remain durable after closure. A new
                # event conflicting with an already accepted one is a conflict;
                # a previously unused key on the closed run is obsolete.
                accepted_key=key=='block-task' or (mode=='failure' and key=='close')
                expected_code='conflict' if accepted_key else 'obsolete_operation'
                assert status==409 and error['code']==expected_code,error
            # Let pending recovery work run across a process restart; the closed
            # tree must remain closed despite late external events.
            await asyncio.to_thread(d.server.kill)
            d.server,_=await asyncio.to_thread(d.start_server)
            await asyncio.sleep(0.5)
            assert len(records(d,name,'owned_activation'))==before
            assert [r['state'] for r in await asyncio.to_thread(tree_rows,d,handle.id)]==[expected,'cancelled','cancelled']
            record(name,dict(ids=ids,concurrency=concurrency,running_task=task_id,
                parent_state=expected,descendants_cancelled=True,parent_terminal_after_descendants=True,
                late_events_rejected=True,no_resurrection_after_restart=True))
        await asyncio.to_thread(worker.stop)

    if 'owned-failure' in names:
        worker=d.start_worker(concurrency=1)
        async with AsyncClient(d.server_url,**options) as client:
            handled=await submit(client,'owned-handled-failure','handled-failure')
            result=await handled.result(timeout=max(60,wait_timeout))
            assert result['handled_child_failure'] is True,result
            rows=await asyncio.to_thread(tree_rows,d,handled.id)
            assert [r['state'] for r in rows]==['succeeded','failed'],rows
            record('owned-handled-failure',dict(parent=handled.id,child=result['child_id'],
                explicit_child_failure_is_inspectable=True))
            premature=await submit(client,'owned-premature-complete','premature-complete')
            ids=await wait_for_tree('owned-premature-complete')
            await waiting(ids['grandchild'])
            await waiting(premature.id)
            await send(d,premature.id,'close','evt-premature-complete')
            await asyncio.to_thread(eventually,lambda: snapshot(d,premature.id)['state']=='failed',
                timeout=wait_timeout,description='invalid successful parent closure drains unfinished children')
            rows=await asyncio.to_thread(tree_rows,d,premature.id)
            assert [r['state'] for r in rows]==['failed','cancelled','cancelled'],rows
            record('owned-premature-complete',dict(ids=ids,successful_close_rejected=True,descendants_drained=True))
        await asyncio.to_thread(worker.stop)


def verify_traces(d, capture, results, records, trace_rows):
    detail=next(row['detail'] for row in results if row['scenario']=='owned-tree')
    ids=detail['ids']
    rows=trace_rows(capture)
    markers=records(d,'owned-tree','owned_activation')
    inspected=[]
    attempts={}
    for marker in markers:
        event=marker['event']
        query=urllib.parse.urlencode(dict(d.scope,task_id=event['ldgtaskid'],attempt_id=event['ldgattemptid']))
        status,_,attempt=exchange(d.server_url,'GET','/v1/attempts/inspect?'+query)
        assert status==200,attempt
        attempts[marker['activation']]=attempt
        process=[span for span in rows if span['name']=='ledgence.attempt.process'
            and span['attributes'].get('ledgence.attempt.id')==event['ldgattemptid']]
        activation=[span for span in rows if span['name']=='ledgence.workflow.activation'
            and span['attributes'].get('ledgence.activation.id')==marker['activation']
            and process and span['parent_span_id']==process[0]['span_id']]
        assert len(process)==len(activation)==1,(marker,process,activation)
        process,activation=process[0],activation[0]
        event_parent=event['traceparent'].split('-')
        assert (process['trace_id'],process['parent_span_id'])==(event_parent[1],event_parent[2])
        assert (activation['trace_id'],activation['parent_span_id'])==(process['trace_id'],process['span_id'])
        processing=attempt['settlement']['command']['processing_trace']['traceparent'].split('-')
        assert (processing[1],processing[2])==(process['trace_id'],process['span_id'])
        if marker['role']!='parent':
            parent=ids['parent' if marker['role']=='child' else 'child']
            assert event['ldgparentworkflowid']==parent and event['ldgrootworkflowid']==ids['parent']
            for span in (process,activation):
                assert span['attributes']['ledgence.workflow.parent.id']==parent,span
                assert span['attributes']['ledgence.workflow.root.id']==ids['parent'],span
        inspected.append(dict(workflow_id=marker['workflow'],activation_id=marker['activation'],
            process_span=process['span_id'],activation_span=activation['span_id']))
    def invocation_ancestry(event, expected):
        # The immutable submission origin is the accepted spawning attempt.
        # Acquisition creates a producer span beneath it and propagates that
        # producer's context in the CloudEvent, preserving the full causal hop.
        task=d.task(event['ldgtaskid'])
        assert task['origin_trace']==expected,(task,expected)
        producer=[span for span in rows if span['name']=='ledgence.invocation.create'
            and span['attributes'].get('ledgence.attempt.id')==event['ldgattemptid']
            and span['attributes'].get('cloudevents.event_id')==event['id']]
        assert len(producer)==1,(event,producer)
        producer=producer[0]
        origin=expected['traceparent'].split('-')
        assert (producer['trace_id'],producer['parent_span_id'])==(origin[1],origin[2])
        carrier=f"00-{producer['trace_id']}-{producer['span_id']}-{producer['flags'] & 255:02x}"
        assert event['traceparent']==carrier,(event,producer)
        assert event.get('tracestate','')==producer['trace_state']
        process=[span for span in rows if span['name']=='ledgence.attempt.process'
            and span['attributes'].get('ledgence.attempt.id')==event['ldgattemptid']]
        assert len(process)==1,process
        process=process[0]
        assert (process['trace_id'],process['parent_span_id'])==(producer['trace_id'],producer['span_id'])
        return task,process

    first={role:next(m for m in markers if m['role']==role and m['continuation']=='start') for role in ids}
    for role,parent_role in [('child','parent'),('grandchild','child')]:
        expected=attempts[first[parent_role]['activation']]['settlement']['command']['processing_trace']
        origin=json.loads(d.sql("SELECT convert_from(submission_bytes,'UTF8')::json->'origin_trace' "
            f"FROM workflow_runs WHERE workflow_id={quote(ids[role])}"))
        assert origin==expected,(role,origin,expected)
        invocation_ancestry(first[role]['event'],expected)
    for ordinary in records(d,'owned-tree','child'):
        query=urllib.parse.urlencode(dict(d.scope,task_id=ordinary['task'],attempt_id=ordinary['attempt_id']))
        status,_,attempt=exchange(d.server_url,'GET','/v1/attempts/inspect?'+query)
        assert status==200,attempt
        role='parent' if ordinary['workflow']==ids['parent'] else 'child'
        expected=attempts[first[role]['activation']]['settlement']['command']['processing_trace']
        task,process=invocation_ancestry(attempt['event'],expected)
        if role=='child':
            assert task['parent_workflow_id']==ids['parent'] and task['root_workflow_id']==ids['parent'],task
            assert process['attributes']['ledgence.workflow.parent.id']==ids['parent']
            assert process['attributes']['ledgence.workflow.root.id']==ids['parent']
    wake=records(d,'owned-tree','owned_wake')[0]['wake']['event']
    original=tuple(wake['traceparent'].split('-')[1:3])
    woken=[span for span in rows if span['name']=='ledgence.workflow.activation'
        and span['attributes'].get('ledgence.workflow.id')==ids['grandchild']
        and span['attributes'].get('ledgence.workflow.wake')=='event']
    assert len(woken)==1,woken
    assert [(link['trace_id'],link['span_id']) for link in woken[0]['links']]==[original]
    assert woken[0]['trace_id']!=original[0],'external event became processing parent'
    return dict(ids=ids,activation_spans=len(inspected),nested_lineage_attributes=True,
        spawning_attempt_ancestry=True,invocation_producer_hop=True,nested_ordinary_task_ancestry=True,external_event_link_preserved=True,
        inspected=inspected)
