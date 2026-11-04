-- The resource row lock serializes this edit with registrations, requeues and
-- scheduler offers. Recover pre-upgrade origins from their immutable owner.
WITH membership AS (
 SELECT q,g FROM unnest($4::text[],$5::text[]) AS m(q,g)
), origins AS (
 SELECT a.request_seq,a.origin_queue AS old_queue,a.fair_group AS old_group,
        COALESCE(a.origin_queue,o.queue) AS queue
 FROM task_admission_requests a LEFT JOIN tasks o
 ON o.task_id=a.task_id AND o.tenant_id=a.tenant_id AND o.namespace=a.namespace
 WHERE a.tenant_id=$1 AND a.namespace=$2 AND a.resource_name=$3
   AND a.state IN ('waiting','preparing','offered')
), changes AS MATERIALIZED (
 SELECT o.request_seq,o.queue,COALESCE(m.g,$6) AS new_group
 FROM origins o LEFT JOIN membership m ON m.q=o.queue
 WHERE (o.old_queue,o.old_group) IS DISTINCT FROM (o.queue,COALESCE(m.g,$6))
), applied AS (
 UPDATE task_admission_requests a SET origin_queue=c.queue,fair_group=c.new_group
 FROM changes c WHERE a.request_seq=c.request_seq AND a.tenant_id=$1 AND a.namespace=$2
   AND a.resource_name=$3 AND a.state IN ('waiting','preparing','offered')
 RETURNING a.request_seq,a.origin_queue,a.fair_group
)
SELECT NOT EXISTS (
 SELECT FROM changes c LEFT JOIN applied a USING(request_seq)
 WHERE a.request_seq IS NULL OR
       (a.origin_queue,a.fair_group) IS DISTINCT FROM (c.queue,c.new_group)
)
