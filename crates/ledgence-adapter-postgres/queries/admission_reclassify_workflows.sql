-- The resource row lock serializes this edit with registrations, requeues and
-- scheduler offers. Recover pre-upgrade origins from their immutable owner.
WITH membership AS (
 SELECT q,g FROM unnest($4::text[],$5::text[]) AS m(q,g)
), origins AS (
 SELECT a.request_seq,a.origin_queue AS old_queue,a.fair_group AS old_group,
        COALESCE(a.origin_queue,o.queue) AS queue,a.short_episode_deadline_ms AS old_deadline,
        CASE WHEN w.deadline_at_ms<a.deadline_at_ms THEN w.deadline_at_ms END AS short_deadline
 FROM admission_requests a LEFT JOIN workflow_runs o
 ON o.workflow_id=a.workflow_id AND o.tenant_id=a.tenant_id AND o.namespace=a.namespace
 LEFT JOIN workflow_admission_waits w ON a.state='waiting' AND w.workflow_id=a.workflow_id
   AND w.parking_activation_id=a.parking_activation_id AND w.closed_at_ms IS NULL
 WHERE a.tenant_id=$1 AND a.namespace=$2 AND a.resource_name=$3
   AND a.state IN ('waiting','awaiting_rewait','offered')
), changes AS MATERIALIZED (
 SELECT o.request_seq,o.queue,COALESCE(m.g,$6) AS new_group,o.short_deadline
 FROM origins o LEFT JOIN membership m ON m.q=o.queue
 WHERE (o.old_queue,o.old_group,o.old_deadline) IS DISTINCT FROM (o.queue,COALESCE(m.g,$6),o.short_deadline)
), applied AS (
 UPDATE admission_requests a SET origin_queue=c.queue,fair_group=c.new_group,short_episode_deadline_ms=c.short_deadline
 FROM changes c WHERE a.request_seq=c.request_seq AND a.tenant_id=$1 AND a.namespace=$2
   AND a.resource_name=$3 AND a.state IN ('waiting','awaiting_rewait','offered')
 RETURNING a.request_seq,a.origin_queue,a.fair_group,a.short_episode_deadline_ms
)
SELECT NOT EXISTS (
 SELECT FROM changes c LEFT JOIN applied a USING(request_seq)
 WHERE a.request_seq IS NULL OR
       (a.origin_queue,a.fair_group,a.short_episode_deadline_ms) IS DISTINCT FROM (c.queue,c.new_group,c.short_deadline)
)
