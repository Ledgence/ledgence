-- Two bounded index probes per configured group, independent of backlog.
SELECT g.position, head.*
FROM unnest($4::text[]) WITH ORDINALITY AS g(name,position)
CROSS JOIN LATERAL (
 SELECT * FROM (
   (SELECT request_seq,origin_queue,workflow_id,origin_activation_id,logical_key,
           NULL::text AS task_id,NULL::bigint AS generation
    FROM admission_requests
    WHERE tenant_id=$1 AND namespace=$2 AND resource_name=$3
          AND fair_group IS NOT NULL AND fair_group=g.name COLLATE "C" AND state='waiting'
    ORDER BY (request_seq + 0::bigint) LIMIT 1)
   UNION ALL
   (SELECT request_seq,origin_queue,NULL,NULL,NULL,task_id,generation
    FROM task_admission_requests
    WHERE tenant_id=$1 AND namespace=$2 AND resource_name=$3
          AND fair_group IS NOT NULL AND fair_group=g.name COLLATE "C" AND state='waiting' AND available_at_ms=0
    ORDER BY (request_seq + 0::bigint) LIMIT 1)
 ) AS subjects ORDER BY request_seq LIMIT 1
) AS head
-- A null timestamp is the optimistic probe. Under the resource lock, time
-- boundaries must settle before the eligible group count can be certified.
WHERE $5::bigint IS NULL OR (
 $5 < ALL(SELECT deadline_at_ms FROM admission_requests
          WHERE tenant_id=$1 AND namespace=$2 AND resource_name=$3 AND state='waiting'
          ORDER BY deadline_at_ms,request_seq LIMIT 1)
 AND $5 < ALL(SELECT short_episode_deadline_ms FROM admission_requests
          WHERE tenant_id=$1 AND namespace=$2 AND resource_name=$3 AND state='waiting'
                AND short_episode_deadline_ms IS NOT NULL
          ORDER BY short_episode_deadline_ms,request_seq LIMIT 1)
 AND $5 < ALL(SELECT available_at_ms FROM task_admission_requests
          WHERE tenant_id=$1 AND namespace=$2 AND resource_name=$3 AND state='waiting'
                AND available_at_ms>0
          ORDER BY available_at_ms,request_seq LIMIT 1)
)
