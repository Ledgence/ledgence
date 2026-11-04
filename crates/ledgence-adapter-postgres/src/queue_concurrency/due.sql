SELECT * FROM (
    (SELECT tenant_id,namespace,queue,task_id,attempt_id,deadline_ms AS due FROM queue_concurrency_holds
     WHERE state <> 'released' AND deadline_ms IS NOT NULL AND stop_requested_at_ms IS NULL
       AND ($1::text IS NULL OR (tenant_id=$1 AND namespace=$2))
       AND (deadline_ms,attempt_id)>($4,$5)
       AND deadline_ms <= floor(extract(epoch FROM statement_timestamp())*1000)::bigint
     ORDER BY deadline_ms,attempt_id LIMIT $3)
    UNION ALL
    (SELECT tenant_id,namespace,queue,task_id,attempt_id,force_at_ms AS due FROM queue_concurrency_holds
     WHERE state <> 'released' AND force_at_ms IS NOT NULL
       AND ($1::text IS NULL OR (tenant_id=$1 AND namespace=$2))
       AND (force_at_ms,attempt_id)>($4,$5)
       AND force_at_ms <= floor(extract(epoch FROM statement_timestamp())*1000)::bigint
     ORDER BY force_at_ms,attempt_id LIMIT $3)
) due ORDER BY due,attempt_id LIMIT $3
