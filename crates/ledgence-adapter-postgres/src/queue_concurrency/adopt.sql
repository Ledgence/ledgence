WITH outstanding AS MATERIALIZED (
    SELECT a.*,COALESCE(s.admission_worker_bytes,r.worker_bytes) AS registered_worker,CASE WHEN a.state <> 'active' THEN 'uncertain'
                    WHEN a.execution_may_have_started THEN 'running' ELSE 'reserved' END AS initial_state
    FROM tasks t JOIN attempts a USING(task_id)
    LEFT JOIN worker_sessions s ON s.session_id=a.worker_session_id
    LEFT JOIN task_admission_requests r ON r.owner_attempt_id=a.attempt_id
    WHERE t.tenant_id=$1 AND t.namespace=$2 AND t.queue=$3
        AND (a.state='active' OR (a.state<>'active' AND a.execution_may_have_started AND a.quiescence='unconfirmed'))
), adopted AS (
    INSERT INTO queue_concurrency_holds (tenant_id,namespace,queue,task_id,attempt_id,lease_id,worker_session_id,
        worker_identity_bytes,consumer_id,generation,policy_revision,state,acquired_at_ms,deadline_ms,grace_ms)
    SELECT $1,$2,$3,task_id,attempt_id,lease_id,worker_session_id,registered_worker,consumer_id,generation,1,initial_state,
        $4::bigint,$4::bigint+$5::bigint,$6 FROM outstanding
    RETURNING state
)
SELECT (SELECT count(*) FROM outstanding) AS expected,count(*) AS inserted,
       count(*) FILTER(WHERE state='uncertain') AS uncertain FROM adopted
